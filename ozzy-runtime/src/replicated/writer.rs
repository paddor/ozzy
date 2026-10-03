//! Pipelined native writer with bounded local admission and explicit confirmation.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ozzy_proto::handshake::{self, Parameters};
use ozzy_proto::{MessageId, PartitionIncarnation, ProducerId};

use super::{AppendKey, DataLimits, Error, Policy, RetryPolicy};
use state::Shared;

mod batch;
mod compression;
mod driver;
mod inbox;
mod payload;
mod pipe;
mod record;
pub(super) mod reservation;
pub use reservation::SharedWriterReservation;
mod runtime;
pub use runtime::WriterRuntime;
mod state;
mod stats;
pub use record::RecordInput;
pub use stats::WriterStats;

/// Hard SDK ceiling for records in one transparent APPEND group.
pub const MAX_APPEND_RECORDS: usize = 2048;

/// Largest producer sequence a writer admits. The bit above it seals admission.
pub const MAX_SEQUENCE: u64 = state::SEALED - 2;

/// Built-in raw payload threshold for adaptive whole-APPEND LZ4.
pub const PAYLOAD_COMPRESSION_THRESHOLD: usize = ozzy_proto::append::ADAPTIVE_LZ4_THRESHOLD;

/// Explicit registered destination, without fabricated group identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionTarget {
    /// Immutable partition in a configured three-broker group.
    Group(PartitionIncarnation),
}

impl PartitionTarget {
    fn metadata_bytes(&self) -> usize {
        match self {
            Self::Group(_) => 89,
        }
    }
}

/// One preprovisioned producer and its bounded outstanding window.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Required confirmation boundary. Must match the broker's configured policy.
    pub policy: Policy,
    /// Preprovisioned group partition or registered local topic/partition.
    pub partition: PartitionTarget,
    /// Existing nonzero partition ownership fence.
    pub owner_epoch: u64,
    /// Existing authorized producer identity.
    pub producer_id: ProducerId,
    /// Nonzero producer fencing epoch. `open_shared` requires a fresh epoch one.
    pub producer_epoch: u64,
    /// First sequence to submit, at most `MAX_SEQUENCE`. Recovered callers must
    /// preserve unresolved IDs.
    pub next_sequence: u64,
    /// Hard protocol request limits, further restricted by broker receive credit.
    pub limits: DataLimits,
    /// Compress eligible APPEND payloads with adaptive LZ4 before sending.
    /// Turn it off when the transport compresses, such as `lz4+tcp://`;
    /// brokers then receive, replicate, and store plain payload bytes.
    pub compress_payloads: bool,
    /// Uncompressed APPEND payload target. A larger permitted record goes alone.
    /// This is not a record-size limit or an intentional collection delay.
    pub batch_target_bytes: usize,
    /// Maximum intentional collection delay. Use `Duration::ZERO` by default.
    /// Full batches and explicit flushes do not wait for this timer.
    pub linger: Duration,
    /// Maximum concurrent producer handles. All handles share bounded admission.
    pub max_producers: usize,
    /// Maximum APPEND requests awaiting full confirmation across all handles.
    /// Nonzero. Partial confirmations keep their request slot occupied.
    /// Separate from finite producer inboxes and OMQ transport queues.
    pub inflight_appends: usize,
}

impl WriterConfig {
    fn transport_backing_bytes(&self) -> usize {
        reservation::transport_backing_bytes(self.limits)
    }

    /// Records one producer handle may admit before request packing.
    /// One ready request keeps the pipeline supplied without multiplying
    /// prefetch latency by the number of requests awaiting confirmation.
    pub(super) fn lane_records(&self) -> usize {
        self.limits.max_records.max(1024)
    }

    /// Payload bytes one producer handle may admit before request packing.
    /// One ready request, allowing an intact oversized record.
    pub(super) fn lane_bytes(&self) -> usize {
        self.batch_target_bytes.max(self.limits.max_record_bytes)
    }

    fn parameters_with_local_group(&self, local_group: bool) -> Result<Parameters, WriterError> {
        let target_valid = match &self.partition {
            PartitionTarget::Group(partition) => {
                partition.as_bytes() != &[0; 16]
                    && (matches!(
                        self.policy,
                        Policy::QuorumDurable | Policy::QuorumReplicatedPersisting
                    ) || local_group && self.policy == Policy::LocalDurable)
            }
        };
        if !target_valid
            || self.producer_id.as_bytes() == &[0; 16]
            || self.owner_epoch == 0
            || self.producer_epoch == 0
            || self.next_sequence > MAX_SEQUENCE
            || self.limits.max_records == 0
            || self.batch_target_bytes == 0
            || tokio::time::Instant::now()
                .checked_add(self.linger)
                .is_none()
            || self.max_producers == 0
            || self.inflight_appends == 0
            || self.inflight_appends > u32::MAX as usize
            || self
                .limits
                .max_records
                .max(1024)
                .checked_mul(self.inflight_appends)
                .and_then(|records| records.checked_mul(self.max_producers))
                .is_none()
        {
            return Err(WriterError::Configuration);
        }
        let mut parameters = Parameters::streaming(
            self.limits,
            handshake::PRODUCER,
            self.limits.max_records as u64,
            self.limits.envelope.max_payload_bytes as u64,
        )
        .map_err(|_| WriterError::Configuration)?;
        if matches!(self.partition, PartitionTarget::Group(_)) {
            parameters.capabilities |= handshake::OWNER_ROUTING;
            parameters.required_capabilities |= handshake::OWNER_ROUTING;
        }
        Ok(parameters)
    }
}

/// A record confirmed under its exact configured owner policy, not processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordReceipt {
    /// Original group partition or local topic/partition.
    pub partition: PartitionTarget,
    /// Original owner fence.
    pub owner_epoch: u64,
    /// Stable producer identity; `first_sequence` identifies this one record.
    pub key: AppendKey,
    /// Original application record identity.
    pub message_id: MessageId,
    /// Stable partition offset.
    pub offset: u64,
    /// Exact confirmation boundary requested by this writer.
    pub policy: Policy,
}

/// Writer failure. An admitted but unconfirmed record may still have been stored.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WriterError {
    /// Payload encoder failed before transmission.
    #[error("payload compression failed")]
    Compression,
    /// An internal inproc link stopped before admitted work completed.
    #[error("SDK inproc pipeline stopped")]
    Pipeline,
    /// Invalid startup identity, limits, or sequence range.
    #[error("invalid streaming writer configuration")]
    Configuration,
    /// The configured producer lane bound has been reached.
    #[error("streaming writer producer lane limit reached")]
    ProducerLimit,
    /// Zero message identity, invalid parts, or a configured packet/window limit.
    #[error("invalid record or streaming writer limit exceeded")]
    RecordLimits,
    /// Writer ownership ended before this observation could complete.
    #[error("streaming writer closed; unresolved outcomes remain unknown")]
    Closed,
    /// Terminal driver cause, not a rejection verdict for every pending record.
    /// Confirmed prefixes remain successful; other outcomes remain unknown.
    #[error("streaming writer failed: {0}")]
    Failed(Arc<Error>),
}

impl From<Error> for WriterError {
    fn from(error: Error) -> Self {
        match error {
            Error::Configuration => Self::Configuration,
            error => Self::Failed(Arc::new(error)),
        }
    }
}

/// Independent confirmation observation. Dropping it does not cancel delivery.
/// It retains only identity and shared progress, never record payload storage.
#[derive(Debug)]
pub struct PendingRecord {
    progress: Arc<state::Progress>,
    completion: Arc<state::RecordCompletion>,
    sequence: u64,
}

impl PendingRecord {
    /// Assigned producer sequence, stable through all retries.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Wait for the exact configured policy. No per-record task is spawned.
    pub async fn confirmed(&self) -> Result<RecordReceipt, WriterError> {
        self.progress.wait(self.sequence() + 1).await?;
        Ok(self.receipt())
    }

    /// Observe completion without registering a waiter or blocking admission.
    pub fn try_confirmed(&self) -> Option<Result<RecordReceipt, WriterError>> {
        self.progress
            .observe(self.sequence() + 1)
            .map(|result| result.map(|()| self.receipt()))
    }

    fn receipt(&self) -> RecordReceipt {
        let offset = *self
            .completion
            .offset
            .get()
            .expect("confirmed record has exact offset");
        self.progress
            .receipt(self.sequence, self.completion.message_id, offset)
    }
}

/// Continuous native writer over existing broker PEER endpoints.
///
/// One driver owns network progress and retries independently of application
/// polling. `send` confirms only local admission; `confirmed` and `flush` wait
/// for the exact configured policy. No durable outbox is supplied. Dropping the last owner stops
/// the driver, while already confirmed observations remain successful.
#[derive(Debug)]
pub struct Writer {
    shared: Arc<Shared>,
    sender: state::Sender,
}

impl Writer {
    /// Reuse a topic's broker sockets for one already opened partition-local
    /// writer epoch. Admission, transparent batching, compression, and retry
    /// identity use the shared driver. Routing interest
    /// starts only when the first record is admitted.
    pub async fn connect_shared(
        routes: &super::TopicRoutes,
        number: u32,
        config: WriterConfig,
        retry: RetryPolicy,
    ) -> Result<Self, WriterError> {
        Self::start_shared(routes, number, config, retry, false).await
    }

    /// Lazily open a fresh partition-local producer after its first record is
    /// admitted. Epoch must be one and the first sequence zero. Opening uses a
    /// stable operation ID through reconnect and leader change; no APPEND is
    /// transmitted until opening reaches the topic's confirmation boundary.
    pub async fn open_shared(
        routes: &super::TopicRoutes,
        number: u32,
        config: WriterConfig,
        retry: RetryPolicy,
    ) -> Result<Self, WriterError> {
        if config.producer_epoch != 1 || config.next_sequence != 0 {
            return Err(WriterError::Configuration);
        }
        Self::start_shared(routes, number, config, retry, true).await
    }

    async fn start_shared(
        routes: &super::TopicRoutes,
        number: u32,
        config: WriterConfig,
        retry: RetryPolicy,
        open: bool,
    ) -> Result<Self, WriterError> {
        retry.validate()?;
        let partition = routes
            .metadata()
            .partition(number)
            .ok_or(WriterError::Configuration)?;
        let local = partition.members.len() == 1;
        config.parameters_with_local_group(local)?;
        if config.partition != PartitionTarget::Group(partition.incarnation)
            || config.policy != routes.metadata().policy()
            || !matches!(partition.members.len(), 1 | 3)
            || !config.linger.is_zero()
        {
            return Err(WriterError::Configuration);
        }
        let reservation =
            super::broker_links::append::Connection::writer_reservation(routes.links(), &config)
                .ok_or(WriterError::Configuration)?;
        let operation = if open {
            Some(ozzy_proto::OperationId::from_bytes(
                *routes
                    .links()
                    .next_request()
                    .map_err(Error::from)?
                    .as_bytes(),
            ))
        } else {
            None
        };
        let routes = routes.clone();
        let runtime = routes.links().runtime().clone();
        runtime
            .driver()
            .clone()
            .spawn(async move {
                let connection = super::broker_links::append::Connection::new(
                    routes.links().clone(),
                    reservation.reply_slots,
                    reservation.writer_bytes,
                    reservation.request_bytes,
                )
                .map_err(Error::from)?;
                let capacity = config.lane_records();
                let (writer, shared) = Shared::open_reserved(
                    runtime,
                    config,
                    capacity,
                    Some(connection.reservation()),
                );
                tokio::spawn(driver::run_shared(
                    shared, connection, routes, number, retry, operation,
                ));
                Ok(writer)
            })
            .await
            .map_err(|_| WriterError::Pipeline)?
    }

    pub(super) fn groups_formed(&self) -> u64 {
        self.shared.groups_formed.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn admit(
        &mut self,
        record: &mut RecordInput,
        bytes: usize,
    ) -> Option<Result<PendingRecord, WriterError>> {
        self.shared.admit(&mut self.sender, record, bytes)
    }

    /// Register a producer handle sharing bounded typed SDK intake.
    /// Move one handle to each producing task or thread.
    pub fn try_clone(&self) -> Result<Self, WriterError> {
        if self.shared.sealed() {
            return Err(WriterError::Closed);
        }
        self.shared
            .producers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.shared.config.max_producers).then(|| n + 1)
            })
            .map_err(|_| WriterError::ProducerLimit)?;
        Ok(Self {
            shared: self.shared.clone(),
            sender: match self.sender.try_clone() {
                Ok(sender) => sender,
                Err(error) => {
                    self.shared.producers.fetch_sub(1, Ordering::AcqRel);
                    return Err(error);
                }
            },
        })
    }

    /// Snapshot actual protocol request sizes, including retries since creation.
    /// These diagnostics are not confirmation or transport-receipt evidence.
    pub fn stats(&self) -> WriterStats {
        self.shared.stats.snapshot()
    }

    /// Wait for bounded local admission and assign one sequence. Cancellation
    /// before admission assigns nothing; dropping the returned handle only stops
    /// observation. Waiting immediately on each handle deliberately serializes work.
    pub async fn send(&mut self, mut record: RecordInput) -> Result<PendingRecord, WriterError> {
        let size = self.shared.validate(&record)?;
        loop {
            if let Some(result) = self.shared.admit(&mut self.sender, &mut record, size) {
                return result;
            }
            // Capture after rolling back any temporary reservation, then recheck
            // availability. Otherwise our own credit notification could spin.
            let seen = self.shared.capacity.generation();
            if self.shared.sealed() || self.shared.inbox.available() {
                continue;
            }
            self.shared.capacity.changed_after(seen).await;
        }
    }

    /// Capture all records admitted before this call, even before first polling.
    pub fn flush(
        &self,
    ) -> impl std::future::Future<Output = Result<(), WriterError>> + Send + 'static + use<> {
        let progress = self.shared.progress.clone();
        let target = self.shared.next_sequence();
        self.shared
            .flush_through
            .fetch_max(target, Ordering::AcqRel);
        self.shared.work.mark();
        async move { progress.wait(target).await }
    }

    /// Seal all producer lanes, confirm their captured prefix, then stop the driver.
    /// Canceling close leaves admission sealed. Dropping the last handle stops
    /// the driver; dropping one clone leaves the other handles running.
    pub async fn close(self) -> Result<(), WriterError> {
        // Sealing and ticket assignment share one atomic, so every sequence
        // below the flushed prefix was assigned before the seal and is pushed
        // by its publisher without waiting.
        self.shared.seal();
        let result = self.flush().await;
        self.shared.stop.close();
        self.shared.finished.closed().await;
        result
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if self.shared.producers.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.fail(WriterError::Closed);
            self.shared.stop.close();
        }
    }
}

#[cfg(test)]
mod tests;
