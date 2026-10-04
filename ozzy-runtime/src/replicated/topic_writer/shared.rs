//! Native topic writers over checked numeric metadata and shared broker links.

use super::{
    Partition, Partitions, PendingRecord, ProducerIdentity, RecordInput, RecordReceipt,
    RetryPolicy, TopicSelection, TopicWriterError, Writer, WriterConfig, WriterError,
};
use crate::{
    replicated::{BrokerLinks, DataLimits, TopicRoutes, WriterStats},
    topic_metadata::TopicMetadata,
};
use ozzy_proto::{ProducerId, TopicId};

mod attachment;
mod session;

/// Protocol and admission bounds per logical partition writer. The broker-link
/// owner's aggregate limits include all these unused queues and active frames.
#[derive(Clone, Debug)]
pub struct SharedTopicWriterConfig {
    /// Request bounds, bounded by directional broker packet limits.
    pub limits: DataLimits,
    /// Adaptive whole-APPEND LZ4. Disable when transport compression is enabled.
    pub compress_payloads: bool,
    /// Uncompressed grouping target; a larger permitted record goes alone.
    pub batch_target_bytes: usize,
    /// Maximum caller handles, sharing independent partition-local sequences.
    pub max_producers: usize,
    /// Outstanding APPEND requests per partition, including partial replies.
    pub inflight_appends: usize,
}

impl SharedTopicWriterConfig {
    fn writer(
        &self,
        partition: ozzy_proto::PartitionIncarnation,
        producer_id: ProducerId,
        producer_epoch: u64,
        next_sequence: u64,
        policy: ozzy_proto::append::Policy,
    ) -> WriterConfig {
        WriterConfig {
            policy,
            partition,
            owner_epoch: 1,
            producer_id,
            producer_epoch,
            next_sequence,
            limits: self.limits,
            compress_payloads: self.compress_payloads,
            batch_target_bytes: self.batch_target_bytes,
            max_producers: self.max_producers,
            inflight_appends: self.inflight_appends,
        }
    }

    /// Shared-link storage per partition writer, including unused capacity.
    /// `reply_metadata_bytes` is the SDK owner's advertised receive bound.
    /// Reserve every partition's `idle_bytes`, admitted requests separately,
    /// and one extra `request_bytes` for writer opening progress. Overflow or
    /// empty windows return `None`. This reserves local SDK storage only.
    pub fn link_reservation(
        &self,
        reply_metadata_bytes: usize,
    ) -> Option<crate::replicated::SharedWriterReservation> {
        crate::replicated::writer::reservation::Bounds {
            limits: self.limits,
            batch_target_bytes: self.batch_target_bytes,
            max_producers: self.max_producers,
            inflight_appends: self.inflight_appends,
        }
        .reservation(reply_metadata_bytes)
    }

    /// Zero intentional delay, adaptive compression, one caller, one request.
    /// Callers choose finite request limits compatible with their memory budget.
    pub fn new(limits: DataLimits) -> Self {
        Self {
            limits,
            compress_payloads: true,
            batch_target_bytes: (64 * 1024).min(limits.envelope.max_payload_bytes),
            max_producers: 1,
            inflight_appends: 1,
        }
    }
}

/// Open a named topic and submit individual records with optional keys. One
/// logical identity spans its partitions; epochs and sequences remain local to
/// each partition. No partition socket or application-built batch is created.
pub struct SharedTopicWriter {
    routes: TopicRoutes,
    producer: ProducerId,
    state: Partitions,
}

impl std::fmt::Debug for SharedTopicWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedTopicWriter")
            .field("topic", &self.metadata().name())
            .field("producer", &self.producer)
            .field("partitions", &self.metadata().partition_count())
            .finish_non_exhaustive()
    }
}

/// Stable topic and numeric destination with the exact record confirmation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedTopicReceipt {
    /// Persistent topic identity, distinct after topic recreation.
    pub topic: TopicId,
    /// Frozen numeric partition, in broker metadata order.
    pub partition: u32,
    /// Exact offset, retry identity, and configured confirmation boundary.
    pub record: RecordReceipt,
}

/// Local admission only. Observation cancellation never changes its identity.
#[derive(Debug)]
pub struct SharedTopicPendingRecord {
    topic: TopicId,
    partition: u32,
    pending: PendingRecord,
}

impl SharedTopicPendingRecord {
    /// Persistent destination, already fixed before sequence assignment.
    pub fn topic(&self) -> TopicId {
        self.topic
    }
    /// Numeric destination, already fixed before sequence assignment.
    pub fn partition(&self) -> u32 {
        self.partition
    }
    /// Independent partition-local producer sequence.
    pub fn sequence(&self) -> u64 {
        self.pending.sequence()
    }
    /// Wait for exact policy confirmation, never transport receipt.
    pub async fn confirmed(&self) -> Result<SharedTopicReceipt, WriterError> {
        self.pending
            .confirmed()
            .await
            .map(|record| self.receipt(record))
    }
    /// Inspect confirmation without waiting.
    pub fn try_confirmed(&self) -> Option<Result<SharedTopicReceipt, WriterError>> {
        self.pending
            .try_confirmed()
            .map(|result| result.map(|record| self.receipt(record)))
    }
    fn receipt(&self, record: RecordReceipt) -> SharedTopicReceipt {
        SharedTopicReceipt {
            topic: self.topic,
            partition: self.partition,
            record,
        }
    }
}

impl SharedTopicWriter {
    /// Look up a named topic and generate a fresh writer identity using the
    /// broker-link owner's injected identity source. Idle partitions send no
    /// producer openings or routing registrations.
    pub async fn open(
        links: &BrokerLinks,
        name: &str,
        config: SharedTopicWriterConfig,
        retry: RetryPolicy,
    ) -> Result<Self, TopicWriterError> {
        let producer = ProducerId::from_bytes(*links.next_request()?.as_bytes());
        Self::open_with_producer(links, name, producer, config, retry).await
    }

    /// Use a caller-selected fresh identity, for trusted provisioning or tests.
    /// Reopening an existing epoch needs explicit resume coordination instead.
    pub async fn open_with_producer(
        links: &BrokerLinks,
        name: &str,
        producer: ProducerId,
        config: SharedTopicWriterConfig,
        retry: RetryPolicy,
    ) -> Result<Self, TopicWriterError> {
        if producer.as_bytes() == &[0; 16] {
            return Err(TopicWriterError::Configuration);
        }
        let metadata = links.topic(name).await?;
        let routes = links.routes(metadata)?;
        // Grow only after each owner's aggregate reservation succeeds. An
        // untrusted large topic cannot preallocate an uncharged writer array.
        let mut entries = Vec::new();
        for number in 0..routes.metadata().partition_count() {
            let number = u32::try_from(number).map_err(|_| TopicWriterError::Configuration)?;
            let partition = routes
                .metadata()
                .partition(number)
                .expect("complete numeric topic");
            let target = partition.incarnation;
            let writer = Writer::open_shared(
                &routes,
                number,
                config.writer(target, producer, 1, 0, routes.metadata().policy()),
                retry,
            )
            .await?;
            entries.push(Partition { target, writer });
        }
        let seed = routes.metadata().partitioner_seed();
        Ok(Self {
            routes,
            producer,
            state: Partitions::new(entries, seed),
        })
    }

    /// Immutable checked identity, configuration, and numeric partition order.
    pub fn metadata(&self) -> &TopicMetadata {
        self.routes.metadata()
    }
    /// One logical identity shared by every partition-local writer session.
    pub fn producer(&self) -> ProducerId {
        self.producer
    }
    /// Save this token once for resume or explicit takeover after a crash.
    pub fn identity(&self) -> ProducerIdentity {
        ProducerIdentity {
            topic: self.metadata().id(),
            producer: self.producer,
        }
    }
    /// Snapshot APPEND admissions for one numeric partition, including retries
    /// and unconfirmed transmissions. Clones share counters. Unknown partitions
    /// return `None`; these statistics never establish confirmation evidence.
    pub fn partition_stats(&self, number: u32) -> Option<WriterStats> {
        self.state
            .entries
            .get(number as usize)
            .map(|partition| partition.writer.stats())
    }
    /// Bounded caller handle; cloning creates no socket or producer epoch.
    pub fn try_clone(&self) -> Result<Self, TopicWriterError> {
        Ok(Self {
            routes: self.routes.clone(),
            producer: self.producer,
            state: self.state.try_clone()?,
        })
    }
    /// Choose a partition before assigning a sequence. Keys are not stored;
    /// absent keys remain sticky until an SDK APPEND group forms, then rotate.
    pub async fn send(
        &mut self,
        record: RecordInput,
        key: Option<&[u8]>,
    ) -> Result<SharedTopicPendingRecord, TopicWriterError> {
        let selection = key.map_or(TopicSelection::Keyless, TopicSelection::Key);
        let (number, pending) = self.state.send(record, selection).await?;
        Ok(SharedTopicPendingRecord {
            topic: self.metadata().id(),
            partition: u32::try_from(number).expect("bounded numeric partition"),
            pending,
        })
    }
    /// Capture all admitted partition prefixes before polling. Each partition
    /// keeps independent progress while other partitions wait for confirmation.
    pub fn flush(
        &self,
    ) -> impl std::future::Future<Output = Result<(), WriterError>> + Send + 'static + use<> {
        self.state.flush()
    }
    /// Seal and drain every partition, including unused ones.
    pub async fn close(self) -> Result<(), WriterError> {
        self.state.close().await
    }
}
