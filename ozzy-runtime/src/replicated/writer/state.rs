pub(super) use super::completion::Completion;
use super::{
    AppendKey, PendingRecord, Policy, ProducerId, RecordInput, RecordReceipt, Writer, WriterConfig,
    WriterError,
};
use super::{WriterRuntime, pipe};
use super::{inbox::Inbox, payload::Body};
use crate::signal::{CloseSignal, DataSignal, StateSignal};
use omq_tokio::message::Payload;
use ozzy_proto::MessageId;
use ozzy_proto::PartitionIncarnation;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Debug)]
pub(super) struct Sender {
    direct: pipe::Sender,
    resources: Option<crate::replicated::broker_links::append::Reservation>,
}

impl Sender {
    pub(super) fn try_clone(&self) -> Result<Self, WriterError> {
        Ok(Self {
            direct: self.direct.try_clone().ok_or(WriterError::Pipeline)?,
            resources: self.resources.clone(),
        })
    }

    pub(super) fn try_send(&mut self, record: Queued) -> Result<(), WriterError> {
        self.direct.try_send(record)
    }
}
/// Set in `next` once admission is sealed. One compare-and-swap then both
/// checks the seal and assigns a ticket, so a sealed writer never issues a
/// sequence that a flush captured before the seal could miss.
pub(super) const SEALED: u64 = 1 << 63;

#[derive(Debug)]
pub(super) struct Shared {
    pub(super) config: WriterConfig,
    _runtime: WriterRuntime,
    pub(super) progress: Arc<Progress>,
    pub(super) inbox: Inbox,
    staged: AtomicU64,
    pub(super) capacity: Arc<StateSignal>,
    pub(super) work: Arc<DataSignal>,
    pub(super) stop: CloseSignal,
    pub(super) finished: CloseSignal,
    /// Next producer sequence below `SEALED`; the `SEALED` bit closes admission.
    pub(super) next: AtomicU64,
    pub(super) flush_through: AtomicU64,
    pub(super) producers: AtomicUsize,
    pub(super) groups_formed: AtomicU64,
    pub(super) stats: super::stats::Counters,
    resources: Option<crate::replicated::broker_links::append::Reservation>,
}
/// Private to the network driver, including across reconnects.
#[derive(Debug)]
pub(super) struct Driver {
    pub(super) shared: Arc<Shared>,
    incoming: pipe::Receiver,
    incoming_batch: VecDeque<Queued>,
    reorder: Vec<Option<Queued>>,
    reordered: usize,
    ready_next: u64,
    pub(super) admission: Admission,
    pub(super) batches: super::batch::PayloadPool,
    _resources: Option<crate::replicated::broker_links::append::Reservation>,
    /// Declared last: fields drop in order, so `finished` closes only after
    /// this driver released `shared` and every other owned resource.
    _finished: Finished,
}

#[derive(Debug)]
struct Finished(CloseSignal);

impl Drop for Finished {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl std::ops::Deref for Driver {
    type Target = Shared;
    fn deref(&self) -> &Shared {
        &self.shared
    }
}
#[derive(Debug)]
pub(super) struct Admission {
    pub(super) records: VecDeque<Queued>,
}
/// One admitted record on its way to the driver. Inline caller bytes remain
/// inline until grouped or sent alone; only multipart records need a length
/// table. Queue and table reservations cover the larger admission entry.
#[derive(Debug)]
pub(super) struct Queued {
    pub(super) encoding: ozzy_proto::data::Encoding,
    /// Admission time, retained only when stage profiling needs it.
    pub(super) admitted_at: Option<tokio::time::Instant>,
    pub(super) sequence: u64,
    pub(super) completion: Completion,
    /// Part lengths of a multipart record; `None` is one part of `body.len()`.
    pub(super) lengths: Option<Box<[u32]>>,
    pub(super) body: Body,
}
pub(super) const QUEUED_SLOT_BYTES: usize = 256;
const _: () = assert!(size_of::<Queued>() <= QUEUED_SLOT_BYTES);

/// Owned record storage beyond the fixed queue entry. Validated multipart
/// lengths stay charged even when every payload part is empty.
pub(super) fn admission_bytes(payload_bytes: usize, parts: usize) -> usize {
    payload_bytes
        + if parts > 1 {
            parts * size_of::<u32>()
        } else {
            0
        }
}

impl Queued {
    pub(super) fn message_id(&self) -> MessageId {
        self.completion.message_id()
    }
    /// Number of parts, including empty parts.
    pub(super) fn parts(&self) -> usize {
        self.lengths.as_ref().map_or(1, |lengths| lengths.len())
    }

    fn admission_bytes(&self) -> usize {
        admission_bytes(self.body.len(), self.parts())
    }

    /// Part lengths in order.
    pub(super) fn lengths(&self) -> impl Iterator<Item = usize> + Clone + '_ {
        let single = self.lengths.is_none().then(|| self.body.len());
        let multipart = self.lengths.as_deref().unwrap_or(&[]);
        single
            .into_iter()
            .chain(multipart.iter().map(|&length| length as usize))
    }
}
impl Admission {
    pub(super) fn profile_queue(&self, records: std::ops::Range<usize>) {
        if crate::profiling::enabled() {
            for record in self.records.range(records) {
                crate::profiling::finish(
                    crate::profiling::Stage::SdkQueue,
                    record.admitted_at.map(tokio::time::Instant::into_std),
                );
            }
        }
    }
    pub(super) fn payload(&mut self, index: usize) -> &Payload {
        let record = &mut self.records[index];
        record.body.payload()
    }
}
#[cfg(test)]
pub(super) struct TestRecord {
    pub(super) payload: Payload,
    pub(super) lengths: Vec<usize>,
}
/// Completion state and immutable writer identity shared by pending handles.
/// Contains no payloads or admission queues.
#[derive(Debug)]
pub(super) struct Progress {
    confirmed: AtomicU64,
    failure: OnceLock<WriterError>,
    // Failure seals the confirmed prefix. Serialize that terminal transition
    // with confirmation publication so observations never change afterward.
    transition: Mutex<Option<u64>>,
    changed: StateSignal,
    partition: PartitionIncarnation,
    owner_epoch: u64,
    producer_id: ProducerId,
    producer_epoch: u64,
    policy: Policy,
}

impl Progress {
    pub(super) fn receipt(
        &self,
        sequence: u64,
        message_id: MessageId,
        offset: u64,
    ) -> RecordReceipt {
        RecordReceipt {
            partition: self.partition,
            owner_epoch: self.owner_epoch,
            key: AppendKey {
                producer_id: self.producer_id,
                producer_epoch: self.producer_epoch,
                first_sequence: sequence,
            },
            message_id,
            offset,
            policy: self.policy,
        }
    }

    pub(super) async fn wait(&self, target: u64) -> Result<(), WriterError> {
        self.changed.wait_for(|| self.observe(target)).await
    }

    pub(super) fn observe(&self, target: u64) -> Option<Result<(), WriterError>> {
        if target <= self.confirmed() {
            return Some(Ok(()));
        }
        let failure = self.failure.get()?;
        // Progress serializes confirmation and terminal failure publication.
        // Reload after acquiring failure so a racing earlier confirmation wins.
        if target <= self.confirmed() {
            Some(Ok(()))
        } else {
            Some(Err(failure.clone()))
        }
    }

    pub(super) fn confirmed(&self) -> u64 {
        self.confirmed.load(Ordering::Acquire)
    }

    fn fail(&self, error: WriterError) {
        let _transition = self
            .transition
            .lock()
            .expect("writer progress transition poisoned");
        self.failure.get_or_init(|| error);
    }

    fn confirm<T>(
        &self,
        first: u64,
        end: u64,
        first_offset: u64,
        ready_next: u64,
        retire: impl FnOnce(usize) -> T,
    ) -> Result<T, WriterError> {
        let mut transition = self
            .transition
            .lock()
            .expect("writer progress transition poisoned");
        if let Some(error) = self.failure.get() {
            return Err(error.clone());
        }
        let confirmed = self.confirmed();
        if first > confirmed || first >= end || end > ready_next || end < confirmed {
            return Err(super::Error::Response.into());
        }
        let count = usize::try_from(end - confirmed).map_err(|_| super::Error::Response)?;
        let last_offset = first_offset
            .checked_add(end - first - 1)
            .ok_or(super::Error::Response)?;
        if transition.is_some_and(|last| {
            if first < confirmed {
                first_offset.checked_add(confirmed - first - 1) != Some(last)
            } else {
                first_offset <= last
            }
        }) {
            return Err(super::Error::Response.into());
        }
        let retired = retire(count);
        *transition = Some(last_offset);
        self.confirmed.store(end, Ordering::Release);
        drop(transition);
        self.changed.notify_changed();
        Ok(retired)
    }
}

impl Shared {
    #[cfg(test)]
    pub(super) fn channel_with_capacity(
        config: WriterConfig,
        lane_capacity: usize,
    ) -> (Writer, Driver) {
        let runtime = WriterRuntime::new().unwrap();
        let owned = runtime.clone();
        futures::executor::block_on(
            runtime
                .driver()
                .spawn(async move { Self::open_reserved(owned, config, lane_capacity, None) }),
        )
        .unwrap()
    }

    pub(super) fn open_reserved(
        runtime: WriterRuntime,
        config: WriterConfig,
        lane_capacity: usize,
        resources: Option<crate::replicated::broker_links::append::Reservation>,
    ) -> (Writer, Driver) {
        let capacity = Arc::new(StateSignal::default());
        let work = Arc::new(DataSignal::default());
        let inbox_capacity = lane_capacity
            .checked_mul(config.max_producers)
            .expect("validated inbox capacity");
        let inbox_bytes = config.lane_bytes().saturating_mul(config.max_producers);
        let inbox = Inbox::new(inbox_capacity, inbox_bytes, capacity.clone());
        let (direct, incoming) = pipe::channel(inbox_capacity, work.clone());
        let batches = super::batch::PayloadPool::new(&config, &work);
        let records = VecDeque::with_capacity(lane_capacity);
        let reorder = (0..inbox_capacity).map(|_| None).collect();
        let next = config.next_sequence;
        let incoming_capacity = config.limits.max_records.max(1024);
        let shared = Arc::new(Self {
            _runtime: runtime,
            progress: Arc::new(Progress {
                confirmed: AtomicU64::new(next),
                failure: OnceLock::new(),
                transition: Mutex::new(None),
                changed: StateSignal::default(),
                partition: config.partition,
                owner_epoch: config.owner_epoch,
                producer_id: config.producer_id,
                producer_epoch: config.producer_epoch,
                policy: config.policy,
            }),
            inbox,
            staged: AtomicU64::new(next),
            capacity,
            work,
            stop: CloseSignal::default(),
            finished: CloseSignal::default(),
            next: AtomicU64::new(next),
            flush_through: AtomicU64::new(next),
            producers: AtomicUsize::new(1),
            groups_formed: AtomicU64::new(0),
            stats: super::stats::Counters::default(),
            config,
            resources: resources.clone(),
        });
        (
            Writer {
                shared: shared.clone(),
                sender: Sender {
                    direct,
                    resources: resources.clone(),
                },
            },
            Driver {
                _finished: Finished(shared.finished.clone()),
                shared,
                incoming,
                incoming_batch: VecDeque::with_capacity(incoming_capacity),
                reorder,
                reordered: 0,
                ready_next: next,
                admission: Admission { records },
                batches,
                _resources: resources,
            },
        )
    }
    pub(super) fn validate(&self, record: &RecordInput) -> Result<usize, WriterError> {
        let limits = self.config.limits;
        let (parts, bytes) = record.shape();
        let metadata = parts
            .checked_mul(4)
            .and_then(|n| n.checked_add(ozzy_proto::append::IDENTITY_METADATA_BYTES + 30));
        if record.message_id.as_bytes() == &[0; 16]
            || parts == 0
            || parts > limits.max_parts
            || metadata.is_none_or(|n| n > limits.envelope.max_metadata_bytes)
            || bytes.is_none_or(|n| {
                n > limits
                    .envelope
                    .max_payload_bytes
                    .min(limits.max_record_bytes)
            })
        {
            return Err(WriterError::RecordLimits);
        }
        Ok(bytes.expect("checked size"))
    }

    pub(super) fn sealed(&self) -> bool {
        self.next.load(Ordering::Acquire) & SEALED != 0
    }
    /// Sequences admitted so far, excluding the seal.
    pub(super) fn next_sequence(&self) -> u64 {
        self.next.load(Ordering::Acquire) & !SEALED
    }
    pub(super) fn seal(&self) {
        self.next.fetch_or(SEALED, Ordering::AcqRel);
        self.capacity.notify_changed();
        self.work.mark();
    }
    #[allow(
        deprecated,
        reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
    )]
    pub(super) fn admit(
        &self,
        sender: &mut Sender,
        record: &mut RecordInput,
        bytes: usize,
    ) -> Option<Result<PendingRecord, WriterError>> {
        if let Some(error) = self.progress.failure.get() {
            return Some(Err(error.clone()));
        }
        if self.sealed() {
            return Some(Err(WriterError::Closed));
        }
        let reservation = self
            .inbox
            .reserve(admission_bytes(bytes, record.parts().len()))?;
        // Normalize caller-owned slices before assigning a sequence. No await
        // or caller destructor interrupts the ticket-to-publication interval,
        // so a flush that captures `next` after the seal covers every ticket.
        let lengths = if record.shape().0 > 1 {
            let lengths: Result<Box<[u32]>, _> = record
                .parts()
                .map(|part| u32::try_from(part.len()))
                .collect();
            let Ok(lengths) = lengths else {
                return Some(Err(WriterError::RecordLimits));
            };
            Some(lengths)
        } else {
            None
        };
        if self.resources.is_some() {
            record.detach_shared();
        }
        let body = Body::take(record, bytes);
        let admitted_at = crate::profiling::enabled().then(tokio::time::Instant::now);
        let Ok(sequence) = self
            .next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |next| {
                (next & SEALED == 0 && next < SEALED - 1).then_some(next + 1)
            })
        else {
            return Some(Err(if self.sealed() {
                WriterError::Closed
            } else {
                WriterError::Configuration
            }));
        };
        let completion = sender.direct.completion(record.message_id);
        let queued = Queued {
            encoding: ozzy_proto::data::Encoding::Raw,
            admitted_at,
            sequence,
            completion: completion.clone(),
            lengths,
            body,
        };
        reservation.publish();
        if let Err(error) = sender.try_send(queued) {
            self.fail(error.clone());
            self.stop.close();
            return Some(Err(error));
        }
        Some(Ok(PendingRecord {
            progress: self.progress.clone(),
            completion,
            sequence,
        }))
    }
    pub(super) fn fail(&self, error: WriterError) {
        self.progress.fail(error);
        self.seal();
        self.progress.changed.notify_changed();
        self.capacity.notify_changed();
        self.work.mark();
    }
}
impl Driver {
    /// Restore sequence order after fair inproc fan-in. Preparation never
    /// changes tickets or releases aggregate admission capacity.
    pub(super) fn drain(&mut self) {
        let mut bytes = 0;
        for _ in 0..self.config.limits.max_records.max(1024) {
            if self.incoming_batch.is_empty() {
                let mut batch = std::mem::take(&mut self.incoming_batch).into();
                match self
                    .incoming
                    .try_recv_batch_into(&mut batch, self.config.limits.max_records.max(1024))
                {
                    Ok(_) | Err(crate::command_channel::TryRecvError::Empty) => {}
                    Err(crate::command_channel::TryRecvError::Disconnected) => {
                        self.incoming_batch = batch.into();
                        self.fail(WriterError::Pipeline);
                        self.stop.close();
                        break;
                    }
                }
                self.incoming_batch = batch.into();
            }
            let Some(record) = self.incoming_batch.pop_front() else {
                break;
            };
            bytes += record.body.len();
            self.accept(record);
            if bytes >= self.config.batch_target_bytes {
                self.work.mark();
                break;
            }
        }
    }

    fn accept(&mut self, record: Queued) {
        if record.sequence == self.ready_next {
            self.ready_next += 1;
            self.admission.records.push_back(record);
        } else {
            let index = (record.sequence % self.reorder.len() as u64) as usize;
            assert!(
                self.reorder[index].is_none(),
                "admission reorder window collision"
            );
            self.reorder[index] = Some(record);
            self.reordered += 1;
            return;
        }
        while self.reordered != 0 {
            let index = (self.ready_next % self.reorder.len() as u64) as usize;
            let Some(record) = self.reorder[index].take() else {
                break;
            };
            assert_eq!(record.sequence, self.ready_next);
            self.reordered -= 1;
            self.ready_next += 1;
            self.admission.records.push_back(record);
        }
    }
    #[cfg(test)]
    pub(super) fn settle(&mut self) {
        self.drain();
    }

    #[cfg(test)]
    pub(super) fn record(&mut self, sequence: u64) -> Option<TestRecord> {
        self.settle();
        let first = self.admission.records.front()?.sequence;
        let index = usize::try_from(sequence.checked_sub(first)?).ok()?;
        self.admission.records.get(index)?;
        let payload = self.admission.payload(index).clone();
        let record = &self.admission.records[index];
        Some(TestRecord {
            payload,
            lengths: record.lengths().collect(),
        })
    }
    /// Transfer an admitted prefix into request ownership. Retries never return
    /// inbox capacity twice, including after reconnect with different grouping.
    pub(super) fn stage(&self, end: u64) {
        let previous = self.staged.fetch_max(end, Ordering::AcqRel);
        if end <= previous {
            return;
        }
        // Staged records stay admitted until confirmed, so the whole newly
        // staged range is still in the admission queue.
        let first = self
            .admission
            .records
            .front()
            .map_or(previous, |record| record.sequence);
        debug_assert!(first <= previous, "confirmation stages its records first");
        let range = previous.saturating_sub(first) as usize..end.saturating_sub(first) as usize;
        let bytes = self
            .admission
            .records
            .range(range)
            .map(Queued::admission_bytes)
            .sum();
        self.inbox.release((end - previous) as usize, bytes);
    }

    /// Remote confirmation releases local retry state and drops original payloads.
    /// Every confirmed record was selected from the admission queue, so no
    /// intake drain is needed to cover `end`.
    pub(super) fn confirm(
        &mut self,
        first: u64,
        end: u64,
        first_offset: u64,
    ) -> Result<usize, WriterError> {
        // Confirmed records were sent, so they left the inbox; releasing here
        // keeps the byte count exact whatever order a caller uses.
        self.stage(end.min(self.ready_next));
        let progress = &self.shared.progress;
        let records = &mut self.admission.records;
        let bytes = progress.confirm(first, end, first_offset, self.ready_next, |count| {
            records
                .drain(..count)
                .map(|record| {
                    record
                        .completion
                        .publish(first_offset + (record.sequence - first));
                    record.body.len()
                })
                .sum()
        })?;
        self.work.mark();
        Ok(bytes)
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        // Also runs if the spawned future is canceled before its first poll.
        self.shared.fail(WriterError::Closed);
        self.shared.stop.close();
        // `finished` closes when `_finished` drops, after `shared`.
    }
}
