//! Generation-fenced partition reads. Cursors contain metadata only; disk
//! execution owns indexes and short-lived source access, never subscribers.

use std::ops::Range;

use ozzy_proto::{MessageId, Offset, OwnerEpoch, PartitionIncarnation};
use ozzy_replication::driver::ValidationTicket;
use ozzy_replication::{JournalGeneration, Prefix, Scope};

use super::commands::Action;
use super::{JournalCompletion, Rejected, ReplicaJournal, SubmitError};

mod buffer;
pub(crate) mod delivery;
pub(crate) use buffer::PartitionReadBuffer;
pub use buffer::PartitionReadLease;

/// Output bounds for one disk read, independent of writer grouping.
#[derive(Debug, Clone, Copy)]
pub struct PartitionReadLimits {
    /// Maximum whole records. Nonzero; no larger than the worker's operation
    /// record limit. A delivery may span any number of stored operations.
    pub max_records: usize,
    /// Maximum total multipart descriptors, including empty parts. Nonzero;
    /// no larger than the worker's operation part limit.
    pub max_parts: usize,
    /// Maximum opaque bytes. Zero admits only empty payloads.
    pub max_payload_bytes: usize,
}

/// Normal read rejection. These never silently seek or skip expired records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PartitionReadError {
    /// Journal replacement, authority or partition ownership invalidated the cursor.
    #[error("partition reader's journal generation or authority changed")]
    Fenced,
    /// No active applied image is available to answer this read.
    #[error("partition reader requires an active applied journal image")]
    Unavailable,
    /// The partition has not been created in applied state.
    #[error("partition is absent from the applied journal image")]
    Partition,
    /// Policy expiration passed the requested offset; never skip implicitly.
    #[error("requested records expired; earliest retained offset is {earliest:?}")]
    RetentionGap {
        /// Earliest offset still visible under the committed retention policy.
        earliest: Offset,
    },
    /// The requested position is later than this broker's applied partition end.
    #[error("requested offset exceeds the applied end {committed_end:?}")]
    Ahead {
        /// Exclusive applied end at the rejected request.
        committed_end: Offset,
    },
    /// Invalid output bounds or a request exceeding its preallocated payload arena.
    #[error("record and part bounds must be nonzero and payload must fit the leased arena")]
    Limits,
    /// A whole record cannot fit the current record/part/payload allowance.
    #[error("next record exceeds read bounds: {bytes} bytes, {parts} parts")]
    RecordTooLarge {
        /// Exact opaque payload size of the next record.
        bytes: usize,
        /// Number of multipart descriptors, including empty parts.
        parts: usize,
    },
}

/// Seek position tied to one active journal generation and partition owner.
/// Copying this value neither pins a file nor allocates a payload buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionReadCursor {
    pub(super) scope: Scope,
    pub(super) generation: JournalGeneration,
    pub(super) partition: PartitionIncarnation,
    pub(super) owner_epoch: OwnerEpoch,
    pub(super) next: Offset,
    pub(super) observed: Prefix,
    pub(super) end: Offset,
    pub(super) retained_from: Offset,
}

impl PartitionReadCursor {
    /// Captured group authority; callers must recheck live authority on completion.
    pub const fn scope(self) -> Scope {
        self.scope
    }
    /// Journal writer incarnation, invalidated by replacement or reopen.
    pub const fn generation(self) -> JournalGeneration {
        self.generation
    }
    /// Selected partition incarnation.
    pub const fn partition(self) -> PartitionIncarnation {
        self.partition
    }
    /// Captured partition ownership fence.
    pub const fn owner_epoch(self) -> OwnerEpoch {
        self.owner_epoch
    }
    /// Next undelivered offset. This is not application-processing progress.
    pub const fn next_offset(self) -> Offset {
        self.next
    }
    /// Applied end captured by the latest completed command, not a latest read proof.
    pub const fn committed_end(self) -> Offset {
        self.end
    }
    /// Logical retention floor captured with that applied end.
    pub const fn retained_from(self) -> Offset {
        self.retained_from
    }
}

#[derive(Debug)]
pub(super) struct ReadRecord {
    pub(super) encoding: ozzy_proto::data::Encoding,
    pub(super) id: MessageId,
    pub(super) parts: Range<usize>,
}

/// Bounded payload delivery retaining the original worker arena lease.
/// Empty output means caught up to the captured applied end. A late completion
/// still requires the caller's subscription and authority-generation checks.
#[derive(Debug)]
pub struct ReadPartition {
    pub(super) cursor: PartitionReadCursor,
    pub(super) first: Offset,
    pub(super) buffer: PartitionReadLease,
}

impl ReadPartition {
    /// Resume only after this delivery is admitted to the subscriber's queue.
    pub const fn cursor(&self) -> PartitionReadCursor {
        self.cursor
    }
    /// First returned partition offset, or the unchanged cursor for caught up.
    pub const fn first_offset(&self) -> Offset {
        self.first
    }
    /// Exact IDs and parts borrowing the bounded lease. No full operation pin.
    pub fn records(
        &self,
    ) -> impl ExactSizeIterator<
        Item = (
            MessageId,
            ozzy_proto::data::Encoding,
            impl ExactSizeIterator<Item = &[u8]> + Clone,
        ),
    > + Clone {
        self.buffer.records().iter().map(|record| {
            (
                record.id,
                record.encoding,
                self.buffer.parts()[record.parts.clone()]
                    .iter()
                    .map(|range| &self.buffer.read_bytes()[range.clone()]),
            )
        })
    }
    /// Clear the delivery and recover its original arena and admission lease.
    pub fn into_buffer(mut self) -> PartitionReadLease {
        self.buffer.clear();
        self.buffer
    }
}

impl<E> ReplicaJournal<E> {
    /// Resolve a starting offset against applied state on the disk executor.
    /// Retains no subscriber slot or snapshot after the command completes.
    pub fn open_reader(
        &mut self,
        ticket: ValidationTicket,
        partition: PartitionIncarnation,
        from: Offset,
    ) -> Result<JournalCompletion<PartitionReadCursor>, SubmitError> {
        self.open_reader_from(ticket, partition, Some(from))
    }

    /// Resolve the applied end as the start: only later records are read.
    pub(crate) fn open_reader_at_end(
        &mut self,
        ticket: ValidationTicket,
        partition: PartitionIncarnation,
    ) -> Result<JournalCompletion<PartitionReadCursor>, SubmitError> {
        self.open_reader_from(ticket, partition, None)
    }

    fn open_reader_from(
        &mut self,
        ticket: ValidationTicket,
        partition: PartitionIncarnation,
        from: Option<Offset>,
    ) -> Result<JournalCompletion<PartitionReadCursor>, SubmitError> {
        self.submit(
            (ticket, partition, from),
            |(ticket, partition, from), done| Action::OpenReader {
                ticket,
                partition,
                from,
                done,
            },
            |action| match action {
                Action::OpenReader {
                    ticket,
                    partition,
                    from,
                    ..
                } => (ticket, partition, from),
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Read available confirmed records into an empty worker-owned arena.
    /// Backpressure returns that arena unchanged. Dropping an admitted completion
    /// does not cancel disk execution; it releases the finished output safely.
    #[expect(
        clippy::result_large_err,
        reason = "return reusable arena ownership on backpressure"
    )]
    pub fn read_partition(
        &mut self,
        cursor: PartitionReadCursor,
        limits: PartitionReadLimits,
        buffer: impl Into<PartitionReadLease>,
    ) -> Result<JournalCompletion<ReadPartition>, Rejected<PartitionReadLease>> {
        self.submit_partition_read(cursor, limits, buffer.into(), delivery::ReadReply::Copied)
    }

    /// Resident selections go straight to the application thread. Cold reads
    /// retain the existing bounded reader-worker execution and arena ownership.
    #[expect(
        clippy::result_large_err,
        reason = "return reusable arena ownership on backpressure"
    )]
    pub(crate) fn read_delivery(
        &mut self,
        cursor: PartitionReadCursor,
        limits: PartitionReadLimits,
        buffer: PartitionReadLease,
    ) -> Result<JournalCompletion<delivery::ReadDelivery>, Rejected<PartitionReadLease>> {
        self.submit_partition_read(cursor, limits, buffer, delivery::ReadReply::Direct)
    }

    #[expect(
        clippy::result_large_err,
        reason = "return reusable arena ownership on backpressure"
    )]
    fn submit_partition_read<T>(
        &mut self,
        cursor: PartitionReadCursor,
        limits: PartitionReadLimits,
        buffer: PartitionReadLease,
        reply: impl FnOnce(
            crate::completion::Sender<Result<T, super::JournalError>>,
        ) -> delivery::ReadReply,
    ) -> Result<JournalCompletion<T>, Rejected<PartitionReadLease>> {
        let read_permit = match self.read_capacity.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(error) => {
                return Err(Rejected {
                    reason: if matches!(error, tokio::sync::TryAcquireError::Closed) {
                        super::SubmitError::Stopped
                    } else {
                        super::SubmitError::Full
                    },
                    value: buffer,
                });
            }
        };
        self.submit(
            buffer,
            |buffer, done| Action::ReadPartition {
                read_permit,
                cursor,
                limits,
                buffer,
                done: reply(done),
            },
            |action| match action {
                Action::ReadPartition { buffer, .. } => buffer,
                _ => unreachable!("submission preserves command kind"),
            },
        )
    }
}
