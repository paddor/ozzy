//! Append bodies: partition batches, resolved attempts, and their summaries.

use super::super::{AppendRecordList, AppendResults};
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};
use smallvec::SmallVec;

/// One record within a canonical append sub-batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRecord<'a> {
    /// Payload representation, preserved through persistence and replay.
    pub encoding: ozzy_proto::data::Encoding,
    /// Application record identity preserved through retry and replay.
    pub message_id: MessageId,
    /// Opaque payload parts in record order.
    pub parts: SmallVec<[&'a [u8]; 2]>,
}

/// One partition-contiguous append within an atomic group operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendBatch<'a> {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Expected partition ownership fence.
    pub owner_epoch: OwnerEpoch,
    /// Producer identity scoped to this partition.
    pub producer_id: ProducerId,
    /// Expected producer-session fence.
    pub producer_epoch: ProducerEpoch,
    /// First contiguous producer-local sequence.
    pub first_sequence: ProducerSequence,
    /// First assigned partition-global record offset.
    pub first_offset: Offset,
    /// Primary-resolved Unix timestamp in milliseconds.
    pub append_timestamp_millis: u64,
    /// Consecutive records in this partition batch.
    pub records: AppendRecordList<'a>,
}

/// One journal append operation containing independently addressed partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Append<'a> {
    /// Independently addressed batches in canonical operation order.
    pub batches: Vec<AppendBatch<'a>>,
}

/// One partition's stored records and outcomes of every producer attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAppend<'a> {
    /// New records in consecutive partition-offset order; may be empty.
    pub batch: AppendBatch<'a>,
    /// Attempt order, logical time, and exact discarded-payload fingerprints.
    pub results: AppendResults<'a>,
}

/// One producer batch held until its records' due times. It assigns schedule
/// sequences, not offsets: `first_offset` is zero and the timestamp is the
/// acceptance time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule<'a> {
    /// Held records in producer sequence order.
    pub batch: AppendBatch<'a>,
    /// Schedule sequence of the first record.
    pub first_schedule: u64,
    /// Absolute Unix due time in milliseconds, one per record.
    pub due_millis: Vec<u64>,
}

/// Position in release order: effective due time, then schedule sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReleaseKey {
    /// The later of due and acceptance time, in Unix milliseconds.
    pub millis: u64,
    /// Schedule sequence; equal due times keep acceptance order.
    pub schedule: u64,
}

/// Where a held record lies: the `Schedule` or `Carry` operation holding it
/// and its index in that operation's batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeldPosition {
    /// Operation number.
    pub operation: u64,
    /// Record index inside the operation's batch.
    pub index: u32,
}

/// Held records copied forward in position order, so retention can retire
/// the operations before `through`. The batch has zero producer fields,
/// offset zero, and the carry timestamp. It is empty when every held record
/// in the range was released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Carry<'a> {
    /// Copied records in position order.
    pub batch: AppendBatch<'a>,
    /// The partition's carry floor before this operation.
    pub from: HeldPosition,
    /// The carry floor after it.
    pub through: HeldPosition,
    /// Release key of each copied record.
    pub keys: Vec<ReleaseKey>,
}

/// Due scheduled records copied into the log in release order. The batch has
/// assigned offsets, the release timestamp, and zero producer fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release<'a> {
    /// Copied records in release order.
    pub batch: AppendBatch<'a>,
    /// Release key of the last copied record.
    pub through: ReleaseKey,
}

/// Metadata needed to validate one append's state transition, without records.
/// This is not a hash proof or authorization to skip canonical body validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendBatchSummary {
    /// Destination partition incarnation.
    pub partition: PartitionIncarnation,
    /// Expected partition ownership epoch.
    pub owner_epoch: OwnerEpoch,
    /// Producer bound to this partition.
    pub producer_id: ProducerId,
    /// Expected producer session epoch.
    pub producer_epoch: ProducerEpoch,
    /// First contiguous producer sequence in this batch.
    pub first_sequence: ProducerSequence,
    /// First assigned partition offset in this batch.
    pub first_offset: Offset,
    /// Stored record count checked against schema bounds. A resolved append
    /// containing only duplicate attempts has zero stored records.
    pub record_count: usize,
    /// Whether every descriptor contains a nonzero message identity.
    /// State admission must reject false; syntax validation alone permits it.
    pub nonzero_message_ids: bool,
}

/// One record's byte position inside its batch, recorded while the schema
/// walker validates descriptors. Offsets are relative to the batch's
/// descriptor table and payload start; the final entry per batch is an end
/// checkpoint with zero parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordPosition {
    /// Byte offset relative to the batch descriptor table.
    pub descriptor: u32,
    /// Byte offset relative to the batch payload start.
    pub payload: u32,
    /// Part count; zero marks the batch end checkpoint.
    pub parts: u32,
}

/// Schema-validated append metadata. Up to four partitions stay on the stack.
/// Record descriptors and all part lengths use the ordinary schema walker;
/// application state must still check epochs, positions, and nonzero IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendSummary {
    pub(in crate::operation) batches: SmallVec<[AppendBatchSummary; 4]>,
}

impl AppendSummary {
    /// Construct a one-batch summary from metadata carried by a validated wire APPEND.
    pub fn single(batch: AppendBatchSummary) -> Self {
        Self {
            batches: smallvec::smallvec![batch],
        }
    }

    /// Partition batches in canonical order, without materialized record tables.
    pub fn batches(&self) -> &[AppendBatchSummary] {
        &self.batches
    }
}
