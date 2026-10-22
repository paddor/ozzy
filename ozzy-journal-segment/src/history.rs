//! Frozen canonical history for replica selection and bounded transfer.

pub(crate) mod asynchronous;
mod load;
mod validation;
pub use validation::{StorageValidationBudget, StorageValidationStep};

use ozzy_journal::operation::validate_operation_body;
use ozzy_journal::progress::JournalGeneration;

use crate::codec::SegmentDigestBuilder;
use crate::{
    CanonicalOperation, ChainPosition, CodecError, DecodeLimits, DecodedOperation, DirectoryError,
    ENTRY_HEADER_BYTES, GroupIdentity, LogPosition, OperationLimits, SealedSegment, SegmentHeader,
    decode_indexed_operation,
};

/// Metadata and deletion protection only. Loading bytes is deliberately absent.
trait Source {
    fn references(&self) -> &[crate::SegmentReference];
}

#[derive(Debug)]
struct HistoryState<S> {
    pin: S,
    identity: GroupIdentity,
    generation: JournalGeneration,
    configuration_epoch: u64,
    promised_view: u64,
    through: LogPosition,
    // Physical validation still covers the whole frozen final group/segment,
    // even when a recovery snapshot stops inside that group.
    physical_through: LogPosition,
    active_seal: SealedSegment,
    decode: DecodeLimits,
    operations: OperationLimits,
    bytes: Vec<u8>,
    entries: Vec<IndexedOperation>,
    entry_limit: usize,
    loaded: Option<LoadedSegment>,
    #[cfg(test)]
    full_scans: std::cell::Cell<usize>,
}

#[derive(Debug)]
struct LoadedSegment {
    index: usize,
    header: SegmentHeader,
    valid_bytes: u64,
    next_group_number: u64,
    next_chain: ChainPosition,
    digest: SegmentDigestBuilder,
    groups: usize,
    decoded_body_bytes: usize,
}

#[derive(Debug)]
struct IndexedOperation {
    offset: usize,
    bytes: usize,
    position: LogPosition,
    body_bytes: usize,
}

/// Consecutive whole operations borrowed from one verified source segment.
///
/// A segment boundary may end a nonempty response before its count/byte budget.
/// This is history evidence, never a vote or application-commit decision.
#[derive(Debug)]
pub struct HistoryChunk<'a> {
    predecessor: LogPosition,
    end: LogPosition,
    operations: Vec<DecodedOperation<'a>>,
}

impl HistoryChunk<'_> {
    /// Verified position immediately before this chunk.
    pub const fn predecessor(&self) -> LogPosition {
        self.predecessor
    }

    /// Exact canonical end, usable as the next request's predecessor.
    pub const fn end(&self) -> LogPosition {
        self.end
    }

    /// Complete decoded operations, preserving original views and identities.
    pub fn operations(&self) -> impl ExactSizeIterator<Item = CanonicalOperation<'_>> {
        self.verified_operations().map(|(operation, _)| operation)
    }

    /// Operations and body digests verified together by the source decoder.
    /// This chunk exposes no mutable bytes; transfer can reuse those digests
    /// without hashing the same body again. Receivers still verify their bytes.
    pub fn verified_operations(
        &self,
    ) -> impl ExactSizeIterator<Item = (CanonicalOperation<'_>, crate::Digest)> {
        self.operations.iter().map(|operation| {
            (
                CanonicalOperation {
                    group_id: operation.group_id,
                    configuration_epoch: operation.configuration_epoch,
                    original_view: operation.original_view,
                    op_number: operation.op_number,
                    previous_digest: operation.previous_digest,
                    kind: operation.kind,
                    body: operation.body.as_ref(),
                },
                operation.body_digest,
            )
        })
    }
}

impl<S: Source> HistoryState<S> {
    /// Persistent source identity, not a remote authentication credential.
    const fn identity(&self) -> GroupIdentity {
        self.identity
    }

    /// Frozen writer incarnation, independent of later live writers.
    const fn generation(&self) -> JournalGeneration {
        self.generation
    }

    /// Exact captured stable canonical tail.
    const fn through(&self) -> LogPosition {
        self.through
    }

    /// Boundary immediately before the first retained operation.
    fn predecessor(&self) -> LogPosition {
        before(self.pin.references()[0].first_chain)
    }

    /// Look up an exact canonical position for a caller's bounded selection cache.
    /// `None` means outside this captured range, never evidence for a negative vote.
    fn position_loaded(&self, op_number: u64) -> Result<Option<LogPosition>, HistoryError> {
        if op_number == self.predecessor().op_number {
            return Ok(Some(self.predecessor()));
        }
        if op_number < self.predecessor().op_number || op_number > self.through.op_number {
            return Ok(None);
        }
        let index = self.segment_for(op_number)?;
        if self
            .loaded
            .as_ref()
            .is_none_or(|loaded| loaded.index != index)
        {
            return Err(HistoryError::Source);
        }
        let entry = self
            .entries
            .binary_search_by_key(&op_number, |entry| entry.position.op_number)
            .map_err(|_| HistoryError::Source)?;
        Ok(Some(self.entries[entry].position))
    }

    /// Read a nonempty bounded response strictly after an exact source predecessor.
    /// Wrong anchors and exhausted/insufficient budgets are errors, never empty
    /// success that could be mistaken for the end of authoritative history.
    fn read_after_loaded(
        &self,
        predecessor: LogPosition,
        max_operations: usize,
        max_body_bytes: usize,
    ) -> Result<HistoryChunk<'_>, HistoryError> {
        let index = self.read_index(predecessor, max_operations, max_body_bytes)?;
        if self
            .loaded
            .as_ref()
            .is_none_or(|loaded| loaded.index != index)
        {
            return Err(HistoryError::Source);
        }
        let reference = self.pin.references()[index];
        let (expected, first) =
            if predecessor.op_number == reference.first_chain.next_op_number() - 1 {
                (before(reference.first_chain), 0)
            } else {
                let entry = self
                    .entries
                    .binary_search_by_key(&predecessor.op_number, |entry| entry.position.op_number)
                    .map_err(|_| HistoryError::Predecessor)?;
                (self.entries[entry].position, entry + 1)
            };
        if predecessor != expected {
            return Err(HistoryError::Predecessor);
        }
        // Reserve only the validated number that could actually fit, never an
        // untrusted request count independent of source/decode bounds.
        let available = self.entries[first..]
            .partition_point(|entry| entry.position.op_number <= self.through.op_number);
        let count = available.min(max_operations);
        let mut operations = Vec::with_capacity(count);
        let mut body_bytes = 0usize;
        let mut end = predecessor;
        for entry in self.entries.iter().skip(first).take(count) {
            if entry.body_bytes > max_body_bytes - body_bytes {
                break;
            }
            let operation = self.read_entry(entry)?;
            if operation.previous_digest != end.digest {
                return Err(HistoryError::Source);
            }
            body_bytes += operation.body.len();
            end = entry.position;
            operations.push(operation);
        }
        if operations.is_empty() {
            return Err(HistoryError::BodyBudget {
                required: self
                    .entries
                    .get(first)
                    .ok_or(HistoryError::Source)?
                    .body_bytes,
                available: max_body_bytes,
            });
        }
        Ok(HistoryChunk {
            predecessor,
            end,
            operations,
        })
    }

    fn segment_for(&self, op: u64) -> Result<usize, HistoryError> {
        self.pin
            .references()
            .iter()
            .rposition(|reference| reference.first_chain.next_op_number() <= op)
            .ok_or(HistoryError::Source)
    }

    fn read_index(
        &self,
        predecessor: LogPosition,
        max_operations: usize,
        max_body_bytes: usize,
    ) -> Result<usize, HistoryError> {
        if max_operations == 0 || max_body_bytes == 0 {
            return Err(HistoryError::Capacity);
        }
        if predecessor.op_number < self.predecessor().op_number
            || predecessor.op_number >= self.through.op_number
        {
            return Err(HistoryError::Predecessor);
        }
        let next_op = predecessor
            .op_number
            .checked_add(1)
            .ok_or(HistoryError::Predecessor)?;
        let index = self.segment_for(next_op)?;
        Ok(index)
    }

    fn seal(&self, index: usize) -> SealedSegment {
        self.pin.references()[index]
            .sealed
            .unwrap_or(self.active_seal)
    }

    /// Position the segment at `index` ends at: its successor's predecessor,
    /// or the physical capture end for the last segment.
    fn segment_end(&self, index: usize) -> LogPosition {
        self.pin
            .references()
            .get(index + 1)
            .map_or(self.physical_through, |next| before(next.first_chain))
    }

    /// An operation must match the captured epoch and view and carry a valid
    /// body.
    fn check_operation(&self, operation: &DecodedOperation<'_>) -> Result<(), HistoryError> {
        if operation.configuration_epoch != self.configuration_epoch
            || operation.original_view > self.promised_view
        {
            return Err(HistoryError::Source);
        }
        validate_operation_body(operation.kind, operation.body.as_ref(), self.operations)?;
        Ok(())
    }

    fn read_entry(&self, entry: &IndexedOperation) -> Result<DecodedOperation<'_>, HistoryError> {
        let loaded = self.loaded.as_ref().ok_or(HistoryError::Source)?;
        let end = entry
            .offset
            .checked_add(entry.bytes)
            .ok_or(HistoryError::Source)?;
        let bytes = self
            .bytes
            .get(entry.offset..end)
            .ok_or(HistoryError::Source)?;
        let operation = decode_indexed_operation(
            &loaded.header,
            entry.offset as u64,
            bytes,
            entry.position.op_number,
            self.decode,
        )?;
        if operation.op_number != entry.position.op_number
            || operation.digest != entry.position.digest
            || operation.body.len() != entry.body_bytes
        {
            return Err(HistoryError::Source);
        }
        self.check_operation(&operation)?;
        Ok(operation)
    }
}

fn before(chain: ChainPosition) -> LogPosition {
    LogPosition {
        op_number: chain.next_op_number() - 1,
        digest: chain.previous_digest(),
    }
}

fn reserve_buffers(
    maximum: u64,
    max_segment_bytes: usize,
) -> Result<(Vec<u8>, Vec<IndexedOperation>, usize), HistoryError> {
    let maximum = usize::try_from(maximum).map_err(|_| HistoryError::Capacity)?;
    if maximum > max_segment_bytes {
        return Err(HistoryError::Capacity);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(maximum)
        .map_err(|_| HistoryError::Capacity)?;
    // Every operation consumes a physical entry header, even when compressed.
    let entry_limit = maximum / ENTRY_HEADER_BYTES;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(entry_limit)
        .map_err(|_| HistoryError::Capacity)?;
    Ok((bytes, entries, entry_limit))
}

pub(super) fn largest_captured_prefix(
    references: &[crate::SegmentReference],
    active_seal: SealedSegment,
) -> Result<u64, HistoryError> {
    references
        .iter()
        .map(|reference| reference.sealed.unwrap_or(active_seal).valid_bytes)
        .max()
        .ok_or(HistoryError::Source)
}

pub(super) fn validate_segment_capacities(
    references: &[crate::SegmentReference],
    max_segment_bytes: usize,
) -> Result<(), HistoryError> {
    let limit = u64::try_from(max_segment_bytes).map_err(|_| HistoryError::Capacity)?;
    if references
        .iter()
        .any(|reference| reference.capacity > limit)
    {
        return Err(HistoryError::Capacity);
    }
    Ok(())
}

/// Frozen-source failure. Corrupt/unreadable history never becomes negative evidence.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    /// Capture cannot export uncertain or incompletely published bytes.
    #[error("history source has unsettled writes or metadata")]
    Unsettled,
    /// A source identity, physical anchor, or canonical boundary differs.
    #[error("history bytes do not match the captured source")]
    Source,
    /// Request does not continue an exact retained source position.
    #[error("history predecessor is outside or conflicts with the captured source")]
    Predecessor,
    /// Resource budget is invalid, insufficient, or cannot be allocated.
    #[error("history read exceeds configured capacity")]
    Capacity,
    /// The verified next operation needs more bytes than this request allows.
    /// The source remains valid; a caller may retry when byte credit increases.
    #[error("history operation needs {required} body bytes, request permits {available}")]
    BodyBudget {
        #[doc = "Capacity required by this captured history."]
        required: usize,
        #[doc = "Available bytes, capacity, or canonical prefix at failure."]
        available: usize,
    },
    /// Source read failed. No claim of nonexistence is justified.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Physical/logical integrity failure.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// Canonical body schema failure.
    #[error(transparent)]
    Operation(#[from] ozzy_journal::operation::OperationCodecError),
    /// Exact source pin or journal boundary could not be captured.
    #[error(transparent)]
    Journal(#[from] DirectoryError),
}
