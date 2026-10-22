//! Incremental physical and canonical validation under explicit work budgets.

use super::{HistoryError, JournalGeneration, LogPosition};
use crate::codec::SegmentDigestBuilder;
use crate::{ChainPosition, CodecError, CurrentReference, SEGMENT_HEADER_BYTES, SegmentHeader};
use std::time::{Duration, Instant};

/// Maximum encoded bytes read and cooperative processing time per step.
///
/// A filesystem call or one physical-group decode cannot be interrupted. Time
/// limits are checked between those units; they are not hard latency guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageValidationBudget {
    /// Maximum physical bytes read in this maintenance step.
    pub max_read_bytes: usize,
    /// Cooperative elapsed-time bound between complete decoding or scan units.
    pub max_work: Duration,
}

impl Default for StorageValidationBudget {
    fn default() -> Self {
        Self {
            max_read_bytes: 256 * 1024,
            max_work: Duration::from_millis(2),
        }
    }
}

impl StorageValidationBudget {
    /// Whether the byte and cooperative time bounds permit a validation step.
    pub const fn is_valid(self) -> bool {
        self.max_read_bytes > 0 && !self.max_work.is_zero()
    }
}

/// Cooperative time bound of one validation step: the first unit always
/// runs; later units run while the step's work time remains.
#[derive(Debug, Clone, Copy)]
pub(super) struct StepClock {
    pub(super) started: Instant,
    pub(super) max_work: Duration,
}

impl StepClock {
    fn allows(self, progressed: bool) -> bool {
        !progressed || self.started.elapsed() < self.max_work
    }
}

#[derive(Debug)]
pub(super) struct SegmentWork<F> {
    pub(super) file: F,
    pub(super) length: usize,
    offset: usize,
    /// The checked segment header and its running digest, once read.
    decoding: Option<(SegmentHeader, SegmentDigestBuilder)>,
    group_number: u64,
    chain: ChainPosition,
    groups: usize,
    decoded_bytes: usize,
}

/// Exact source and work completed by one successful validation step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageValidationStep {
    /// Exact captured manifest-selection reference.
    pub current: CurrentReference,
    /// Owning journal generation fencing obsolete physical completions.
    pub generation: JournalGeneration,
    /// Exact inclusive canonical operation prefix captured by this work.
    pub through: LogPosition,
    /// Physical segment identity.
    pub segment_id: u64,
    /// Encoded segment bytes read this step, excluding authority metadata.
    pub checked_bytes: u64,
    /// False while reading or decoding the current segment is incomplete.
    pub segment_complete: bool,
    /// Includes the current segment until its final digest is verified.
    pub remaining_segments: usize,
}

impl<F> SegmentWork<F> {
    pub(super) fn new(file: F, length: usize, reference: crate::SegmentReference) -> Self {
        Self {
            file,
            length,
            offset: SEGMENT_HEADER_BYTES,
            decoding: None,
            group_number: reference.first_group_number,
            chain: reference.first_chain,
            groups: 0,
            decoded_bytes: 0,
        }
    }

    pub(super) fn advance<S: super::Source>(
        &mut self,
        history: &super::HistoryState<S>,
        index: usize,
        clock: StepClock,
        progressed: bool,
    ) -> Result<bool, HistoryError> {
        let reference = history.pin.references()[index];
        let mut progressed = progressed;
        if self.decoding.is_none()
            && history.bytes.len() >= SEGMENT_HEADER_BYTES
            && clock.allows(progressed)
        {
            let header = crate::decode_segment_header(&history.bytes[..SEGMENT_HEADER_BYTES])?;
            if header.group_id() != history.identity.group_id
                || header.segment_id() != reference.segment_id
                || header.capacity() != reference.capacity
            {
                return Err(HistoryError::Source);
            }
            let digest = SegmentDigestBuilder::new(&header);
            self.decoding = Some((header, digest));
            progressed = true;
        }
        while self.decoding.is_some()
            && self.offset < history.bytes.len()
            && clock.allows(progressed)
        {
            if !self.decode_next(history)? {
                break;
            }
            progressed = true;
        }
        let Some((_, digest)) = self
            .decoding
            .as_ref()
            .filter(|_| self.offset == self.length)
        else {
            return Ok(false);
        };
        if digest.finish() != history.seal(index).digest
            || super::before(self.chain) != history.segment_end(index)
        {
            return Err(HistoryError::Source);
        }
        Ok(true)
    }

    fn decode_next<S: super::Source>(
        &mut self,
        history: &super::HistoryState<S>,
    ) -> Result<bool, HistoryError> {
        if self.groups == history.decode.max_groups {
            return Err(CodecError::LimitExceeded {
                kind: "physical group count",
                actual: self.groups + 1,
                limit: history.decode.max_groups,
            }
            .into());
        }
        let Some((header, digest)) = self.decoding.as_mut() else {
            return Ok(false);
        };
        let group = match crate::decode_group(
            header,
            crate::GroupStart::new(self.group_number, self.offset as u64, self.chain),
            &history.bytes[self.offset..],
            history.decode,
        ) {
            Ok(group) => group,
            Err(CodecError::Truncated { .. }) if history.bytes.len() < self.length => {
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        };
        for operation in &group.operations {
            history.check_operation(operation)?;
            self.decoded_bytes = self
                .decoded_bytes
                .checked_add(operation.body.len())
                .ok_or(HistoryError::Capacity)?;
        }
        if self.decoded_bytes > history.decode.max_segment_decoded_body_bytes {
            return Err(CodecError::SegmentDecodedBodyLimit {
                actual: self.decoded_bytes,
                limit: history.decode.max_segment_decoded_body_bytes,
            }
            .into());
        }
        digest.push(group.digest);
        self.offset = usize::try_from(group.end_offset).map_err(|_| HistoryError::Capacity)?;
        self.group_number = self
            .group_number
            .checked_add(1)
            .ok_or(HistoryError::Capacity)?;
        self.chain = group.next_chain;
        self.groups += 1;
        Ok(true)
    }
}
