//! Incremental physical and canonical validation under explicit work budgets.

use super::{HistoryError, JournalGeneration, JournalHistory, LogPosition, OpenGroupJournal};
use crate::codec::SegmentDigestBuilder;
use crate::{ChainPosition, CodecError, CurrentReference, SEGMENT_HEADER_BYTES, SegmentHeader};
use std::fs::{self, File};
use std::io::Read;
use std::time::{Duration, Instant};

/// Maximum encoded bytes read and cooperative processing time per step.
///
/// A filesystem call or one physical-group decode cannot be interrupted. Time
/// limits are checked between those units; they are not hard latency guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageValidationBudget {
    pub max_read_bytes: usize,
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
    pub const fn is_valid(self) -> bool {
        self.max_read_bytes > 0 && !self.max_work.is_zero()
    }
}

/// Private captured source and cursor. No vote or repair authority.
#[derive(Debug)]
pub struct StorageValidation {
    history: JournalHistory,
    current: CurrentReference,
    next: usize,
    active: Option<SegmentWork<File>>,
}

#[derive(Debug)]
pub(super) struct SegmentWork<F> {
    pub(super) file: F,
    pub(super) length: usize,
    offset: usize,
    header: Option<SegmentHeader>,
    digest: Option<SegmentDigestBuilder>,
    group_number: u64,
    chain: ChainPosition,
    groups: usize,
    decoded_bytes: usize,
}

/// Exact source and work completed by one successful validation step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageValidationStep {
    pub current: CurrentReference,
    pub generation: JournalGeneration,
    pub through: LogPosition,
    pub segment_id: u64,
    /// Encoded segment bytes read this step, excluding authority metadata.
    pub checked_bytes: u64,
    /// False while reading or decoding the current segment is incomplete.
    pub segment_complete: bool,
    /// Includes the current segment until its final digest is verified.
    pub remaining_segments: usize,
}

impl OpenGroupJournal {
    /// Capture stable storage with one bounded segment arena and metadata list.
    pub fn begin_storage_validation(
        &self,
        max_segment_bytes: usize,
    ) -> Result<StorageValidation, HistoryError> {
        self.validate_authority_files()?;
        Ok(StorageValidation {
            history: self.freeze_history(max_segment_bytes)?,
            current: self.directory().current(),
            next: 0,
            active: None,
        })
    }

    /// Inspect completed buffered writes without claiming crash durability.
    pub fn begin_written_storage_validation(
        &self,
        max_segment_bytes: usize,
    ) -> Result<StorageValidation, HistoryError> {
        self.validate_authority_files()?;
        Ok(StorageValidation {
            history: self.freeze_written_history(max_segment_bytes)?,
            current: self.directory().current(),
            next: 0,
            active: None,
        })
    }
}

impl StorageValidation {
    pub const fn generation(&self) -> JournalGeneration {
        self.history.generation()
    }

    /// Validate one complete captured segment without yielding within it.
    pub fn validate_next(&mut self) -> Result<Option<StorageValidationStep>, HistoryError> {
        self.validate_next_with_budget(StorageValidationBudget {
            max_read_bytes: usize::MAX,
            max_work: Duration::MAX,
        })
    }

    /// Read at most the byte budget and yield between complete decoding units.
    ///
    /// Every successful incomplete step advances either bytes read or validation.
    /// Errors leave the cycle unusable: callers must fence the source, never skip
    /// a damaged group. A fresh cycle rereads bytes; cached history cannot satisfy it.
    pub fn validate_next_with_budget(
        &mut self,
        budget: StorageValidationBudget,
    ) -> Result<Option<StorageValidationStep>, HistoryError> {
        if !budget.is_valid() {
            return Err(HistoryError::Capacity);
        }
        let started = Instant::now();
        let Some(reference) = self.history.state.pin.references().get(self.next).copied() else {
            return Ok(None);
        };
        if self.active.is_none() {
            self.open_segment()?;
        }
        let active = self.active.as_mut().unwrap();
        let read = budget
            .max_read_bytes
            .min(active.length - self.history.state.bytes.len());
        if read > 0 {
            let start = self.history.state.bytes.len();
            self.history.state.bytes.resize(start + read, 0);
            active
                .file
                .read_exact(&mut self.history.state.bytes[start..])?;
        }
        let complete = active.advance(&self.history.state, self.next, budget, started, read > 0)?;
        if complete {
            self.active = None;
            self.next += 1;
        }
        Ok(Some(StorageValidationStep {
            current: self.current,
            generation: self.history.generation(),
            through: self.history.through(),
            segment_id: reference.segment_id,
            checked_bytes: read as u64,
            segment_complete: complete,
            remaining_segments: self.history.state.pin.references().len() - self.next,
        }))
    }

    fn open_segment(&mut self) -> Result<(), HistoryError> {
        let reference = self.history.state.pin.references()[self.next];
        let seal = self.history.state.seal(self.next);
        let length = usize::try_from(seal.valid_bytes).map_err(|_| HistoryError::Capacity)?;
        if length > self.history.state.bytes.capacity() || length < SEGMENT_HEADER_BYTES {
            return Err(HistoryError::Source);
        }
        let path = self
            .history
            .state
            .pin
            .segment_path(reference.segment_id)
            .ok_or(HistoryError::Source)?;
        if !fs::symlink_metadata(&path)?.file_type().is_file() {
            return Err(HistoryError::Source);
        }
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file()
            || metadata.len() < seal.valid_bytes
            || metadata.len() > reference.capacity
        {
            return Err(HistoryError::Source);
        }
        self.history.state.bytes.clear();
        self.history.state.loaded = None;
        self.active = Some(SegmentWork::new(file, length, reference));
        Ok(())
    }
}

impl<F> SegmentWork<F> {
    pub(super) fn new(file: F, length: usize, reference: crate::SegmentReference) -> Self {
        Self {
            file,
            length,
            offset: SEGMENT_HEADER_BYTES,
            header: None,
            digest: None,
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
        budget: StorageValidationBudget,
        started: Instant,
        progressed: bool,
    ) -> Result<bool, HistoryError> {
        let reference = history.pin.references()[index];
        let mut progressed = progressed;
        if self.header.is_none()
            && history.bytes.len() >= SEGMENT_HEADER_BYTES
            && (!progressed || started.elapsed() < budget.max_work)
        {
            let header = crate::decode_segment_header(&history.bytes[..SEGMENT_HEADER_BYTES])?;
            if header.group_id() != history.identity.group_id
                || header.segment_id() != reference.segment_id
                || header.capacity() != reference.capacity
            {
                return Err(HistoryError::Source);
            }
            self.digest = Some(SegmentDigestBuilder::new(&header));
            self.header = Some(header);
            progressed = true;
        }
        while self.header.is_some()
            && self.offset < history.bytes.len()
            && (!progressed || started.elapsed() < budget.max_work)
        {
            if !self.decode_next(history)? {
                break;
            }
            progressed = true;
        }
        let complete = self.header.is_some() && self.offset == self.length;
        if complete {
            let expected = history
                .pin
                .references()
                .get(index + 1)
                .map_or(history.physical_through, |next| {
                    super::before(next.first_chain)
                });
            if self.digest.as_ref().unwrap().finish() != history.seal(index).digest
                || super::before(self.chain) != expected
            {
                return Err(HistoryError::Source);
            }
        }
        Ok(complete)
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
        let group = match crate::decode_group(
            self.header.as_ref().unwrap(),
            self.group_number,
            self.offset as u64,
            self.chain,
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
            if operation.configuration_epoch != history.configuration_epoch
                || operation.original_view > history.promised_view
            {
                return Err(HistoryError::Source);
            }
            ozzy_journal::operation::validate_operation_body(
                operation.kind,
                &operation.body,
                history.operations,
            )?;
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
        self.digest.as_mut().unwrap().push(group.digest);
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
