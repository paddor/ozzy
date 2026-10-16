use super::{Error, History};
use crate::async_files::Span;
use crate::history::validation::{SegmentWork, StepClock};
use crate::{
    CurrentReference, SEGMENT_HEADER_BYTES, StorageValidationBudget, StorageValidationStep,
};
use ozzy_io::{Handle, Operation};
use ozzy_journal::progress::JournalGeneration;
use std::time::{Duration, Instant};

/// Fresh physical/canonical scrub of one captured source. Errors and canceled
/// steps fence the cursor; restarting requires a new authority-checked capture.
#[derive(Debug)]
pub struct Validation {
    history: History,
    current: CurrentReference,
    next: usize,
    active: Option<SegmentWork<Handle>>,
    faulted: bool,
}

impl Validation {
    pub(crate) const fn new(history: History, current: CurrentReference) -> Self {
        Self {
            history,
            current,
            next: 0,
            active: None,
            faulted: false,
        }
    }

    /// Journal-owner generation fencing these captured bytes or completions.
    pub const fn generation(&self) -> JournalGeneration {
        self.history.generation()
    }

    /// Validate one whole segment. Jobs still obey the backend transfer bound;
    /// use the budgeted form to bound CPU processing between actor turns too.
    pub async fn validate_next(&mut self) -> Result<Option<StorageValidationStep>, Error> {
        self.validate_next_with_budget(StorageValidationBudget {
            max_read_bytes: usize::MAX,
            max_work: Duration::MAX,
        })
        .await
    }

    /// Scrub a bounded part of one captured segment; errors or cancellation fence the cursor.
    pub async fn validate_next_with_budget(
        &mut self,
        budget: StorageValidationBudget,
    ) -> Result<Option<StorageValidationStep>, Error> {
        if self.faulted {
            return Err(Error::Source);
        }
        if !budget.is_valid() {
            return Err(Error::Capacity);
        }
        self.faulted = true;
        let result = self.step(budget).await;
        if result.is_ok() {
            self.faulted = false;
        }
        result
    }

    async fn step(
        &mut self,
        budget: StorageValidationBudget,
    ) -> Result<Option<StorageValidationStep>, Error> {
        let started = Instant::now();
        let Some(reference) = self.history.state.pin.references.get(self.next).copied() else {
            return Ok(None);
        };
        // The open segment lives here during a step and in `self.active`
        // between steps; a failed step faults the validator and drops it.
        let mut active = match self.active.take() {
            Some(active) => active,
            None => self.open().await?,
        };
        let start = self.history.state.bytes.len();
        let read = budget.max_read_bytes.min(active.length - start);
        let span = Span {
            offset: start as u64,
            length: read,
        };
        self.history
            .access
            .read_append(
                &active.file,
                span,
                self.history.chunk,
                &mut self.history.state.bytes,
            )
            .await?;
        let clock = StepClock {
            started,
            max_work: budget.max_work,
        };
        let complete = active.advance(&self.history.state, self.next, clock, read > 0)?;
        if complete {
            self.history
                .access
                .done(Operation::Close {
                    handle: active.file,
                })
                .await?;
            self.next += 1;
        } else {
            self.active = Some(active);
        }
        Ok(Some(StorageValidationStep {
            current: self.current,
            generation: self.history.generation(),
            through: self.history.through(),
            segment_id: reference.segment_id,
            checked_bytes: read as u64,
            segment_complete: complete,
            remaining_segments: self.history.state.pin.references.len() - self.next,
        }))
    }

    async fn open(&mut self) -> Result<SegmentWork<Handle>, Error> {
        let reference = self.history.state.pin.references[self.next];
        let seal = self.history.state.seal(self.next);
        let length = usize::try_from(seal.valid_bytes).map_err(|_| Error::Capacity)?;
        if length > self.history.state.bytes.capacity() || length < SEGMENT_HEADER_BYTES {
            return Err(Error::Source);
        }
        let file = self.history.open_segment(self.next).await?;
        self.history.state.bytes.clear();
        self.history.state.loaded = None;
        Ok(SegmentWork::new(file, length, reference))
    }
}
