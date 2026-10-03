use super::{Error, History};
use crate::history::validation::SegmentWork;
use crate::{
    CurrentReference, SEGMENT_HEADER_BYTES, StorageValidationBudget, StorageValidationStep,
};
use ozzy_io::{Handle, OpenMode, Operation};
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
        if self.active.is_none() {
            self.open().await?;
        }
        let active = self.active.as_mut().expect("active segment opened");
        let start = self.history.state.bytes.len();
        let read = budget.max_read_bytes.min(active.length - start);
        self.history
            .access
            .read_append(
                &active.file,
                start as u64,
                read,
                self.history.chunk,
                &mut self.history.state.bytes,
            )
            .await?;
        let complete = active.advance(&self.history.state, self.next, budget, started, read > 0)?;
        if complete {
            let active = self.active.take().expect("validated segment is active");
            self.history
                .access
                .done(Operation::Close {
                    handle: active.file,
                })
                .await?;
            self.next += 1;
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

    async fn open(&mut self) -> Result<(), Error> {
        let reference = self.history.state.pin.references[self.next];
        let seal = self.history.state.seal(self.next);
        let length = usize::try_from(seal.valid_bytes).map_err(|_| Error::Capacity)?;
        if length > self.history.state.bytes.capacity() || length < SEGMENT_HEADER_BYTES {
            return Err(Error::Source);
        }
        let file = self
            .history
            .access
            .open(
                self.history.state.pin.path(self.next),
                OpenMode::Read,
                false,
                false,
            )
            .await?;
        let actual = self.history.access.length(&file).await?;
        if actual < seal.valid_bytes || actual > reference.capacity {
            return Err(Error::Source);
        }
        self.history.state.bytes.clear();
        self.history.state.loaded = None;
        self.active = Some(SegmentWork::new(file, length, reference));
        Ok(())
    }
}
