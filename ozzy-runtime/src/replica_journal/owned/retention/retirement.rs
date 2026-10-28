//! Retirement: a settled turn captures the confirmed image and the sealed
//! prefix, a job writes the checkpoint and scans the prefix beside later
//! writes, and an owner step between writes installs both.

use super::super::{JournalError, OwnedJournal};
use super::charge;
use crate::replica_journal::authority::{position, successor};
use ozzy_core::{
    checkpoint::Layout,
    state::{CanonicalState, StateLimits, StateSnapshotLimits, canonical_state_schema_digest},
};
use ozzy_io::signal::yield_once;
use ozzy_journal_segment::{
    CanonicalCheckpointError, CheckpointFiles, CheckpointSource, GroupJournal, LogPosition,
    RetentionFloors, RetiredPrefix, RetirementBudget, RetirementScan, ScannedPrefix,
};
use ozzy_proto::CheckpointId;
use ozzy_replication::driver::ValidationTicket;

impl OwnedJournal {
    /// Select a durable checkpoint of exactly the settled confirmed image, then
    /// unreference only a validated sealed prefix below those committed floors.
    /// Runs every retirement step inline. Cancellation fences this owner.
    /// Captured readers keep their file pins.
    pub async fn retire_confirmed_history(
        &mut self,
        ticket: ValidationTicket,
        id: CheckpointId,
        budget: RetirementBudget,
    ) -> Result<RetiredPrefix, JournalError> {
        self.begin_retirement(ticket, id, budget).await?;
        let work = self
            .take_retirement_work()
            .ok_or(JournalError::CompletionMismatch)?;
        let done = Box::pin(work.run()).await;
        self.complete_retirement(done);
        self.finish_retirement()
            .await?
            .ok_or(JournalError::CompletionMismatch)
    }

    /// Settled step of a retirement: publish the exact confirmed position and
    /// capture its state, identity rows, and the sealed prefix to retire. A
    /// job then encodes and writes the checkpoint and scans the prefix beside
    /// later writes; `finish_retirement` installs both between writes.
    pub(in crate::replica_journal) async fn begin_retirement(
        &mut self,
        ticket: ValidationTicket,
        id: CheckpointId,
        budget: RetirementBudget,
    ) -> Result<(), JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        self.require_settled(ticket)?;
        if self.retention_busy() || self.packing() {
            return Err(JournalError::CompletionMismatch);
        }
        let scratch = self.charge_retention()?;
        let state = self.images()?.committed().clone();
        let floors =
            RetentionFloors::from_canonical_state(&state)?.protect_from(self.held_operation()?);
        let applied = position(self.applied);
        // Repeated bounded passes reuse only the exact selected checkpoint.
        // The scan still revalidates its files and checks the floors.
        let build = self
            .journal
            .ready()?
            .manifest()
            .checkpoint
            .is_none_or(|selected| selected.position != applied);
        let rows = if build && self.dedup.limits.is_some() {
            let memory = &self.append_memory;
            Some(
                self.dedup
                    .checkpoint_rows(|bytes| charge(memory.as_ref(), bytes))?,
            )
        } else {
            None
        };
        self.faulted = true;
        let journal = self.journal.ready_mut()?;
        journal
            .sync_through(journal.writer().written_position())
            .await?;
        let checkpoint = if build {
            let chunk_bytes = self.limits.checkpoint.max_chunk_bytes;
            let source =
                publish_checkpoint(journal, applied, state.revision(), id, chunk_bytes).await?;
            Some(CheckpointImage {
                source,
                state,
                rows,
            })
        } else {
            None
        };
        let scan =
            journal.prepare_retirement_scan(floors, applied, budget, &self.retention.proofs)?;
        self.retention.attempts += 1;
        self.retention.retiring = Retiring::Captured(Box::new(RetirementWork {
            attempt: self.retention.attempts,
            checkpoint,
            scan,
            limits: (self.recovery.state, self.recovery.snapshot),
            _scratch: scratch,
        }));
        self.faulted = false;
        Ok(())
    }

    /// Captured retirement work for the shard to run as a job.
    pub(in crate::replica_journal) fn take_retirement_work(&mut self) -> Option<RetirementWork> {
        match std::mem::take(&mut self.retention.retiring) {
            Retiring::Captured(work) => {
                self.retention.retiring = Retiring::Running(work.attempt);
                Some(*work)
            }
            other => {
                self.retention.retiring = other;
                None
            }
        }
    }

    /// Keep finished work only for the attempt still running; an abandoned
    /// attempt's files become unselected leftovers.
    pub(in crate::replica_journal) fn complete_retirement(&mut self, done: RetirementDone) {
        if matches!(self.retention.retiring, Retiring::Running(attempt) if attempt == done.attempt)
        {
            self.retention.retiring = Retiring::Done(Box::new(done));
        }
    }

    /// Whether a retirement is captured, running, or awaiting installation.
    pub(in crate::replica_journal) fn retiring(&self) -> bool {
        !matches!(self.retention.retiring, Retiring::Idle)
    }

    /// Whether finished retirement work waits for an owner step between writes.
    pub(in crate::replica_journal) fn retirement_ready(&self) -> bool {
        matches!(self.retention.retiring, Retiring::Done(_)) && self.journal.ready().is_ok()
    }

    /// Drop retirement work captured before the journal's selection changed.
    pub(in crate::replica_journal) fn abandon_retirement(&mut self) {
        self.retention.retiring = Retiring::Idle;
    }

    /// Owner step between writes: select the new checkpoint and unreference
    /// the scanned prefix in one manifest publication, then narrow identity
    /// coverage. Later accepted operations keep their claims. A failed write,
    /// validation, or scan fences this owner.
    pub(in crate::replica_journal) async fn finish_retirement(
        &mut self,
    ) -> Result<Option<RetiredPrefix>, JournalError> {
        self.healthy()?;
        let Retiring::Done(done) = std::mem::take(&mut self.retention.retiring) else {
            return Ok(None);
        };
        self.faulted = true;
        let (files, prefix) = done.result?;
        let journal = self.journal.ready_mut()?;
        let retired = journal.install_retirement(files.as_ref(), prefix).await?;
        drop(files);
        if let Some(reader) = &mut self.reader {
            reader.retired(journal, &retired.unreferenced_segment_ids)?;
        }
        // Retirement changes selected storage, never application state.
        // Narrow identity coverage to the retained history. Rebuilding it
        // would index every surviving sealed segment while nothing is admitted.
        let first = journal.manifest().segments[0];
        self.images
            .as_mut()
            .ok_or(JournalError::CompletionMismatch)?
            .map_identity_indexes(|identities| identities.retire_prefix(first))?;
        self.replay.clear();
        self.storage_validation.clear();
        self.retention
            .retain_segments(|id| !retired.unreferenced_segment_ids.contains(&id));
        self.faulted = false;
        crate::profiling::event(crate::profiling::Event::RetirementInstalled);
        Ok(Some(retired))
    }
}

/// Publish the confirmed position `applied`, whose image has `revision`, as
/// accepted and committed, then prepare a checkpoint of that image.
async fn publish_checkpoint(
    journal: &mut GroupJournal,
    applied: LogPosition,
    revision: u64,
    id: CheckpointId,
    chunk_bytes: usize,
) -> Result<CheckpointSource, JournalError> {
    let mut next = successor(journal.manifest()).ok_or(JournalError::Configuration)?;
    next.accepted = applied;
    next.committed = applied;
    journal.install_metadata(next).await?;
    if revision != journal.committed_position()?.op_number {
        return Err(JournalError::CompletionMismatch);
    }
    Ok(journal.prepare_checkpoint_source(id, canonical_state_schema_digest(), chunk_bytes)?)
}

/// Confirmed state and identity rows captured by the owner; a job encodes
/// and writes them as one checkpoint image.
#[derive(Debug)]
struct CheckpointImage {
    source: CheckpointSource,
    state: CanonicalState,
    rows: Option<super::super::dedup::checkpoint::CheckpointRows>,
}

impl CheckpointImage {
    /// Encode the layout and state, then write them with the identity rows.
    async fn write(self, snapshot: StateSnapshotLimits) -> Result<CheckpointFiles, JournalError> {
        let mut steps = 0_usize;
        let bytes = self
            .state
            .encode_snapshot_cooperative(snapshot, async |_| {
                steps += 1;
                if steps.is_multiple_of(STATE_ENCODE_STEPS) {
                    yield_once().await;
                }
            })
            .await
            .map_err(CanonicalCheckpointError::from)?;
        let layout = Layout::new(
            bytes.len() as u64,
            self.rows.as_ref().map(|rows| rows.identities),
        )
        .map_err(CanonicalCheckpointError::from)?;
        layout
            .validate_state(&self.state)
            .map_err(CanonicalCheckpointError::from)?;
        let plan = self.source.plan(
            layout
                .encoded_bytes()
                .map_err(CanonicalCheckpointError::from)?,
        )?;
        let mut writer = plan.start().await?;
        let fragment = writer.fragment_bytes();
        let rows = self.rows.as_ref().map_or(&[][..], |rows| &rows.bytes[..]);
        for part in [&layout.encode()[..], &bytes, rows] {
            for chunk in part.chunks(fragment) {
                writer.push(chunk).await?;
            }
        }
        Ok(writer.finish().await?)
    }
}

/// State snapshot entries encoded between yields to other shard work.
const STATE_ENCODE_STEPS: usize = 1024;

/// Retirement file work that borrows no journal owner.
#[derive(Debug)]
pub(in crate::replica_journal) struct RetirementWork {
    attempt: u64,
    checkpoint: Option<CheckpointImage>,
    scan: RetirementScan,
    limits: (StateLimits, StateSnapshotLimits),
    _scratch: Option<crate::memory::Charge>,
}

impl RetirementWork {
    /// Write and read back the checkpoint, then scan the prefix.
    pub(in crate::replica_journal) async fn run(self) -> RetirementDone {
        let Self {
            attempt,
            checkpoint,
            scan,
            limits: (state, snapshot),
            _scratch,
        } = self;
        let result = async {
            let files = match checkpoint {
                Some(image) => {
                    let files = image.write(snapshot).await?;
                    files.decode_canonical_state(state, snapshot).await?;
                    Some(files)
                }
                None => None,
            };
            Ok((files, scan.scan().await?))
        }
        .await;
        RetirementDone { attempt, result }
    }
}

/// Finished retirement work awaiting its owner step.
#[derive(Debug)]
pub(in crate::replica_journal) struct RetirementDone {
    attempt: u64,
    result: Result<(Option<CheckpointFiles>, ScannedPrefix), JournalError>,
}

/// Where a retirement stands between its settled turn and its installation.
#[derive(Debug, Default)]
pub(super) enum Retiring {
    #[default]
    Idle,
    /// Captured by a settled turn; the shard starts it as a job.
    Captured(Box<RetirementWork>),
    /// Detached work in flight for this attempt.
    Running(u64),
    /// Detached work finished; an owner step between writes installs it.
    Done(Box<RetirementDone>),
}
