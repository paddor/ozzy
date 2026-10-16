//! Removal of storage objects that the selected manifest no longer
//! references, as a job beside later writes.
//!
//! A settled retention turn captures the selection in memory; the job lists
//! directories, unlinks, and syncs while the partition keeps admitting and
//! writing. It removes only objects older than its capture. Leftovers newer
//! than the active segment can only come from before this owner opened, so
//! the first passes after opening sweep every class exclusively instead.

use super::super::{JournalError, OwnedJournal, StorageCleanup};
use crate::profiling::{Event, Stage};
use ozzy_journal_segment::{CheckpointReference, SegmentReference, UnselectedFiles};
use ozzy_proto::CheckpointId;
use ozzy_replication::driver::ValidationTicket;

/// Unselected objects one pass removes per cleanup class.
const CLEANUP_BUDGET: usize = 8;

/// Manifest selection after a complete cleanup pass, with the unselected
/// objects it kept for live pins.
#[derive(Debug)]
pub(super) struct Cleaned {
    generation: u64,
    segments: Vec<SegmentReference>,
    checkpoint: Option<CheckpointReference>,
    pinned_segments: Vec<u64>,
    pinned_checkpoints: Vec<CheckpointId>,
}

/// Classes a cleanup pass owes, and the record it leaves once complete.
pub(super) struct Owed {
    sources: bool,
    checkpoints: bool,
    next: Cleaned,
}

/// Where a cleanup pass stands between its capture and its result.
#[derive(Debug, Default)]
pub(super) enum Cleaning {
    #[default]
    Idle,
    /// Captured by a settled turn; the shard starts it as a job.
    Captured(Box<CleanupWork>),
    /// Detached work in flight for this attempt.
    Running(u64),
}

/// One cleanup pass over a captured selection. It borrows no journal owner.
#[derive(Debug)]
pub(in crate::replica_journal) struct CleanupWork {
    attempt: u64,
    files: UnselectedFiles,
    /// Segments and their indexes are owed a pass.
    sources: bool,
    /// Checkpoints are owed a pass.
    checkpoints: bool,
    next: Cleaned,
}

/// A finished pass awaiting its owner step.
#[derive(Debug)]
pub(in crate::replica_journal) struct CleanupDone {
    attempt: u64,
    next: Cleaned,
    /// Whether every class removed fewer objects than its budget.
    result: Result<bool, JournalError>,
}

impl CleanupWork {
    /// Validate the directory once, then remove each owed class.
    pub(in crate::replica_journal) async fn run(self) -> CleanupDone {
        let Self {
            attempt,
            mut files,
            sources,
            checkpoints,
            mut next,
        } = self;
        let started = crate::profiling::start();
        let result = pass(&mut files, (sources, checkpoints), &mut next).await;
        crate::profiling::finish(Stage::RetentionCleanup, started);
        CleanupDone {
            attempt,
            next,
            result,
        }
    }
}

async fn pass(
    files: &mut UnselectedFiles,
    (sources, checkpoints): (bool, bool),
    next: &mut Cleaned,
) -> Result<bool, JournalError> {
    files.validate().await?;
    let mut clean = true;
    if sources {
        let segments = files.reclaim_segments(CLEANUP_BUDGET).await?;
        clean &= removed(segments.removed_segment_ids.len());
        next.pinned_segments.extend(segments.pinned_segment_ids);
    }
    if checkpoints {
        let removed_checkpoints = files.reclaim_checkpoints(CLEANUP_BUDGET).await?;
        clean &= removed(removed_checkpoints.removed_checkpoint_ids.len());
        next.pinned_checkpoints
            .extend(removed_checkpoints.pinned_checkpoint_ids);
    }
    if sources {
        clean &= removed(files.reclaim_indexes(CLEANUP_BUDGET).await?.removed_files);
    }
    let metadata = files.reclaim_metadata(CLEANUP_BUDGET).await?;
    clean &= removed(
        metadata.removed_manifest_generations.len() + metadata.removed_temporary_files.len(),
    );
    Ok(clean)
}

/// Count one class's removals. `true` when it finished within its budget.
fn removed(objects: usize) -> bool {
    crate::profiling::count(Event::StorageCleanupRemoved, objects as u64);
    objects < CLEANUP_BUDGET
}

impl OwnedJournal {
    /// Clean unselected storage if a pass is owed: an exclusive sweep until
    /// one completes after opening, then a captured job beside writes.
    pub(super) async fn clean_unselected(
        &mut self,
        ticket: ValidationTicket,
    ) -> Result<(), JournalError> {
        let Some(owed) = self.owed_cleanup()? else {
            return Ok(());
        };
        if self.retention.swept {
            return self.capture_cleanup(owed);
        }
        self.sweep_unselected(ticket, owed).await
    }

    /// The classes a pass owes, with the record it leaves once complete.
    /// Selection changes only when the manifest does, so a pass is owed after
    /// a publication, while objects remain, or once a pinned object is
    /// released. Segments, their indexes, and checkpoints become unselected
    /// only when a publication changes their references, so after a
    /// metadata-only publication a pass removes old manifests alone.
    fn owed_cleanup(&mut self) -> Result<Option<Owed>, JournalError> {
        let journal = self.journal.ready()?;
        let manifest = journal.manifest();
        let (sources, checkpoints) = match &self.retention.cleaned {
            Some(cleaned)
                if journal.pins_released(&cleaned.pinned_segments, &cleaned.pinned_checkpoints) =>
            {
                (true, true)
            }
            Some(cleaned) if cleaned.generation == manifest.generation => return Ok(None),
            Some(cleaned) => (
                cleaned
                    .segments
                    .iter()
                    .any(|segment| !manifest.segments.contains(segment)),
                cleaned
                    .checkpoint
                    .is_some_and(|checkpoint| manifest.checkpoint != Some(checkpoint)),
            ),
            None => (true, true),
        };
        let mut next = Cleaned {
            generation: manifest.generation,
            segments: manifest.segments.clone(),
            checkpoint: manifest.checkpoint,
            pinned_segments: Vec::new(),
            pinned_checkpoints: Vec::new(),
        };
        // A skipped class keeps its pins; none of them was released.
        if let Some(cleaned) = self.retention.cleaned.take() {
            if !sources {
                next.pinned_segments = cleaned.pinned_segments;
            }
            if !checkpoints {
                next.pinned_checkpoints = cleaned.pinned_checkpoints;
            }
        }
        Ok(Some(Owed {
            sources,
            checkpoints,
            next,
        }))
    }

    /// Capture the selection for a cleanup job beside later writes.
    fn capture_cleanup(&mut self, owed: Owed) -> Result<(), JournalError> {
        let files = self.journal.ready()?.capture_unselected()?;
        self.retention.attempts += 1;
        self.retention.cleaning = Cleaning::Captured(Box::new(CleanupWork {
            attempt: self.retention.attempts,
            files,
            sources: owed.sources,
            checkpoints: owed.checkpoints,
            next: owed.next,
        }));
        Ok(())
    }

    /// Sweep every owed class while holding the journal, including objects
    /// newer than the active segment.
    async fn sweep_unselected(
        &mut self,
        ticket: ValidationTicket,
        owed: Owed,
    ) -> Result<(), JournalError> {
        let Owed {
            sources,
            checkpoints,
            mut next,
        } = owed;
        let started = crate::profiling::start();
        let mut clean = true;
        for (kind, due) in [
            (StorageCleanup::Segments, sources),
            (StorageCleanup::Checkpoints, checkpoints),
            (StorageCleanup::Indexes, sources),
            (StorageCleanup::Metadata, true),
        ] {
            if !due {
                continue;
            }
            let cleaned = self.cleanup_storage(ticket, kind, CLEANUP_BUDGET).await?;
            clean &= removed(cleaned.removed_objects);
            next.pinned_segments.extend(cleaned.pinned_segment_ids);
            next.pinned_checkpoints
                .extend(cleaned.pinned_checkpoint_ids);
        }
        crate::profiling::finish(Stage::RetentionCleanup, started);
        if clean {
            self.retention.swept = true;
            self.retention.cleaned = Some(next);
        }
        Ok(())
    }

    /// Captured cleanup work for the shard to run as a job.
    pub(in crate::replica_journal) fn take_cleanup_work(&mut self) -> Option<CleanupWork> {
        match std::mem::take(&mut self.retention.cleaning) {
            Cleaning::Captured(work) => {
                self.retention.cleaning = Cleaning::Running(work.attempt);
                Some(*work)
            }
            other => {
                self.retention.cleaning = other;
                None
            }
        }
    }

    /// Install a finished pass. A failed one fences this owner: it may have
    /// stopped between unlinking a file and syncing its directory.
    pub(in crate::replica_journal) fn complete_cleanup(&mut self, done: CleanupDone) {
        if !matches!(self.retention.cleaning, Cleaning::Running(attempt) if attempt == done.attempt)
        {
            return;
        }
        self.retention.cleaning = Cleaning::Idle;
        match done.result {
            Ok(true) => self.retention.cleaned = Some(done.next),
            Ok(false) => {}
            Err(error) => {
                crate::replica_journal::commands::fault_log(&error);
                self.faulted = true;
            }
        }
    }

    /// Whether a cleanup pass is captured or running.
    pub(in crate::replica_journal) fn cleaning(&self) -> bool {
        !matches!(self.retention.cleaning, Cleaning::Idle)
    }
}
