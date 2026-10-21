//! Packing: a replica whose journal runs low on segments rewrites a run of
//! sparse sealed segments as one. Followers group operations on their own
//! timing, so one that wrote small groups would otherwise fill before its
//! leader and hold its writes. The owner captures a run, a job measures and
//! rewrites it beside writes, and an owner step between writes selects the
//! packed segment. A pack never overlaps retirement or cleanup: each would
//! change or delete what the other captured.

use super::super::{JournalError, OwnedJournal, room::record_reserve};
use super::charge;
use crate::profiling::{Level, log};
use ozzy_journal_segment::{PackJob, PackLimits, PackOutcome, PackedSegments};

/// File generations a pack tries past leftovers of earlier attempts.
const PACK_PROBES: usize = 16;

/// Where a pack stands between its capture and its installation.
#[derive(Debug, Default)]
pub(in crate::replica_journal) enum Packing {
    #[default]
    Idle,
    /// Detached work in flight for this attempt.
    Running(u64),
    /// A packed segment awaits an owner step between writes.
    Done(Box<PackedSegments>),
}

/// One captured pack job and the memory it holds.
#[derive(Debug)]
pub(in crate::replica_journal) struct PackWork {
    attempt: u64,
    job: PackJob,
    _scratch: Option<crate::memory::Charge>,
}

/// A finished pack job awaiting its owner step.
#[derive(Debug)]
pub(in crate::replica_journal) struct PackDone {
    attempt: u64,
    result: Result<PackOutcome, JournalError>,
}

impl PackWork {
    /// Measure and rewrite the captured run.
    pub(in crate::replica_journal) async fn run(self) -> PackDone {
        let Self {
            attempt,
            job,
            _scratch,
        } = self;
        PackDone {
            attempt,
            result: job.run().await.map_err(Into::into),
        }
    }
}

impl OwnedJournal {
    fn pack_limits(&self) -> PackLimits {
        PackLimits {
            // Live writes submit groups of this body size, so the backend
            // takes packed groups of it.
            max_group_bytes: self.writeback.limits.max_body_bytes,
            max_probes: PACK_PROBES,
        }
    }

    /// Whether the journal is low on segments: at most half again the
    /// record reserve is free, so packing starts before records stop.
    fn pack_due(&self) -> Result<bool, JournalError> {
        let max = self.limits.metadata.max_segments as u64;
        let used = self.journal.readable()?.manifest().segments.len() as u64;
        let reserve = record_reserve(max);
        Ok(max.saturating_sub(used) <= reserve + reserve / 2)
    }

    /// Capture a pack when the journal is low on segments in a normal view,
    /// and no retention job or earlier pack is in progress. A plan that
    /// found nothing waits for the next manifest.
    pub(in crate::replica_journal) fn take_pack_work(&mut self) -> Option<PackWork> {
        if self.faulted
            || self.packing()
            || self.retention_busy()
            || self.images.is_none()
            || self.selected.is_some()
            || !self.pack_due().ok()?
        {
            return None;
        }
        let journal = self.journal.readable().ok()?;
        let manifest = journal.manifest();
        if manifest.promised_view != manifest.last_normal_view
            || self.retention.unpackable == Some(manifest.generation)
        {
            return None;
        }
        let limits = self.pack_limits();
        let scratch = match journal.pack_scratch_bytes(limits) {
            Ok(bytes) => charge(self.append_memory.as_ref(), bytes).ok()?,
            Err(_) => return None,
        };
        match journal.prepare_pack(limits) {
            Ok(Some(job)) => {
                self.retention.attempts += 1;
                self.retention.packing = Packing::Running(self.retention.attempts);
                Some(PackWork {
                    attempt: self.retention.attempts,
                    job,
                    _scratch: scratch,
                })
            }
            Ok(None) => {
                self.retention.unpackable = Some(manifest.generation);
                None
            }
            Err(error) => {
                crate::replica_journal::commands::fault_log(&error.into());
                self.faulted = true;
                None
            }
        }
    }

    /// Keep a finished pack only for the attempt still running. Measured
    /// layouts help later plans either way. A failed job leaves at most an
    /// unselected file and waits for the next manifest.
    pub(in crate::replica_journal) fn complete_pack(&mut self, done: PackDone) {
        if !matches!(self.retention.packing, Packing::Running(attempt) if attempt == done.attempt) {
            return;
        }
        self.retention.packing = Packing::Idle;
        let generation = self
            .journal
            .readable()
            .map(|journal| journal.manifest().generation)
            .ok();
        let mut outcome = match done.result {
            Ok(outcome) => outcome,
            Err(error) => {
                log(
                    Level::Warn,
                    &format!("replica journal pack failed: {error}"),
                );
                self.retention.unpackable = generation;
                return;
            }
        };
        if let Ok(journal) = self.journal.readable() {
            journal.record_pack_layouts(&outcome);
        }
        match outcome.take_packed() {
            Some(packed) => self.retention.packing = Packing::Done(Box::new(packed)),
            None => self.retention.unpackable = generation,
        }
    }

    /// Whether a pack is running or awaits installation.
    pub(in crate::replica_journal) fn packing(&self) -> bool {
        !matches!(self.retention.packing, Packing::Idle)
    }

    /// Whether a packed segment can install in an owner step between writes.
    /// A cleanup job captured before it could delete the packed file.
    pub(in crate::replica_journal) fn pack_ready(&self) -> bool {
        matches!(self.retention.packing, Packing::Done(_))
            && !self.cleaning()
            && self.journal.ready().is_ok()
    }

    /// Drop a pack captured before the journal's selection changed.
    pub(in crate::replica_journal) fn abandon_pack(&mut self) {
        self.retention.packing = Packing::Idle;
    }

    /// Owner step between writes: select the packed segment, then forget
    /// cached state of the segments it replaced. A run the selection moved
    /// past installs nothing. A failed publication fences this owner.
    pub(in crate::replica_journal) async fn finish_pack(&mut self) -> Result<(), JournalError> {
        self.healthy()?;
        let Packing::Done(packed) = std::mem::take(&mut self.retention.packing) else {
            return Ok(());
        };
        let journal = self.journal.ready_mut()?;
        if !journal.pack_selected(&packed) {
            return Ok(());
        }
        self.faulted = true;
        let replaced: Vec<u64> = packed
            .replaced()
            .iter()
            .map(|reference| reference.segment_id)
            .collect();
        let proof = packed.retirement_proof(&self.retention.proofs);
        journal.install_pack(*packed).await?;
        if let Some(reader) = &mut self.reader {
            reader.packed(journal, &replaced)?;
        }
        self.retention.retain_segments(|id| !replaced.contains(&id));
        let partition = self.images.as_ref().and_then(|images| {
            images
                .speculative()
                .partitions()
                .next()
                .map(|(partition, _)| partition)
        });
        if let Some(proof) = proof {
            if let Some(partition) = partition {
                let summary = proof.retention_segment(partition);
                self.retention.sealed.insert(summary.id, summary);
            }
            self.retention.proofs.push(proof);
        }
        self.replay.clear();
        self.storage_validation.clear();
        self.release_writes();
        self.faulted = false;
        log(
            Level::Info,
            &format!(
                "replica journal packed {} sealed segments into one",
                replaced.len()
            ),
        );
        Ok(())
    }
}
