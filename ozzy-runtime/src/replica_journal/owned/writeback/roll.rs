//! Segment rolls: publish the successor segment outside the journal borrow
//! and install it on this owner.

use super::{JournalError, JournalOwner, Observation, OwnedJournal};
use ozzy_journal_segment::{CompletedJournalRoll, PreparedJournalRoll};
use std::rc::Rc;

/// Detached segment publication. Other actors and this owner's logical
/// admission may continue, but physical writes await exact installation.
#[derive(Debug)]
#[must_use = "publish and install, or fence and reopen the owner"]
pub struct PreparedRoll {
    work: PreparedJournalRoll,
    observed: Observation,
}

/// Publication result awaiting exact-generation installation by its owner.
#[derive(Debug)]
#[must_use = "install on the originating journal"]
pub struct CompletedRoll {
    completed: CompletedJournalRoll,
    observed: Observation,
}

impl PreparedRoll {
    /// Publish selected segment metadata through the shared async backend.
    pub async fn publish(self) -> CompletedRoll {
        let Self { work, observed } = self;
        CompletedRoll {
            completed: work.publish().await,
            observed,
        }
    }
}

impl OwnedJournal {
    /// Start a roll after physical reservations settle. Accepted queued groups
    /// remain owned and may span the roll without changing retry boundaries.
    pub fn begin_roll(&mut self, max_probes: usize) -> Result<PreparedRoll, JournalError> {
        self.healthy()?;
        if !self.can_roll()? {
            return Err(JournalError::RollCapacity);
        }
        self.journal.finish_writes()?;
        let capacity = self.journal.ready()?.writer().header().capacity();
        self.faulted = true;
        let (pending, work) = self
            .journal
            .take_ready()?
            .begin_owned_roll(capacity, max_probes)?;
        self.journal = JournalOwner::Rolling(Box::new(pending));
        self.faulted = false;
        Ok(PreparedRoll {
            work,
            observed: self.writeback.observation(),
        })
    }

    /// Install this owner's exact completed roll, keeping queued canonical
    /// requests and their immutable encoded bodies unchanged.
    pub fn complete_roll(&mut self, done: CompletedRoll) -> Result<(), JournalError> {
        self.healthy()?;
        self.faulted = true;
        let CompletedRoll {
            completed,
            mut observed,
        } = done;
        if !Rc::ptr_eq(&observed.abandoned, &self.writeback.abandoned) {
            return Err(JournalError::CompletionMismatch);
        }
        self.journal =
            JournalOwner::Ready(Box::new(self.journal.take_rolling()?.complete(completed)?));
        let reader = self.reader.as_mut().ok_or(JournalError::AppendMismatch)?;
        reader.rolled(self.journal.readable()?)?;
        // The sealed segment's read index already holds its record ends, so
        // neither retention planning nor retirement need scan the segment.
        let partition = self.images.as_ref().and_then(|images| {
            images
                .speculative()
                .partitions()
                .next()
                .map(|(partition, _)| partition)
        });
        if let Some(proof) = reader.sealed_retirement_proof(self.journal.readable()?)? {
            if let Some(partition) = partition {
                let summary = proof.retention_segment(partition);
                self.retention.sealed.insert(summary.id, summary);
            }
            self.retention.proofs.push(proof);
        }
        observed.armed = false;
        self.faulted = false;
        self.release_writes();
        Ok(())
    }
}
