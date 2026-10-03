use super::{JournalError, JournalOwner, Observation, OwnedJournal, Rc, SyncTicket};
use ozzy_journal_segment::{AsyncCompletedJournalSync, AsyncPreparedJournalSync};

/// Captured data/evidence barrier. Normal admission and later writes continue;
/// metadata mutation and segment roll wait for owner observation.
#[derive(Debug)]
#[must_use = "publish and install, or fence and reopen this owner"]
pub struct PreparedSync {
    ticket: SyncTicket,
    work: AsyncPreparedJournalSync,
    observed: Observation,
}

/// Exact physical barrier result awaiting its originating journal and driver.
#[derive(Debug)]
#[must_use = "install on the originating journal"]
pub struct CompletedSync {
    ticket: SyncTicket,
    work: AsyncCompletedJournalSync,
    observed: Observation,
}

impl PreparedSync {
    /// Use the shared backend without borrowing partition or journal state.
    pub async fn publish(self) -> CompletedSync {
        let Self {
            ticket,
            work,
            observed,
        } = self;
        CompletedSync {
            ticket,
            work: work.publish().await,
            observed,
        }
    }
}

impl OwnedJournal {
    /// Capture the installed prefix before returning. Later physical writes
    /// cannot extend the evidence being published or the returned core ticket.
    pub fn begin_sync(&mut self, ticket: SyncTicket) -> Result<PreparedSync, JournalError> {
        self.healthy()?;
        let journal = self.journal.readable()?;
        let written = journal.writer().written_position();
        if ticket.generation() != written.generation()
            || ticket.through().0 > written.next_chain().next_op_number() - 1
            || matches!(&self.journal, JournalOwner::Writing(pipeline) if pipeline.sync_pending())
        {
            return Err(JournalError::CompletionMismatch);
        }
        let work = self
            .journal
            .pipeline(self.writeback.limits.max_operations)?
            .prepare_sync()?;
        Ok(PreparedSync {
            ticket,
            work,
            observed: self.writeback.observation(),
        })
    }

    /// Only exact owner observation grants the driver its sync completion.
    /// Abandoning work/results fences this owner even if its bytes reached disk.
    pub fn complete_sync(&mut self, done: CompletedSync) -> Result<SyncTicket, JournalError> {
        self.healthy()?;
        self.faulted = true;
        let CompletedSync {
            ticket,
            work,
            mut observed,
        } = done;
        if !Rc::ptr_eq(&observed.abandoned, &self.writeback.abandoned) {
            return Err(JournalError::CompletionMismatch);
        }
        let JournalOwner::Writing(pipeline) = &mut self.journal else {
            return Err(JournalError::CompletionMismatch);
        };
        let position = pipeline.complete_sync(work)?;
        if position.generation() != ticket.generation()
            || ticket.through().0 > position.next_chain().next_op_number() - 1
        {
            return Err(JournalError::CompletionMismatch);
        }
        if pipeline.pending() == 0 {
            self.journal.finish_writes()?;
        }
        observed.armed = false;
        self.faulted = false;
        Ok(ticket)
    }
}
