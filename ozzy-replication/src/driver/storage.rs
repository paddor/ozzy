//! Local storage completions and failures: promise publication, writes,
//! barriers, application, and uncertain I/O.

use super::{DriverError, ReplicaDriver, Role};
use crate::{
    JournalGeneration, Prefix, PromiseTicket, ReplicationError, SyncTicket, ViewChangeError,
    WriteTicket,
};
use std::time::Duration;

impl ReplicaDriver {
    /// Note when local storage last made progress while it had work, or
    /// `None` while it has none. Busy storage that completes nothing for the
    /// primary timeout is suspicion, as an undurable admitted write is: other
    /// storage work can hold admission back.
    pub fn observe_storage(
        &mut self,
        busy_since: Option<Duration>,
        now: Duration,
    ) -> Result<(), DriverError> {
        self.observe_time(now)?;
        self.storage_busy_since = busy_since;
        Ok(())
    }

    /// Observe actual durable publication of the exact outstanding promise.
    pub fn complete_promise(&mut self, ticket: PromiseTicket) -> Result<(), DriverError> {
        self.changing()?.complete_promise(ticket)?;
        Ok(())
    }

    /// Report an uncertain metadata publication. No election retry can reuse it.
    pub fn fail_promise(&mut self, ticket: PromiseTicket) -> Result<(), DriverError> {
        self.changing()?.fail_promise(ticket)?;
        Ok(())
    }

    /// Settle actual complete writes, including writes admitted before a timeout.
    /// Completion while changing view never restores normal voting or commit.
    pub fn complete_write(&mut self, ticket: WriteTicket) -> Result<(), DriverError> {
        match &mut self.role {
            Role::Normal(normal) => normal.complete_write(ticket)?,
            Role::ViewChanging(changing) => changing.complete_write(ticket)?,
            Role::Installing(_) => return Err(ReplicationError::WrongRole.into()),
            Role::Fenced(_) => return Err(ViewChangeError::Faulted.into()),
        }
        Ok(())
    }

    /// Install an exact `O_DSYNC` completion. The write is durable at once.
    pub fn complete_durable_write(
        &mut self,
        ticket: WriteTicket,
        now: Duration,
    ) -> Result<(), DriverError> {
        self.observe_time(now)?;
        match &mut self.role {
            Role::Normal(normal) => {
                let previous = normal.snapshot();
                normal.complete_durable_write(ticket)?;
                let current = normal.snapshot();
                self.observe_normal_progress(&previous, &current, now);
            }
            Role::ViewChanging(changing) => changing.normal.complete_durable_write(ticket)?,
            Role::Installing(_) => return Err(ReplicationError::WrongRole.into()),
            Role::Fenced(_) => return Err(ViewChangeError::Faulted.into()),
        }
        Ok(())
    }

    /// Complete deterministic ordered application through an established commit.
    /// Application replay/identity-index I/O stays outside this driver.
    pub fn apply_through(&mut self, through: Prefix) -> Result<(), DriverError> {
        let Role::Normal(normal) = &mut self.role else {
            return Err(ReplicationError::NotNormal.into());
        };
        let was_ready = normal.snapshot().ready_for_appends;
        normal.apply_through(through)?;
        if !was_ready && normal.snapshot().ready_for_appends {
            self.progress_deadline = self.now.saturating_add(self.timing.primary_timeout);
            self.election_delay = self.timing.election_timeout;
        }
        Ok(())
    }

    /// Fence on uncertain storage failure. A stale writer cannot fault a new one.
    pub fn fail_io(&mut self, generation: JournalGeneration) -> Result<(), DriverError> {
        match &mut self.role {
            Role::Normal(normal) => normal.fail_io(generation)?,
            Role::ViewChanging(changing) => changing.fail_io(generation)?,
            Role::Installing(installing) => {
                let ticket = installing.ticket();
                if generation != ticket.generation() {
                    return Err(ViewChangeError::StaleInstallation.into());
                }
                installing.fail(ticket)?;
            }
            Role::Fenced(_) => return Err(ViewChangeError::Faulted.into()),
        }
        Ok(())
    }

    /// Capture the written prefix for a real disk barrier, never a simulated ACK.
    pub fn begin_sync(&self) -> Result<SyncTicket, DriverError> {
        match &self.role {
            Role::Normal(normal) => Ok(normal.begin_sync()?),
            Role::ViewChanging(changing) => Ok(changing.begin_sync()?),
            Role::Installing(_) => Err(ReplicationError::WrongRole.into()),
            Role::Fenced(_) => Err(ViewChangeError::Faulted.into()),
        }
    }

    /// Observe a completed barrier. Commit and persistence deadlines advance
    /// independently; a late old-view completion only settles the frozen log.
    pub fn complete_sync(&mut self, ticket: SyncTicket, now: Duration) -> Result<(), DriverError> {
        self.observe_time(now)?;
        match &mut self.role {
            Role::Normal(normal) => {
                let previous = normal.snapshot();
                normal.complete_sync(ticket)?;
                let current = normal.snapshot();
                self.observe_normal_progress(&previous, &current, now);
            }
            Role::ViewChanging(changing) => changing.complete_sync(ticket)?,
            Role::Installing(_) => return Err(ReplicationError::WrongRole.into()),
            Role::Fenced(_) => return Err(ViewChangeError::Faulted.into()),
        }
        Ok(())
    }
}
