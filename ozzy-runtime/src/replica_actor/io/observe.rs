//! Shared journal observation for external scheduling and controlled simulation.

use super::super::{ActorError, ReplicaActor};
use super::PendingIo;
use std::{task::Poll, time::Duration};

impl ReplicaActor {
    /// Whether any journal command or persistence is outstanding.
    pub(in crate::replica_actor) fn disk_pending(&self) -> bool {
        self.pending.is_some()
            || self.pending_replay.is_some()
            || self.releases.running()
            || self.pending_sync.is_some()
            || !self.pending_persistence.is_empty()
    }

    /// Report outstanding storage work to the driver for this round.
    pub(in crate::replica_actor) fn observe_storage(
        &mut self,
        now: Duration,
    ) -> Result<(), ActorError> {
        // The view waits on this copy's own promise or installation I/O;
        // timing out while it runs would only discard the work.
        if matches!(
            self.pending,
            Some(
                PendingIo::Promise(_)
                    | PendingIo::Begin(_)
                    | PendingIo::Chunk(_)
                    | PendingIo::Finish(_)
            )
        ) {
            self.driver.history_progress(now)?;
        }
        // Storage that has work and completes none of it holds the role as
        // a stalled write does, though no write was admitted.
        let busy = self.disk_pending();
        // A command of many jobs on a slow device is not a stall while its
        // jobs complete.
        let jobs = self.journal.completed_jobs();
        if !busy || jobs != self.storage_jobs {
            self.storage_progress_at = Some(now);
            self.storage_jobs = jobs;
        }
        let since = *self.storage_progress_at.get_or_insert(now);
        self.driver.observe_storage(busy.then_some(since), now)?;
        Ok(())
    }

    pub(crate) async fn observe_journal(
        &mut self,
        now: Duration,
        wait_for_work: bool,
    ) -> Result<bool, ActorError> {
        if !wait_for_work && !self.disk_pending() {
            // Detached readers/maintenance can have work without core receipts.
            return std::future::poll_fn(|cx| {
                Poll::Ready(match self.journal.poll_stopped(cx) {
                    Poll::Pending => Ok(false),
                    Poll::Ready(error) => Err(ActorError::Journal(error)),
                })
            })
            .await;
        }
        loop {
            let available = self.journal.available_command_slots() != 0;
            tokio::select! {
            biased;
            result = present(self.pending_persistence.front_mut()), if !self.pending_persistence.is_empty() => {
                self.pending_persistence.pop_front();
                self.complete_persisted(result?, now)?;
            }
            result = present(self.pending_sync.as_mut().map(super::PendingSync::wait)), if available && self.pending_sync.is_some() => {
                self.pending_sync = None;
                self.complete_sync_event(result?, now)?;
            }
            result = present(self.pending.as_mut().map(super::PendingIo::wait)), if available && self.pending.is_some() => {
                self.pending = None;
                self.complete(result?, now)?;
            }
            result = present(self.pending_replay.as_mut().map(super::PendingReplay::wait)), if available && self.pending_replay.is_some() => {
                self.pending_replay = None;
                self.complete(result?, now)?;
            }
            result = present(self.releases.step()), if available && self.releases.running() => {
                self.releases.complete(result, now, &mut self.work.proposals)?;
            }
            changed = self.journal.availability_changed(available) => {
                changed?;
                continue;
            }
            () = std::future::ready(()), if wait_for_work && self.more_work => return Ok(false),
            () = self.ingress.ready(), if wait_for_work && self.pending.is_none() => return Ok(false),
            }
            self.storage_progress_at = Some(now);
            return Ok(true);
        }
    }
}

/// Await work that is present. Each select branch guards the absent case,
/// which would never resolve.
async fn present<F: std::future::Future>(work: Option<F>) -> F::Output {
    match work {
        Some(work) => work.await,
        None => std::future::pending().await,
    }
}
