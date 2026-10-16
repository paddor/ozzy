//! Shared journal observation for external scheduling and controlled simulation.

use super::super::{ActorError, ReplicaActor};
use std::{task::Poll, time::Duration};

impl ReplicaActor {
    pub(crate) async fn observe_journal(
        &mut self,
        now: Duration,
        wait_for_work: bool,
    ) -> Result<bool, ActorError> {
        if !wait_for_work
            && self.pending_persistence.is_empty()
            && self.pending_sync.is_none()
            && self.pending.is_none()
            && self.pending_replay.is_none()
        {
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
            changed = self.journal.availability_changed(available) => {
                changed?;
                continue;
            }
            () = std::future::ready(()), if wait_for_work && self.more_work => return Ok(false),
            () = self.ingress.ready(), if wait_for_work && self.pending.is_none() => return Ok(false),
            }
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
