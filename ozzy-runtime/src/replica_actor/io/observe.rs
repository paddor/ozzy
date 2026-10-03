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
            result = async { self.pending_persistence.front_mut().expect("guarded persistence").await }, if !self.pending_persistence.is_empty() => {
                self.pending_persistence.pop_front();
                self.complete_persisted(result?, now)?;
            }
            result = async { self.pending_sync.as_mut().expect("guarded sync").wait().await }, if available && self.pending_sync.is_some() => {
                self.pending_sync = None;
                self.complete_sync_event(result?, now)?;
            }
            result = async { self.pending.as_mut().expect("guarded journal action").wait().await }, if available && self.pending.is_some() => {
                self.pending = None;
                self.complete(result?, now)?;
            }
            result = async { self.pending_replay.as_mut().expect("guarded replay").wait().await }, if available && self.pending_replay.is_some() => {
                self.pending_replay = None;
                self.complete(result?, now)?;
            }
            changed = self.journal.availability_changed(available) => {
                changed?;
                continue;
            }
            () = self.ready_work.ready(), if wait_for_work => return Ok(false),
            () = self.ingress.ready(), if wait_for_work && self.pending.is_none() => return Ok(false),
            }
            return Ok(true);
        }
    }
}
