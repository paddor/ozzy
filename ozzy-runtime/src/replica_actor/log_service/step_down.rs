//! Handing the log to another broker: refuse appends, let the ones in
//! flight commit, then leave the view for the next one the target leads.

use super::LogService;
use crate::log::{StepDownError, StepDownRequest};
use crate::replica_actor::PartitionActor;
use ozzy_proto::NodeId;
use ozzy_replication::ReplicationError;
use ozzy_replication::driver::DriverError;
use std::time::Duration;

/// How long accepted records may take to commit before the step-down
/// gives up.
const WAIT: Duration = Duration::from_secs(1);

/// The least time between two step-downs that start a view change.
const INTERVAL: Duration = Duration::from_secs(1);

/// A step-down waiting for accepted records to commit.
#[derive(Debug)]
pub(super) struct Stepping {
    request: StepDownRequest,
    deadline: Duration,
}

impl LogService {
    /// Take `request`, or answer why it cannot run.
    pub(super) fn start_step_down(
        &mut self,
        request: StepDownRequest,
        local: NodeId,
        now: Duration,
    ) {
        let limited = self.stepping.is_some()
            || self
                .stepped_at
                .is_some_and(|at| now < at.saturating_add(INTERVAL));
        let refusal = match self.term {
            None => Some(StepDownError::NotLeader),
            Some(term) if term.scope.view != request.view => Some(StepDownError::View {
                named: request.view,
                current: term.scope.view,
            }),
            Some(_) if request.target == local => Some(StepDownError::Target),
            Some(_) if limited => Some(StepDownError::Limited),
            Some(_) => None,
        };
        if let Some(error) = refusal {
            let _ = request.reply.send(Err(error));
            return;
        }
        self.stepping = Some(Stepping {
            request,
            deadline: now.saturating_add(WAIT),
        });
    }

    /// Answer the waiting step-down, if any, with `error`.
    pub(super) fn refuse_step_down(&mut self, error: StepDownError) {
        if let Some(stepping) = self.stepping.take() {
            let _ = stepping.request.reply.send(Err(error));
        }
    }

    /// Leave the view once nothing accepted is uncommitted. The leader's
    /// heartbeat visits the partition, so the wait ends without new input.
    pub(super) fn poll_step_down(&mut self, actor: &mut PartitionActor, now: Duration) -> bool {
        let Some(stepping) = &self.stepping else {
            return false;
        };
        if self
            .term
            .is_none_or(|term| term.scope.view != stepping.request.view)
        {
            self.refuse_step_down(StepDownError::NotLeader);
            return true;
        }
        let left = if self.appends.is_empty() && self.trim.is_none() {
            actor.step_down(stepping.request.target)
        } else {
            Ok(None)
        };
        let answer = match left {
            Ok(Some(view)) => {
                self.stepped_at = Some(now);
                Ok(view)
            }
            Ok(None) if now < stepping.deadline => return false,
            Ok(None) => Err(StepDownError::Busy),
            Err(DriverError::Replication(ReplicationError::UnknownVoter)) => {
                Err(StepDownError::Target)
            }
            Err(DriverError::Replication(ReplicationError::WrongRole)) => {
                Err(StepDownError::NotLeader)
            }
            Err(_) => Err(StepDownError::Stopped),
        };
        if let Some(stepping) = self.stepping.take() {
            let _ = stepping.request.reply.send(answer);
        }
        true
    }
}
