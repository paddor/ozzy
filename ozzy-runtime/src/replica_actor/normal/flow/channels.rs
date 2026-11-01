//! Opening a backup's receipt channel at prefixes verified in local history
//! or the journal, and releasing what the channel received.

use super::{no_channel, remote_peer};
use crate::replica_actor::{
    ActorError, HistoryReason, PendingIo, Prefix, ReplicaActor, SendClass, require_normal, wire,
};
use crate::replica_journal::ReplicationPositions;
use ozzy_replication::Commit;
use ozzy_replication::flow::{OpenRequest, Report, Transmitter};
use std::time::Duration;

impl ReplicaActor {
    fn known_prefix(&self, wanted: Prefix) -> Result<Option<Prefix>, ActorError> {
        let snapshot = require_normal(&self.driver)?.snapshot();
        for known in [snapshot.applied, snapshot.committed, snapshot.accepted] {
            if known.op == wanted.op {
                return if known == wanted {
                    Ok(Some(known))
                } else {
                    Err(ActorError::history(HistoryReason::ReceiveWindow))
                };
            }
        }
        let Some(first) = self.work.flow.history.front() else {
            return Ok(None);
        };
        let Some(index) = wanted
            .op
            .0
            .checked_sub(first.prefix.op.0)
            .and_then(|index| usize::try_from(index).ok())
        else {
            return Ok(None);
        };
        let Some(known) = self
            .work
            .flow
            .history
            .get(index)
            .map(|operation| operation.prefix)
        else {
            return Ok(None);
        };
        debug_assert_eq!(known.op, wanted.op, "flow history lost contiguity");
        if known == wanted {
            Ok(Some(known))
        } else {
            Err(ActorError::history(HistoryReason::ReceiveWindow))
        }
    }

    /// The base of `report`'s channel, if that channel is open here. It was
    /// verified at open, and retention may have retired it since, so neither a
    /// live receipt nor a later journal lookup reads it again.
    fn established_base(&self, voter: usize, report: Report) -> Result<Option<Prefix>, ActorError> {
        let Some(sender) = self.work.flow.peers[voter]
            .as_ref()
            .and_then(Transmitter::sender)
            .filter(|sender| sender.channel() == report.channel)
        else {
            return Ok(None);
        };
        let base = sender.report().base;
        if base != report.base {
            return Err(ActorError::history(HistoryReason::ReceiveWindow));
        }
        Ok(Some(base))
    }

    pub(super) fn try_open_flow(
        &mut self,
        voter: usize,
        request: OpenRequest,
        now: Duration,
    ) -> Result<bool, ActorError> {
        let report = request.report();
        let base = match self.established_base(voter, report)? {
            Some(base) => Some(base),
            None => self.known_prefix(report.base)?,
        };
        let (Some(base), Some(received)) = (base, self.known_prefix(report.received)?) else {
            return Ok(false);
        };
        self.adopt_flow(voter, request, [base, received], now)
    }

    /// Open the requested channel from its `[base, received]` prefixes, as
    /// found in local history.
    fn adopt_flow(
        &mut self,
        voter: usize,
        request: OpenRequest,
        [base, received]: [Prefix; 2],
        now: Duration,
    ) -> Result<bool, ActorError> {
        let same_channel = self.work.flow.peers[voter]
            .as_ref()
            .and_then(Transmitter::sender)
            .is_some_and(|sender| sender.channel() == request.report().channel);
        let opened = remote_peer(&mut self.work.flow.peers, voter)?
            .open_verified(request, base, received, now)?;
        if opened {
            // Old-incarnation packets keep their old epochs.
            if !same_channel {
                self.outbox
                    .discard(self.configuration.voters()[voter], SendClass::Data);
            }
            self.release_received_prefix(voter)?;
        }
        Ok(opened)
    }

    pub(super) fn release_received_prefix(&mut self, voter: usize) -> Result<(), ActorError> {
        let received = remote_peer(&mut self.work.flow.peers, voter)?
            .sender()
            .ok_or_else(no_channel)?
            .received();
        let snapshot = require_normal(&self.driver)?.snapshot();
        // The primary's independent quorum commit, not receipt, authorizes this.
        let committed = if received.op <= snapshot.committed.op {
            received
        } else {
            snapshot.committed
        };
        self.announce(
            voter,
            Commit {
                scope: snapshot.scope,
                committed,
            },
            false,
        )
    }

    /// True reserves this journal turn, including a barrier drain for a real lookup.
    pub(in crate::replica_actor::normal) fn schedule_flow_verification(
        &mut self,
    ) -> Result<bool, ActorError> {
        if self.work.needs_sync {
            return Ok(false);
        }
        for voter in 0..3 {
            let Some(request) = self.work.flow.peers[voter]
                .as_ref()
                .and_then(Transmitter::candidate)
            else {
                continue;
            };
            let report = request.report();
            let snapshot = require_normal(&self.driver)?.snapshot();
            if report.received.op > snapshot.accepted.op {
                continue;
            }
            if self.pending_sync.is_some() || !self.pending_persistence.is_empty() {
                return Ok(true); // Reserve quiescence only for an eligible lookup.
            }
            let oldest = if self.established_base(voter, report)?.is_some() {
                report.received.op
            } else {
                report.base.op
            };
            self.pending = Some(PendingIo::FlowPositions(
                self.journal.replication_positions(
                    self.driver.begin_validation()?,
                    [oldest, report.received.op],
                )?,
                voter,
                request,
            ));
            return Ok(true);
        }
        Ok(false)
    }

    pub(in crate::replica_actor) fn complete_flow_verification(
        &mut self,
        positions: &ReplicationPositions,
        voter: usize,
        request: OpenRequest,
        now: Duration,
    ) -> Result<(), ActorError> {
        if !self.application_ready() {
            return Ok(());
        }
        let snapshot = require_normal(&self.driver)?.snapshot();
        if snapshot.scope != positions.ticket.scope()
            || snapshot.journal.generation != positions.ticket.generation()
            || self.work.flow.peers[voter]
                .as_ref()
                .and_then(Transmitter::candidate)
                != Some(request)
        {
            return Ok(());
        }
        let [base, received] = positions.positions;
        let base = self.established_base(voter, request.report())?.or(base);
        if let (Some(base), Some(received)) = (base, received) {
            self.adopt_flow(voter, request, [base, received], now)?;
        } else if positions.requested[0] < positions.retained_predecessor.op {
            self.notify_retired(
                self.configuration.voters()[voter],
                snapshot.scope,
                wire::HistoryFence::Receive(request.report().channel.epoch),
                positions.retained_predecessor,
            )?;
        } else {
            return Err(ActorError::history(HistoryReason::ReceiveWindow));
        }
        // Proposals and repair wait behind a pending lookup, and repair is what
        // moves a lagging peer's report. Repeating this one would hold both.
        remote_peer(&mut self.work.flow.peers, voter)?.forget_candidate(request);
        Ok(())
    }
}
