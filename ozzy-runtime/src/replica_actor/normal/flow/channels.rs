//! Opening a backup's receipt channel at prefixes verified in local history
//! or the journal, and releasing what the channel received.

use super::{no_channel, remote_peer};
use crate::replica_actor::{
    ActorError, HistoryReason, PendingIo, Prefix, ReplicaActor, SendClass, require_normal, wire,
};
use crate::replica_journal::ReplicationPositions;
use ozzy_replication::Commit;
use ozzy_replication::flow::{OpenRequest, Transmitter};
use ozzy_replication::wire::Control;
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

    pub(super) fn try_open_flow(
        &mut self,
        voter: usize,
        request: OpenRequest,
        now: Duration,
    ) -> Result<bool, ActorError> {
        let report = request.report();
        // A channel's base was independently verified at open. Do not reread
        // old journal history on every live PUB receipt after cache eviction.
        let established = self.work.flow.peers[voter]
            .as_ref()
            .and_then(Transmitter::sender)
            .filter(|sender| sender.channel() == report.channel);
        let base = if let Some(sender) = established {
            let base = sender.report().base;
            if base != report.base {
                return Err(ActorError::history(HistoryReason::ReceiveWindow));
            }
            Some(base)
        } else {
            self.known_prefix(report.base)?
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
        self.control(
            self.configuration.voters()[voter],
            Control::Commit(Commit {
                scope: snapshot.scope,
                committed,
            }),
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
            self.pending = Some(PendingIo::FlowPositions(
                self.journal.replication_positions(
                    self.driver.begin_validation()?,
                    [report.base.op, report.received.op],
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
        let [Some(base), Some(received)] = positions.positions else {
            if request.report().base.op < positions.retained_predecessor.op {
                return self.notify_retired(
                    self.configuration.voters()[voter],
                    snapshot.scope,
                    wire::HistoryFence::Receive(request.report().channel.epoch),
                    positions.retained_predecessor,
                );
            }
            return Err(ActorError::history(HistoryReason::ReceiveWindow));
        };
        self.adopt_flow(voter, request, [base, received], now)?;
        Ok(())
    }
}
