//! Receipt reports: a backup publishes its receive ledger; the primary
//! observes full and compact reports.

use ozzy_replication::flow::{Report, StatusOutcome, Transmitter};
use ozzy_replication::wire::FlowState;

use super::{
    ActorError, Duration, Message, ReceiptBinding, ReplicaActor, SendClass, control_message,
    no_channel, remote_peer, wire,
};

impl ReplicaActor {
    /// Observe a backup's receipt report on the primary.
    pub(super) fn observe_flow_state(
        &mut self,
        voter: usize,
        state: FlowState,
        now: Duration,
    ) -> Result<(), ActorError> {
        let result = remote_peer(&mut self.work.flow.peers, voter)?.observe_with_repair_limit(
            state.report,
            state.request_id,
            state.repair_limit,
            now,
        )?;
        let established = self.work.flow.peers[voter]
            .as_ref()
            .and_then(Transmitter::sender)
            .is_some_and(|sender| sender.report() == state.report);
        if (result != StatusOutcome::Ignored || established)
            && self.work.flow.handles[voter].is_none_or(|(channel, handle)| {
                channel != state.report.channel || handle <= state.handle
            })
        {
            self.work.flow.handles[voter] = Some((state.report.channel, state.handle));
        }
        if result == StatusOutcome::Verify {
            let request = remote_peer(&mut self.work.flow.peers, voter)?
                .candidate()
                .ok_or_else(no_channel)?;
            self.try_open_flow(voter, request, now)?;
        } else if result == StatusOutcome::Observed {
            self.release_received_prefix(voter)?;
        }
        Ok(())
    }

    pub(in crate::replica_actor) fn publish_receipt(&mut self) -> Result<(), ActorError> {
        if !self.application_ready() {
            return Ok(());
        }
        let report = self.work.receive.ledger.report();
        if self.work.flow.published == Some(report) || report.channel.scope != self.driver.scope() {
            return Ok(());
        }
        let primary = self.configuration.primary(report.channel.scope.view);
        if primary == self.local {
            return Ok(());
        }
        let voter = self
            .configuration
            .voters()
            .iter()
            .position(|peer| *peer == primary)
            .ok_or_else(no_channel)?;
        self.send_flow_state(voter, report, None, None)?;
        self.work.flow.published = Some(report);
        Ok(())
    }

    pub(super) fn send_flow_state(
        &mut self,
        voter: usize,
        report: Report,
        request_id: Option<ozzy_proto::RequestId>,
        repair_limit: Option<ozzy_replication::OpNumber>,
    ) -> Result<(), ActorError> {
        let to = self.configuration.voters()[voter];
        let Some(session) = self.session(to) else {
            return Ok(());
        };
        let channel = report.channel;
        let bound = match self.work.flow.local_handle {
            Some(bound) if bound.channel == channel && bound.session == session => bound,
            _ => {
                let bound = ReceiptBinding {
                    voter,
                    channel,
                    session,
                    handle: self.ids.compact_handle()?,
                    established: false,
                };
                self.work.flow.local_handle = Some(bound);
                bound
            }
        };
        let (handle, confirmed) = (bound.handle, bound.established);
        let state = FlowState {
            handle,
            report,
            request_id,
            repair_limit,
        };
        if state.request_id.is_none() && confirmed {
            let compact = wire::CompactState::from_report(handle, state.report)?.encode()?;
            crate::profiling::event(crate::profiling::Event::CompactReceipt);
            return self.enqueue(to, SendClass::Receipt, Message::from_slice(&compact));
        }
        crate::profiling::event(crate::profiling::Event::FullReceipt);
        let encoded = wire::encode_flow_state(
            self.local,
            session,
            state,
            &mut self.metadata,
            self.wire_limits,
        )?;
        let message = control_message(encoded, &self.metadata);
        self.enqueue(
            to,
            if state.request_id.is_some() {
                SendClass::Exchange
            } else {
                SendClass::Receipt
            },
            message,
        )
    }

    pub(in crate::replica_actor) fn receive_compact(
        &mut self,
        voter: usize,
        compact: wire::CompactState,
        now: Duration,
    ) -> Result<(), ActorError> {
        if !self.application_ready()
            || self.configuration.primary(self.driver.scope().view) != self.local
        {
            return Ok(());
        }
        let Some((channel, handle)) = self.work.flow.handles[voter] else {
            return Ok(());
        };
        let Some(sender) = self.work.flow.peers[voter]
            .as_ref()
            .and_then(Transmitter::sender)
            .filter(|sender| sender.channel() == channel && compact.handle == handle)
        else {
            return Ok(());
        };
        let bound = sender.report();
        let received = if bound.received.op == compact.received {
            Some(bound.received)
        } else {
            sender
                .outstanding()
                .find(|op| op.prefix.op == compact.received)
                .map(|op| op.prefix)
        };
        let Some(received) = received else {
            return Ok(());
        };
        let report = compact.report(bound, received)?;
        if remote_peer(&mut self.work.flow.peers, voter)?.observe(report, None, now)?
            == StatusOutcome::Observed
        {
            self.release_received_prefix(voter)?;
        }
        Ok(())
    }
}
