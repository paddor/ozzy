//! Partition scheduling without owning a PEER or PUB socket.

use super::{ActorError, ActorStatus, ReplicaActor};
use crate::replica_journal::{JournalExecution, ShardJournal};
use crate::replica_transport::FlushProgress;
use crate::replica_transport::SendClass;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{LinkSessionId, NodeId};
use ozzy_replication::wire::PeerBinding;
use std::time::Duration;

/// One partition on a shared application shard. A broker scheduler supplies
/// decoded transport messages, monotonic timer observations and bounded sends.
/// Each partition waits for storage independently. No thread or task is created.
#[derive(Debug)]
pub struct ScheduledReplica<E = ShardJournal> {
    actor: Box<ReplicaActor<E>>,
    now: Duration,
    active: bool,
}

impl<E: JournalExecution> ScheduledReplica<E> {
    pub(super) fn read_access(
        &mut self,
    ) -> (
        Option<ozzy_replication::driver::ValidationTicket>,
        &mut crate::replica_journal::ReplicaJournal<E>,
    ) {
        let ticket = (self.active
            && self.actor.application_ready()
            && self
                .actor
                .configuration
                .primary(self.actor.driver.scope().view)
                == self.actor.local)
            .then(|| self.actor.driver.begin_validation().ok())
            .flatten();
        (ticket, &mut self.actor.journal)
    }

    /// Take a startup submission lane before shared scheduling begins.
    pub fn take_submitter(&mut self) -> Option<super::ProposalSubmitter> {
        self.actor.take_submitter()
    }

    /// Lease a fixed empty native request arena from the existing journal pool.
    pub fn lease_proposal_buffer_with_limits(
        &self,
        limits: ozzy_replication::PipelineLimits,
    ) -> Result<crate::replica_journal::ProposalBuffer, ActorError> {
        self.actor.lease_proposal_buffer_with_limits(limits)
    }

    /// Bind follower staging to allowance supplied by the shared shard owner.
    /// Call before serving; every replacement receive arena keeps this source.
    pub fn bind_receive_capacity(
        &mut self,
        capacity: &crate::memory::Capacity,
    ) -> Result<(), ActorError> {
        self.actor.work.bind_receive_capacity(capacity)?;
        self.actor.history_receive_allocator = Some(capacity.allocator());
        Ok(())
    }

    /// Pending received proposals still need the canonical destination allowance.
    pub fn receive_has_work(&self) -> bool {
        self.actor.work.receive_has_work()
            || matches!(self.actor.pending, Some(super::PendingIo::Chunk(_)))
    }

    /// One requested history reply, independent of normal follower readiness.
    /// The shard must back its exact donor link before admitting its payload.
    /// Request correlation, source identity, and installation stay actor-owned.
    pub fn ops_receive_demand(&self) -> Option<ozzy_replication::wire::FetchOps> {
        let request = self.actor.transfer?.request;
        (self.active
            && request.source.voter != self.actor.local
            && request.scope == self.actor.driver.scope()
            && self.actor.session(request.source.voter).is_some()
            && self.actor.pending.is_none()
            && self.actor.buffer.is_some())
        .then_some(request)
    }

    /// Transfer an actor without per-partition socket services.
    pub fn new(actor: ReplicaActor<E>) -> Result<Self, ScheduleError> {
        Ok(Self {
            actor: Box::new(actor),
            now: Duration::ZERO,
            active: true,
        })
    }

    /// One bounded scheduling turn, including timers and journal admission.
    pub fn advance(&mut self, now: Duration) -> Result<(), ScheduleError> {
        self.observe(now)?;
        let result = self.actor.advance(now);
        self.finish(result)
    }

    /// Input has the authenticated adapter's sender routing frame, followed by
    /// the three Ozzy frames. Routing identity alone is not authentication.
    pub fn receive(&mut self, message: &Message, now: Duration) -> Result<(), ScheduleError> {
        self.observe(now)?;
        let result = self.actor.receive(message, now);
        self.finish(result)
    }

    /// Replace one independently established broker link. The adapter supplies a
    /// fresh session and fences its own old grants and queued replies first.
    /// An obsolete replacement cannot overwrite a newer binding. Duplicates are
    /// harmless. This changes no election, journal, receive-credit, or proposal
    /// authority. Already admitted file work continues under its existing tickets.
    pub fn replace_session(
        &mut self,
        peer: NodeId,
        expected: LinkSessionId,
        replacement: LinkSessionId,
        now: Duration,
    ) -> Result<bool, ScheduleError> {
        let voter = self
            .actor
            .configuration
            .voters()
            .iter()
            .position(|node| *node == peer && peer != self.actor.local)
            .ok_or(ScheduleError::Binding)?;
        if replacement.as_bytes() == &[0; 16] {
            return Err(ScheduleError::Binding);
        }
        self.observe(now)?;
        let current = self.actor.config.sessions[voter];
        if current == replacement {
            return Ok(false);
        }
        if current != expected {
            return Err(ScheduleError::Binding);
        }
        let binding = PeerBinding::new(self.actor.configuration, peer, replacement)
            .map_err(|_| ScheduleError::Binding)?
            .with_receive_epoch(self.actor.work.receive_epoch());
        let result = self.actor.work.flow.replace_session(voter, now);
        self.finish(result)?;
        self.actor.config.sessions[voter] = replacement;
        self.actor.bindings[voter] = Some(binding);
        self.discard_link(voter, now);
        Ok(true)
    }

    /// Fence a disconnected broker before accepting more input from it. This
    /// leaves membership, journal work, accepted proposals, and spent receive
    /// credit intact. Reconnect uses `replace_session` with a zero expected ID.
    /// A delayed disconnect cannot fence a newer independently negotiated link.
    pub fn disconnect_session(
        &mut self,
        peer: NodeId,
        expected: LinkSessionId,
        now: Duration,
    ) -> Result<bool, ScheduleError> {
        let voter = self
            .actor
            .configuration
            .voters()
            .iter()
            .position(|node| *node == peer && peer != self.actor.local)
            .ok_or(ScheduleError::Binding)?;
        self.observe(now)?;
        let current = self.actor.config.sessions[voter];
        if current.as_bytes() == &[0; 16] {
            return Ok(false);
        }
        if current != expected {
            return Err(ScheduleError::Binding);
        }
        let result = self.actor.work.flow.replace_session(voter, now);
        self.finish(result)?;
        self.actor.config.sessions[voter] = LinkSessionId::from_bytes([0; 16]);
        self.actor.bindings[voter] = None;
        self.discard_link(voter, now);
        Ok(true)
    }

    fn discard_link(&mut self, voter: usize, now: Duration) {
        let peer = self.actor.configuration.voters()[voter];
        for class in [
            SendClass::Control,
            SendClass::Data,
            SendClass::Receipt,
            SendClass::Exchange,
        ] {
            self.actor.outbox.discard(peer, class);
        }
        self.actor.ack_at = now;
        self.actor.ready_work.mark();
    }

    /// Attempt bounded outbox progress. A full destination must return the exact
    /// message. The broker owns destination readiness and retries after capacity
    /// returns; transport submission is never a replication confirmation.
    pub fn flush(
        &mut self,
        try_send: impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Result<FlushProgress, ScheduleError> {
        self.observe(self.now)?;
        // Publish the latest counters once, after the bounded input/journal
        // turn. Retaining several packets never queues intermediate reports.
        let result = self.actor.publish_receipt().and_then(|()| {
            self.actor
                .outbox
                .flush_with(try_send)
                .map_err(ActorError::Transport)
        });
        self.finish(result)
    }

    /// Wait for one journal observation or actionable local work. The broker
    /// polls this independently for each actor, alongside messages and timers.
    /// Canceling the wait retains pending journal commands inside the actor.
    /// Call `advance` again after wakeup. Time is the supplied observation, never
    /// sampled from a backend or reconstructed from physical completion time.
    pub async fn changed(&mut self, now: Duration) -> Result<(), ScheduleError> {
        self.observe(now)?;
        let result = self.actor.observe_journal(now, true).await;
        self.finish(result).map(|_| ())
    }

    /// Coalesced observation only, never a substitute for partition authority.
    pub fn status(&self) -> ActorStatus {
        *self.actor.status.borrow()
    }

    /// Current per-partition route, including proposed leader changes. This is
    /// a routing hint. Only actor validation and activation permit admission.
    pub fn authority_hint(&self) -> ozzy_proto::nack::AuthorityHint {
        super::response::authority_hint(self.actor.configuration, self.actor.driver.scope())
    }

    pub(super) fn route_state(
        &self,
        partition: ozzy_proto::PartitionIncarnation,
    ) -> ozzy_proto::directory::RouteState {
        let hint = self.authority_hint();
        ozzy_proto::directory::RouteState {
            group: hint.authority.group_id,
            config_epoch: hint.authority.config_epoch,
            partition,
            members: (*self.actor.configuration.voters()).into(),
            view: hint.authority.view,
            leader: self.status().application_ready.then_some(hint.primary),
        }
    }

    pub(super) fn observe_route(&self, route: &mut ozzy_proto::directory::RouteState) -> bool {
        let hint = self.authority_hint();
        if route.group != hint.authority.group_id
            || route.config_epoch != hint.authority.config_epoch
            || route.members.as_ref() != self.actor.configuration.voters()
        {
            return false;
        }
        route.view = hint.authority.view;
        route.leader = self.status().application_ready.then_some(hint.primary);
        true
    }

    pub(super) fn native_identity(&self) -> (NodeId, ozzy_proto::append::Policy) {
        (self.actor.local, self.actor.configuration.append_policy())
    }

    /// Current follower receive window and its leader. This is an observation,
    /// never permission to consume shard memory. Absent while authority is not
    /// ready, on leaders, or before receive state matches the installed scope.
    pub fn receive_credit(&self) -> Option<(NodeId, ozzy_replication::flow::Report)> {
        let report = self.actor.work.receive_report();
        let leader = self
            .actor
            .configuration
            .primary(self.actor.driver.scope().view);
        (self.active
            && self.actor.application_ready()
            && leader != self.actor.local
            && report.channel.scope == self.actor.driver.scope()
            && self.actor.session(leader).is_some())
        .then_some((leader, report))
    }

    /// Free partition capacity after unused grants and retained operations.
    /// This observation still needs backing from the shard and dispatcher.
    pub fn receive_capacity(&self) -> Option<ozzy_replication::PipelineLimits> {
        self.receive_credit()?;
        Some(self.actor.work.receive_capacity())
    }

    /// Exact follower epoch independent of link availability or temporary
    /// intake fencing. Used to revoke unused reservations during disconnect.
    pub fn receive_channel(&self) -> Option<ozzy_replication::flow::Channel> {
        let channel = self.actor.work.receive_report().channel;
        (self.active
            && self.actor.work.reserved_receive_credit()
            && self.actor.driver.scope() == channel.scope
            && self.actor.configuration.primary(channel.scope.view) != self.actor.local)
            .then_some(channel)
    }

    /// Outstanding tail advertised by the current, independently bound leader.
    /// A shard may use this hint to schedule capacity for active partitions.
    /// It proves no history or confirmation and grants no credit. Reconnect and
    /// scope changes clear it; receiving through the hinted tail consumes it.
    pub fn receive_target(&self) -> Option<ozzy_replication::OpNumber> {
        let (_, report) = self.receive_credit()?;
        self.actor
            .work
            .flow
            .receive_target()
            .filter(|&tail| tail > report.received.op)
    }

    /// Conservative next-packet body demand from the currently bound leader.
    pub fn receive_body_bytes(&self) -> Option<usize> {
        self.receive_target()?;
        self.actor.work.flow.receive_body_bytes()
    }

    /// Advertise capacity only after the shard backs it with destination and
    /// dispatch reservations. This exact epoch must still be current. Failing
    /// admission changes no receive counters and does not stop the actor.
    /// Actual retained-buffer release, not receipt or confirmation, permits
    /// the shard to reuse its byte reservations.
    pub fn grant_receive(
        &mut self,
        channel: ozzy_replication::flow::Channel,
        operations: u64,
        bytes: u64,
    ) -> Result<(), ozzy_replication::flow::FlowError> {
        use ozzy_replication::flow::FlowError;
        let (_, current) = self.receive_credit().ok_or(FlowError::Channel)?;
        if current.channel != channel {
            return Err(FlowError::Channel);
        }
        self.actor.work.grant_receive(channel, operations, bytes)?;
        self.actor.ready_work.mark();
        Ok(())
    }

    /// Fence unused follower credit with a fresh injected receive epoch. The
    /// shard must revoke the old dispatcher token before reusing its unused
    /// reservation. Queued, validating, and accepted bodies remain charged and
    /// continue normally; revocation is neither cancellation nor confirmation.
    /// Works after link loss too. Membership, view, and link session stay fixed.
    pub fn revoke_receive(
        &mut self,
        channel: ozzy_replication::flow::Channel,
    ) -> Result<ozzy_replication::flow::Report, ScheduleError> {
        use ozzy_replication::flow::FlowError;
        self.observe(self.now)?;
        if !self.actor.work.reserved_receive_credit() {
            return Err(FlowError::Invalid.into());
        }
        if self.receive_channel() != Some(channel) {
            return Err(FlowError::Channel.into());
        }
        let result = self.actor.revoke_unused_receive();
        self.finish(result)
    }

    /// Close intake and drain admitted journal work without adding confirmation.
    pub async fn shutdown(mut self) -> Result<(), ActorError> {
        self.actor.shutdown().await
    }

    fn observe(&mut self, now: Duration) -> Result<(), ScheduleError> {
        if !self.active {
            return Err(ScheduleError::Stopped);
        }
        if now < self.now {
            return Err(ScheduleError::TimeReversed);
        }
        self.now = now;
        Ok(())
    }

    fn finish<T>(&mut self, result: Result<T, ActorError>) -> Result<T, ScheduleError> {
        if result.is_err() {
            self.active = false;
            self.actor.ingress.close();
        }
        self.actor.publish_status(self.active);
        result.map_err(ScheduleError::Actor)
    }
}

/// Rejected external schedule or terminal partition failure.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleError {
    /// Stale receive epoch or unsupported credit mode; the actor stays active.
    #[error(transparent)]
    Credit(#[from] ozzy_replication::flow::FlowError),
    /// Unknown/local peer, zero session, or replacement of an obsolete binding.
    #[error("invalid or obsolete partition link binding")]
    Binding,
    /// This interface cannot drive legacy socket-owning services.
    /// No event was delivered because its time precedes an earlier observation.
    #[error("partition scheduling time moved backward")]
    TimeReversed,
    /// A previous actor error closed intake and further event delivery.
    #[error("partition actor is stopped")]
    Stopped,
    /// Partition authority, storage or transport rejected the transition.
    #[error(transparent)]
    Actor(#[from] ActorError),
}
