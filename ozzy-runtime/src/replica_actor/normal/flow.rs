//! Receipt and repair transport policy; durable votes remain in the replication driver.

use crate::replica_actor::HistoryReason;
use crate::replica_actor::require_normal;
use crate::replica_transport::EnqueueError;
use std::collections::VecDeque;

use ozzy_replication::flow::{Report, TransmitError, Transmitter};
use ozzy_replication::wire::FlowMessage;
use ozzy_replication::{Configuration, PipelineLimits};

mod channels;
mod packets;
mod receipt;

use super::{
    ActorError, Duration, JournalGeneration, Message, NodeId, Prefix, ReplicaActor, Scope,
    SendClass,
};
use crate::replica_actor::{ActorConfig, control_message, wire};

#[derive(Debug, Clone, Copy)]
struct ReceiptBinding {
    voter: usize,
    channel: ozzy_replication::flow::Channel,
    session: ozzy_proto::LinkSessionId,
    handle: u32,
    established: bool,
}

#[derive(Debug)]
pub(in crate::replica_actor) struct Flow {
    key: Option<(Scope, JournalGeneration)>,
    pub(in crate::replica_actor) publication_enabled: bool,
    pub(in crate::replica_actor) publication: Option<Message>,
    pub(in crate::replica_actor) peers: [Option<Transmitter>; 3],
    history: VecDeque<ozzy_replication::flow::Operation>,
    history_limit: usize,
    pub(super) scratch: Vec<ozzy_replication::flow::Operation>,
    published: Option<Report>,
    handles: [Option<(ozzy_replication::flow::Channel, u32)>; 3],
    local_handle: Option<ReceiptBinding>,
    receive_progress: ozzy_core::live::LiveProgress,
    missing: Option<ozzy_replication::OpNumber>,
    pub(in crate::replica_actor) pending_publication: Option<ozzy_replication::OpNumber>,
}

/// One transmitter per remote voter; the local voter has none.
pub(in crate::replica_actor) fn remote_transmitters(
    configuration: Configuration,
    local: NodeId,
    scope: Scope,
    config: &ActorConfig,
    ids: &mut crate::replica_actor::ActorIds,
) -> Result<[Option<Transmitter>; 3], ActorError> {
    let mut peers = [None, None, None];
    for (index, peer) in configuration.voters().iter().enumerate() {
        if *peer != local {
            peers[index] = Some(Transmitter::new(
                scope,
                config.pipeline,
                ids.request()?,
                config.flow_probe,
                Duration::ZERO,
            )?);
        }
    }
    Ok(peers)
}

impl Flow {
    pub(in crate::replica_actor) fn new(
        peers: [Option<Transmitter>; 3],
        key: Option<(Scope, JournalGeneration)>,
        config: &ActorConfig,
    ) -> Result<Self, ActorError> {
        let history_limit = config
            .pipeline
            .max_operations
            .checked_add(config.replay_cache.max_operations)
            .ok_or(ActorError::Limits)?;
        let mut receive_progress = ozzy_core::live::LiveProgress::new(config.flow_probe.initial);
        receive_progress.advanced(0, Duration::ZERO);
        Ok(Self {
            key,
            publication_enabled: false,
            publication: None,
            peers,
            history: VecDeque::with_capacity(history_limit),
            history_limit,
            scratch: Vec::with_capacity(config.pipeline.max_operations),
            published: None,
            handles: [None; 3],
            local_handle: None,
            receive_progress,
            missing: None,
            pending_publication: None,
        })
    }

    pub(super) fn change_scope(&mut self, scope: Scope, now: Duration) -> Result<(), ActorError> {
        self.key = None;
        self.publication = None;
        self.history.clear();
        self.published = None;
        self.handles = [None; 3];
        self.local_handle = None;
        self.reset_receipt(ozzy_replication::OpNumber(0), now);
        for peer in self.peers.iter_mut().flatten() {
            peer.change_scope(scope, now)?;
        }
        Ok(())
    }

    pub(in crate::replica_actor) fn replace_session(
        &mut self,
        voter: usize,
        now: Duration,
    ) -> Result<(), ActorError> {
        remote_peer(&mut self.peers, voter)?.replace_session(now)?;
        self.published = None;
        self.handles[voter] = None;
        // Dispatcher fencing can discard the held frame on a link change.
        self.pending_publication = None;
        if self
            .local_handle
            .as_ref()
            .is_some_and(|bound| bound.voter == voter)
        {
            self.local_handle = None;
        }
        Ok(())
    }

    pub(in crate::replica_actor) fn clear_compact_receipt(&mut self) {
        if let Some(bound) = &mut self.local_handle {
            bound.established = false;
        }
    }

    pub(in crate::replica_actor) fn observe_receipt(
        &mut self,
        received: ozzy_replication::OpNumber,
        missing: Option<ozzy_replication::OpNumber>,
        publication: bool,
        now: Duration,
    ) {
        if publication {
            self.receive_progress.advanced(received.0, now);
        }
        if self.missing.is_some_and(|end| end <= received) {
            self.missing = None;
        }
        if self.pending_publication.is_some_and(|end| end <= received) {
            self.pending_publication = None;
        }
        if let Some(end) = missing.filter(|end| *end > received) {
            self.missing = Some(self.missing.map_or(end, |previous| previous.max(end)));
        }
    }

    fn reset_receipt(&mut self, received: ozzy_replication::OpNumber, now: Duration) {
        self.receive_progress.reset();
        self.receive_progress.advanced(received.0, now);
        self.missing = None;
        self.pending_publication = None;
    }

    pub(super) fn remember(&mut self, operation: ozzy_replication::flow::Operation) {
        debug_assert!(
            self.history.back().is_none_or(
                |previous| previous.prefix.op.0.checked_add(1) == Some(operation.prefix.op.0)
            ),
            "flow history must remain contiguous"
        );
        if self.history.len() == self.history_limit {
            self.history.pop_front();
        }
        self.history.push_back(operation);
    }

    /// The last operation a probe answer lets the primary repair. `None`,
    /// always so without publication, sets no limit.
    fn repair_limit(&self, received: u64, now: Duration) -> Option<u64> {
        if !self.publication_enabled {
            return None;
        }
        if self.pending_publication.is_some() {
            // The shard already owns the next contiguous frame. Repair cannot
            // make local room for it.
            return Some(received);
        }
        self.receive_progress
            .repair_limit(received, self.missing.map(|end| end.0), now)
    }

    /// Stage the remembered operations after `cursor` through `end` in
    /// `scratch`. A gap in that history misses; a fresh send also needs its
    /// receipt room in `available`.
    fn stage_operations(
        &mut self,
        cursor: Prefix,
        end: Prefix,
        available: Option<PipelineLimits>,
    ) -> Result<(), PacketSend> {
        self.scratch.clear();
        self.scratch.extend(
            self.history
                .iter()
                .filter(|op| op.prefix.op > cursor.op && op.prefix.op <= end.op)
                .copied(),
        );
        if self.scratch.len() as u64 != end.op.0 - cursor.op.0 {
            return Err(PacketSend::Miss);
        }
        if let Some(available) = available
            && (self.scratch.len() > available.max_operations
                || self.scratch.iter().map(|op| op.body_bytes).sum::<u64>()
                    > available.max_body_bytes as u64)
        {
            return Err(PacketSend::Wait);
        }
        Ok(())
    }
}

pub(super) fn operation(operation: wire::Operation<'_>) -> ozzy_replication::flow::Operation {
    ozzy_replication::flow::Operation {
        prefix: operation.prefix(),
        previous_digest: operation.canonical().previous_digest,
        body_bytes: operation.canonical().body.len() as u64,
    }
}

#[derive(Debug)]
pub(super) enum PacketSend {
    Sent,
    Wait,
    Miss,
}

impl ReplicaActor {
    pub(in crate::replica_actor) fn receive_flow(
        &mut self,
        voter: usize,
        message: FlowMessage<'_>,
        now: Duration,
    ) -> Result<(), ActorError> {
        let from = self.configuration.voters()[voter];
        if !self.application_ready() {
            return Ok(());
        }
        let primary = self.configuration.primary(self.driver.scope().view);
        match message {
            FlowMessage::Probe(probe) if from == primary && probe.scope == self.driver.scope() => {
                let report = self.work.receive.ledger.report();
                if report.channel.scope == probe.scope {
                    let limit = self
                        .work
                        .flow
                        .repair_limit(report.received.op.0, now)
                        .map(|limit| ozzy_replication::OpNumber(limit.min(probe.tail.op.0)));
                    self.send_flow_state(voter, report, Some(probe.request_id), limit)?;
                }
            }
            FlowMessage::State(state) if primary == self.local => {
                self.observe_flow_state(voter, state, now)?;
            }
            FlowMessage::Prepare { batch, .. } if from == primary => {
                self.receive_prepare(from, batch, now)?;
                if let Some(bound) = &mut self.work.flow.local_handle
                    && bound.channel == self.work.receive.ledger.report().channel
                    && bound.voter == voter
                {
                    bound.established = true;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Whether the receipt ledger belongs to this core's scope and journal
    /// generation. Activation leaves the old one until the next `flow_round`.
    pub(super) fn receipt_current(&self, snapshot: &ozzy_replication::ReplicaSnapshot) -> bool {
        self.work.flow.key == Some((snapshot.scope, snapshot.journal.generation))
    }

    pub(super) fn flow_round(&mut self, now: Duration) -> Result<(), ActorError> {
        if !self.application_ready() {
            return Ok(());
        }
        let snapshot = require_normal(&self.driver)?.snapshot();
        let key = (snapshot.scope, snapshot.journal.generation);
        if self.work.flow.key != Some(key) {
            // A suffix staged in the old ledger would be admitted outside the new one.
            if snapshot.pending_operations != 0 || self.work.receive_has_work() {
                return Err(ActorError::history(HistoryReason::ReceiveWindow));
            }
            self.work
                .receive
                .ledger
                .reinitialize(self.ids.channel(snapshot.scope)?, snapshot.applied)
                .map_err(TransmitError::from)?;
            self.work.flow.key = Some(key);
            self.restart_receipt(now);
        }
        if self.configuration.primary(snapshot.scope.view) != self.local {
            self.work
                .receive
                .ledger
                .release(require_normal(&self.driver)?.reclaimed_prefix())
                .map_err(TransmitError::from)?;
            return Ok(());
        }
        for voter in 0..3 {
            self.send_probe(voter, snapshot.accepted, now)?;
        }
        Ok(())
    }

    /// Probe a backup whose receipt or commit cursor trails `accepted`.
    fn send_probe(
        &mut self,
        voter: usize,
        accepted: Prefix,
        now: Duration,
    ) -> Result<(), ActorError> {
        let to = self.configuration.voters()[voter];
        let Some(session) = self.session(to) else {
            return Ok(());
        };
        let Some(peer) = &mut self.work.flow.peers[voter] else {
            return Ok(());
        };
        if peer
            .sender()
            .is_some_and(|sender| sender.received() == accepted)
            && self.work.cursors[voter].is_some_and(|cursor| cursor.op >= accepted.op)
        {
            return Ok(());
        }
        let queued = self
            .outbox
            .queued(to, SendClass::Data)
            .ok_or(ActorError::Enqueue(EnqueueError::Peer))?
            .0
            > 0;
        let Some(probe) = peer.poll_probe(accepted, queued, now)? else {
            return Ok(());
        };
        let encoded = wire::encode_flow_probe(
            self.local,
            session,
            probe,
            &mut self.metadata,
            self.wire_limits,
        )?;
        let message = control_message(encoded, &self.metadata);
        self.enqueue(to, SendClass::Exchange, message)?;
        crate::profiling::event(crate::profiling::Event::ReplicaProbe);
        Ok(())
    }

    /// Republish receipt from the receive ledger's new channel or retracted
    /// prefix, and bind PEER sessions to its epoch.
    fn restart_receipt(&mut self, now: Duration) {
        self.work.flow.published = None;
        let received = self.work.receive_report().received.op;
        self.work.flow.reset_receipt(received, now);
        self.bind_receive_epoch();
    }

    fn bind_receive_epoch(&mut self) {
        let epoch = self.work.receive_epoch();
        for binding in self.bindings.iter_mut().flatten() {
            *binding = binding.with_receive_epoch(epoch);
        }
    }

    pub(super) fn retract_received(&mut self, now: Duration) -> Result<(), ActorError> {
        if let Some(snapshot) = self
            .driver
            .normal()
            .map(ozzy_replication::NormalReplica::snapshot)
            && self.work.flow.key == Some((snapshot.scope, snapshot.journal.generation))
            && self.work.receive.ledger.report().channel.scope == snapshot.scope
        {
            self.work
                .receive
                .ledger
                .retract(self.ids.channel(snapshot.scope)?.epoch, snapshot.accepted)
                .map_err(TransmitError::from)?;
            self.restart_receipt(now);
        }
        for buffer in self.work.receive.reset().into_iter().flatten() {
            self.recycle(buffer)?;
        }
        Ok(())
    }
}

/// No transmitter or verified channel serves this voter.
pub(super) fn no_channel() -> ActorError {
    TransmitError::from(ozzy_replication::flow::FlowError::Channel).into()
}

/// The transmitter for one remote voter; the local voter has none.
pub(in crate::replica_actor) fn remote_peer(
    peers: &mut [Option<Transmitter>; 3],
    voter: usize,
) -> Result<&mut Transmitter, ActorError> {
    peers
        .get_mut(voter)
        .and_then(Option::as_mut)
        .ok_or_else(no_channel)
}
