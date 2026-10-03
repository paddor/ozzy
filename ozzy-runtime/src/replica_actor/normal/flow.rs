//! Receipt and repair transport policy; durable votes remain in the replication driver.

use std::collections::VecDeque;

use ozzy_proto::Opcode;
use ozzy_replication::Configuration;
use ozzy_replication::flow::{
    OpenRequest, ReceiveEpoch, Report, StatusOutcome, TransmitError, Transmitter,
};
use ozzy_replication::wire::{FlowMessage, FlowState};

use super::{
    ActorError, Duration, JournalGeneration, Message, NodeId, Prefix, ReplicaActor, Scope,
    SendClass,
};
use crate::replica_actor::{ActorConfig, Bytes, PendingIo, wire};
use crate::replica_journal::ReplicationPositions;

#[derive(Debug)]
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

impl Flow {
    pub(in crate::replica_actor) fn new(
        configuration: Configuration,
        local: NodeId,
        scope: Scope,
        key: Option<(Scope, JournalGeneration)>,
        config: &ActorConfig,
        ids: &mut crate::replica_actor::ActorIds,
    ) -> Result<Self, ActorError> {
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
        self.peers[voter]
            .as_mut()
            .expect("remote peer")
            .replace_session(now)?;
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
                        .publication_enabled
                        .then(|| {
                            if self.work.flow.pending_publication.is_some() {
                                // The shard already owns the next contiguous
                                // frame. Repair cannot make local room for it.
                                return Some(report.received.op.0);
                            }
                            self.work.flow.receive_progress.repair_limit(
                                report.received.op.0,
                                self.work.flow.missing.map(|end| end.0),
                                now,
                            )
                        })
                        .flatten()
                        .map(|limit| ozzy_replication::OpNumber(limit.min(probe.tail.op.0)));
                    self.send_flow_state(voter, report, Some(probe.request_id), limit)?;
                }
            }
            FlowMessage::State(state) if primary == self.local => {
                let result = self.work.flow.peers[voter]
                    .as_mut()
                    .expect("remote peer")
                    .observe_with_repair_limit(
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
                    let request = self.work.flow.peers[voter]
                        .as_ref()
                        .and_then(Transmitter::candidate)
                        .expect("history candidate");
                    self.try_open_flow(voter, request, now)?;
                } else if result == StatusOutcome::Observed {
                    self.release_received_prefix(voter)?;
                }
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

    pub(super) fn flow_round(&mut self, now: Duration) -> Result<(), ActorError> {
        if !self.application_ready() {
            return Ok(());
        }
        let snapshot = self.driver.normal().expect("normal flow").snapshot();
        let key = (snapshot.scope, snapshot.journal.generation);
        if self.work.flow.key != Some(key) {
            if snapshot.pending_operations != 0 {
                return Err(ActorError::History);
            }
            self.work
                .receive
                .ledger
                .reinitialize(self.ids.channel(snapshot.scope)?, snapshot.applied)
                .map_err(TransmitError::from)?;
            self.work.flow.key = Some(key);
            self.work.flow.published = None;
            let received = self.work.receive_report().received.op;
            self.work.flow.reset_receipt(received, now);
            self.bind_receive_epoch();
        }
        if self.configuration.primary(snapshot.scope.view) != self.local {
            self.work
                .receive
                .ledger
                .release(
                    self.driver
                        .normal()
                        .expect("normal flow")
                        .reclaimed_prefix(),
                )
                .map_err(TransmitError::from)?;
            return Ok(());
        }
        for voter in 0..3 {
            let to = self.configuration.voters()[voter];
            let Some(session) = self.session(to) else {
                continue;
            };
            let Some(peer) = &mut self.work.flow.peers[voter] else {
                continue;
            };
            if peer
                .sender()
                .is_some_and(|sender| sender.received() == snapshot.accepted)
                && self.work.cursors[voter].is_some_and(|cursor| cursor.op >= snapshot.accepted.op)
            {
                continue;
            }
            let queued = self.outbox.queued(to, SendClass::Data).expect("peer").0 > 0;
            let Some(probe) = peer.poll_probe(snapshot.accepted, queued, now)? else {
                continue;
            };
            let encoded = wire::encode_flow_probe(
                self.local,
                session,
                probe,
                &mut self.metadata,
                self.wire_limits,
            )?;
            let message = Message::multipart([
                Bytes::copy_from_slice(&encoded.header),
                Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
                Bytes::new(),
            ]);
            self.enqueue(to, SendClass::Exchange, message)?;
            crate::profiling::event(crate::profiling::Event::ReplicaProbe);
        }
        Ok(())
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
            self.work.flow.published = None;
            let received = self.work.receive_report().received.op;
            self.work.flow.reset_receipt(received, now);
            self.bind_receive_epoch();
        }
        self.work.receive.reset();
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
            .expect("primary");
        self.send_flow_state(voter, report, None, None)?;
        self.work.flow.published = Some(report);
        Ok(())
    }

    fn send_flow_state(
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
        if self
            .work
            .flow
            .local_handle
            .as_ref()
            .is_none_or(|bound| bound.channel != channel || bound.session != session)
        {
            self.work.flow.local_handle = Some(ReceiptBinding {
                voter,
                channel,
                session,
                handle: crate::replica_actor::ActorIds::compact_handle()?,
                established: false,
            });
        }
        let bound = self.work.flow.local_handle.as_ref().expect("bound channel");
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
        let message = Message::multipart([
            Bytes::copy_from_slice(&encoded.header),
            Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
            Bytes::new(),
        ]);
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
        if self.work.flow.peers[voter]
            .as_mut()
            .expect("bound peer")
            .observe(report, None, now)?
            == StatusOutcome::Observed
        {
            self.release_received_prefix(voter)?;
        }
        Ok(())
    }

    fn known_prefix(&self, wanted: Prefix) -> Result<Option<Prefix>, ActorError> {
        let snapshot = self
            .driver
            .normal()
            .expect("normal flow history")
            .snapshot();
        for known in [
            Prefix::GENESIS,
            snapshot.applied,
            snapshot.committed,
            snapshot.accepted,
        ] {
            if known.op == wanted.op {
                return if known == wanted {
                    Ok(Some(known))
                } else {
                    Err(ActorError::History)
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
            Err(ActorError::History)
        }
    }

    fn try_open_flow(
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
                return Err(ActorError::History);
            }
            Some(base)
        } else {
            self.known_prefix(report.base)?
        };
        let (Some(base), Some(received)) = (base, self.known_prefix(report.received)?) else {
            return Ok(false);
        };
        self.adopt_flow(voter, request, base, received, now)
    }

    fn adopt_flow(
        &mut self,
        voter: usize,
        request: OpenRequest,
        base: Prefix,
        received: Prefix,
        now: Duration,
    ) -> Result<bool, ActorError> {
        let same_channel = self.work.flow.peers[voter]
            .as_ref()
            .and_then(Transmitter::sender)
            .is_some_and(|sender| sender.channel() == request.report().channel);
        let opened = self.work.flow.peers[voter]
            .as_mut()
            .expect("remote peer")
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

    fn release_received_prefix(&mut self, voter: usize) -> Result<(), ActorError> {
        let received = self.work.flow.peers[voter]
            .as_ref()
            .and_then(Transmitter::sender)
            .expect("verified flow")
            .received();
        let snapshot = self.driver.normal().expect("normal primary").snapshot();
        // The primary's independent quorum commit, not receipt, authorizes this.
        let committed = if received.op <= snapshot.committed.op {
            received
        } else {
            snapshot.committed
        };
        self.control(
            self.configuration.voters()[voter],
            super::Control::Commit(super::Commit {
                scope: snapshot.scope,
                committed,
            }),
        )
    }

    /// True reserves this journal turn, including a barrier drain for a real lookup.
    pub(super) fn schedule_flow_verification(&mut self) -> Result<bool, ActorError> {
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
            let snapshot = self.driver.normal().expect("normal flow").snapshot();
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
        let snapshot = self.driver.normal().expect("normal flow").snapshot();
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
            return Err(ActorError::History);
        };
        self.adopt_flow(voter, request, base, received, now)?;
        Ok(())
    }

    pub(super) fn send_live(&mut self) -> Result<(), ActorError> {
        if !self.application_ready() {
            return Ok(());
        }
        if self.work.flow.publication_enabled && self.work.flow.publication.is_none() {
            let snapshot = self.driver.normal().expect("normal publication").snapshot();
            if let Some(live) = self
                .work
                .live
                .iter_mut()
                .find(|live| !live.published && live.scope == snapshot.scope)
            {
                let packet = live
                    .packets
                    .iter()
                    .flatten()
                    .next()
                    .expect("new canonical packet");
                let frames: [&[u8]; 3] =
                    std::array::from_fn(|i| packet.part_slice(i).expect("template"));
                let encoded = wire::encode_publication(
                    self.local,
                    &frames,
                    snapshot.committed,
                    &mut self.metadata,
                    self.wire_limits,
                )?;
                self.work.flow.publication = Some(Message::multipart([
                    Bytes::copy_from_slice(snapshot.scope.group_id.as_bytes()),
                    Bytes::copy_from_slice(&encoded.header),
                    Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
                    packet.part_bytes(2).expect("template body"),
                ]));
                live.published = true;
                self.ready_work.mark();
            }
        }
        for index in 0..self.work.live.len() {
            for voter in 0..3 {
                let live = &mut self.work.live[index];
                if live.scope != self.driver.scope() {
                    continue;
                }
                let Some(packet) = live.packets[voter].take() else {
                    continue;
                };
                let (predecessor, end) = (live.predecessor, live.end);
                let (_, packet) = self.send_cached_packet(voter, packet, predecessor, end)?;
                self.work.live[index].packets[voter] = Some(packet);
            }
        }
        Ok(())
    }

    pub(super) fn send_cached_packet(
        &mut self,
        voter: usize,
        packet: Message,
        predecessor: Prefix,
        end: Prefix,
    ) -> Result<(PacketSend, Message), ActorError> {
        // Return the owned template on every nonterminal path. Polling a live
        // window without local room/new work must not clone its multipart vectors.
        let to = self.configuration.voters()[voter];
        let Some(session) = self.session(to) else {
            return Ok((PacketSend::Wait, packet));
        };
        let Some(peer) = &self.work.flow.peers[voter] else {
            return Ok((PacketSend::Wait, packet));
        };
        let Some(sender) = peer.sender() else {
            return Ok((PacketSend::Wait, packet));
        };
        if self.work.flow.handles[voter].is_none_or(|(channel, _)| channel != sender.channel()) {
            return Ok((PacketSend::Wait, packet));
        }
        let queued = self.outbox.queued(to, SendClass::Data).expect("peer");
        let repair = peer.repair(queued.0 > 0);
        // An existing retry blocked on local transport must not become a fresh send.
        if repair.is_none() && peer.repair(false).is_some() {
            return Ok((PacketSend::Wait, packet));
        }
        if self.work.flow.publication_enabled && repair.is_none() && !peer.needs_catch_up() {
            return Ok((PacketSend::Wait, packet));
        }
        let cursor = repair.map_or(sender.sent(), |repair| repair.after);
        if cursor.op < predecessor.op || cursor.op >= end.op {
            return Ok((PacketSend::Miss, packet));
        }
        if repair.is_some_and(|repair| end.op > repair.through.op) {
            return Ok((PacketSend::Miss, packet));
        }
        if self.work.flow.publication_enabled
            && repair.is_none()
            && peer
                .catch_up_through()
                .is_none_or(|through| end.op > through)
        {
            return Ok((PacketSend::Miss, packet));
        }
        let epoch = sender.channel().epoch;
        self.work.flow.scratch.clear();
        self.work.flow.scratch.extend(
            self.work
                .flow
                .history
                .iter()
                .filter(|op| op.prefix.op > cursor.op && op.prefix.op <= end.op)
                .copied(),
        );
        if self.work.flow.scratch.len() as u64 != end.op.0 - cursor.op.0 {
            return Ok((PacketSend::Miss, packet));
        }
        if repair.is_none() {
            let available = sender.available();
            if self.work.flow.scratch.len() > available.max_operations
                || self
                    .work
                    .flow
                    .scratch
                    .iter()
                    .map(|op| op.body_bytes)
                    .sum::<u64>()
                    > available.max_body_bytes as u64
            {
                return Ok((PacketSend::Wait, packet));
            }
        }
        let bytes = bound_packet_bytes(&packet);
        if queued.0 == self.config.data.messages || bytes > self.config.data.bytes - queued.1 {
            return Ok((PacketSend::Wait, packet));
        }
        let committed = self
            .driver
            .normal()
            .expect("normal data")
            .snapshot()
            .committed;
        let commit = if end.op <= committed.op {
            end
        } else {
            committed
        };
        let packet = self.bind_packet(packet, epoch, commit, session)?;
        let peer = self.work.flow.peers[voter].as_mut().expect("remote peer");
        if let Some(repair) = repair {
            peer.record_repair(repair, end)
        } else {
            peer.record_send(&self.work.flow.scratch)
        }
        .map_err(TransmitError::from)?;
        crate::profiling::count(
            crate::profiling::Event::ReplicaPayloadBytes,
            packet.part_bytes(2).expect("payload").len() as u64,
        );
        self.outbox
            .try_enqueue(to, SendClass::Data, packet.clone())
            .map_err(|(error, _)| ActorError::Enqueue(error))?;
        crate::profiling::event(if repair.is_some() {
            crate::profiling::Event::ReplicaRepair
        } else {
            crate::profiling::Event::ReplicaSend
        });
        Ok((PacketSend::Sent, packet))
    }

    pub(super) fn bind_packet(
        &mut self,
        message: Message,
        epoch: ReceiveEpoch,
        commit: Prefix,
        session: ozzy_proto::LinkSessionId,
    ) -> Result<Message, ActorError> {
        let frames: [Bytes; 3] =
            std::array::from_fn(|index| message.part_bytes(index).expect("owned template"));
        let envelope = if frames[0].is_empty() {
            ozzy_proto::Envelope {
                opcode: Opcode::Prepare,
                response: false,
                request_id: None,
                sender: self.local,
                session: None,
            }
        } else {
            ozzy_proto::decode_packet(
                &frames.each_ref().map(AsRef::as_ref),
                self.wire_limits.envelope,
            )
            .map_err(wire::WireError::from)?
            .envelope
        };
        let old_offset = if envelope.opcode == Opcode::PrepareFlow {
            96
        } else {
            80
        };
        let metadata = &frames[1];
        let mut prefix = [0u8; 40];
        prefix[..8].copy_from_slice(&commit.op.0.to_be_bytes());
        prefix[8..].copy_from_slice(commit.digest.as_bytes());
        if old_offset == 96
            && metadata[80..96] == epoch.get().to_be_bytes()
            && metadata[136..176] == prefix
            && envelope.session == Some(session)
        {
            return Ok(message);
        }
        let length = metadata.len() + 96 - old_offset;
        self.metadata[..80].copy_from_slice(&metadata[..80]);
        self.metadata[80..96].copy_from_slice(&epoch.get().to_be_bytes());
        self.metadata[96..length].copy_from_slice(&metadata[old_offset..]);
        self.metadata[136..176].copy_from_slice(&prefix);
        let header = ozzy_proto::Envelope {
            opcode: Opcode::PrepareFlow,
            session: Some(session),
            ..envelope
        }
        .encode_header(length, frames[2].len(), self.wire_limits.envelope)
        .map_err(wire::WireError::from)?;
        Ok(Message::multipart([
            Bytes::copy_from_slice(&header),
            Bytes::copy_from_slice(&self.metadata[..length]),
            frames[2].clone(),
        ]))
    }
}

fn bound_packet_bytes(packet: &Message) -> usize {
    let header = packet.part_slice(0).expect("template header");
    let added = if header.is_empty() {
        80 // Envelope plus newly bound receive epoch.
    } else if header[5] == Opcode::Prepare as u8 {
        16 // Receive epoch; the template already has an envelope.
    } else {
        0
    };
    16 + packet.byte_len() + added // OMQ destination identity precedes Ozzy frames.
}
