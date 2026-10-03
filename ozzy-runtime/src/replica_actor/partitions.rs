//! Bounded shared-shard scheduling. Partition authority stays inside each actor.

use futures::{StreamExt, stream::FuturesUnordered, task::AtomicWaker};
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{GroupId, LinkSessionId, NodeId, PartitionIncarnation, directory::RouteState};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use super::shared_readers::{SharedReaderConfig, SharedReaders};
use super::{
    ActorError, ActorStatus, LocalActor, NativeIntake, NativeIntakeError, NativeReceive,
    ScheduleError, ScheduledRecovery, ScheduledReplica,
};
use crate::frontend::Links;

/// Explicit partition mode. A failed group can never become a local actor.
#[derive(Debug)]
pub enum PartitionActor {
    /// Explicit single-broker local durability.
    Local(Box<LocalActor>),
    /// Fixed-three authority, including election after restart.
    Replicated(ScheduledReplica),
    /// Explicit nonvoting replacement. Full publication precedes normal election.
    Recovering(Box<ScheduledRecovery>),
}

/// Coalesced observation, never a substitute for partition authority.
#[derive(Debug, Clone, Copy)]
#[expect(
    clippy::large_enum_variant,
    reason = "bounded copied status does not allocate on each observation"
)]
pub enum PartitionStatus {
    /// Local accepted, durable, and applied positions.
    Local(ozzy_replication::local::Snapshot),
    /// Per-partition replicated role and positions.
    Replicated(ActorStatus),
}

impl PartitionActor {
    pub(super) fn read_access(
        &mut self,
    ) -> Option<(
        Option<ozzy_replication::driver::ValidationTicket>,
        &mut crate::replica_journal::ReplicaJournal<crate::replica_journal::ShardJournal>,
    )> {
        match self {
            Self::Local(actor) => Some(actor.read_access()),
            Self::Replicated(actor) => Some(actor.read_access()),
            Self::Recovering(_) => None,
        }
    }

    /// Lease fixed startup arenas before transferring the partition to its scheduler.
    pub fn lease_proposal_buffer_with_limits(
        &self,
        limits: ozzy_replication::PipelineLimits,
    ) -> Result<crate::replica_journal::ProposalBuffer, ActorError> {
        match self {
            Self::Local(actor) => actor.lease_proposal_buffer_with_limits(limits),
            Self::Replicated(actor) => actor.lease_proposal_buffer_with_limits(limits),
            Self::Recovering(_) => Err(ActorError::StartupMismatch),
        }
    }

    /// Take one remaining fixed submission lane for shard-local services.
    pub fn take_submitter(&mut self) -> Option<super::ProposalSubmitter> {
        match self {
            Self::Local(actor) => actor.take_submitter(),
            Self::Replicated(actor) => actor.take_submitter(),
            Self::Recovering(_) => None,
        }
    }

    /// Followers allocate only against their destination's shard-issued capacity.
    pub fn bind_receive_capacity(
        &mut self,
        capacity: &crate::memory::Capacity,
    ) -> Result<(), ActorError> {
        match self {
            Self::Local(_) => Ok(()),
            Self::Replicated(actor) => actor.bind_receive_capacity(capacity),
            Self::Recovering(actor) => actor.bind_receive_capacity(capacity),
        }
    }
    /// Immutable group identity, independent of current role.
    pub fn group(&self) -> GroupId {
        match self {
            Self::Local(actor) => actor.group(),
            Self::Replicated(actor) => actor.status().scope.group_id,
            Self::Recovering(actor) => actor.status().scope.group_id,
        }
    }

    /// Inspect progress without granting authority.
    pub fn status(&self) -> PartitionStatus {
        match self {
            Self::Local(actor) => PartitionStatus::Local(actor.snapshot()),
            Self::Replicated(actor) => PartitionStatus::Replicated(actor.status()),
            Self::Recovering(actor) => PartitionStatus::Replicated(actor.status()),
        }
    }

    /// Current leader hint. It neither activates authority nor confirms history.
    pub fn authority_hint(&self) -> ozzy_proto::nack::AuthorityHint {
        match self {
            Self::Local(actor) => actor.authority_hint(),
            Self::Replicated(actor) => actor.authority_hint(),
            Self::Recovering(actor) => actor.authority_hint(),
        }
    }

    /// Allocate one initial watch identity from this actor's actual configuration.
    /// The incarnation comes from its checked partition journal placement.
    pub fn route_state(&self, partition: PartitionIncarnation) -> RouteState {
        match self {
            Self::Local(actor) => actor.route_state(partition),
            Self::Replicated(actor) => actor.route_state(partition),
            Self::Recovering(actor) => actor.route_state(partition),
        }
    }

    fn observe_route(&self, route: &mut RouteState) -> bool {
        match self {
            Self::Local(actor) => actor.observe_route(route),
            Self::Replicated(actor) => actor.observe_route(route),
            Self::Recovering(actor) => actor.observe_route(route),
        }
    }

    pub(super) fn native_identity(&self) -> (GroupId, NodeId, ozzy_proto::append::Policy) {
        let (local, policy) = match self {
            Self::Local(actor) => (
                actor.authority_hint().primary,
                ozzy_proto::append::Policy::LocalDurable,
            ),
            Self::Replicated(actor) => actor.native_identity(),
            Self::Recovering(actor) => actor.native_identity(),
        };
        (self.group(), local, policy)
    }

    /// Close intake and drain the existing storage owner asynchronously.
    pub async fn shutdown(self) -> Result<(), ActorError> {
        match self {
            Self::Local(actor) => actor.shutdown().await,
            Self::Replicated(actor) => actor.shutdown().await,
            Self::Recovering(actor) => Box::pin(actor.shutdown()).await,
        }
    }
}

/// One application shard's fixed actor set. No actor or journal threads/tasks
/// are spawned here. All file futures use the actors' shared backend clients.
#[derive(Debug)]
pub struct PartitionActors {
    actors: Vec<PartitionActor>,
    native: Vec<Option<NativeService>>,
    readers: Vec<Option<SharedReaders>>,
    recovered: VecDeque<GroupId>,
    index: BTreeMap<GroupId, usize>,
    turn_actors: usize,
    next: usize,
    remaining: usize,
    now: Duration,
    stopped: bool,
    wakeups: Option<Wakeups>,
    turns: u64,
}

#[derive(Debug)]
struct NativeService {
    intake: NativeIntake,
    links: Links,
}

/// Which partitions have something to observe. Storage results and ready
/// proposals wake their own partition. Input marks its partition. Timers
/// have no wakeup, so every partition is visited once per interval.
#[derive(Debug)]
struct Wakeups {
    shard: Arc<AtomicWaker>,
    woken: Vec<Arc<Woken>>,
    wakers: Vec<Waker>,
    /// The shared transport refused this partition's last send.
    refused: Vec<bool>,
    interval: Duration,
    visited: Option<Duration>,
}

#[derive(Debug)]
struct Woken {
    set: AtomicBool,
    shard: Arc<AtomicWaker>,
}

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.set.store(true, Ordering::Release);
        self.shard.wake();
    }
}

impl PartitionActors {
    /// Actors must already have taken their bounded proposal lanes and leases.
    /// Empty shards are valid. `maximum` bounds metadata; `turn_actors` bounds
    /// CPU turns regardless of partition count. Payload admission stays separate.
    pub fn new(
        actors: Vec<PartitionActor>,
        maximum: usize,
        turn_actors: usize,
    ) -> Result<Self, PartitionError> {
        if maximum == 0 || actors.len() > maximum || turn_actors == 0 || turn_actors > 1024 {
            return Err(PartitionError::Limits);
        }
        let mut index = BTreeMap::new();
        for (slot, actor) in actors.iter().enumerate() {
            if index.insert(actor.group(), slot).is_some() {
                return Err(PartitionError::Duplicate);
            }
        }
        Ok(Self {
            recovered: VecDeque::with_capacity(actors.len()),
            native: actors.iter().map(|_| None).collect(),
            readers: actors.iter().map(|_| None).collect(),
            actors,
            index,
            turn_actors,
            next: 0,
            remaining: 0,
            now: Duration::ZERO,
            stopped: false,
            wakeups: None,
            turns: 0,
        })
    }

    /// Visit only partitions with input, a wakeup, or a refused send, and
    /// every partition once per `interval` for its timers. Without this
    /// every call visits every partition. The interval bounds how late a
    /// timer or a missed wakeup is observed; 1 ms to 1 s.
    pub fn with_timer_interval(mut self, interval: Duration) -> Result<Self, PartitionError> {
        if interval < Duration::from_millis(1) || interval > Duration::from_secs(1) {
            return Err(PartitionError::Limits);
        }
        let shard = Arc::new(AtomicWaker::new());
        let woken: Vec<_> = self
            .actors
            .iter()
            .map(|_| {
                Arc::new(Woken {
                    set: AtomicBool::new(true),
                    shard: shard.clone(),
                })
            })
            .collect();
        self.wakeups = Some(Wakeups {
            wakers: woken.iter().cloned().map(Waker::from).collect(),
            refused: vec![false; woken.len()],
            woken,
            shard,
            interval,
            visited: None,
        });
        Ok(self)
    }

    /// Partition visits since creation. A scheduling diagnostic.
    pub fn turns(&self) -> u64 {
        self.turns
    }

    fn touch(&self, slot: usize) {
        if let Some(wakeups) = &self.wakeups {
            wakeups.woken[slot].set.store(true, Ordering::Release);
        }
    }

    /// Number of configured partition actors.
    pub fn len(&self) -> usize {
        self.actors.len()
    }
    /// Whether this shard has no assigned partitions.
    pub fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }

    /// Observe one newly published recovery handoff. A scheduling turn creates
    /// at most `turn_actors` notices. Drain them and install normal services
    /// before polling again. Group scan order need not match the caller's scan.
    pub fn take_recovered(&mut self) -> Option<GroupId> {
        self.recovered.pop_front()
    }

    /// Configure native services after recovery publication, before the next
    /// normal scheduling turn. No proposal resources exist during recovery.
    /// A partition with installed client or reader services cannot be reopened.
    pub fn startup_actor(
        &mut self,
        group: GroupId,
    ) -> Result<Option<&mut PartitionActor>, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if self.stopped || self.native[slot].is_some() || self.readers[slot].is_some() {
            return Err(PartitionError::NativeConfiguration);
        }
        if matches!(self.actors[slot], PartitionActor::Recovering(_)) {
            return Ok(None);
        }
        Ok(Some(&mut self.actors[slot]))
    }

    /// Inspect one known partition's current state.
    pub fn status(&self, group: GroupId) -> Option<PartitionStatus> {
        self.index
            .get(&group)
            .map(|&slot| self.actors[slot].status())
    }

    /// Initial watch identity. This observation cannot activate write authority.
    pub fn route_state(
        &self,
        group: GroupId,
        partition: PartitionIncarnation,
    ) -> Option<RouteState> {
        self.index
            .get(&group)
            .map(|&slot| self.actors[slot].route_state(partition))
    }

    pub(super) fn observe_route(&self, route: &mut RouteState) -> bool {
        !self.stopped
            && self
                .index
                .get(&route.group)
                .is_some_and(|&slot| self.actors[slot].observe_route(route))
    }

    /// Attach a socket-free client adapter on the same partition shard. Take its
    /// proposal lane and leases before transferring the actor. Links are the
    /// frontend's independently established observations, shared across groups.
    pub fn install_native(
        &mut self,
        intake: NativeIntake,
        links: Links,
    ) -> Result<(), PartitionError> {
        let identity = intake.identity();
        let slot = *self.index.get(&identity.0).ok_or(PartitionError::Unknown)?;
        if self.stopped
            || self.native[slot].is_some()
            || self.actors[slot].native_identity() != identity
        {
            return Err(PartitionError::NativeConfiguration);
        }
        self.native[slot] = Some(NativeService { intake, links });
        self.touch(slot);
        Ok(())
    }

    /// Attach bounded reader subscriptions to this partition's existing journal
    /// and frontend sessions. It creates no physical connection or file worker.
    pub fn install_readers(
        &mut self,
        group: GroupId,
        config: SharedReaderConfig,
        links: Links,
    ) -> Result<(), PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if self.stopped || self.readers[slot].is_some() {
            return Err(PartitionError::NativeConfiguration);
        }
        let readers = SharedReaders::new(&mut self.actors[slot], config, links)
            .map_err(|source| PartitionError::Actor { group, source })?;
        self.readers[slot] = Some(readers);
        self.touch(slot);
        Ok(())
    }

    /// Whether this partition still owns native proposals or pending replies.
    /// This is a scheduling observation, independent of leadership and physical
    /// buffer release. Unknown groups or missing native adapters fail closed.
    pub fn native_has_work(&self, group: GroupId) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if matches!(self.actors[slot], PartitionActor::Recovering(_)) {
            return Ok(false);
        }
        self.native[slot]
            .as_ref()
            .map(|native| native.intake.has_work())
            .ok_or(PartitionError::NativeConfiguration)
    }

    /// Whether one client's writer still owns native proposals or replies on
    /// this partition. Other writers do not count. Unknown groups or missing
    /// native adapters fail closed.
    pub fn native_writer_has_work(
        &self,
        group: GroupId,
        node: NodeId,
        producer: ozzy_proto::ProducerId,
    ) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if matches!(self.actors[slot], PartitionActor::Recovering(_)) {
            return Ok(false);
        }
        self.native[slot]
            .as_ref()
            .map(|native| native.intake.writer_has_work(node, producer))
            .ok_or(PartitionError::NativeConfiguration)
    }

    /// Deliver a shard-admitted client command through the current shared link.
    /// Session replacement rejects old queued input before proposal admission.
    /// Supports explicit local and fixed-three actors through the same codec.
    pub fn receive_client(
        &mut self,
        group: GroupId,
        message: &Message,
        now: Duration,
    ) -> Result<NativeReceive, PartitionError> {
        self.deliver_client(group, message, now, false)
    }

    /// Return a bounded retry reply for a current client while local partition
    /// initialization is pending. No producer or append proposal is admitted.
    pub fn defer_client(
        &mut self,
        group: GroupId,
        message: &Message,
        now: Duration,
    ) -> Result<NativeReceive, PartitionError> {
        self.deliver_client(group, message, now, true)
    }

    fn deliver_client(
        &mut self,
        group: GroupId,
        message: &Message,
        now: Duration,
        deferred: bool,
    ) -> Result<NativeReceive, PartitionError> {
        self.observe(now)?;
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if matches!(self.actors[slot], PartitionActor::Recovering(_)) {
            return Ok(NativeReceive::Ignored);
        }
        self.touch(slot);
        let native = self.native[slot]
            .as_mut()
            .ok_or(PartitionError::NativeConfiguration)?;
        let Some(peer) = message
            .part_slice(0)
            .and_then(|bytes| bytes.try_into().ok())
            .map(NodeId::from_bytes)
        else {
            return Ok(NativeReceive::Ignored);
        };
        let Some(link) = native.links.get(peer) else {
            return Ok(NativeReceive::Ignored);
        };
        // Admission does not carry a wakeup itself. Poll every actor again even
        // when the new request targets an earlier point in the current scan.
        self.remaining = self.actors.len();
        let hint = self.actors[slot].authority_hint();
        if SharedReaders::supports(message)
            && let Some(readers) = &mut self.readers[slot]
        {
            let ready = !deferred
                && self.actors[slot]
                    .read_access()
                    .is_some_and(|access| access.0.is_some());
            return readers
                .receive(message, hint, ready)
                .map_err(|source| PartitionError::Actor { group, source });
        }
        let received = if deferred {
            native.intake.defer(message, link, hint)
        } else {
            native.intake.receive(message, link, hint)
        };
        received.map_err(|source| PartitionError::Native { group, source })
    }

    /// Observe a ready follower's exact receive epoch for shard admission.
    /// Local partitions and replicated leaders have no follower receive window.
    pub fn receive_credit(
        &self,
        group: GroupId,
    ) -> Option<(NodeId, ozzy_replication::flow::Report)> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => None,
            PartitionActor::Replicated(actor) => actor.receive_credit(),
        }
    }

    /// Exact follower epoch remains observable while its transport link or
    /// application intake is fenced. It does not grant receive capacity.
    pub fn receive_channel(&self, group: GroupId) -> Option<ozzy_replication::flow::Channel> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => None,
            PartitionActor::Replicated(actor) => actor.receive_channel(),
        }
    }

    /// Free follower capacity includes previously retained operations and unused
    /// grants. Shard-wide backing still needs separate admission.
    pub fn receive_capacity(&self, group: GroupId) -> Option<ozzy_replication::PipelineLimits> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => None,
            PartitionActor::Replicated(actor) => actor.receive_capacity(),
        }
    }

    /// Observe current leader demand without assigning buffers to idle followers.
    /// This is an admission scheduling hint, never verified history or authority.
    pub fn receive_target(&self, group: GroupId) -> Option<ozzy_replication::OpNumber> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => None,
            PartitionActor::Replicated(actor) => actor.receive_target(),
        }
    }

    /// Next-packet allocation hint. This supplies no replication authority.
    pub fn receive_body_bytes(&self, group: GroupId) -> Option<usize> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => None,
            PartitionActor::Replicated(actor) => actor.receive_body_bytes(),
        }
    }

    /// Requested history replies use separate correlation from normal flow.
    /// They can be needed before the partition is ready to receive APPENDs.
    pub fn ops_receive_demand(&self, group: GroupId) -> Option<ozzy_replication::wire::FetchOps> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Local(_) => None,
            PartitionActor::Replicated(actor) => actor.ops_receive_demand(),
            PartitionActor::Recovering(actor) => actor.ops_receive_demand(),
        }
    }

    /// Staged, validating, or installing data keeps its reservation until settled.
    pub fn receive_has_work(&self, group: GroupId) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        Ok(match &self.actors[slot] {
            PartitionActor::Local(_) => false,
            PartitionActor::Replicated(actor) => actor.receive_has_work(),
            PartitionActor::Recovering(actor) => actor.receive_has_work(),
        })
    }

    /// Advertise already reserved shard capacity to one exact partition epoch.
    /// This does not allocate memory or dispatch slots. The caller must acquire
    /// those reservations first and retain them until unused credit is fenced
    /// or its admitted buffers are actually released.
    pub fn grant_receive(
        &mut self,
        channel: ozzy_replication::flow::Channel,
        operations: u64,
        bytes: u64,
    ) -> Result<(), ozzy_replication::flow::FlowError> {
        use ozzy_replication::flow::FlowError;
        if self.stopped {
            return Err(FlowError::Channel);
        }
        let slot = *self
            .index
            .get(&channel.scope.group_id)
            .ok_or(FlowError::Channel)?;
        self.touch(slot);
        match &mut self.actors[slot] {
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => Err(FlowError::Channel),
            PartitionActor::Replicated(actor) => actor.grant_receive(channel, operations, bytes),
        }
    }

    /// Revoke only unused follower credit before reallocating shard capacity.
    /// The returned fresh epoch retains every previously received body charge.
    /// Old dispatcher tokens must also be revoked before issuing replacements.
    pub fn revoke_receive(
        &mut self,
        channel: ozzy_replication::flow::Channel,
    ) -> Result<ozzy_replication::flow::Report, PartitionError> {
        if self.stopped {
            return Err(PartitionError::Stopped);
        }
        let group = channel.scope.group_id;
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        self.touch(slot);
        match &mut self.actors[slot] {
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => Err(PartitionError::Mode),
            PartitionActor::Replicated(actor) => actor
                .revoke_receive(channel)
                .map_err(|source| PartitionError::Replica { group, source }),
        }
    }

    /// Synchronous bounded delivery after shared frontend admission. The actor
    /// validates the complete protocol command and its own current link session.
    /// Local partitions have no broker replication commands.
    pub fn receive(
        &mut self,
        group: GroupId,
        message: &Message,
        now: Duration,
    ) -> Result<(), PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        self.observe(now)?;
        self.touch(slot);
        match &mut self.actors[slot] {
            PartitionActor::Local(_) => Err(PartitionError::Mode),
            PartitionActor::Replicated(actor) => actor
                .receive(message, now)
                .map_err(|source| PartitionError::Replica { group, source }),
            PartitionActor::Recovering(actor) => actor
                .receive(message, now)
                .map_err(|source| PartitionError::Replica { group, source }),
        }
    }

    /// Apply one independently established link fence to its partition actor.
    pub fn replace_session(
        &mut self,
        group: GroupId,
        peer: NodeId,
        expected: LinkSessionId,
        replacement: LinkSessionId,
        now: Duration,
    ) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        self.observe(now)?;
        self.touch(slot);
        match &mut self.actors[slot] {
            PartitionActor::Local(_) => Err(PartitionError::Mode),
            PartitionActor::Replicated(actor) => actor
                .replace_session(peer, expected, replacement, now)
                .map_err(|source| PartitionError::Replica { group, source }),
            PartitionActor::Recovering(actor) => actor
                .replace_session(peer, expected, replacement, now)
                .map_err(|source| PartitionError::Replica { group, source }),
        }
    }

    /// Fence one independently observed disconnect on its partition actor.
    pub fn disconnect_session(
        &mut self,
        group: GroupId,
        peer: NodeId,
        expected: LinkSessionId,
        now: Duration,
    ) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        self.observe(now)?;
        self.touch(slot);
        match &mut self.actors[slot] {
            PartitionActor::Local(_) => Err(PartitionError::Mode),
            PartitionActor::Replicated(actor) => actor
                .disconnect_session(peer, expected, now)
                .map_err(|source| PartitionError::Replica { group, source }),
            PartitionActor::Recovering(actor) => actor
                .disconnect_session(peer, expected, now)
                .map_err(|source| PartitionError::Replica { group, source }),
        }
    }

    /// Poll at most `turn_actors` partition turns. Pending file work never blocks
    /// another actor. Partial scans self-wake; a full idle scan sleeps until an
    /// actor wakes it or the caller supplies input, transport progress, or time.
    ///
    /// `try_send` must return the owning message on `Full` and arrange a wakeup
    /// when capacity returns. Timer observations come from the broker/simulator.
    /// Cancellation retains scan position and every admitted storage operation.
    pub fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
        now: Duration,
        mut try_send: impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Poll<Result<(), PartitionError>> {
        if let Err(error) = self.observe(now) {
            return Poll::Ready(Err(error));
        }
        if let Some(wakeups) = self.wakeups.take() {
            let (wakeups, result) = self.poll_woken(wakeups, cx, now, &mut try_send);
            self.wakeups = Some(wakeups);
            return match result {
                Ok(()) => Poll::Pending,
                Err(error) => Poll::Ready(Err(error)),
            };
        }
        if self.remaining == 0 {
            self.remaining = self.actors.len();
            // A complete scan ends where it began. Start the next one a
            // partition later, so a shared bounded transport serves every
            // partition first in turn. A shard may host no partition.
            if !self.actors.is_empty() {
                self.next = (self.next + 1) % self.actors.len();
            }
        }
        let mut changed = false;
        for _ in 0..self.turn_actors.min(self.remaining) {
            let slot = self.next;
            self.next = (self.next + 1) % self.actors.len();
            self.remaining -= 1;
            match self.visit(slot, cx, now, &mut try_send) {
                Ok(progress) => changed |= progress,
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
        if changed || self.remaining > 0 {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    /// Visit at most `turn_actors` partitions that have something to observe.
    fn poll_woken(
        &mut self,
        mut wakeups: Wakeups,
        cx: &mut Context<'_>,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> (Wakeups, Result<(), PartitionError>) {
        wakeups.shard.register(cx.waker());
        let timers = wakeups
            .visited
            .is_none_or(|visited| now >= visited + wakeups.interval);
        if timers {
            wakeups.visited = Some(now);
        }
        let count = self.actors.len();
        // Start one partition later on every call, so a shared bounded
        // transport serves every partition first in turn.
        let first = if count == 0 {
            0
        } else {
            (self.next + 1) % count
        };
        self.next = first;
        let mut visits = 0;
        let mut again = false;
        for offset in 0..count {
            let slot = (first + offset) % count;
            let woken = wakeups.woken[slot].set.load(Ordering::Acquire);
            if !(timers || woken || wakeups.refused[slot]) {
                continue;
            }
            if visits == self.turn_actors {
                // Keep the rest for the next call, including timers.
                if timers {
                    wakeups.woken[slot].set.store(true, Ordering::Release);
                }
                again = true;
                continue;
            }
            visits += 1;
            wakeups.woken[slot].set.store(false, Ordering::Release);
            wakeups.refused[slot] = false;
            let mut refused = false;
            let mut partition = Context::from_waker(&wakeups.wakers[slot]);
            let visited = self.visit(slot, &mut partition, now, &mut |cx, message| {
                let sent = try_send(cx, message);
                refused |= matches!(sent, Err(TrySendError::Full(_)));
                sent
            });
            wakeups.refused[slot] = refused;
            match visited {
                // Progress can leave more to observe in the same partition.
                Ok(true) => {
                    wakeups.woken[slot].set.store(true, Ordering::Release);
                    again = true;
                }
                Ok(false) => {}
                Err(error) => return (wakeups, Err(error)),
            }
        }
        if again {
            cx.waker().wake_by_ref();
        }
        (wakeups, Ok(()))
    }

    fn visit(
        &mut self,
        slot: usize,
        cx: &mut Context<'_>,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, PartitionError> {
        self.turns = self.turns.wrapping_add(1);
        let recovering = matches!(self.actors[slot], PartitionActor::Recovering(_));
        let result =
            poll_actor(&mut self.actors[slot], cx, now, &mut *try_send).and_then(|mut changed| {
                if let Some(readers) = &mut self.readers[slot] {
                    let group = self.actors[slot].group();
                    changed |= readers
                        .poll(&mut self.actors[slot], cx, &mut *try_send)
                        .map_err(|source| PartitionError::Actor { group, source })?;
                }
                let Some(native) = &mut self.native[slot] else {
                    return Ok(changed);
                };
                let group = self.actors[slot].group();
                native
                    .intake
                    .poll_progress(
                        cx,
                        self.actors[slot].authority_hint(),
                        |peer| native.links.get(peer),
                        &mut *try_send,
                    )
                    .map(|advanced| changed || advanced)
                    .map_err(|source| PartitionError::Native { group, source })
            });
        if recovering && matches!(self.actors[slot], PartitionActor::Replicated(_)) {
            self.recovered.push_back(self.actors[slot].group());
        }
        if result.is_err() {
            self.stopped = true;
        }
        result
    }

    /// Drain every partition concurrently, including after one partition errors.
    /// No first slow drain can prevent another actor's barriers from progressing.
    pub async fn shutdown(self) -> Result<(), PartitionError> {
        // Drop protocol observers and unused proposal arenas before draining.
        // Already admitted work stays owned by the partition actors.
        drop(self.native);
        drop(self.readers);
        let mut drains = FuturesUnordered::new();
        for actor in self.actors {
            drains.push(async move {
                let group = actor.group();
                actor
                    .shutdown()
                    .await
                    .map_err(|source| PartitionError::Actor { group, source })
            });
        }
        let mut failure = None;
        while let Some(result) = drains.next().await {
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn observe(&mut self, now: Duration) -> Result<(), PartitionError> {
        if self.stopped {
            return Err(PartitionError::Stopped);
        }
        if now < self.now {
            return Err(PartitionError::TimeReversed);
        }
        self.now = now;
        Ok(())
    }
}

fn poll_actor(
    actor: &mut PartitionActor,
    cx: &mut Context<'_>,
    now: Duration,
    try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
) -> Result<bool, PartitionError> {
    let group = actor.group();
    match actor {
        PartitionActor::Local(actor) => match actor.poll_progress(cx) {
            Poll::Pending => Ok(false),
            Poll::Ready(Ok(())) => Err(PartitionError::Stopped),
            Poll::Ready(Err(source)) => Err(PartitionError::Actor { group, source }),
        },
        PartitionActor::Replicated(actor) => {
            let error = |source| PartitionError::Replica { group, source };
            actor.advance(now).map_err(error)?;
            let changed = {
                let event = std::pin::pin!(actor.changed(now));
                match event.poll(cx) {
                    Poll::Ready(result) => {
                        result.map_err(error)?;
                        true
                    }
                    Poll::Pending => false,
                }
            };
            let sent = actor
                .flush(|message| try_send(cx, message))
                .map_err(error)?
                .advanced();
            Ok(changed || sent)
        }
        PartitionActor::Recovering(recovery) => {
            let changed = recovery
                .poll_progress(cx, now, &mut *try_send)
                .map_err(|source| PartitionError::Replica { group, source })?;
            if let Some(normal) = recovery.take_ready() {
                *actor = PartitionActor::Replicated(normal);
                return Ok(true);
            }
            Ok(changed)
        }
    }
}

/// Rejected schedule/input, or a terminal actor error requiring shard shutdown.
#[derive(Debug, thiserror::Error)]
pub enum PartitionError {
    /// Client adapter does not match the partition/broker/policy or already exists.
    #[error("invalid partition native client adapter")]
    NativeConfiguration,
    /// Bounded client adapter failed after intake. Outstanding writes may persist.
    #[error("partition {group}: {source}")]
    Native {
        /// Affected partition group.
        group: GroupId,
        /// Original adapter failure.
        #[source]
        source: NativeIntakeError,
    },
    /// Invalid actor count or per-poll work bound.
    #[error("invalid partition scheduling limits")]
    Limits,
    /// More than one actor claims a group.
    #[error("duplicate partition group")]
    Duplicate,
    /// Input targets a group not hosted on this shard.
    #[error("unknown partition group")]
    Unknown,
    /// Replication input was directed to explicit single-broker authority.
    #[error("local partition cannot receive replicated-group traffic")]
    Mode,
    /// A caller supplied an older clock observation.
    #[error("partition scheduler time moved backward")]
    TimeReversed,
    /// A previous actor failure ended scheduler progress.
    #[error("partition scheduler stopped")]
    Stopped,
    /// Actor or asynchronous shutdown failure.
    #[error("partition {group}: {source}")]
    Actor {
        /// Affected partition group.
        group: GroupId,
        /// Original authority or storage error.
        #[source]
        source: ActorError,
    },
    /// A replicated actor rejected scheduling or protocol progress.
    #[error("partition {group}: {source}")]
    Replica {
        /// Affected partition group.
        group: GroupId,
        /// Original scheduling error.
        #[source]
        source: ScheduleError,
    },
}
