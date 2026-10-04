//! Nonvoting recovery on a shared shard, including owned retry and handoff.

use super::{ActorError, ActorStatus, Outcome, RecoveryActor, RecoveryStorage, Transition};
use crate::replica_actor::HistoryReason;
use crate::{
    replica_actor::{ScheduleError, ScheduledReplica},
    replica_journal::ShardRecoveringJournal,
    replica_transport::SendClass,
};
use futures::{FutureExt, future::LocalBoxFuture};
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{LinkSessionId, NodeId, PartitionIncarnation, directory::RouteState};
use ozzy_replication::{
    Configuration,
    wire::{FetchOps, PeerBinding},
};
use std::{
    task::{Context, Poll},
    time::Duration,
};

enum Phase<J: RecoveryStorage> {
    Active(Box<RecoveryActor<J>>),
    Changing(LocalBoxFuture<'static, Result<Transition<J>, ActorError>>),
    Ready(ScheduledReplica),
    Stopped,
}

/// One explicit nonvoting replacement on an application shard. Storage waits,
/// attempt restart, and publication/handoff suspend only this partition. The
/// broker must install normal services before polling the returned replica.
pub struct ScheduledRecovery<J: RecoveryStorage = ShardRecoveringJournal> {
    phase: Phase<J>,
    configuration: Configuration,
    local: NodeId,
    sessions: [LinkSessionId; 3],
    status: tokio::sync::watch::Receiver<ActorStatus>,
    owner: Option<crate::memory::Owner>,
    now: Duration,
    origin: Option<Duration>,
    active: bool,
}

impl<J: RecoveryStorage> std::fmt::Debug for ScheduledRecovery<J> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduledRecovery")
            .field("configuration", &self.configuration)
            .field("status", &*self.status.borrow())
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl<J: RecoveryStorage + 'static> ScheduledRecovery<J> {
    /// Shared scheduling hands off without remotely granted follower capacity.
    /// Bind the empty transfer arena to a destination allowance before polling.
    pub fn new(actor: RecoveryActor<J>) -> Self {
        Self {
            configuration: actor.configuration,
            local: actor.local,
            sessions: actor.config.sessions,
            status: actor.subscribe(),
            phase: Phase::Active(Box::new(actor)),
            owner: None,
            now: Duration::ZERO,
            origin: None,
            active: true,
        }
    }

    /// Charge recovery buffers to the shard's bounded replication memory.
    pub fn bind_receive_owner(&mut self, owner: &crate::memory::Owner) -> Result<(), ActorError> {
        let Phase::Active(actor) = &mut self.phase else {
            return Err(ActorError::StartupMismatch);
        };
        actor
            .buffer
            .as_mut()
            .ok_or_else(|| ActorError::history(HistoryReason::Recovery))?
            .bind_allocator(&owner.allocator())?;
        self.owner = Some(owner.clone());
        Ok(())
    }

    /// Current observation stays nonvoting throughout transfer and publication.
    pub fn status(&self) -> ActorStatus {
        *self.status.borrow()
    }

    /// Whether a controlled schedule can interrupt owned close/reopen/handoff.
    /// Polling and canceling observations never gives up that transition.
    #[cfg(feature = "simulation")]
    pub fn transition_pending(&self) -> bool {
        matches!(self.phase, Phase::Changing(_))
    }

    pub(in crate::replica_actor) fn native_identity(&self) -> (NodeId, ozzy_proto::append::Policy) {
        (self.local, self.configuration.append_policy())
    }

    pub(in crate::replica_actor) fn authority_hint(&self) -> ozzy_proto::nack::AuthorityHint {
        crate::replica_actor::response::authority_hint(self.configuration, self.status().scope)
    }

    pub(in crate::replica_actor) fn route_state(
        &self,
        partition: PartitionIncarnation,
    ) -> RouteState {
        let scope = self.status().scope;
        RouteState {
            group: scope.group_id,
            config_epoch: scope.configuration_epoch,
            partition,
            members: (*self.configuration.voters()).into(),
            view: scope.view,
            leader: None,
        }
    }

    pub(in crate::replica_actor) fn observe_route(&self, route: &mut RouteState) -> bool {
        let scope = self.status().scope;
        if route.group != scope.group_id
            || route.config_epoch != scope.configuration_epoch
            || route.members.as_ref() != self.configuration.voters()
        {
            return false;
        }
        route.view = scope.view;
        route.leader = None;
        true
    }

    /// Exact history correlation needing destination and dispatch reservations.
    pub fn ops_receive_demand(&self) -> Option<FetchOps> {
        let Phase::Active(actor) = &self.phase else {
            return None;
        };
        let request = actor.fetch?.0;
        (self.active
            && !actor.abandoning
            && actor.pending.is_none()
            && actor.buffer.is_some()
            && self.session(request.source.voter).is_some())
        .then_some(request)
    }

    /// Exact checkpoint range requiring the same bounded data-plane reservations.
    pub fn checkpoint_receive_demand(&self) -> Option<ozzy_replication::wire::CheckpointRequest> {
        let Phase::Active(actor) = &self.phase else {
            return None;
        };
        let request = actor.checkpoint_fetch?.0;
        (self.active
            && !actor.abandoning
            && actor.pending.is_none()
            && self.session(request.source.voter).is_some())
        .then_some(request)
    }

    /// Installing a chunk keeps its receive reservation until backing is freed.
    pub fn receive_has_work(&self) -> bool {
        matches!(&self.phase, Phase::Active(actor) if matches!(actor.pending, Some(super::Pending::Chunk(_) | super::Pending::Checkpoint(_))))
    }

    /// Session comes from the frontend's independently established link table.
    pub fn replace_session(
        &mut self,
        peer: NodeId,
        expected: LinkSessionId,
        replacement: LinkSessionId,
        now: Duration,
    ) -> Result<bool, ScheduleError> {
        if replacement.as_bytes() == &[0; 16] {
            return Err(ScheduleError::Binding);
        }
        self.change_session(peer, expected, replacement, now)
    }

    /// A late disconnect cannot fence a newer link. Admitted disk work remains.
    pub fn disconnect_session(
        &mut self,
        peer: NodeId,
        expected: LinkSessionId,
        now: Duration,
    ) -> Result<bool, ScheduleError> {
        self.change_session(peer, expected, LinkSessionId::from_bytes([0; 16]), now)
    }

    fn change_session(
        &mut self,
        peer: NodeId,
        expected: LinkSessionId,
        replacement: LinkSessionId,
        now: Duration,
    ) -> Result<bool, ScheduleError> {
        self.observe(now)?;
        let slot = self
            .configuration
            .voters()
            .iter()
            .position(|node| *node == peer && peer != self.local)
            .ok_or(ScheduleError::Binding)?;
        let current = self.sessions[slot];
        if current == replacement {
            return Ok(false);
        }
        if current != expected {
            return Err(ScheduleError::Binding);
        }
        self.sessions[slot] = replacement;
        if let Phase::Active(actor) = &mut self.phase {
            set_session(
                actor,
                slot,
                replacement,
                now.saturating_sub(self.origin.unwrap_or(now)),
            )?;
        } else if let Phase::Ready(actor) = &mut self.phase {
            if replacement.as_bytes() == &[0; 16] {
                actor.disconnect_session(peer, current, now)?;
            } else {
                actor.replace_session(peer, current, replacement, now)?;
            }
        }
        Ok(true)
    }

    fn session(&self, peer: NodeId) -> Option<LinkSessionId> {
        let slot = self
            .configuration
            .voters()
            .iter()
            .position(|node| *node == peer)?;
        (self.sessions[slot].as_bytes() != &[0; 16]).then_some(self.sessions[slot])
    }

    /// Deliver only broker recovery/history commands. Changing lifecycle phases
    /// admit no new input; donors retry their bounded protocol messages.
    pub fn receive(&mut self, message: &Message, now: Duration) -> Result<(), ScheduleError> {
        self.observe(now)?;
        let relative = now.saturating_sub(self.origin.unwrap_or(now));
        let result = match &mut self.phase {
            Phase::Active(actor) => actor.receive(message, relative),
            _ => Ok(()),
        };
        self.finish(result)
    }

    /// Poll one bounded recovery turn. Canceling its observation retains pending
    /// disk work and the owned lifecycle future. Retry timers restart at zero.
    pub fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
        now: Duration,
        try_send: impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, ScheduleError> {
        self.observe(now)?;
        let result = match &mut self.phase {
            Phase::Active(_) => self.poll_active(cx, now, try_send),
            Phase::Changing(_) => self.poll_transition(cx, now),
            Phase::Ready(_) => Ok(false),
            Phase::Stopped => return Err(ScheduleError::Stopped),
        };
        self.finish(result)
    }

    fn poll_active(
        &mut self,
        cx: &mut Context<'_>,
        now: Duration,
        mut try_send: impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, ActorError> {
        let relative = now.saturating_sub(self.origin.unwrap_or(now));
        let Phase::Active(actor) = &mut self.phase else {
            unreachable!("active phase");
        };
        let mut outcome = actor.advance(relative)?;
        let mut changed = false;
        if outcome.is_none()
            && let Poll::Ready(result) = poll_work(actor, cx, relative)
        {
            outcome = result?;
            changed = true;
        }
        if outcome.is_none() {
            changed |= actor
                .outbox
                .flush_with(|message| try_send(cx, message))?
                .advanced();
            actor.publish(true);
            return Ok(changed);
        }
        actor.publish(true);
        let Phase::Active(actor) = std::mem::replace(&mut self.phase, Phase::Stopped) else {
            unreachable!("active phase");
        };
        self.phase = Phase::Changing(
            actor
                .transition(outcome.expect("lifecycle outcome"))
                .boxed_local(),
        );
        cx.waker().wake_by_ref();
        Ok(true)
    }

    fn poll_transition(&mut self, cx: &mut Context<'_>, now: Duration) -> Result<bool, ActorError> {
        let Phase::Changing(future) = &mut self.phase else {
            unreachable!("changing phase");
        };
        let Poll::Ready(result) = future.as_mut().poll(cx) else {
            return Ok(false);
        };
        // A settled future must never be polled again, including on error.
        self.phase = Phase::Stopped;
        self.phase = match result? {
            Transition::Retry(mut actor) => {
                for (slot, session) in self.sessions.iter().copied().enumerate() {
                    if self.configuration.voters()[slot] != self.local {
                        set_session(&mut actor, slot, session, Duration::ZERO)?;
                    }
                }
                if let Some(owner) = &self.owner {
                    actor
                        .buffer
                        .as_mut()
                        .ok_or_else(|| ActorError::history(HistoryReason::Recovery))?
                        .bind_allocator(&owner.allocator())?;
                }
                self.origin = Some(now);
                Phase::Active(actor)
            }
            Transition::Normal(actor) => {
                let prior = actor.config.sessions;
                let mut actor = ScheduledReplica::new(*actor).map_err(schedule_actor)?;
                for (slot, &peer) in self.configuration.voters().iter().enumerate() {
                    if peer == self.local || prior[slot] == self.sessions[slot] {
                        continue;
                    }
                    if self.sessions[slot].as_bytes() == &[0; 16] {
                        actor
                            .disconnect_session(peer, prior[slot], now)
                            .map_err(schedule_actor)?;
                    } else {
                        actor
                            .replace_session(peer, prior[slot], self.sessions[slot], now)
                            .map_err(schedule_actor)?;
                    }
                }
                if let Some(owner) = &self.owner {
                    actor.bind_receive_owner(owner)?;
                }
                Phase::Ready(actor)
            }
        };
        cx.waker().wake_by_ref();
        Ok(true)
    }

    /// Take the fully published, election-fenced actor exactly once. Install its
    /// native services before its first normal scheduling turn.
    pub fn take_ready(&mut self) -> Option<ScheduledReplica> {
        if !matches!(self.phase, Phase::Ready(_)) {
            return None;
        }
        let Phase::Ready(actor) = std::mem::replace(&mut self.phase, Phase::Stopped) else {
            unreachable!("ready phase");
        };
        Some(actor)
    }

    /// Settle admitted I/O, including a canceled transition observation.
    pub async fn shutdown(self) -> Result<(), ActorError> {
        match self.phase {
            Phase::Active(actor) => Box::pin(actor.shutdown()).await,
            Phase::Changing(future) => match future.await? {
                Transition::Retry(actor) => Box::pin(actor.shutdown()).await,
                Transition::Normal(mut actor) => Box::pin(actor.shutdown()).await,
            },
            Phase::Ready(actor) => Box::pin(actor.shutdown()).await,
            Phase::Stopped => Ok(()),
        }
    }

    fn observe(&mut self, now: Duration) -> Result<(), ScheduleError> {
        if !self.active {
            return Err(ScheduleError::Stopped);
        }
        if now < self.now {
            return Err(ScheduleError::TimeReversed);
        }
        self.origin.get_or_insert(now);
        self.now = now;
        Ok(())
    }

    fn finish<T>(&mut self, result: Result<T, ActorError>) -> Result<T, ScheduleError> {
        if result.is_err() {
            self.active = false;
        }
        if let Phase::Active(actor) = &self.phase {
            actor.publish(self.active);
        }
        result.map_err(ScheduleError::Actor)
    }
}

fn set_session<J: RecoveryStorage>(
    actor: &mut RecoveryActor<J>,
    slot: usize,
    session: LinkSessionId,
    now: Duration,
) -> Result<(), ActorError> {
    let peer = actor.configuration.voters()[slot];
    actor.config.sessions[slot] = session;
    actor.bindings[slot] = if session.as_bytes() == &[0; 16] {
        None
    } else {
        Some(PeerBinding::new(actor.configuration, peer, session)?)
    };
    for class in [
        SendClass::Control,
        SendClass::Data,
        SendClass::Receipt,
        SendClass::Exchange,
    ] {
        actor.outbox.discard(peer, class);
    }
    actor.retries[slot].at = now;
    if let Some((request, retry)) = &mut actor.checkpoint_fetch
        && request.source.voter == peer
    {
        retry.at = now;
    }
    if let Some((request, retry)) = &mut actor.fetch
        && request.source.voter == peer
    {
        retry.at = now;
    }
    Ok(())
}

fn poll_work<J: RecoveryStorage>(
    actor: &mut RecoveryActor<J>,
    cx: &mut Context<'_>,
    now: Duration,
) -> Poll<Result<Option<Outcome>, ActorError>> {
    if let Some(pending) = &mut actor.pending {
        let result = {
            let waiting = std::pin::pin!(pending.wait());
            waiting.poll(cx)
        };
        if let Poll::Ready(result) = result {
            actor.pending = None;
            return Poll::Ready(result.and_then(|completed| actor.complete(completed, now)));
        }
    }
    if let Poll::Ready(error) = actor
        .journal
        .as_mut()
        .expect("recovery owner")
        .poll_stopped(cx)
    {
        return Poll::Ready(Err(ActorError::Journal(error)));
    }
    Poll::Pending
}

fn schedule_actor(error: ScheduleError) -> ActorError {
    match error {
        ScheduleError::Actor(error) => error,
        _ => ActorError::StartupMismatch,
    }
}

impl ScheduledRecovery<ShardRecoveringJournal> {
    pub(in crate::replica_actor) fn quarantine(normal: ScheduledReplica) -> Self {
        let owner = normal.receive_owner.clone();
        let super::ReplicaActor {
            configuration,
            local,
            config,
            journal,
            mut ids,
            status,
            ..
        } = normal.into_actor();
        let sessions = config.sessions;
        let observation = status.subscribe();
        status.send_modify(|state| {
            state.normal = None;
            state.application_ready = false;
            state.disk_pending = true;
        });
        let future = async move {
            let generations = crate::replica_journal::OwnedRecoveryGenerations {
                attempt: ids.generation()?,
                temporary: ids.generation()?,
            };
            let (journal, startup) = Box::pin(journal.into_recovering(generations)).await?;
            let mut actor = RecoveryActor::new_with_ids(
                journal,
                startup,
                config,
                super::RecoveryTiming::default(),
                ids,
            )?;
            actor.status = status;
            Ok(Transition::Retry(Box::new(actor)))
        }
        .boxed_local();
        Self {
            phase: Phase::Changing(future),
            configuration,
            local,
            sessions,
            status: observation,
            owner,
            now: Duration::ZERO,
            origin: None,
            active: true,
        }
    }
}
