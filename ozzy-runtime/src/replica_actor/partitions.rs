//! Bounded shared-shard scheduling. Partition authority stays inside each actor.

use futures::{StreamExt, stream::FuturesUnordered};
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{GroupId, PartitionIncarnation, directory::RouteState};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    task::{Context, Poll},
    time::Duration,
};

use super::shared_readers::SharedReaders;
use super::{
    ActorError, GroupCoordinator, NativeIntake, NativeIntakeError, ScheduleError, ScheduledRecovery,
};
use crate::frontend::Links;
use wakeups::Wakeups;

mod actor;
mod replication;
mod services;
mod wakeups;
mod woken;

pub use actor::{PartitionActor, PartitionStatus, SessionReplacement};

/// One application shard's fixed actor set. No actor or journal threads/tasks
/// are spawned here. All file futures use the actors' shared backend clients.
#[derive(Debug)]
pub struct PartitionActors {
    actors: Vec<PartitionActor>,
    native: Vec<Option<NativeService>>,
    coordinators: Vec<Option<NativeCoordinator>>,
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

#[derive(Debug)]
struct NativeCoordinator {
    coordinator: GroupCoordinator,
    links: Links,
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
            coordinators: actors.iter().map(|_| None).collect(),
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

    /// Partition visits since creation. A scheduling diagnostic.
    pub fn turns(&self) -> u64 {
        self.turns
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

    /// Inspect one known partition's current state.
    pub fn status(&self, group: GroupId) -> Option<PartitionStatus> {
        self.index
            .get(&group)
            .map(|&slot| self.actors[slot].status())
    }

    /// Whether a known local partition has no write or durable evidence outstanding.
    #[cfg(test)]
    pub(crate) fn storage_idle(&self, group: GroupId) -> Option<bool> {
        self.index
            .get(&group)
            .and_then(|&slot| self.actors[slot].storage_idle())
    }

    /// Committed facts of one known partition, for diagnostics.
    pub fn facts(
        &self,
        group: GroupId,
        partition: PartitionIncarnation,
    ) -> Option<crate::replica_journal::PartitionFacts> {
        self.index
            .get(&group)
            .and_then(|&slot| self.actors[slot].facts(partition))
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

    fn visit(
        &mut self,
        slot: usize,
        cx: &mut Context<'_>,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, PartitionError> {
        self.turns = self.turns.wrapping_add(1);
        if matches!(&self.actors[slot], PartitionActor::Replicated(normal) if normal.needs_checkpoint_recovery())
        {
            let actor = match self.actors.swap_remove(slot) {
                PartitionActor::Replicated(normal) => {
                    PartitionActor::Recovering(Box::new(ScheduledRecovery::quarantine(normal)))
                }
                actor => actor,
            };
            self.native[slot] = None;
            self.coordinators[slot] = None;
            self.readers[slot] = None;
            self.actors.push(actor);
            let last = self.actors.len() - 1;
            self.actors.swap(slot, last);
        }
        let recovering = matches!(self.actors[slot], PartitionActor::Recovering(_));
        let result =
            poll_actor(&mut self.actors[slot], cx, now, &mut *try_send).and_then(|changed| {
                let served = self.poll_services(slot, cx, now, try_send)?;
                Ok(changed || served)
            });
        if recovering && matches!(self.actors[slot], PartitionActor::Replicated(_)) {
            self.recovered.push_back(self.actors[slot].group());
        }
        if result.is_err() {
            self.stopped = true;
        }
        result
    }

    /// Poll the partition's readers, group coordinator, and native intake.
    fn poll_services(
        &mut self,
        slot: usize,
        cx: &mut Context<'_>,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, PartitionError> {
        let group = self.actors[slot].group();
        let mut changed = false;
        if let Some(readers) = &mut self.readers[slot] {
            changed |= readers
                .poll(&mut self.actors[slot], cx, &mut *try_send)
                .map_err(|source| PartitionError::Actor { group, source })?;
        }
        if let Some(service) = &mut self.coordinators[slot] {
            let (coordinator, links) = (&mut service.coordinator, &service.links);
            changed |= coordinator
                .flush_replies(cx, |peer| links.get(peer), &mut *try_send)
                .and_then(|sent| {
                    Ok(coordinator.poll_progress(cx, now, &mut self.actors[slot])? | sent)
                })
                .map_err(|source| PartitionError::Native { group, source })?;
        }
        let Some(native) = &mut self.native[slot] else {
            return Ok(changed);
        };
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
    }

    /// Drain every partition concurrently, including after one partition errors.
    /// No first slow drain can prevent another actor's barriers from progressing.
    pub async fn shutdown(self) -> Result<(), PartitionError> {
        // Drop protocol observers and unused proposal arenas before draining.
        // Already admitted work stays owned by the partition actors.
        drop(self.native);
        drop(self.coordinators);
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
        PartitionActor::Local(actor) => {
            actor.observe_time(now);
            match actor.poll_progress(cx) {
                Poll::Pending => Ok(false),
                Poll::Ready(Ok(())) => Err(PartitionError::Stopped),
                Poll::Ready(Err(source)) => Err(PartitionError::Actor { group, source }),
            }
        }
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
