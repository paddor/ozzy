//! Background membership owner: join, heartbeat, leave, and activation relay.

use super::sources::{Observation, Sources};
use crate::{
    replicated::{
        BrokerLinkError, ProgressError, RetryPolicy, TopicRoutes, broker_links::OwnerClock,
        retry::classify,
    },
    signal::{CloseSignal, StateSignal},
};
use futures::{StreamExt, stream::FuturesUnordered};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, GroupId, NodeId, Offset, OperationId, PartitionIncarnation,
    append::Authority,
    group::{self, JoinReply, JoinRequest, Mode, Owned, Start, Status, Target},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::watch;

mod activation;
mod requests;
use activation::ActivationRetry;
use requests::{assign, join};

/// Placeholder authority; the request helper fills the selected leader's.
const UNSELECTED: Authority = Authority {
    group_id: GroupId::from_bytes([0; 16]),
    config_epoch: 0,
    view: 0,
};

/// Source coordinates confirmed by the source leader for one committed target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Activation {
    pub(super) number: u32,
    pub(super) partition: PartitionIncarnation,
    pub(super) plan_revision: u64,
    pub(super) epoch: u64,
    pub(super) cursor: Offset,
    pub(super) retained_from: Offset,
}

/// Terminal membership outcome, published once before the owner exits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Failure {
    Fenced,
    Closed,
    Deleted,
    InitialPosition {
        partition: u32,
        position: group::InitialPosition,
    },
    Response,
    Broker(String),
}

/// Latest committed membership as observed by the background owner.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct View {
    pub(super) generation: u64,
    pub(super) group: Option<ConsumerGroupId>,
    pub(super) targets: Vec<Target>,
    pub(super) activations: Vec<Activation>,
    pub(super) failure: Option<Failure>,
}

impl View {
    pub(super) fn targets(&self, partition: PartitionIncarnation, plan_revision: u64) -> bool {
        find(&self.targets, partition, plan_revision).is_some()
    }
}

pub(super) struct Settings {
    pub(super) name: String,
    pub(super) member: ConsumerMemberId,
    pub(super) initial: Start,
    pub(super) heartbeat: Duration,
    pub(super) drain: Duration,
    pub(super) retry: RetryPolicy,
}

type Joining = Pin<Box<dyn Future<Output = Result<JoinReply, BrokerLinkError>> + Send>>;
type Assigning =
    Pin<Box<dyn Future<Output = (Target, Result<group::AssignReply, BrokerLinkError>)> + Send>>;

/// What the membership owner shares with its reader.
pub(super) struct Channels {
    /// Committed membership the owner publishes.
    pub(super) view: watch::Sender<View>,
    /// Sources the reader still owns.
    pub(super) sources: watch::Receiver<Vec<Observation>>,
    /// Closed when the reader leaves the group.
    pub(super) leave: CloseSignal,
    /// Signaled on every published change.
    pub(super) activity: Arc<StateSignal>,
}

/// Spawn on the SDK owner. The reader publishes owned sources; this owner
/// reports them, relays activatable targets, and leaves once `leave` closes.
pub(super) fn start(
    routes: TopicRoutes,
    settings: Settings,
    channels: Channels,
) -> tokio::task::JoinHandle<()> {
    let Channels {
        view,
        sources,
        leave,
        activity,
    } = channels;
    let executor = routes.links().runtime().driver().clone();
    executor.spawn(async move {
        let coordinator = routes.metadata().coordinator(&settings.name);
        let mut membership = Membership {
            coordinator: (coordinator.number, coordinator.incarnation),
            join: OperationId::new(),
            mode: Mode::Join,
            group: None,
            incarnation: None,
            generation: 0,
            sources: Sources::new(settings.drain),
            targets: Vec::new(),
            activations: BTreeMap::new(),
            skipped: BTreeSet::new(),
            inflight: BTreeSet::new(),
            activation_retries: BTreeMap::new(),
            routes,
            settings,
        };
        if let Err(failure) = membership.run(&view, sources, leave, &activity).await {
            view.send_modify(|view| view.failure = Some(failure));
            activity.notify_changed();
        }
    })
}

struct Membership {
    routes: TopicRoutes,
    settings: Settings,
    coordinator: (u32, PartitionIncarnation),
    join: OperationId,
    mode: Mode,
    group: Option<ConsumerGroupId>,
    /// First incarnation this reader joined. Rejoins keep naming it, so a
    /// deleted group is reported instead of silently recreated.
    incarnation: Option<ConsumerGroupId>,
    generation: u64,
    sources: Sources,
    targets: Vec<Target>,
    activations: BTreeMap<PartitionIncarnation, Activation>,
    /// Revisions a source already superseded; a newer plan replaces them.
    skipped: BTreeSet<(PartitionIncarnation, u64)>,
    inflight: BTreeSet<(PartitionIncarnation, u64)>,
    activation_retries: BTreeMap<(PartitionIncarnation, u64), ActivationRetry>,
}

impl Membership {
    async fn run(
        &mut self,
        view: &watch::Sender<View>,
        mut sources: watch::Receiver<Vec<Observation>>,
        leave: CloseSignal,
        activity: &StateSignal,
    ) -> Result<(), Failure> {
        let links = self.routes.links().clone();
        let clock = links.clock();
        self.routes.interest(self.coordinator.0).map_err(failure)?;
        let mut assigning = FuturesUnordered::<Assigning>::new();
        let mut joining: Option<Joining> = None;
        let mut due = clock.now();
        let mut backoff = self.settings.retry.initial_backoff;
        let mut leaving = std::pin::pin!(leave.closed());
        let mut reported = Vec::new();
        loop {
            let seen = activity.generation();
            let owned = self.owned(&mut sources, &clock);
            if reported != owned {
                reported = owned;
                if joining.is_none() {
                    due = clock.now();
                }
            }
            if joining.is_none() && clock.now() >= due {
                joining = Some(self.start_join(reported.clone()));
            }
            self.activate(&mut assigning, clock.now());
            let activation_due = self.activation_due();
            tokio::select! {
                biased;
                () = links.closed() => return Err(Failure::Closed),
                () = leaving.as_mut() => {
                    return self.leave(reported).await;
                }
                changed = sources.changed() => {
                    if changed.is_err() {
                        return self.leave(reported).await;
                    }
                    self.sources.replace(&sources.borrow_and_update());
                    // Released sources unblock their next owner promptly.
                    if joining.is_none() {
                        due = clock.now();
                    }
                }
                result = async {
                    match joining.as_mut() {
                        Some(joining) => joining.await,
                        None => std::future::pending().await,
                    }
                }, if joining.is_some() => {
                    joining = None;
                    due = self.complete_join(result, &mut assigning, &mut backoff, clock.now())?;
                    self.publish(view, activity);
                }
                Some((target, result)) = assigning.next(), if !assigning.is_empty() => {
                    self.assigned(target, result, clock.now())?;
                    self.publish(view, activity);
                }
                () = activity.changed_after(seen) => {}
                () = clock.until_due(activation_due) => {}
                () = clock.until_due(self.sources.deadline()) => {}
                () = clock.until(due), if joining.is_none() => {}
            }
        }
    }

    /// Take a changed source list, then the sources this reader owns now.
    fn owned(
        &mut self,
        sources: &mut watch::Receiver<Vec<Observation>>,
        clock: &OwnerClock,
    ) -> Vec<Owned> {
        if sources.has_changed().unwrap_or(false) {
            self.sources.replace(&sources.borrow_and_update());
        }
        self.sources
            .owned(&self.targets, self.generation, clock.now())
    }

    /// A join or rejoin request reporting the `owned` sources.
    fn start_join(&self, owned: Vec<Owned>) -> Joining {
        Box::pin(join(
            self.routes.clone(),
            self.coordinator.0,
            self.request(self.mode, owned),
            self.settings.retry.response_timeout,
        ))
    }

    fn complete_join(
        &mut self,
        result: Result<JoinReply, BrokerLinkError>,
        assigning: &mut FuturesUnordered<Assigning>,
        backoff: &mut Duration,
        now: Duration,
    ) -> Result<Duration, Failure> {
        match result {
            Ok(reply) => {
                let rejoined = self.joined(reply)?;
                if rejoined {
                    assigning.clear();
                }
                *backoff = self.settings.retry.initial_backoff;
                Ok(if rejoined {
                    now
                } else {
                    now.saturating_add(self.settings.heartbeat)
                })
            }
            Err(BrokerLinkError::Progress(ProgressError::Fenced)) => {
                self.rejoin()?;
                assigning.clear();
                Ok(now)
            }
            Err(error) => {
                self.retry(self.coordinator.0, error)?;
                let due = now.saturating_add(*backoff);
                *backoff = self.settings.retry.next_backoff(*backoff);
                Ok(due)
            }
        }
    }

    fn request(&self, mode: Mode, owned: Vec<Owned>) -> JoinRequest {
        JoinRequest {
            // The request helper fills the selected coordinator leader.
            authority: UNSELECTED,
            coordinator: self.coordinator.1,
            name: self.settings.name.clone(),
            member: self.settings.member,
            join: self.join,
            group: self.incarnation,
            mode,
            initial: self.settings.initial,
            owned,
        }
    }

    /// Best-effort cooperative leave. The coordinator timeout covers a lost one.
    async fn leave(&mut self, owned: Vec<Owned>) -> Result<(), Failure> {
        if self.mode == Mode::Join && self.group.is_none() {
            return Ok(());
        }
        let clock = self.routes.links().clock();
        let deadline = clock
            .now()
            .saturating_add(self.settings.retry.response_timeout);
        while clock.now() < deadline {
            let request = self.request(Mode::Leave, owned.clone());
            let attempt = join(
                self.routes.clone(),
                self.coordinator.0,
                request,
                self.settings.retry.response_timeout,
            );
            let result = tokio::select! {
                result = attempt => result,
                () = clock.until(deadline) => return Ok(()),
            };
            match result {
                Ok(_) | Err(BrokerLinkError::Progress(ProgressError::Fenced)) => return Ok(()),
                Err(error) => {
                    self.retry(self.coordinator.0, error)?;
                    clock
                        .until(
                            clock
                                .now()
                                .saturating_add(self.settings.retry.initial_backoff),
                        )
                        .await;
                }
            }
        }
        Ok(())
    }

    fn rejoin(&mut self) -> Result<(), Failure> {
        self.sources.fence();
        self.generation = self.generation.checked_add(1).ok_or(Failure::Response)?;
        self.join = OperationId::new();
        self.mode = Mode::Join;
        self.group = None;
        self.targets.clear();
        self.activations.clear();
        self.skipped.clear();
        self.inflight.clear();
        self.activation_retries.clear();
        Ok(())
    }

    fn joined(&mut self, reply: JoinReply) -> Result<bool, Failure> {
        if reply.coordinator != self.coordinator.1 {
            return Err(Failure::Response);
        }
        let zero = ConsumerGroupId::from_bytes([0; 16]);
        if reply.status == Status::Deleted
            || reply.group != zero && self.incarnation.is_some_and(|group| group != reply.group)
        {
            return Err(Failure::Deleted);
        }
        if reply.group != zero {
            self.group = Some(reply.group);
            self.incarnation = Some(reply.group);
        }
        let targets = match reply.status {
            Status::Member => {
                self.mode = Mode::Heartbeat;
                reply.targets
            }
            Status::Joining => Vec::new(),
            Status::Unknown => {
                self.rejoin()?;
                return Ok(true);
            }
            Status::Deleted => return Err(Failure::Deleted),
        };
        let metadata = self.routes.metadata();
        if targets.iter().any(|target| {
            metadata
                .partition(target.number)
                .is_none_or(|partition| partition.incarnation != target.partition)
        }) {
            return Err(Failure::Response);
        }
        self.targets = targets;
        let targets = &self.targets;
        let current = |partition, revision| find(targets, partition, revision).is_some();
        self.activations
            .retain(|&partition, activation| current(partition, activation.plan_revision));
        self.skipped
            .retain(|&(partition, revision)| current(partition, revision));
        self.activation_retries
            .retain(|&(partition, revision), _| current(partition, revision));
        Ok(false)
    }

    fn retry(&self, number: u32, error: BrokerLinkError) -> Result<(), Failure> {
        if classify(&error, None).map_err(failure)?.is_none() {
            return Err(failure(error));
        }
        self.routes.refresh(number).map_err(failure)
    }

    fn publish(&self, view: &watch::Sender<View>, activity: &StateSignal) {
        let next = View {
            generation: self.generation,
            group: self.group,
            targets: self.targets.clone(),
            activations: self.activations.values().copied().collect(),
            failure: None,
        };
        if view.send_if_modified(|current| {
            if *current == next {
                return false;
            }
            *current = next;
            true
        }) {
            activity.notify_changed();
        }
    }
}

fn failure(error: BrokerLinkError) -> Failure {
    match error {
        BrokerLinkError::Progress(ProgressError::Fenced) => Failure::Fenced,
        BrokerLinkError::Closed => Failure::Closed,
        error => Failure::Broker(error.to_string()),
    }
}

/// The target for `(partition, revision)`, if `targets` lists it.
fn find(targets: &[Target], partition: PartitionIncarnation, revision: u64) -> Option<&Target> {
    targets
        .iter()
        .find(|target| target.partition == partition && target.plan_revision == revision)
}
