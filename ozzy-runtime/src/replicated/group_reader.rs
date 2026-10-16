//! Automatic consumer-group membership over shared broker links.
//!
//! A background owner joins the group's coordinator, heartbeats owned sources,
//! and relays committed targets to their source leaders. The reader delivers
//! each activated source from its captured cursor through one processing
//! window, and releases revoked sources after they drain or a bounded deadline.

mod membership;
mod sources;

use super::{
    BrokerLinkError, BrokerLinks, PendingProgress, ProcessingIdentity, ProcessingToken,
    ProgressError, RetryPolicy, TopicReader, TopicReaderError, TopicRecord,
    processing::{driver, window::Window},
};
use crate::signal::{CloseSignal, StateSignal};
use membership::{Failure, Settings, View};
use ozzy_proto::{
    ConsumerMemberId, Offset, PartitionIncarnation,
    group::{self, Owned},
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, watch};

/// First position of a group that has no persisted cursor for a source yet.
pub use ozzy_proto::group::Start as GroupStart;

/// Group reader configuration. Use [`Self::new`] for the documented defaults.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GroupReaderConfig {
    /// Application member identity, stable across restarts of this worker.
    /// A newer instance with the same identity fences this one.
    pub member: ConsumerMemberId,
    /// Creation selector, used only when this join creates the group.
    pub start: GroupStart,
    /// Membership heartbeat interval, below the broker membership timeout.
    pub heartbeat: Duration,
    /// Delivered but unconfirmed records per owned source.
    pub window: usize,
    /// Local bound for confirming a revoked source's acknowledgments.
    pub drain: Duration,
    /// Reader refresh after silence or an unavailable leader.
    pub refresh: Duration,
    /// Request timeout and backoff for membership, activation, and progress.
    pub retry: RetryPolicy,
}

impl GroupReaderConfig {
    /// Earliest creation position, one-second heartbeats, 1024-record windows,
    /// and a five-second drain bound.
    pub fn new(member: ConsumerMemberId) -> Self {
        Self {
            member,
            start: GroupStart::Earliest,
            heartbeat: Duration::from_secs(1),
            window: 1024,
            drain: Duration::from_secs(5),
            refresh: Duration::from_millis(100),
            retry: RetryPolicy::default(),
        }
    }

    fn validate(&self, name: &str) -> Result<(), GroupReaderError> {
        if name.is_empty()
            || name.len() > group::MAX_NAME_BYTES
            || self.member.as_bytes() == &[0; 16]
            || self.heartbeat < Duration::from_millis(1)
            || self.heartbeat >= Duration::from_secs(10)
            || self.window == 0
            || self.window > 1 << 24
            || self.drain > Duration::from_hours(1)
            || self.retry.validate().is_err()
        {
            return Err(GroupReaderError::Configuration);
        }
        Ok(())
    }
}

/// One delivered record with the token that acknowledges its processing.
#[derive(Debug)]
pub struct GroupRecord {
    /// Original confirmed record.
    pub record: TopicRecord,
    /// Submit through [`GroupReader::acknowledge`] after processing.
    pub token: ProcessingToken,
}

/// Group membership or delivery failed. No error advances a group cursor.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GroupReaderError {
    /// Invalid name, member identity, or bounds.
    #[error("invalid group reader configuration")]
    Configuration,
    /// Reader closed or previously failed.
    #[error("group reader closed")]
    Closed,
    /// A stale membership instance was fenced. The background owner rejoins
    /// automatically; tokens from the old instance remain fenced.
    #[error("group membership is fenced")]
    Fenced,
    /// The group incarnation this reader joined was deleted. The reader
    /// stops; a new reader with the same name creates a new incarnation.
    #[error("consumer group was deleted")]
    Deleted,
    /// The creation offset is outside a source's confirmed retained history.
    #[error("partition {partition}: initial position {:?} outside {:?}..{:?}", position.cursor, position.retained_from, position.end)]
    InitialPosition {
        /// Numeric topic partition.
        partition: u32,
        /// Requested cursor and the source's confirmed bounds.
        position: group::InitialPosition,
    },
    /// The group's persisted cursor precedes retained history. Never skipped.
    #[error("partition {partition}: cursor {cursor:?} precedes retained history {retained_from:?}")]
    HistoryExpired {
        /// Numeric topic partition.
        partition: u32,
        /// Persisted exclusive cursor.
        cursor: Offset,
        /// Earliest retained offset.
        retained_from: Offset,
    },
    /// Record delivery failed.
    #[error(transparent)]
    Reader(#[from] TopicReaderError),
    /// Broker link or protocol failed.
    #[error(transparent)]
    Broker(#[from] BrokerLinkError),
    /// Processing progress failed for an owned source.
    #[error(transparent)]
    Progress(#[from] ProgressError),
}

impl From<Failure> for GroupReaderError {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Fenced => Self::Fenced,
            Failure::Closed => Self::Closed,
            Failure::Deleted => Self::Deleted,
            Failure::InitialPosition {
                partition,
                position,
            } => Self::InitialPosition {
                partition,
                position,
            },
            Failure::Response => Self::Broker(BrokerLinkError::Response),
            Failure::Broker(error) => Self::Broker(BrokerLinkError::Failed(error)),
        }
    }
}

#[derive(Debug)]
struct Source {
    number: u32,
    partition: PartitionIncarnation,
    plan_revision: u64,
    epoch: u64,
    window: Window,
    stop: CloseSignal,
}

#[derive(Debug)]
struct Draining {
    source: Source,
}

/// Consumer-group reader sharing the SDK's broker links. Each owned source
/// has one processing window; acknowledgments confirm cumulatively.
pub struct GroupReader {
    reader: TopicReader,
    config: GroupReaderConfig,
    budget: Arc<Semaphore>,
    view: watch::Receiver<View>,
    observations: watch::Sender<Vec<sources::Observation>>,
    activity: Arc<StateSignal>,
    observations_dirty: bool,
    leave: CloseSignal,
    membership: Option<tokio::task::JoinHandle<()>>,
    group: Option<ozzy_proto::ConsumerGroupId>,
    generation: u64,
    sources: BTreeMap<u32, Source>,
    draining: Vec<Draining>,
    failed: bool,
}

impl std::fmt::Debug for GroupReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupReader")
            .field("member", &self.config.member)
            .field("sources", &self.sources.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl GroupReader {
    /// Look up the topic and join `name` in the background. `next` delivers
    /// records once the coordinator assigns and a source activates them.
    pub async fn open(
        links: BrokerLinks,
        topic: &str,
        name: &str,
        config: GroupReaderConfig,
    ) -> Result<Self, GroupReaderError> {
        config.validate(name)?;
        links.require_groups()?;
        let metadata = links.topic(topic).await?;
        let partitions = metadata.partition_count();
        if partitions == 0 || partitions > group::MAX_PARTITIONS {
            return Err(GroupReaderError::Configuration);
        }
        let routes = links.reader_routes(metadata).await?;
        let reader = TopicReader::sources(routes.clone(), config.refresh)?;
        // Each source window and its shared confirmation state; a revoked copy
        // can still drain while the same source activates again.
        let budget = partitions
            .checked_mul(2)
            .and_then(|count| count.checked_mul(Window::reservation_bytes(config.window)?))
            .filter(|&bytes| bytes <= Semaphore::MAX_PERMITS)
            .ok_or(GroupReaderError::Configuration)?;
        let (view_sender, view) = watch::channel(View::default());
        let (observations, sources_receiver) = watch::channel(Vec::new());
        let leave = CloseSignal::default();
        let activity = Arc::new(StateSignal::default());
        let membership = membership::start(
            routes,
            Settings {
                name: name.to_owned(),
                member: config.member,
                initial: config.start,
                heartbeat: config.heartbeat,
                drain: config.drain,
                retry: config.retry,
            },
            membership::Channels {
                view: view_sender,
                sources: sources_receiver,
                leave: leave.clone(),
                activity: activity.clone(),
            },
        );
        Ok(Self {
            reader,
            config,
            budget: Arc::new(Semaphore::new(budget)),
            view,
            observations,
            activity,
            observations_dirty: false,
            leave,
            membership: Some(membership),
            group: None,
            generation: 0,
            sources: BTreeMap::new(),
            draining: Vec::new(),
            failed: false,
        })
    }

    /// Numeric partitions currently delivered to this member.
    pub fn partitions(&self) -> Vec<u32> {
        self.sources.keys().copied().collect()
    }

    /// Wait for one record of an owned source. Cancellation retains delivery
    /// state; a record is never returned twice by this instance.
    pub async fn next(&mut self) -> Result<GroupRecord, GroupReaderError> {
        loop {
            if self.failed {
                return Err(GroupReaderError::Closed);
            }
            let seen = self.activity.generation();
            if let Err(error) = self.reconcile() {
                self.fail();
                return Err(error);
            }
            let Self {
                reader,
                sources,
                activity,
                ..
            } = self;
            let gate = |number| {
                sources
                    .get(&number)
                    .is_some_and(|source| source.window.has_capacity())
            };
            let record = match reader.next_or(gate, activity.changed_after(seen)).await {
                Err(()) => continue,
                Ok(Ok(record)) => record,
                Ok(Err(error)) => {
                    self.fail();
                    return Err(error.into());
                }
            };
            // A source revoked after the gate no longer owns this record.
            let Some(source) = self.sources.get_mut(&record.partition) else {
                continue;
            };
            match source.window.deliver(record.offset) {
                Ok(token) => return Ok(GroupRecord { record, token }),
                // The source moved; its next owner resumes at the confirmed cursor.
                Err(ProgressError::Fenced | ProgressError::Closed) => {
                    let number = source.number;
                    self.revoke(number);
                }
                Err(error) => {
                    self.fail();
                    return Err(error.into());
                }
            }
        }
    }

    /// Submit one processed record. Confirmation is cumulative per source; a
    /// source that moved to another member rejects late acknowledgments.
    pub fn acknowledge(
        &mut self,
        token: ProcessingToken,
    ) -> Result<PendingProgress, ProgressError> {
        if self.view.has_changed().unwrap_or(true)
            && self.generation != self.view.borrow().generation
        {
            return Err(ProgressError::Fenced);
        }
        let identity = token.identity();
        if self.group != Some(identity.group) {
            return Err(ProgressError::Fenced);
        }
        let source = self
            .sources
            .values_mut()
            .chain(
                self.draining
                    .iter_mut()
                    .map(|draining| &mut draining.source),
            )
            .find(|source| {
                source.partition == identity.partition && source.epoch == identity.assignment_epoch
            })
            .ok_or(ProgressError::Fenced)?;
        let pending = source.window.submit(token)?;
        Ok(pending)
    }

    /// Stop delivery, let submitted acknowledgments confirm within the drain
    /// bound, then leave the group so its sources move without a timeout.
    pub async fn close(mut self) -> Result<(), GroupReaderError> {
        let numbers: Vec<u32> = self.sources.keys().copied().collect();
        for number in numbers {
            self.revoke(number);
        }
        loop {
            let seen = self.activity.generation();
            self.drain();
            self.publish_sources();
            if self.draining.is_empty() {
                break;
            }
            self.activity.changed_after(seen).await;
        }
        self.leave.close();
        if let Some(membership) = self.membership.take() {
            let _ = membership.await;
        }
        self.failed = true;
        self.reader.close().await?;
        Ok(())
    }

    fn fail(&mut self) {
        self.failed = true;
        let numbers: Vec<u32> = self.sources.keys().copied().collect();
        for number in numbers {
            self.reader.detach(number);
        }
        for mut source in std::mem::take(&mut self.sources).into_values() {
            source.window.close(ProgressError::Closed);
        }
        for mut draining in std::mem::take(&mut self.draining) {
            draining.source.window.close(ProgressError::Closed);
        }
        self.leave.close();
    }

    /// Apply the latest membership view: release revoked sources, activate
    /// confirmed ones, and report what this instance still owns.
    fn reconcile(&mut self) -> Result<(), GroupReaderError> {
        if !self.view.has_changed().unwrap_or(true) {
            return self.reconcile_progress();
        }
        let view = self.view.borrow_and_update().clone();
        if let Some(failure) = view.failure {
            return Err(failure.into());
        }
        self.rejoin(view.generation);
        self.revoke_untargeted(&view)?;
        if let Some(group) = view.group {
            if self.group.is_some_and(|current| current != group) {
                return Err(GroupReaderError::Fenced);
            }
            self.group = Some(group);
        }
        for activation in &view.activations {
            if self
                .sources
                .get(&activation.number)
                .is_some_and(|source| source.plan_revision == activation.plan_revision)
                || !view.targets(activation.partition, activation.plan_revision)
            {
                continue;
            }
            self.activate(activation)?;
        }
        self.reconcile_progress()
    }

    /// Revoke sources the view no longer targets or whose window stopped.
    fn revoke_untargeted(&mut self, view: &membership::View) -> Result<(), GroupReaderError> {
        let revoked: Vec<u32> = self
            .sources
            .values()
            .filter(|source| {
                !view.targets(source.partition, source.plan_revision)
                    || source.window.stopped().is_some()
            })
            .map(|source| source.number)
            .collect();
        for number in revoked {
            let source = &self.sources[&number];
            if let Some(error) = source.window.stopped()
                && !matches!(error, ProgressError::Fenced | ProgressError::Closed)
            {
                return Err(error.into());
            }
            self.revoke(number);
        }
        Ok(())
    }

    /// Start reading one confirmed target from its activation cursor, with
    /// its own progress window and driver.
    fn activate(&mut self, activation: &membership::Activation) -> Result<(), GroupReaderError> {
        if activation.cursor < activation.retained_from {
            return Err(GroupReaderError::HistoryExpired {
                partition: activation.number,
                cursor: activation.cursor,
                retained_from: activation.retained_from,
            });
        }
        // An older copy of this source can no longer confirm anything.
        for draining in &mut self.draining {
            if draining.source.partition == activation.partition {
                draining.source.window.close(ProgressError::Fenced);
            }
        }
        let identity = ProcessingIdentity {
            group: self
                .group
                .ok_or(GroupReaderError::Broker(BrokerLinkError::Response))?,
            partition: activation.partition,
            member: self.config.member,
            assignment_epoch: activation.epoch,
            policy: self.reader.policy(),
        };
        let (window, mut publisher) = Window::new(
            identity,
            activation.cursor,
            self.config.window,
            &self.budget,
        )?;
        let stop = CloseSignal::default();
        publisher.notify_on(self.activity.clone());
        drop(driver::start(
            self.reader.routes().clone(),
            activation.number,
            publisher,
            self.config.retry,
            Some(stop.clone()),
        )?);
        self.reader.attach(activation.number, activation.cursor)?;
        self.observations_dirty = true;
        self.sources.insert(
            activation.number,
            Source {
                number: activation.number,
                partition: activation.partition,
                plan_revision: activation.plan_revision,
                epoch: activation.epoch,
                window,
                stop,
            },
        );
        Ok(())
    }

    fn reconcile_progress(&mut self) -> Result<(), GroupReaderError> {
        while let Some((number, error)) = self
            .sources
            .values()
            .find_map(|source| source.window.stopped().map(|error| (source.number, error)))
        {
            if !matches!(error, ProgressError::Fenced | ProgressError::Closed) {
                return Err(error.into());
            }
            self.revoke(number);
        }
        for source in self.sources.values_mut() {
            source.window.retire(self.config.window);
        }
        self.drain();
        self.publish_sources();
        Ok(())
    }

    fn rejoin(&mut self, generation: u64) {
        if self.generation != generation {
            self.generation = generation;
            self.group = None;
            self.observations_dirty = true;
            for mut source in std::mem::take(&mut self.sources).into_values() {
                self.reader.detach(source.number);
                source.window.close(ProgressError::Fenced);
            }
            for mut draining in std::mem::take(&mut self.draining) {
                draining.source.window.close(ProgressError::Fenced);
            }
        }
    }

    /// Stop delivery now; confirmations continue until drained or the deadline.
    fn revoke(&mut self, number: u32) {
        let Some(source) = self.sources.remove(&number) else {
            return;
        };
        self.reader.detach(number);
        self.observations_dirty = true;
        self.draining.push(Draining { source });
    }

    fn drain(&mut self) {
        let window = self.config.window;
        let before = self.draining.len();
        self.draining.retain_mut(|draining| {
            draining.source.window.retire(window);
            if draining.source.window.drained() {
                draining.source.window.close(ProgressError::Fenced);
                return false;
            }
            true
        });
        self.observations_dirty |= before != self.draining.len();
    }

    fn publish_sources(&mut self) {
        if !std::mem::take(&mut self.observations_dirty) {
            return;
        }
        let observations = self
            .sources
            .values()
            .map(|source| (source, false))
            .chain(
                self.draining
                    .iter()
                    .map(|draining| (&draining.source, true)),
            )
            .map(|(source, draining)| sources::Observation {
                owned: Owned {
                    partition: source.partition,
                    plan_revision: source.plan_revision,
                    epoch: source.epoch,
                },
                progress: source.window.observer(),
                stop: source.stop.clone(),
                generation: self.generation,
                draining,
            })
            .collect();
        self.observations.send_replace(observations);
    }
}

impl Drop for GroupReader {
    fn drop(&mut self) {
        // Windows close with their sources; the owner leaves cooperatively.
        self.leave.close();
    }
}
