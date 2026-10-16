use super::super::{
    BrokerLinkError, BrokerLinksConfig, Established, Shared,
    driver::{self, Lease},
};
use super::{Registry, Rejection, State};
use crate::topic_metadata::RouteCache;
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{
    Envelope, GroupId, LinkSessionId, NodeId, Opcode, Packet, RequestId, directory, nack,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;

const TURN: usize = 32;

struct Entry {
    state: Arc<State>,
    /// This broker's routes for the group, from the entry's one watch.
    cache: RouteCache,
    session: LinkSessionId,
    watch: RequestId,
    revision: u64,
    retry_at: Duration,
    rejected: Option<u64>,
}

impl Entry {
    /// Publish the group's routes from `broker` to callers.
    fn publish(&self, broker: NodeId, group: GroupId) {
        self.state
            .publish_routes(group, broker, self.cache.published_observations(group));
    }

    /// Apply `update` to the group's cache, then publish its routes.
    fn update<R>(
        &mut self,
        broker: NodeId,
        group: GroupId,
        update: impl FnOnce(&mut RouteCache) -> R,
    ) -> R {
        let result = update(&mut self.cache);
        self.publish(broker, group);
        result
    }
}

struct Pending {
    group: GroupId,
    request: RequestId,
    revision: u64,
    deadline: Duration,
    frame: Message,
    sent: bool,
}

/// A group whose routes to request, at the routing revision they answer.
#[derive(Clone, Copy)]
struct Snapshot {
    group: GroupId,
    revision: u64,
}

/// One bounded watcher beside an existing broker socket owner. There is one
/// outstanding snapshot per broker and two owned frame slots through release.
pub(in crate::replicated::broker_links) struct Watcher {
    shared: Arc<Shared>,
    remote: NodeId,
    entries: BTreeMap<GroupId, Entry>,
    ids: BTreeMap<RequestId, GroupId>,
    pending: Option<Pending>,
    frames: Arc<Semaphore>,
    metadata: Vec<u8>,
    timeout: Duration,
    retry: Duration,
    generation: u64,
    remaining: usize,
    turn: Option<GroupId>,
    capacity_wait: bool,
    timers: BTreeSet<(Duration, GroupId)>,
    limits: ozzy_proto::EnvelopeLimits,
    cleanup: Option<GroupId>,
    cleanup_remaining: usize,
}

impl Watcher {
    pub(in crate::replicated::broker_links) fn new(
        shared: Arc<Shared>,
        remote: NodeId,
        config: &BrokerLinksConfig,
    ) -> Self {
        Self {
            shared,
            remote,
            entries: BTreeMap::new(),
            ids: BTreeMap::new(),
            pending: None,
            frames: Arc::new(Semaphore::new(if config.routing_bytes == 0 {
                0
            } else {
                2
            })),
            metadata: Vec::with_capacity(if config.routing_bytes == 0 { 0 } else { 64 }),
            timeout: config.request_timeout,
            retry: config.retry_interval,
            generation: 0,
            remaining: 0,
            turn: None,
            capacity_wait: false,
            timers: BTreeSet::new(),
            limits: config.parameters.receive.envelope,
            cleanup: None,
            cleanup_remaining: 0,
        }
    }

    /// Visit every group again, from the turn cursor.
    fn rescan(&mut self) {
        self.remaining = usize::MAX;
    }

    pub(in crate::replicated::broker_links) fn fence(&mut self) {
        self.pending = None;
        self.capacity_wait = false;
        self.cleanup = None;
        self.cleanup_remaining = self.entries.len();
        self.rescan();
    }

    /// One bounded turn over the `routing` groups to watch on the live `link`.
    pub(in crate::replicated::broker_links) fn progress(
        &mut self,
        now: Duration,
        link: Option<Established>,
        routing: &Registry,
    ) -> Result<bool, BrokerLinkError> {
        let cleanup = self.prune(link.map(|link| link.session));
        self.progress_routes(now, link, routing)
            .map(|routes| routes || cleanup)
    }

    /// Drop cached routes of watches bound to a session other than `current`.
    fn prune(&mut self, current: Option<LinkSessionId>) -> bool {
        if self.cleanup_remaining == 0 {
            return false;
        }
        for _ in 0..TURN {
            if self.cleanup_remaining == 0 {
                break;
            }
            self.cleanup_remaining -= 1;
            let Some(group) =
                crate::turn::after(&self.entries, self.cleanup.as_ref()).map(|(&group, _)| group)
            else {
                break;
            };
            self.cleanup = Some(group);
            let Some(entry) = self.entries.get_mut(&group) else {
                break;
            };
            let session = entry.session;
            if current != Some(session)
                && entry.update(self.remote, group, |cache| {
                    cache.disconnect(self.remote, session)
                })
            {
                entry.state.changed.notify_changed();
            }
        }
        self.cleanup_remaining != 0
    }

    fn progress_routes(
        &mut self,
        now: Duration,
        link: Option<Established>,
        routing: &Registry,
    ) -> Result<bool, BrokerLinkError> {
        // New interest or freed capacity restarts the scan; so do a fenced
        // session, due timers, and broker notices.
        let generation = self.shared.interests.generation();
        if self.generation != generation
            || self.capacity_wait && self.frames.available_permits() != 0
        {
            self.generation = generation;
            self.rescan();
            self.capacity_wait = false;
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.deadline <= now)
        {
            self.retry_pending(now)?;
        }
        self.expire_timers(now);
        let Some(link) = link else {
            return Ok(false);
        };
        if self.pending.is_some() {
            return Ok(false);
        }
        // A rescan visits each group once.
        self.remaining = self.remaining.min(routing.groups.len());
        for _ in 0..TURN {
            if self.remaining == 0 {
                break;
            }
            self.remaining -= 1;
            let Some((&group, state)) = crate::turn::after(&routing.groups, self.turn.as_ref())
            else {
                break;
            };
            self.turn = Some(group);
            if state.topic.broker(self.remote).is_none() {
                continue;
            }
            let snapshot = Snapshot {
                group,
                revision: state.revision(group),
            };
            if snapshot.revision == 0 || !self.wants(snapshot, link.session, now) {
                continue;
            }
            let Ok(permit) = self.frames.clone().try_acquire_owned() else {
                self.capacity_wait = true;
                self.remaining += 1;
                break;
            };
            self.bind(group, state, link.session)?;
            let lease = Arc::new(Lease { _permit: permit });
            self.prepare(snapshot, link, now, &lease)?;
            return Ok(false);
        }
        Ok(self.remaining != 0 && !self.capacity_wait)
    }

    /// Clear the retry time of each group whose timer is due by `now`, a
    /// bounded turn at a time.
    fn expire_timers(&mut self, now: Duration) {
        for _ in 0..TURN {
            let Some(&(deadline, group)) = self.timers.first() else {
                break;
            };
            if deadline > now {
                break;
            }
            self.timers.pop_first();
            if let Some(entry) = self.entries.get_mut(&group) {
                entry.retry_at = Duration::ZERO;
            }
            self.rescan();
        }
    }

    /// Whether `snapshot` needs a request on the live `session`: the group
    /// has no entry on it, or its retry is due, the broker did not reject
    /// this revision, and the revision or its routes are stale.
    fn wants(&self, snapshot: Snapshot, session: LinkSessionId, now: Duration) -> bool {
        self.entries.get(&snapshot.group).is_none_or(|entry| {
            entry.session != session
                || entry.retry_at <= now
                    && entry.rejected != Some(snapshot.revision)
                    && (entry.revision != snapshot.revision
                        || entry.cache.needs_snapshot(self.remote, entry.watch))
        })
    }

    /// Replace the entry of `group` unless it already watches on `session`.
    fn bind(
        &mut self,
        group: GroupId,
        state: &Arc<State>,
        session: LinkSessionId,
    ) -> Result<(), BrokerLinkError> {
        if self
            .entries
            .get(&group)
            .is_some_and(|entry| entry.session == session)
        {
            return Ok(());
        }
        if let Some(old) = self.entries.remove(&group) {
            self.ids.remove(&old.watch);
            self.timers.remove(&(old.retry_at, group));
        }
        let watch = self.shared.next_request()?;
        // One registration: this entry's watch of its one group.
        let mut cache = RouteCache::shared(state.topic.clone(), 1, 1)?;
        cache.bind(self.remote, session)?;
        cache.register(self.remote, session, watch, &[group])?;
        let entry = Entry {
            state: state.clone(),
            cache,
            session,
            watch,
            revision: 0,
            retry_at: Duration::ZERO,
            rejected: None,
        };
        entry.publish(self.remote, group);
        self.ids.insert(watch, group);
        self.entries.insert(group, entry);
        Ok(())
    }

    /// Frame the snapshot request of the bound entry on the live `link`.
    fn prepare(
        &mut self,
        snapshot: Snapshot,
        link: Established,
        now: Duration,
        lease: &Arc<Lease>,
    ) -> Result<(), BrokerLinkError> {
        let group = snapshot.group;
        let entry = &self.entries[&group];
        let request = self.shared.next_request()?;
        let header = directory::encode_request(
            Envelope {
                opcode: Opcode::StateSnapshotRequest,
                response: false,
                request_id: Some(request),
                sender: self.shared.local,
                session: Some(link.session),
            },
            &directory::SnapshotRequest {
                watch: entry.watch,
                groups: vec![group],
            },
            &mut self.metadata,
            link.send.envelope,
            directory::Limits::default(),
        )?;
        self.pending = Some(Pending {
            group,
            request,
            revision: snapshot.revision,
            deadline: now.saturating_add(self.timeout),
            frame: driver::track(
                &crate::native_frames::message(
                    self.remote.as_bytes(),
                    header,
                    &self.metadata,
                    Bytes::new(),
                ),
                lease,
                false,
            ),
            sent: false,
        });
        Ok(())
    }

    pub(in crate::replicated::broker_links) fn frame(&self) -> Option<&Message> {
        self.pending
            .as_ref()
            .filter(|pending| !pending.sent)
            .map(|pending| &pending.frame)
    }

    /// Mark the frame last returned by [`Self::frame`] as sent.
    pub(in crate::replicated::broker_links) fn sent(&mut self) {
        if let Some(pending) = &mut self.pending {
            pending.sent = true;
        }
    }

    pub(in crate::replicated::broker_links) fn deadline(&self) -> Option<Duration> {
        self.pending
            .iter()
            .map(|pending| pending.deadline)
            .chain(self.timers.first().map(|&(deadline, _)| deadline))
            .min()
    }

    pub(in crate::replicated::broker_links) async fn capacity_ready(&self) {
        if self.capacity_wait {
            let _ = self.frames.acquire().await;
        } else {
            std::future::pending::<()>().await;
        }
    }

    fn retry_pending(&mut self, now: Duration) -> Result<(), BrokerLinkError> {
        if let Some(pending) = self.pending.take()
            && let Some(entry) = self.entries.get_mut(&pending.group)
        {
            let (session, watch) = (entry.session, entry.watch);
            entry.update(self.remote, pending.group, |cache| {
                cache.resync(self.remote, session, directory::Resync { watch })
            })?;
            self.timers.remove(&(entry.retry_at, pending.group));
            entry.retry_at = now.saturating_add(self.retry);
            self.timers.insert((entry.retry_at, pending.group));
            entry.state.changed.notify_changed();
        }
        self.rescan();
        Ok(())
    }

    /// Consume a watch reply or notice on the live `session`. `false` when
    /// `packet` is neither.
    pub(in crate::replicated::broker_links) fn receive(
        &mut self,
        packet: Packet<'_>,
        session: Option<LinkSessionId>,
        now: Duration,
    ) -> Result<bool, BrokerLinkError> {
        if packet.envelope.session != session || session.is_none() {
            return Ok(false);
        }
        let reply = self
            .pending
            .as_ref()
            .filter(|pending| {
                pending.sent
                    && packet.envelope.response
                    && packet.envelope.request_id == Some(pending.request)
            })
            .map(|pending| (pending.group, pending.revision));
        if let Some((group, revision)) = reply {
            return self.receive_reply(packet, now, group, revision);
        }
        let resync = match (packet.envelope.opcode, packet.envelope.response) {
            (Opcode::StateUpdate, false) => false,
            (Opcode::StateResync, false) => true,
            _ => return Ok(false),
        };
        let Some(watch) = packet
            .metadata
            .get(1..17)
            .and_then(|bytes| bytes.try_into().ok())
            .map(RequestId::from_bytes)
        else {
            return Ok(true);
        };
        let Some(&group) = self.ids.get(&watch) else {
            return Ok(true);
        };
        let Some(entry) = self.entries.get_mut(&group) else {
            return Ok(true);
        };
        let session = entry.session;
        if packet.envelope.session != Some(session) {
            return Ok(true);
        }
        let limits = self.limits;
        if resync {
            let notice = directory::decode_resync(packet, limits)?;
            entry.update(self.remote, group, |cache| {
                cache.resync(self.remote, session, notice)
            })?;
        } else {
            let update = directory::decode_update(packet, limits, directory::Limits::default())?;
            entry.update(self.remote, group, |cache| {
                cache.update(self.remote, session, update)
            })?;
        }
        entry.state.changed.notify_changed();
        self.rescan();
        Ok(true)
    }

    /// Answer to the outstanding snapshot request for `group` at `revision`.
    fn receive_reply(
        &mut self,
        packet: Packet<'_>,
        now: Duration,
        group: GroupId,
        revision: u64,
    ) -> Result<bool, BrokerLinkError> {
        if !matches!(packet.envelope.opcode, Opcode::StateSnapshot | Opcode::Nack) {
            return Ok(true);
        }
        let limits = self.limits;
        if packet.envelope.opcode == Opcode::Nack {
            let reply = nack::decode(packet, limits)?;
            self.retry_pending(now)?;
            if reply.retry == nack::RetryClass::Permanent
                && let Some(entry) = self.entries.get_mut(&group)
            {
                entry.rejected = Some(revision);
                entry.state.reject(
                    group,
                    self.remote,
                    Rejection {
                        session: entry.session,
                        code: reply.code,
                        retry: reply.retry,
                    },
                );
                entry.state.changed.notify_changed();
            }
            return Ok(true);
        }
        let snapshot = directory::decode_snapshot(packet, limits, directory::Limits::default())?;
        let Some(entry) = self.entries.get_mut(&group) else {
            self.pending = None;
            return Ok(true);
        };
        if snapshot.watch != entry.watch {
            return Ok(true);
        }
        let session = entry.session;
        entry.update(self.remote, group, |cache| {
            cache.snapshot(self.remote, session, snapshot)
        })?;
        entry.state.clear_rejection(group, self.remote);
        entry.revision = revision;
        self.timers.remove(&(entry.retry_at, group));
        entry.retry_at = Duration::ZERO;
        entry.rejected = None;
        entry.state.changed.notify_changed();
        self.pending = None;
        self.rescan();
        Ok(true)
    }
}
