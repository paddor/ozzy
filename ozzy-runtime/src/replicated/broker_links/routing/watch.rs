use super::super::{
    BrokerLinkError, BrokerLinksConfig, Shared,
    driver::{self, Lease},
};
use super::{Rejection, State};
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{
    Envelope, GroupId, LinkSessionId, NodeId, Opcode, Packet, RequestId, directory, nack,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;

const TURN: usize = 32;

struct Entry {
    state: Arc<State>,
    session: LinkSessionId,
    watch: RequestId,
    revision: u64,
    retry_at: Duration,
    rejected: Option<u64>,
}

struct Pending {
    group: GroupId,
    request: RequestId,
    revision: u64,
    deadline: Duration,
    frame: Message,
    sent: bool,
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

    pub(in crate::replicated::broker_links) fn fence(&mut self) {
        self.pending = None;
        self.capacity_wait = false;
        self.cleanup = None;
        self.cleanup_remaining = self.entries.len();
        self.remaining = self
            .shared
            .routing
            .lock()
            .expect("SDK routing poisoned")
            .groups
            .len();
    }

    pub(in crate::replicated::broker_links) fn progress(
        &mut self,
        now: Duration,
    ) -> Result<bool, BrokerLinkError> {
        let cleanup = self.prune();
        self.progress_routes(now).map(|routes| routes || cleanup)
    }

    fn prune(&mut self) -> bool {
        if self.cleanup_remaining == 0 {
            return false;
        }
        let current = self.shared.sessions.session(self.remote);
        for _ in 0..TURN {
            if self.cleanup_remaining == 0 {
                break;
            }
            self.cleanup_remaining -= 1;
            let entry = self
                .cleanup
                .and_then(|last| self.entries.range((Excluded(last), Unbounded)).next())
                .or_else(|| self.entries.first_key_value());
            let Some((&group, entry)) = entry else {
                break;
            };
            self.cleanup = Some(group);
            if current != Some(entry.session)
                && entry
                    .state
                    .cache
                    .lock()
                    .expect("SDK route cache poisoned")
                    .disconnect(self.remote, entry.session)
            {
                entry.state.changed.notify_changed();
            }
        }
        self.cleanup_remaining != 0
    }

    fn progress_routes(&mut self, now: Duration) -> Result<bool, BrokerLinkError> {
        let generation = self.shared.changed.generation();
        if self.generation != generation
            || self.capacity_wait && self.frames.available_permits() != 0
        {
            self.generation = generation;
            self.remaining = self
                .shared
                .routing
                .lock()
                .expect("SDK routing poisoned")
                .groups
                .len();
            self.capacity_wait = false;
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.deadline <= now)
        {
            self.retry_pending(now)?;
        }
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
            self.remaining = self
                .shared
                .routing
                .lock()
                .expect("SDK routing poisoned")
                .groups
                .len();
        }
        let Some(session) = self.shared.sessions.session(self.remote) else {
            return Ok(false);
        };
        if self.pending.is_some() {
            return Ok(false);
        }
        for _ in 0..TURN {
            if self.remaining == 0 {
                break;
            }
            self.remaining -= 1;
            let target = {
                let registry = self.shared.routing.lock().expect("SDK routing poisoned");
                self.turn
                    .and_then(|last| registry.groups.range((Excluded(last), Unbounded)).next())
                    .or_else(|| registry.groups.first_key_value())
                    .map(|(&group, state)| (group, state.clone()))
            };
            let Some((group, state)) = target else {
                break;
            };
            self.turn = Some(group);
            if state.topic.broker(self.remote).is_none() {
                continue;
            }
            let revision = state.interests.lock().expect("SDK interests poisoned")[&group];
            let ready = self.entries.get(&group).is_none_or(|entry| {
                entry.session != session
                    || entry.retry_at <= now
                        && entry.rejected != Some(revision)
                        && (entry.revision != revision
                            || entry
                                .state
                                .cache
                                .lock()
                                .expect("SDK route cache poisoned")
                                .needs_snapshot(self.remote, entry.watch))
            });
            if !ready {
                continue;
            }
            let Ok(permit) = self.frames.clone().try_acquire_owned() else {
                self.capacity_wait = true;
                self.remaining += 1;
                break;
            };
            self.prepare(
                group,
                state,
                session,
                revision,
                now,
                &Arc::new(Lease { _permit: permit }),
            )?;
            return Ok(false);
        }
        Ok(self.remaining != 0 && !self.capacity_wait)
    }

    fn prepare(
        &mut self,
        group: GroupId,
        state: Arc<State>,
        session: LinkSessionId,
        revision: u64,
        now: Duration,
        lease: &Arc<Lease>,
    ) -> Result<(), BrokerLinkError> {
        if self
            .entries
            .get(&group)
            .is_none_or(|entry| entry.session != session)
        {
            if let Some(old) = self.entries.remove(&group) {
                self.ids.remove(&old.watch);
                self.timers.remove(&(old.retry_at, group));
            }
            let watch = self.shared.next_request()?;
            {
                let mut cache = state.cache.lock().expect("SDK route cache poisoned");
                cache.bind(self.remote, session)?;
                cache.register(self.remote, session, watch, &[group])?;
            }
            self.ids.insert(watch, group);
            self.entries.insert(
                group,
                Entry {
                    state,
                    session,
                    watch,
                    revision: 0,
                    retry_at: Duration::ZERO,
                    rejected: None,
                },
            );
        }
        let entry = &self.entries[&group];
        let request = self.shared.next_request()?;
        let header = directory::encode_request(
            Envelope {
                opcode: Opcode::StateSnapshotRequest,
                response: false,
                request_id: Some(request),
                sender: self.shared.local,
                session: Some(session),
            },
            &directory::SnapshotRequest {
                watch: entry.watch,
                groups: vec![group],
            },
            &mut self.metadata,
            self.shared.sessions.send_limits(self.remote)?.envelope,
            directory::Limits::default(),
        )?;
        self.pending = Some(Pending {
            group,
            request,
            revision,
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

    pub(in crate::replicated::broker_links) fn sent(&mut self) {
        self.pending.as_mut().expect("prepared watch").sent = true;
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
        let pending = self.pending.take().expect("outstanding watch");
        let entry = self
            .entries
            .get_mut(&pending.group)
            .expect("registered watch");
        entry
            .state
            .cache
            .lock()
            .expect("SDK route cache poisoned")
            .resync(
                self.remote,
                entry.session,
                directory::Resync { watch: entry.watch },
            )?;
        self.timers.remove(&(entry.retry_at, pending.group));
        entry.retry_at = now.saturating_add(self.retry);
        self.timers.insert((entry.retry_at, pending.group));
        entry.state.changed.notify_changed();
        self.remaining = self
            .shared
            .routing
            .lock()
            .expect("SDK routing poisoned")
            .groups
            .len();
        Ok(())
    }

    pub(in crate::replicated::broker_links) fn receive(
        &mut self,
        packet: Packet<'_>,
        now: Duration,
    ) -> Result<bool, BrokerLinkError> {
        if packet.envelope.session != self.shared.sessions.session(self.remote)
            || packet.envelope.session.is_none()
        {
            return Ok(false);
        }
        let is_reply = self.pending.as_ref().is_some_and(|pending| {
            pending.sent
                && packet.envelope.response
                && packet.envelope.request_id == Some(pending.request)
        });
        let limits = self.limits;
        if is_reply {
            return self.receive_reply(packet, limits, now);
        }
        if !matches!(
            packet.envelope.opcode,
            Opcode::StateUpdate | Opcode::StateResync
        ) || packet.envelope.response
        {
            return Ok(false);
        }
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
        let entry = &self.entries[&group];
        if packet.envelope.session != Some(entry.session) {
            return Ok(true);
        }
        let mut cache = entry.state.cache.lock().expect("SDK route cache poisoned");
        match packet.envelope.opcode {
            Opcode::StateUpdate => {
                cache.update(
                    self.remote,
                    entry.session,
                    directory::decode_update(packet, limits, directory::Limits::default())?,
                )?;
            }
            Opcode::StateResync => {
                cache.resync(
                    self.remote,
                    entry.session,
                    directory::decode_resync(packet, limits)?,
                )?;
            }
            _ => unreachable!(),
        }
        drop(cache);
        entry.state.changed.notify_changed();
        self.remaining = self
            .shared
            .routing
            .lock()
            .expect("SDK routing poisoned")
            .groups
            .len();
        Ok(true)
    }

    fn receive_reply(
        &mut self,
        packet: Packet<'_>,
        limits: ozzy_proto::EnvelopeLimits,
        now: Duration,
    ) -> Result<bool, BrokerLinkError> {
        if !matches!(packet.envelope.opcode, Opcode::StateSnapshot | Opcode::Nack) {
            return Ok(true);
        }
        if packet.envelope.opcode == Opcode::Nack {
            let reply = nack::decode(packet, limits)?;
            let (group, revision) = self
                .pending
                .as_ref()
                .map(|pending| (pending.group, pending.revision))
                .unwrap();
            self.retry_pending(now)?;
            if reply.retry == nack::RetryClass::Permanent {
                let entry = self.entries.get_mut(&group).unwrap();
                entry.rejected = Some(revision);
                entry
                    .state
                    .rejections
                    .lock()
                    .expect("SDK route rejections poisoned")
                    .insert(
                        (group, self.remote),
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
        let pending = self.pending.as_ref().expect("matching watch reply");
        let entry = self
            .entries
            .get_mut(&pending.group)
            .expect("registered watch");
        if snapshot.watch != entry.watch {
            return Ok(true);
        }
        entry
            .state
            .cache
            .lock()
            .expect("SDK route cache poisoned")
            .snapshot(self.remote, entry.session, snapshot)?;
        entry
            .state
            .rejections
            .lock()
            .expect("SDK route rejections poisoned")
            .remove(&(pending.group, self.remote));
        entry.revision = pending.revision;
        self.timers.remove(&(entry.retry_at, pending.group));
        entry.retry_at = Duration::ZERO;
        entry.rejected = None;
        entry.state.changed.notify_changed();
        self.pending = None;
        self.remaining = self
            .shared
            .routing
            .lock()
            .expect("SDK routing poisoned")
            .groups
            .len();
        Ok(true)
    }
}
