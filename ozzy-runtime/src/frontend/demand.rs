//! Coalesced grant requests. No request retains a transport payload.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ozzy_proto::NodeId;
use tokio::sync::mpsc;

use super::{Binding, GrantTarget, Routed, Service, SetupError, Subject};
use crate::{dispatch::Class, signal::StateSignal};

/// A refused attempt needs destination-owned capacity. This routing hint grants
/// no producer validity, partition authority, or accepted application work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GrantRequest {
    /// Independently established session. Check the current link before granting.
    pub binding: Binding,
    /// Configured destination and independent admission class.
    pub route: Routed,
}

impl GrantRequest {
    /// Control covers this shard. Data stays partition- and writer-scoped.
    pub fn target(self) -> GrantTarget {
        match self.route.class {
            Class::Control => GrantTarget::Control(self.route.placement.shard),
            Class::Data => GrantTarget::Partition(Subject {
                group: self.route.placement.group,
                writer: self.route.writer,
            }),
        }
    }
}

#[derive(Clone, Debug)]
struct State {
    pending: [Vec<Option<(GrantRequest, usize)>>; 2],
    /// Occupied entries of both classes.
    count: usize,
    per_peer: usize,
    closed: bool,
}

#[derive(Debug)]
struct Shared {
    current: ArcSwap<State>,
    changed: StateSignal,
}

#[derive(Debug)]
pub(super) struct Requests {
    state: State,
    shared: Arc<Shared>,
    dismissed: mpsc::Receiver<GrantRequest>,
}

/// One shard's bounded metadata observations. Requests coalesce until grant
/// installation or dismissal. There is still one payload fanring per shard.
#[derive(Debug)]
pub struct GrantRequests {
    shared: Arc<Shared>,
    dismiss: mpsc::Sender<GrantRequest>,
    observed: Arc<State>,
    suppressed: Vec<GrantRequest>,
    next: [usize; 2],
    class: usize,
}

impl GrantRequests {
    /// Capture before inspecting requests and destination capacity.
    pub fn generation(&self) -> u64 {
        self.shared.changed.generation()
    }

    /// Cancel-safe state observation, independent of the next inspection.
    pub fn changed_after(&self, generation: u64) -> impl Future<Output = ()> + use<> {
        let shared = self.shared.clone();
        async move { shared.changed.changed_after(generation).await }
    }

    /// Rotate between classes and pending scopes. Failed grant admission leaves
    /// the request present; wait for destination capacity instead of spinning.
    pub fn next_request(&mut self) -> Option<GrantRequest> {
        self.next_request_with_bytes().map(|(request, _)| request)
    }

    /// Capture scope and backing charge together. Dispatcher installation may
    /// remove this demand immediately afterward; the observed size stays valid.
    pub fn next_request_with_bytes(&mut self) -> Option<(GrantRequest, usize)> {
        self.refresh();
        let state = &self.observed;
        if state.count == 0 || state.closed {
            return None;
        }
        for _ in 0..2 {
            let class = self.class;
            self.class ^= 1;
            let pending = &state.pending[class];
            for _ in 0..pending.len() {
                let index = self.next[class];
                self.next[class] = (index + 1) % pending.len();
                if let Some(request) = pending[index]
                    && !self.suppressed.contains(&request.0)
                {
                    return Some(request);
                }
            }
        }
        None
    }

    /// Copy at most `maximum` pending scopes into `output`, rotating between
    /// classes and scopes across calls. Requests stay pending until installation
    /// or dismissal reaches the dispatcher.
    pub fn pending_into(&mut self, output: &mut Vec<(GrantRequest, usize)>, maximum: usize) {
        self.refresh();
        let state = &self.observed;
        if state.count == 0 || state.closed {
            return;
        }
        for _ in 0..2 {
            let class = self.class;
            self.class ^= 1;
            let pending = &state.pending[class];
            let first = self.next[class];
            for offset in 0..pending.len() {
                if output.len() == maximum {
                    return;
                }
                let index = (first + offset) % pending.len();
                if let Some(request) = pending[index]
                    && !self.suppressed.contains(&request.0)
                {
                    output.push(request);
                    self.next[class] = (index + 1) % pending.len();
                }
            }
        }
    }

    /// Dismiss this exact session/scope after extending an existing token or
    /// refusing follower work. A later retry can ask again. Releases no payload.
    pub fn dismiss(&mut self, request: GrantRequest) {
        self.refresh();
        if self.observed.closed
            || self.suppressed.contains(&request)
            || !self
                .observed
                .pending
                .iter()
                .flatten()
                .any(|entry| entry.is_some_and(|(pending, _)| pending == request))
        {
            return;
        }
        match self.dismiss.try_send(request) {
            Ok(()) => self.suppressed.push(request),
            Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                panic!("bounded grant dismissal mailbox overflowed")
            }
        }
    }

    /// Conservative backing charge observed by the receive adapter for this
    /// refused scope. No payload is retained. Repeated demand keeps the maximum.
    pub fn retained_bytes(&self, request: GrantRequest) -> Option<usize> {
        let state = self.shared.current.load_full();
        if Arc::ptr_eq(&state, &self.observed) && self.suppressed.contains(&request) {
            return None;
        }
        state.pending[index(request.route.class)]
            .iter()
            .flatten()
            .find(|(pending, _)| *pending == request)
            .map(|(_, bytes)| *bytes)
    }

    /// Frontend exit grants no continuing admission rights.
    pub fn is_closed(&self) -> bool {
        self.shared.current.load().closed
    }

    fn refresh(&mut self) {
        let current = self.shared.current.load_full();
        if !Arc::ptr_eq(&current, &self.observed) {
            self.observed = current;
            self.suppressed.clear();
        }
    }
}

impl Requests {
    fn new(maximum: usize, per_peer: usize) -> Result<(Self, GrantRequests), SetupError> {
        if !(1..=65_536).contains(&maximum) || per_peer == 0 {
            return Err(SetupError::Limits);
        }
        let table = || {
            let mut slots = Vec::new();
            slots
                .try_reserve_exact(maximum)
                .map_err(|_| SetupError::Allocation)?;
            slots.resize(maximum, None);
            Ok(slots)
        };
        let state = State {
            pending: [table()?, table()?],
            count: 0,
            per_peer,
            closed: false,
        };
        let shared = Arc::new(Shared {
            current: ArcSwap::from_pointee(state.clone()),
            changed: StateSignal::default(),
        });
        let (dismiss, dismissed) = mpsc::channel(maximum * 2);
        Ok((
            Self {
                state,
                shared: shared.clone(),
                dismissed,
            },
            GrantRequests {
                observed: shared.current.load_full(),
                shared,
                dismiss,
                suppressed: Vec::new(),
                next: [0; 2],
                class: 0,
            },
        ))
    }

    pub(super) fn request(&mut self, request: GrantRequest, bytes: usize) {
        let mut changed = self.drain_dismissed();
        let pending = &mut self.state.pending[index(request.route.class)];
        if let Some((_, old_bytes)) = pending
            .iter_mut()
            .flatten()
            .find(|(old, _)| *old == request)
        {
            if bytes > *old_bytes {
                *old_bytes = bytes;
                changed = true;
            }
            self.publish_if(changed);
            return;
        }
        if pending
            .iter()
            .flatten()
            .filter(|(old, _)| old.binding.peer == request.binding.peer)
            .count()
            >= self.state.per_peer
        {
            self.publish_if(changed);
            return;
        }
        if let Some(slot) = pending.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some((request, bytes));
            self.state.count += 1;
            changed = true;
        }
        self.publish_if(changed);
    }

    pub(super) fn installed(&mut self, peer: NodeId, target: GrantTarget, class: Class) {
        let changed = self.drain_dismissed();
        let removed = self.state.remove(|request| {
            request.binding.peer == peer
                && request.route.class == class
                && match target {
                    GrantTarget::Control(shard) => request.route.placement.shard == shard,
                    GrantTarget::Partition(subject) => {
                        request.route.placement.group == subject.group
                            && request.route.writer == subject.writer
                    }
                }
        });
        self.publish_if(changed || removed);
    }

    pub(super) fn fence(&mut self, peer: NodeId) {
        let changed = self.drain_dismissed();
        let removed = self.state.remove(|request| request.binding.peer == peer);
        self.publish_if(changed || removed);
    }

    fn drain_dismissed(&mut self) -> bool {
        let mut changed = false;
        while let Ok(request) = self.dismissed.try_recv() {
            changed |= self.state.remove(|pending| *pending == request);
        }
        changed
    }

    fn publish_if(&self, changed: bool) {
        if changed {
            self.shared.current.store(Arc::new(self.state.clone()));
            self.shared.changed.notify_changed();
        }
    }
}

impl State {
    fn remove(&mut self, predicate: impl Fn(&GrantRequest) -> bool) -> bool {
        if self.count == 0 {
            return false;
        }
        let mut removed = 0;
        for slot in self.pending.iter_mut().flatten() {
            if slot.as_ref().is_some_and(|(request, _)| predicate(request)) {
                *slot = None;
                removed += 1;
            }
        }
        self.count -= removed;
        removed != 0
    }
}

impl Drop for Requests {
    fn drop(&mut self) {
        self.state.closed = true;
        self.state
            .pending
            .iter_mut()
            .flatten()
            .for_each(|slot| *slot = None);
        self.state.count = 0;
        self.publish_if(true);
    }
}

impl Service {
    /// Reserve independent request metadata for one configured shard before
    /// traffic. `maximum` bounds pending scopes per class; the dispatcher grant
    /// limit bounds each peer within the table. Configure at most once per shard.
    pub fn grant_requests(
        &mut self,
        shard: u32,
        maximum: usize,
    ) -> Result<GrantRequests, SetupError> {
        if !self.dispatcher.routes.shards.contains(&shard) || self.requests.contains_key(&shard) {
            return Err(SetupError::Destination);
        }
        let (requests, receiver) = Requests::new(maximum, self.dispatcher.limits.grants_per_class)?;
        self.requests.insert(shard, requests);
        Ok(receiver)
    }

    pub(super) fn fence_requests(&mut self, peer: NodeId) {
        for requests in self.requests.values_mut() {
            requests.fence(peer);
        }
    }
}

const fn index(class: Class) -> usize {
    match class {
        Class::Data => 0,
        Class::Control => 1,
    }
}

#[cfg(test)]
mod tests;
