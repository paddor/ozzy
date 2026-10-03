//! Aggregate transport reservations, including unused reply capacity and aliases.

use ozzy_proto::NodeId;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::AppendLinkLimits;
use crate::signal::StateSignal;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Cost {
    pub(super) writers: usize,
    pub(super) requests: usize,
    pub(super) records: usize,
    pub(super) bytes: usize,
    pub(super) peer_bytes: usize,
    pub(super) progress_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct PeerLimit {
    pub(super) node: NodeId,
    pub(super) records: u64,
    pub(super) bytes: u64,
}

#[derive(Debug, Default)]
struct Used {
    writers: AtomicUsize,
    requests: AtomicUsize,
    records: AtomicUsize,
    bytes: AtomicUsize,
}

#[derive(Debug)]
struct PeerUsed {
    node: NodeId,
    records: AtomicUsize,
    bytes: AtomicUsize,
}

impl PeerUsed {
    fn new(node: NodeId) -> Self {
        Self {
            node,
            records: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug)]
pub(super) struct Budget {
    limits: Option<AppendLinkLimits>,
    used: Used,
    // There are at most three configured brokers. A slot is assigned once;
    // request admission only reads its atomic counters after that.
    peers: [OnceLock<PeerUsed>; 3],
    // Opening and dropping a writer is rare. APPEND admission never takes this.
    progress: Mutex<BTreeMap<usize, usize>>,
    progress_max: AtomicUsize,
    pub(super) changed: StateSignal,
}

fn fits(held: usize, extra: usize, limit: usize) -> bool {
    held.checked_add(extra).is_some_and(|sum| sum <= limit)
}

fn reserve(counter: &AtomicUsize, extra: usize, limit: usize) -> bool {
    if extra == 0 {
        return true;
    }
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
            held.checked_add(extra).filter(|&sum| sum <= limit)
        })
        .is_ok()
}

fn release(counter: &AtomicUsize, amount: usize) {
    if amount != 0 {
        let previous = counter.fetch_sub(amount, Ordering::AcqRel);
        assert!(previous >= amount, "SDK APPEND reservation underflow");
    }
}

impl Budget {
    pub(super) fn new(limits: Option<AppendLinkLimits>) -> Self {
        Self {
            limits,
            used: Used::default(),
            peers: std::array::from_fn(|_| OnceLock::new()),
            progress: Mutex::new(BTreeMap::new()),
            progress_max: AtomicUsize::new(0),
            changed: StateSignal::default(),
        }
    }

    fn peer(&self, node: NodeId) -> Option<&PeerUsed> {
        loop {
            if let Some(found) = self
                .peers
                .iter()
                .filter_map(OnceLock::get)
                .find(|p| p.node == node)
            {
                return Some(found);
            }
            let vacant = self.peers.iter().find(|slot| slot.get().is_none())?;
            if vacant.set(PeerUsed::new(node)).is_ok() {
                return vacant.get();
            }
        }
    }

    pub(super) fn possible(&self, cost: Cost, peer: Option<PeerLimit>) -> bool {
        self.fits(cost, peer, false)
    }

    pub(super) fn available(&self, cost: Cost, peer: Option<PeerLimit>) -> bool {
        self.fits(cost, peer, true)
    }

    fn fits(&self, cost: Cost, peer: Option<PeerLimit>, current: bool) -> bool {
        let Some(limits) = self.limits else {
            return false;
        };
        let held = |counter: &AtomicUsize| {
            if current {
                counter.load(Ordering::Acquire)
            } else {
                0
            }
        };
        let progress = if current {
            self.progress_max.load(Ordering::Acquire)
        } else {
            0
        };
        fits(held(&self.used.writers), cost.writers, limits.writers)
            && fits(held(&self.used.requests), cost.requests, limits.requests)
            && fits(held(&self.used.records), cost.records, limits.records)
            && fits(held(&self.used.bytes), cost.bytes, limits.bytes)
            && (cost.writers == 0
                || held(&self.used.bytes)
                    .checked_add(cost.bytes)
                    .and_then(|bytes| bytes.checked_add(progress.max(cost.progress_bytes)))
                    .is_some_and(|bytes| bytes <= limits.bytes))
            && peer.is_none_or(|peer| {
                let existing = if current { self.peer(peer.node) } else { None };
                let records = existing.map_or(0, |used| held(&used.records));
                let bytes = existing.map_or(0, |used| held(&used.bytes));
                usize::try_from(peer.records).is_ok_and(|limit| fits(records, cost.records, limit))
                    && usize::try_from(peer.bytes)
                        .is_ok_and(|limit| fits(bytes, cost.peer_bytes, limit))
            })
    }

    pub(super) fn acquire(
        self: &Arc<Self>,
        cost: Cost,
        peer: Option<PeerLimit>,
    ) -> Option<Arc<Lease>> {
        let limits = self.limits?;
        let peer_used = peer.and_then(|limit| self.peer(limit.node));
        if peer.is_some() && peer_used.is_none() {
            return None;
        }
        let mut progress =
            (cost.writers != 0).then(|| self.progress.lock().expect("SDK writer budget poisoned"));
        let headroom = progress.as_ref().map_or(0, |_| {
            self.progress_max
                .load(Ordering::Acquire)
                .max(cost.progress_bytes)
        });
        let bytes_limit = limits.bytes.checked_sub(headroom)?;

        if !reserve(&self.used.writers, cost.writers, limits.writers) {
            return None;
        }
        if !reserve(&self.used.requests, cost.requests, limits.requests) {
            release(&self.used.writers, cost.writers);
            self.changed.notify_changed();
            return None;
        }
        if !reserve(&self.used.records, cost.records, limits.records) {
            release(&self.used.requests, cost.requests);
            release(&self.used.writers, cost.writers);
            self.changed.notify_changed();
            return None;
        }
        if !reserve(&self.used.bytes, cost.bytes, bytes_limit) {
            release(&self.used.records, cost.records);
            release(&self.used.requests, cost.requests);
            release(&self.used.writers, cost.writers);
            self.changed.notify_changed();
            return None;
        }
        if let (Some(limit), Some(used)) = (peer, peer_used) {
            let records = usize::try_from(limit.records).ok();
            let bytes = usize::try_from(limit.bytes).ok();
            if !records.is_some_and(|cap| reserve(&used.records, cost.records, cap)) {
                self.rollback(cost);
                return None;
            }
            if !bytes.is_some_and(|cap| reserve(&used.bytes, cost.peer_bytes, cap)) {
                release(&used.records, cost.records);
                self.rollback(cost);
                return None;
            }
        }
        if let Some(progress) = progress.as_mut() {
            *progress.entry(cost.progress_bytes).or_default() += 1;
            self.progress_max.store(
                progress.last_key_value().map_or(0, |(&bytes, _)| bytes),
                Ordering::Release,
            );
        }
        Some(Arc::new(Lease {
            budget: self.clone(),
            cost,
            peer: peer.map(|peer| peer.node),
        }))
    }

    fn rollback(&self, cost: Cost) {
        release(&self.used.bytes, cost.bytes);
        release(&self.used.records, cost.records);
        release(&self.used.requests, cost.requests);
        release(&self.used.writers, cost.writers);
        self.changed.notify_changed();
    }
}

#[derive(Debug)]
pub(super) struct Lease {
    budget: Arc<Budget>,
    cost: Cost,
    peer: Option<NodeId>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(peer) = self.peer {
            let used = self.budget.peer(peer).expect("reserved SDK peer");
            release(&used.records, self.cost.records);
            release(&used.bytes, self.cost.peer_bytes);
        }
        if self.cost.writers != 0 {
            let mut progress = self
                .budget
                .progress
                .lock()
                .expect("SDK writer budget poisoned");
            let count = progress
                .get_mut(&self.cost.progress_bytes)
                .expect("reserved SDK progress");
            *count -= 1;
            if *count == 0 {
                progress.remove(&self.cost.progress_bytes);
            }
            self.budget.progress_max.store(
                progress.last_key_value().map_or(0, |(&bytes, _)| bytes),
                Ordering::Release,
            );
        }
        self.budget.rollback(self.cost);
    }
}
