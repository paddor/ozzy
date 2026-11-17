//! Which partitions a shard visits: the one wake path of each partition.
//! Storage results, submitted proposals, and input wake their own partition;
//! each visit reports the partition's next timer, and whether its role
//! waits for the storage lane to complete any job. The shard finds marked
//! partitions in a bit set and due timers in an ordered set, so a turn
//! costs what it visits, not what the shard holds. A safety sweep visits a
//! bounded run of partitions per interval and counts the work only it found.

use super::bits::Bits;
use super::woken::{Ready, Visiting, Woken};
use super::{PartitionActors, PartitionError};
use crate::replica_actor::deadline::Earliest;
use omq_tokio::{Message, TrySendError};
use std::{
    collections::BTreeSet,
    sync::Arc,
    task::{Context, Waker},
    time::Duration,
};

/// Rounds of one partition visit while it makes progress or wakes itself.
const VISIT_ROUNDS: usize = 4;

/// What one partition visit did.
#[derive(Debug, Clone, Copy)]
struct Visit {
    /// Some round made progress.
    progressed: bool,
    /// The last round made progress or the partition woke itself, so it
    /// wants another visit.
    more: bool,
}

#[derive(Debug)]
pub(super) struct Wakeups {
    ready: Arc<Ready>,
    woken: Vec<Arc<Woken>>,
    wakers: Vec<Waker>,
    /// Each partition's mark number, fixed while it lives in the shard.
    ids: Vec<usize>,
    /// The slot of each mark number in use.
    slots: Vec<Option<usize>>,
    free: Vec<usize>,
    /// Mark numbers taken from `ready` or due by timer, not visited yet.
    pending: Bits,
    /// The mark number a turn looks for first.
    first: usize,
    /// Each partition's next timer, as of its last visit.
    deadlines: Vec<Option<Duration>>,
    /// The same timers by mark number, earliest first.
    timers: BTreeSet<(Duration, usize)>,
    /// Mark numbers of partitions whose role suspected stalled storage at
    /// its last visit. The lane completing a job, for any partition, ends
    /// that.
    stalled: BTreeSet<usize>,
    sweep: Duration,
    swept: Option<Duration>,
    /// The slot the next sweep starts at.
    cursor: usize,
    /// Visits where only the sweep found progress: missed wakeups.
    missed: u64,
    /// A `poll_progress` follows before the shard waits, so input marks
    /// its partition without waking the shard.
    deferred: bool,
}

impl Wakeups {
    fn new(slots: usize, capacity: usize, sweep: Duration) -> Self {
        let capacity = capacity.max(slots);
        let ready = Arc::new(Ready::new(capacity));
        let woken: Vec<_> = (0..slots)
            .map(|id| Arc::new(Woken::new(ready.clone(), id)))
            .collect();
        Self {
            wakers: woken.iter().cloned().map(Waker::from).collect(),
            ids: (0..slots).collect(),
            slots: (0..capacity).map(|id| (id < slots).then_some(id)).collect(),
            free: (slots..capacity).rev().collect(),
            pending: Bits::new(capacity),
            first: 0,
            deadlines: vec![None; slots],
            timers: BTreeSet::new(),
            stalled: BTreeSet::new(),
            woken,
            ready,
            sweep,
            swept: None,
            cursor: 0,
            missed: 0,
            deferred: false,
        }
    }

    /// No mark number is left for one more partition.
    pub(super) fn full(&self) -> bool {
        self.free.is_empty()
    }

    /// Marks for one more partition, set so it gets its first visit.
    pub(super) fn grow(&mut self) {
        let Some(id) = self.free.pop() else {
            return;
        };
        let woken = Arc::new(Woken::new(self.ready.clone(), id));
        self.wakers.push(Waker::from(woken.clone()));
        self.woken.push(woken);
        self.slots[id] = Some(self.ids.len());
        self.ids.push(id);
        self.deadlines.push(None);
        self.ready.wake();
    }

    /// Drop the marks of `slot`. The last partition takes the slot, as in
    /// the actor list, and keeps its mark number. A waker of the dropped
    /// partition that fires later marks a number nobody holds, or its next
    /// holder, which then gets one visit for nothing.
    pub(super) fn swap_remove(&mut self, slot: usize) {
        let id = self.ids.swap_remove(slot);
        if let Some(deadline) = self.deadlines.swap_remove(slot) {
            self.timers.remove(&(deadline, id));
        }
        self.stalled.remove(&id);
        self.pending.clear(id);
        self.slots[id] = None;
        self.free.push(id);
        self.woken.swap_remove(slot);
        self.wakers.swap_remove(slot);
        if let Some(&moved) = self.ids.get(slot) {
            self.slots[moved] = Some(slot);
        }
    }

    /// Gather what calls for a visit: marks set since the last turn and
    /// timers that came due.
    fn collect(&mut self, now: Duration) {
        self.ready.drain(self.pending.words());
        while let Some(&(deadline, id)) = self.timers.first() {
            if deadline > now {
                break;
            }
            self.timers.pop_first();
            if let Some(slot) = self.slots[id] {
                self.deadlines[slot] = None;
            }
            self.pending.set(id);
        }
    }

    /// The mark number this turn looks for first: one later on every call,
    /// so a shared bounded transport serves every partition first in turn.
    fn rotate(&mut self) -> usize {
        self.first = (self.first + 1) % self.slots.len().max(1);
        self.first
    }

    /// Something other than the sweep already calls for a visit.
    fn marked(&self, slot: usize) -> bool {
        self.pending.test(self.ids[slot]) || self.woken[slot].is_set()
    }

    /// Keep what a visit of `slot` left: a refused send to retry at the
    /// next turn, the next timer, and whether the role waits for storage.
    fn record(&mut self, slot: usize, refused: bool, deadline: Option<Duration>, stalled: bool) {
        let id = self.ids[slot];
        if refused {
            self.woken[slot].keep();
        }
        if self.deadlines[slot] != deadline {
            if let Some(old) = self.deadlines[slot] {
                self.timers.remove(&(old, id));
            }
            if let Some(new) = deadline {
                self.timers.insert((new, id));
            }
            self.deadlines[slot] = deadline;
        }
        if stalled {
            self.stalled.insert(id);
        } else {
            self.stalled.remove(&id);
        }
    }
}

impl PartitionActors {
    /// Visit only partitions with input, a wakeup, a refused send, or a due
    /// timer. As a safety net for a wakeup nobody registered, also visit
    /// the next `turn_actors` partitions once per `sweep`, so a shard with
    /// more partitions takes that many intervals to see each once. Without
    /// this every call visits every partition. 1 ms to 1 s.
    pub fn with_safety_sweep(mut self, sweep: Duration) -> Result<Self, PartitionError> {
        if sweep < Duration::from_millis(1) || sweep > Duration::from_secs(1) {
            return Err(PartitionError::Limits);
        }
        self.wakeups = Some(Box::new(Wakeups::new(
            self.actors.len(),
            self.maximum,
            sweep,
        )));
        Ok(self)
    }

    /// Visits where only the safety sweep found progress, each a wakeup
    /// nobody registered. Zero without wakeups.
    pub fn sweep_progress(&self) -> u64 {
        self.wakeups.as_ref().map_or(0, |wakeups| wakeups.missed)
    }

    /// When the caller must poll again without a wakeup: the earliest
    /// partition timer or the next safety sweep. `None` without wakeups,
    /// where the caller polls on its own schedule.
    pub fn next_deadline(&self) -> Option<Duration> {
        let wakeups = self.wakeups.as_ref()?;
        let sweep = wakeups
            .swept
            .map_or(Duration::ZERO, |swept| swept.saturating_add(wakeups.sweep));
        let timer = wakeups.timers.first().map(|&(deadline, _)| deadline);
        Some(
            timer
                .into_iter()
                .chain(self.withdrawal_deadline())
                .fold(sweep, Duration::min),
        )
    }

    /// Until the next `poll_progress`, input marks its partition without
    /// waking the shard, since that call visits it anyway. The caller must
    /// poll progress before it waits again.
    pub fn defer_wakes(&mut self) {
        if let Some(wakeups) = &mut self.wakeups {
            wakeups.deferred = true;
        }
    }

    /// Visit every partition at the next poll, so each re-arms its timers
    /// after a change none of them registered, such as the clock guard
    /// resuming. Without wakeups every poll visits every partition anyway.
    pub fn wake_all(&self) {
        for slot in 0..self.actors.len() {
            self.touch(slot);
        }
    }

    /// Mark a partition that received work outside its own visit, and wake
    /// the shard to visit it unless a poll is already due.
    pub(super) fn touch(&self, slot: usize) {
        match &self.wakeups {
            Some(wakeups) if wakeups.deferred => wakeups.woken[slot].keep(),
            Some(wakeups) => wakeups.wakers[slot].wake_by_ref(),
            None => {}
        }
    }

    /// Visit at most `turn_actors` partitions that have something to
    /// observe, and at most as many for a due sweep.
    pub(super) fn poll_woken(
        &mut self,
        mut wakeups: Box<Wakeups>,
        cx: &mut Context<'_>,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> (Box<Wakeups>, Result<(), PartitionError>) {
        wakeups.deferred = false;
        wakeups.ready.register(cx.waker());
        wakeups.collect(now);
        self.mark_moved(&mut wakeups);
        let result = self.turn_woken(&mut wakeups, now, try_send).map(|again| {
            if again {
                cx.waker().wake_by_ref();
            }
        });
        (wakeups, result)
    }

    /// Whether the shard must turn again at once.
    fn turn_woken(
        &mut self,
        wakeups: &mut Wakeups,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, PartitionError> {
        let due = self.visit_due(wakeups, now, try_send)?;
        let swept = self.sweep_some(wakeups, now, try_send)?;
        // A visit above can complete a job after the turn passed a
        // partition that waits for one.
        Ok(self.mark_moved(wakeups) || due || swept)
    }

    /// Visit pending partitions in mark order from this turn's first.
    fn visit_due(
        &mut self,
        wakeups: &mut Wakeups,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, PartitionError> {
        let first = wakeups.rotate();
        let (mut at, mut wrapped, mut visits, mut again) = (first, false, 0, false);
        loop {
            let id = match wakeups.pending.next(at) {
                Some(id) if !wrapped || id < first => id,
                None if !wrapped => {
                    (at, wrapped) = (0, true);
                    continue;
                }
                _ => break,
            };
            if visits == self.turn_actors {
                // The rest stays pending for the next call.
                return Ok(true);
            }
            at = id + 1;
            wakeups.pending.clear(id);
            // A mark of a partition that left names nobody.
            let Some(slot) = wakeups.slots[id] else {
                continue;
            };
            visits += 1;
            again |= self.visit_woken(wakeups, slot, now, try_send)?.more;
        }
        Ok(again)
    }

    /// Once per sweep interval, visit the next run of partitions that
    /// nothing else called for.
    fn sweep_some(
        &mut self,
        wakeups: &mut Wakeups,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, PartitionError> {
        if wakeups
            .swept
            .is_some_and(|swept| now < swept.saturating_add(wakeups.sweep))
        {
            return Ok(false);
        }
        wakeups.swept = Some(now);
        let count = self.actors.len();
        let mut again = false;
        for _ in 0..self.turn_actors.min(count) {
            let slot = wakeups.cursor % count;
            wakeups.cursor = slot + 1;
            if wakeups.marked(slot) {
                continue;
            }
            let visit = self.visit_woken(wakeups, slot, now, try_send)?;
            if visit.progressed {
                wakeups.missed = wakeups.missed.saturating_add(1);
                crate::profiling::event(crate::profiling::Event::PartitionSweepProgress);
            }
            again |= visit.more;
        }
        Ok(again)
    }

    /// Make partitions pending whose role waits for the storage lane, once
    /// the lane completed a job.
    fn mark_moved(&self, wakeups: &mut Wakeups) -> bool {
        let mut moved = false;
        for &id in &wakeups.stalled {
            if wakeups.slots[id].is_some_and(|slot| self.storage_moved(slot)) {
                wakeups.pending.set(id);
                moved = true;
            }
        }
        moved
    }

    fn storage_moved(&self, slot: usize) -> bool {
        self.actors[slot].storage_moved() == Some(true)
    }

    /// Visit a partition under its own waker, then record its next timer.
    /// Progress often enables more work in the same partition, such as a
    /// vote after an observed write, and the partition's own wakes during a
    /// round only set its mark. Repeat rounds while either happens, up to
    /// `VISIT_ROUNDS`, instead of spending whole shard turns.
    fn visit_woken(
        &mut self,
        wakeups: &mut Wakeups,
        slot: usize,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<Visit, PartitionError> {
        let mut refused = false;
        let mut partition = Context::from_waker(&wakeups.wakers[slot]);
        let visiting = Visiting::enter(&wakeups.woken[slot]);
        let mut visited = Ok(Visit {
            progressed: false,
            more: false,
        });
        for _ in 0..VISIT_ROUNDS {
            wakeups.woken[slot].take();
            let round = self.visit(slot, &mut partition, now, &mut |cx, message| {
                let sent = try_send(cx, message);
                refused |= matches!(sent, Err(TrySendError::Full(_)));
                sent
            });
            visited = visited.and_then(|visit| {
                let progress = round?;
                Ok(Visit {
                    progressed: visit.progressed || progress,
                    more: progress || wakeups.woken[slot].is_set(),
                })
            });
            if !visited.as_ref().is_ok_and(|visit| visit.more) {
                break;
            }
        }
        drop(visiting);
        // Progress can leave more to observe in the same partition.
        if visited.as_ref().is_ok_and(|visit| visit.more) {
            wakeups.woken[slot].keep();
        }
        let deadline = self.slot_deadline(slot, now);
        let stalled = self.actors[slot].storage_moved().is_some();
        wakeups.record(slot, refused, deadline, stalled);
        visited
    }

    /// The partition actor's timers and its group coordinator's.
    fn slot_deadline(&self, slot: usize, now: Duration) -> Option<Duration> {
        let coordinator = self.coordinators[slot]
            .as_ref()
            .and_then(|service| service.coordinator.next_deadline(now));
        Earliest::after(now)
            .at(self.actors[slot].next_deadline(now))
            .at(coordinator)
            .get()
    }
}

#[cfg(test)]
mod tests {
    use super::Wakeups;
    use std::time::Duration;

    const fn at(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    /// Pending mark numbers in order, taken.
    fn pending(wakeups: &mut Wakeups) -> Vec<usize> {
        let mut found = Vec::new();
        while let Some(id) = wakeups.pending.next(0) {
            wakeups.pending.clear(id);
            found.push(id);
        }
        found
    }

    #[test]
    fn new_partitions_are_pending_and_due_timers_join_them() {
        let mut wakeups = Wakeups::new(3, 4, at(100));
        wakeups.collect(at(0));
        assert_eq!(pending(&mut wakeups), [0, 1, 2]);
        wakeups.record(0, false, Some(at(30)), false);
        wakeups.record(1, false, Some(at(20)), false);
        // A later timer replaces the earlier one of the same partition.
        wakeups.record(1, false, Some(at(40)), false);
        wakeups.collect(at(29));
        assert_eq!(pending(&mut wakeups), [] as [usize; 0]);
        wakeups.collect(at(30));
        assert_eq!(pending(&mut wakeups), [0]);
        assert_eq!(wakeups.deadlines, [None, Some(at(40)), None]);
        assert_eq!(wakeups.timers.first(), Some(&(at(40), 1)));
    }

    #[test]
    fn a_refused_send_and_a_wake_mark_only_their_partition() {
        let mut wakeups = Wakeups::new(3, 3, at(100));
        wakeups.collect(at(0));
        pending(&mut wakeups);
        for woken in &wakeups.woken {
            woken.take();
        }
        wakeups.record(2, true, None, false);
        wakeups.wakers[0].wake_by_ref();
        assert!(wakeups.marked(0) && !wakeups.marked(1) && wakeups.marked(2));
        wakeups.collect(at(0));
        assert_eq!(pending(&mut wakeups), [0, 2]);
    }

    #[test]
    fn a_partition_that_moves_slots_keeps_its_mark_number() {
        let mut wakeups = Wakeups::new(3, 3, at(100));
        assert!(wakeups.full());
        wakeups.collect(at(0));
        pending(&mut wakeups);
        wakeups.record(0, false, Some(at(10)), true);
        wakeups.record(2, false, Some(at(20)), true);
        let gone = wakeups.wakers[0].clone();
        wakeups.swap_remove(0);
        // The last partition took slot 0 with its number, timer and stall.
        assert_eq!(wakeups.ids, [2, 1]);
        assert_eq!(wakeups.slots, [None, Some(1), Some(0)]);
        assert_eq!(wakeups.timers.iter().collect::<Vec<_>>(), [&(at(20), 2)]);
        assert_eq!(wakeups.stalled.iter().collect::<Vec<_>>(), [&2]);
        // A waker of the removed partition names nobody.
        gone.wake();
        wakeups.collect(at(0));
        assert_eq!(pending(&mut wakeups), [0]);
        assert_eq!(wakeups.slots[0], None);
        // The next partition takes the free number and starts marked.
        assert!(!wakeups.full());
        wakeups.grow();
        assert_eq!(wakeups.ids, [2, 1, 0]);
        wakeups.collect(at(0));
        assert_eq!(pending(&mut wakeups), [0]);
        assert_eq!(wakeups.rotate(), 1);
    }
}
