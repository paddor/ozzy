//! Which partitions a shard visits: the one wake path of each partition.
//! Storage results, submitted proposals, and input wake their own partition;
//! each visit reports the partition's next timer. A safety sweep visits every
//! partition once per interval and counts the work only it found.

use super::woken::Woken;
use super::{PartitionActors, PartitionError};
use crate::replica_actor::deadline::Earliest;
use futures::task::AtomicWaker;
use omq_tokio::{Message, TrySendError};
use std::{
    sync::Arc,
    task::{Context, Waker},
    time::Duration,
};

#[derive(Debug)]
pub(super) struct Wakeups {
    shard: Arc<AtomicWaker>,
    woken: Vec<Arc<Woken>>,
    wakers: Vec<Waker>,
    /// The shared transport refused this partition's last send.
    refused: Vec<bool>,
    /// Each partition's next timer, as of its last visit.
    deadlines: Vec<Option<Duration>>,
    sweep: Duration,
    swept: Option<Duration>,
}

impl Wakeups {
    fn new(slots: usize, sweep: Duration) -> Self {
        let shard = Arc::new(AtomicWaker::new());
        let woken: Vec<_> = (0..slots)
            .map(|_| Arc::new(Woken::new(shard.clone())))
            .collect();
        Self {
            wakers: woken.iter().cloned().map(Waker::from).collect(),
            refused: vec![false; slots],
            deadlines: vec![None; slots],
            woken,
            shard,
            sweep,
            swept: None,
        }
    }

    /// Something other than the sweep calls for a visit.
    fn due(&self, slot: usize, now: Duration) -> bool {
        self.woken[slot].is_set()
            || self.refused[slot]
            || self.deadlines[slot].is_some_and(|deadline| now >= deadline)
    }
}

impl PartitionActors {
    /// Visit only partitions with input, a wakeup, a refused send, or a due
    /// timer, and every partition once per `sweep` as a safety net for a
    /// wakeup nobody registered. Without this every call visits every
    /// partition. 1 ms to 1 s.
    pub fn with_safety_sweep(mut self, sweep: Duration) -> Result<Self, PartitionError> {
        if sweep < Duration::from_millis(1) || sweep > Duration::from_secs(1) {
            return Err(PartitionError::Limits);
        }
        self.wakeups = Some(Wakeups::new(self.actors.len(), sweep));
        Ok(self)
    }

    /// When the caller must poll again without a wakeup: the earliest
    /// partition timer or the next safety sweep. `None` without wakeups,
    /// where the caller polls on its own schedule.
    pub fn next_deadline(&self) -> Option<Duration> {
        let wakeups = self.wakeups.as_ref()?;
        let sweep = wakeups
            .swept
            .map_or(Duration::ZERO, |swept| swept.saturating_add(wakeups.sweep));
        Some(
            wakeups
                .deadlines
                .iter()
                .flatten()
                .fold(sweep, |earliest, deadline| earliest.min(*deadline)),
        )
    }

    /// Mark a partition that received work outside its own visit, and wake
    /// the shard to visit it.
    pub(super) fn touch(&self, slot: usize) {
        if let Some(wakeups) = &self.wakeups {
            wakeups.wakers[slot].wake_by_ref();
        }
    }

    /// Visit at most `turn_actors` partitions that have something to observe.
    pub(super) fn poll_woken(
        &mut self,
        mut wakeups: Wakeups,
        cx: &mut Context<'_>,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> (Wakeups, Result<(), PartitionError>) {
        wakeups.shard.register(cx.waker());
        let sweep = wakeups
            .swept
            .is_none_or(|swept| now >= swept.saturating_add(wakeups.sweep));
        if sweep {
            wakeups.swept = Some(now);
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
            let due = wakeups.due(slot, now);
            if !(sweep || due) {
                continue;
            }
            if visits == self.turn_actors {
                // Keep the rest for the next call, including the sweep.
                if sweep {
                    wakeups.woken[slot].keep();
                }
                again = true;
                continue;
            }
            visits += 1;
            match self.visit_woken(&mut wakeups, slot, now, try_send) {
                // Progress can leave more to observe in the same partition.
                Ok(true) => {
                    if !due {
                        crate::profiling::event(crate::profiling::Event::PartitionSweepProgress);
                    }
                    wakeups.woken[slot].keep();
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

    /// One visit under the partition's own waker, then its next timer.
    fn visit_woken(
        &mut self,
        wakeups: &mut Wakeups,
        slot: usize,
        now: Duration,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, PartitionError> {
        wakeups.woken[slot].take();
        let mut refused = false;
        let mut partition = Context::from_waker(&wakeups.wakers[slot]);
        let visited = self.visit(slot, &mut partition, now, &mut |cx, message| {
            let sent = try_send(cx, message);
            refused |= matches!(sent, Err(TrySendError::Full(_)));
            sent
        });
        wakeups.refused[slot] = refused;
        wakeups.deadlines[slot] = self.slot_deadline(slot, now);
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
