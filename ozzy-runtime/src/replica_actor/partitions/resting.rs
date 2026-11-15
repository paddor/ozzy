//! The shard's part in resting groups: it reports its turns to the broker
//! beats, and changes its epoch when a group may have left a view in which
//! a peer still trusts it.

use super::{PartitionActor, PartitionActors};
use crate::frontend::Beats;
use std::time::Duration;

#[derive(Debug)]
pub(super) struct ShardBeats {
    beats: Beats,
    shard: u32,
    /// Beat generation every group has looked at.
    seen: Option<u64>,
    /// A group asked for a new epoch.
    withdraw: bool,
    /// When the epoch last changed. A change wakes every group, so changes
    /// keep one beat interval apart.
    withdrew: Option<Duration>,
}

impl ShardBeats {
    fn due(&self) -> Option<Duration> {
        self.withdraw.then(|| {
            self.withdrew.map_or(Duration::ZERO, |at| {
                at.saturating_add(self.beats.timing().interval)
            })
        })
    }
}

impl PartitionActors {
    /// Let replicated groups rest on `beats`, which report this shard as
    /// `shard`. Without this every group keeps its own heartbeat.
    #[must_use]
    pub fn with_beats(mut self, beats: Beats, shard: u32) -> Self {
        self.beats = Some(ShardBeats {
            beats,
            shard,
            seen: None,
            withdraw: false,
            withdrew: None,
        });
        for slot in 0..self.actors.len() {
            self.install_beats(slot);
        }
        self
    }

    /// Whether broker beats stand in for the heartbeats or votes of a
    /// known replicated group. A diagnostic.
    pub fn resting(&self, group: ozzy_proto::GroupId) -> Option<bool> {
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Replicated(actor) => Some(actor.resting()),
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => None,
        }
    }

    /// Hand the beats to the replicated actor in `slot`.
    pub(super) fn install_beats(&mut self, slot: usize) {
        if let Some(state) = &self.beats
            && let PartitionActor::Replicated(actor) = &mut self.actors[slot]
        {
            actor.install_beats(state.beats.clone(), state.shard);
        }
    }

    /// A group left its view, its role, or the shard while a peer may
    /// still trust it. The next round changes the epoch.
    pub(super) fn request_withdrawal(&mut self) {
        if let Some(state) = &mut self.beats {
            state.withdraw = true;
        }
    }

    /// Change the epoch now, without waiting for a round, as when the
    /// shard stops.
    pub(super) fn withdraw_now(&self) {
        if let Some(state) = &self.beats {
            state.beats.withdraw(state.shard);
        }
    }

    /// Ask the actor in `slot` whether it left a view a peer may still
    /// trust it in.
    pub(super) fn collect_withdrawal(&mut self, slot: usize) {
        if let PartitionActor::Replicated(actor) = &mut self.actors[slot]
            && actor.take_withdrawal()
        {
            self.request_withdrawal();
        }
    }

    /// Report this turn, and change the epoch when a group asked for it.
    /// After that, or once a broker peer fell silent or changed what its
    /// beats say, every group looks at its agreements again.
    pub(super) fn beat_round(&mut self, now: Duration) {
        let Some(state) = &mut self.beats else {
            return;
        };
        state.beats.turn(state.shard);
        let generation = state.beats.generation();
        let mut wake = state.seen.replace(generation) != Some(generation);
        if state.due().is_some_and(|due| now >= due) {
            state.withdraw = false;
            state.withdrew = Some(now);
            state.beats.withdraw(state.shard);
            crate::profiling::event(crate::profiling::Event::ShardEpochChange);
            wake = true;
        }
        if wake {
            self.wake_all();
        }
    }

    /// When a requested epoch change is due.
    pub(super) fn withdrawal_deadline(&self) -> Option<Duration> {
        self.beats.as_ref().and_then(ShardBeats::due)
    }
}
