//! Independent ordered record identities and full-byte workload digests.

use super::super::{config, measurement::Counts, pacing::Plan};
use super::{Config, GroupId, Result, Value, Window, error};
use ozzy_proto::MessageId;
use ozzy_runtime::{replicated::TopicRecord, topic_metadata::TopicMetadata};
use serde_json::json;
use std::collections::BTreeSet;

#[cfg(test)]
mod tests;

struct Lane {
    counts: Counts,
    plan: Option<Plan>,
    partition: usize,
    /// Verified records. Leads `counts.total` while timings wait for a flush.
    verified: u64,
}

/// A verified record whose completion time is read later.
#[derive(Clone, Copy)]
struct Untimed {
    lane: usize,
    start: u64,
    due: Option<u64>,
}

pub(in crate::bench::timed) struct Oracle {
    offsets: Vec<Option<u64>>,
    lanes: Vec<Option<Lane>>,
    copy: usize,
    untimed: Vec<Untimed>,
    /// Held records start their timings at release: the send clock plus
    /// this delay, which their due time never precedes. The window, pacing
    /// plan, and so the latency cohort move with it.
    delay: u64,
}

#[derive(Clone, Copy)]
pub(in crate::bench::timed) struct Record<'a> {
    pub(in crate::bench::timed) partition: u32,
    pub(in crate::bench::timed) offset: u64,
    pub(in crate::bench::timed) id: MessageId,
    pub(in crate::bench::timed) payload: &'a [u8],
}

impl<'a> Record<'a> {
    /// The oracle view of one delivery, after checking that it came from
    /// this topic's current partition incarnation as one payload part.
    pub(in crate::bench::timed) fn delivered(
        metadata: &TopicMetadata,
        record: &'a TopicRecord,
    ) -> Result<Self> {
        let expected = metadata
            .partition(record.partition)
            .ok_or_else(|| error("reader returned an unknown partition"))?;
        if record.topic != metadata.id() || record.incarnation != expected.incarnation {
            return Err(error("reader topic or partition identity mismatch"));
        }
        let [payload] = record.payload.as_slice() else {
            return Err(error("invalid record layout"));
        };
        Ok(Self {
            partition: record.partition,
            offset: record.offset.get(),
            id: record.message_id,
            payload,
        })
    }
}

impl Oracle {
    pub(in crate::bench::timed) fn new(
        config: &Config,
        count: usize,
        partitions: &[u32],
        copy: usize,
        window: Window,
    ) -> Result<Self> {
        let delay = config.delay_ns();
        let window = window.delayed(delay)?;
        let selected: BTreeSet<_> = partitions.iter().copied().collect();
        if count == 0
            || selected.len() != partitions.len()
            || partitions.iter().any(|&number| number as usize >= count)
        {
            return Err(error("invalid oracle partition assignment"));
        }
        let mut lanes = Vec::new();
        for lane in 0..config.logical_writers() {
            if !selected.contains(&(config.feed_partition(lane, count) as u32)) {
                lanes.push(None);
                continue;
            }
            let mut counts = Counts::new(window);
            let plan = Plan::new(config, window, config.args.native.feeds.primary(lane))?;
            if let Some(plan) = &plan {
                counts.enable_schedule(plan.bounds());
            }
            lanes.push(Some(Lane {
                counts,
                plan,
                partition: config.feed_partition(lane, count),
                verified: 0,
            }));
        }
        let offsets = (0..count)
            .map(|number| selected.contains(&(number as u32)).then_some(0))
            .collect();
        Ok(Self {
            offsets,
            lanes,
            copy,
            untimed: Vec::new(),
            delay,
        })
    }

    pub(in crate::bench::timed) fn observe(
        &mut self,
        config: &Config,
        group: GroupId,
        record: Record<'_>,
        completed: u64,
    ) -> Result<()> {
        let untimed = self.verify(config, group, record)?;
        self.time(untimed, completed)
    }

    /// Verify `record` now; [`Self::flush`] times it later.
    pub(in crate::bench::timed) fn observe_untimed(
        &mut self,
        config: &Config,
        group: GroupId,
        record: Record<'_>,
    ) -> Result<()> {
        let untimed = self.verify(config, group, record)?;
        self.untimed.push(untimed);
        Ok(())
    }

    /// Complete every record verified since the last flush at `completed`.
    pub(in crate::bench::timed) fn flush(&mut self, completed: u64) -> Result<()> {
        let mut untimed = std::mem::take(&mut self.untimed);
        for record in untimed.drain(..) {
            self.time(record, completed)?;
        }
        self.untimed = untimed;
        Ok(())
    }

    fn verify(&mut self, config: &Config, group: GroupId, record: Record<'_>) -> Result<Untimed> {
        let partition = record.partition as usize;
        let offset = self
            .offsets
            .get(partition)
            .copied()
            .flatten()
            .ok_or_else(|| error("reader returned an unassigned partition"))?;
        if record.offset != offset {
            return Err(error("reader partition offset gap"));
        }
        let lane = usize::from(record.id.as_bytes()[7] ^ group.as_bytes()[7]);
        let state = self
            .lanes
            .get_mut(lane)
            .and_then(Option::as_mut)
            .ok_or_else(|| error("reader returned an unknown writer"))?;
        let sequence = state.verified;
        if state.partition != partition || record.id != config::message_id(group, lane, sequence) {
            return Err(error(
                "reader message identity gap, duplicate, or substitution",
            ));
        }
        let bytes = record.payload;
        if bytes.len() != config.args.record_bytes {
            return Err(error("invalid record size"));
        }
        let Some(&clock) = bytes.first_chunk() else {
            return Err(error("record lacks its send clock"));
        };
        let start = u64::from_be_bytes(clock)
            .checked_add(self.delay)
            .ok_or_else(|| error("record release clock overflow"))?;
        // Compare these original bytes with the writer digest outside broker
        // state. Regenerating bodies on the reader would change the workload.
        state.counts.record_bytes(record.id, bytes);
        let due = state.plan.map(|plan| plan.due(sequence)).transpose()?;
        state.verified += 1;
        self.offsets[partition] = Some(
            offset
                .checked_add(1)
                .ok_or_else(|| error("reader offset overflow"))?,
        );
        Ok(Untimed { lane, start, due })
    }

    fn time(&mut self, record: Untimed, completed: u64) -> Result<()> {
        if completed < record.start && self.delay != 0 {
            return Err(error("reader saw a held record before its due time"));
        }
        let counts = &mut self
            .lanes
            .get_mut(record.lane)
            .and_then(Option::as_mut)
            .ok_or_else(|| error("reader returned an unknown writer"))?
            .counts;
        match record.due {
            Some(due) => counts.scheduled_complete(due, record.start, completed),
            None => counts.complete(record.start, completed, 1),
        }
    }

    pub(in crate::bench::timed) fn complete(&self, target: &[u64]) -> Result<bool> {
        if target.len() != self.lanes.len() {
            return Err(error("invalid writer count length"));
        }
        let mut complete = true;
        for (state, &target) in self.lanes.iter().zip(target) {
            let Some(state) = state else {
                continue;
            };
            if state.verified > target {
                return Err(error("reader observed unsubmitted records"));
            }
            complete &= state.verified == target;
        }
        Ok(complete)
    }

    pub(in crate::bench::timed) fn positions(&self) -> Vec<(u32, u64)> {
        self.offsets
            .iter()
            .enumerate()
            .filter_map(|(number, &offset)| offset.map(|offset| (number as u32, offset)))
            .collect()
    }

    pub(in crate::bench::timed) fn pending(&self, target: Option<&[u64]>) -> Vec<Value> {
        self.lanes
            .iter()
            .enumerate()
            .filter_map(|(lane, state)| {
                let state = state.as_ref()?;
                let target = target.and_then(|counts| counts.get(lane)).copied();
                (target != Some(state.verified)).then(|| {
                    json!({"lane":lane, "partition":state.partition,
                        "verified":state.verified, "target":target})
                })
            })
            .collect()
    }

    pub(in crate::bench::timed) fn rows(self) -> Vec<Value> {
        debug_assert!(self.untimed.is_empty(), "rows before the last flush");
        self.lanes
            .into_iter()
            .enumerate()
            .filter_map(|(lane, state)| {
                state.map(|state| {
                    let mut row = state.counts.report(lane);
                    row["copy"] = json!(self.copy);
                    row["partition"] = json!(state.partition);
                    row
                })
            })
            .collect()
    }
}
