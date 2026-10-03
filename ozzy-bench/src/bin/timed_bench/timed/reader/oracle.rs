//! Independent ordered record identities and full-byte workload digests.

use super::super::{config, measurement::Counts, pacing::Plan};
use super::{Config, GroupId, Result, Value, Window, error};
use ozzy_proto::MessageId;
use serde_json::json;
use std::collections::BTreeSet;

#[cfg(test)]
mod tests;

struct Lane {
    counts: Counts,
    plan: Option<Plan>,
}

pub(in crate::bench::timed) struct Oracle {
    partitions: usize,
    offsets: Vec<Option<u64>>,
    lanes: Vec<Option<Lane>>,
    copy: usize,
}

#[derive(Clone, Copy)]
pub(in crate::bench::timed) struct Record<'a> {
    pub(in crate::bench::timed) partition: u32,
    pub(in crate::bench::timed) offset: u64,
    pub(in crate::bench::timed) id: MessageId,
    pub(in crate::bench::timed) payload: &'a [u8],
}

impl Oracle {
    pub(in crate::bench::timed) fn new(
        config: &Config,
        count: usize,
        partitions: &[u32],
        copy: usize,
        window: Window,
    ) -> Result<Self> {
        let selected: BTreeSet<_> = partitions.iter().copied().collect();
        if count == 0
            || selected.len() != partitions.len()
            || partitions.iter().any(|&number| number as usize >= count)
        {
            return Err(error("invalid oracle partition assignment"));
        }
        let mut lanes = Vec::new();
        for lane in 0..config.writers {
            if !selected.contains(&((lane % count) as u32)) {
                lanes.push(None);
                continue;
            }
            let mut counts = Counts::new(window);
            let plan = Plan::new(config, window, lane)?;
            if let Some(plan) = &plan {
                counts.enable_schedule(plan.bounds());
            }
            lanes.push(Some(Lane { counts, plan }));
        }
        let offsets = (0..count)
            .map(|number| selected.contains(&(number as u32)).then_some(0))
            .collect();
        Ok(Self {
            partitions: count,
            offsets,
            lanes,
            copy,
        })
    }

    pub(in crate::bench::timed) fn observe(
        &mut self,
        config: &Config,
        group: GroupId,
        record: Record<'_>,
        completed: u64,
    ) -> Result<()> {
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
        let sequence = state.counts.total;
        if lane % self.partitions != partition
            || record.id != config::message_id(group, lane, sequence)
        {
            return Err(error(
                "reader message identity gap, duplicate, or substitution",
            ));
        }
        let bytes = record.payload;
        if bytes.len() != config.args.record_bytes {
            return Err(error("invalid record size"));
        }
        let start = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        // Compare these original bytes with the writer digest outside broker
        // state. Regenerating bodies on the reader would change the workload.
        state.counts.record_bytes(record.id, bytes);
        if let Some(plan) = state.plan {
            state
                .counts
                .scheduled_complete(plan.due(sequence)?, start, completed)?;
        } else {
            state.counts.complete(start, completed, 1)?;
        }
        self.offsets[partition] = Some(
            offset
                .checked_add(1)
                .ok_or_else(|| error("reader offset overflow"))?,
        );
        Ok(())
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
            if state.counts.total > target {
                return Err(error("reader observed unsubmitted records"));
            }
            complete &= state.counts.total == target;
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
                (target != Some(state.counts.total)).then(|| {
                    json!({"lane":lane, "partition":lane % self.partitions,
                        "verified":state.counts.total, "target":target})
                })
            })
            .collect()
    }

    pub(in crate::bench::timed) fn rows(self) -> Vec<Value> {
        self.lanes
            .into_iter()
            .enumerate()
            .filter_map(|(lane, state)| {
                state.map(|state| {
                    let mut row = state.counts.report(lane);
                    row["copy"] = json!(self.copy);
                    row["partition"] = json!(lane % self.partitions);
                    row
                })
            })
            .collect()
    }
}
