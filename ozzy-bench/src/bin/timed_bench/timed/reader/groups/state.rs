//! One independent verifier per group, shared only within its reader owner.

use super::super::oracle::Oracle;
use super::{Config, GroupId, Record, Result, Value, Window, error};
use ozzy_journal::{integrity, operation::Digest};
use ozzy_proto::{MessageId, PartitionIncarnation};
use std::collections::BTreeMap;
use tokio::sync::watch;

pub(super) struct State {
    pub(super) oracle: Oracle,
    positions: Vec<u64>,
    unconfirmed: BTreeMap<(u32, u64), (MessageId, Digest)>,
    incarnations: Vec<PartitionIncarnation>,
    capacity: usize,
    changed: watch::Sender<u64>,
}

impl State {
    pub(super) fn new(
        config: &Config,
        copy: usize,
        window: Window,
        incarnations: Vec<PartitionIncarnation>,
    ) -> Result<Self> {
        let partitions = (0..config.partitions() as u32).collect::<Vec<_>>();
        let oracle = Oracle::new(config, config.partitions(), &partitions, copy, window)?;
        let (changed, _) = watch::channel(0);
        Ok(Self {
            oracle,
            positions: vec![0; partitions.len()],
            unconfirmed: BTreeMap::new(),
            incarnations,
            capacity: partitions
                .len()
                .checked_mul(config.args.native.group_members as usize)
                .and_then(|n| n.checked_mul(config.args.native.ack_window as usize))
                .ok_or_else(|| error("group verification window overflow"))?,
            changed,
        })
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub(super) fn observe(
        &mut self,
        config: &Config,
        group: GroupId,
        record: Record<'_>,
        now: u64,
    ) -> Result<bool> {
        let position = *self
            .positions
            .get(record.partition as usize)
            .ok_or_else(|| error("unknown group partition"))?;
        let digest = integrity::hash("ozzy benchmark handoff evidence", record.payload);
        let key = (record.partition, record.offset);
        let redelivered = record.offset < position;
        if redelivered {
            if self.unconfirmed.get(&key) != Some(&(record.id, digest)) {
                return Err(error(
                    "handoff redelivery lacks matching unconfirmed byte/identity evidence",
                ));
            }
        } else {
            if self.unconfirmed.len() >= self.capacity {
                return Err(error("group verification window exhausted"));
            }
            self.oracle.observe(config, group, record, now)?;
            self.unconfirmed.insert(key, (record.id, digest));
            self.positions[record.partition as usize] = record
                .offset
                .checked_add(1)
                .ok_or_else(|| error("group offset overflow"))?;
        }
        Ok(redelivered)
    }

    pub(super) fn confirm(&mut self, incarnation: PartitionIncarnation, cursor: u64) -> Result<()> {
        let partition = self
            .incarnations
            .iter()
            .position(|value| *value == incarnation)
            .ok_or_else(|| error("unknown progress partition"))? as u32;
        if cursor > self.positions[partition as usize] {
            return Err(error("progress exceeds independently observed records"));
        }
        let covered = self
            .unconfirmed
            .range((partition, 0)..(partition, cursor))
            .map(|(&key, _)| key)
            .collect::<Vec<_>>();
        for key in covered {
            self.unconfirmed.remove(&key);
        }
        self.notify();
        Ok(())
    }

    pub(super) fn complete(&self, target: Option<&[u64]>) -> Result<bool> {
        Ok(match target {
            Some(target) => self.oracle.complete(target)? && self.unconfirmed.is_empty(),
            None => false,
        })
    }

    fn notify(&self) {
        self.changed.send_modify(|generation| *generation += 1);
    }

    pub(super) fn detail(&self, copy: usize) -> Value {
        serde_json::json!({"copy":copy, "positions":self.oracle.positions(), "unconfirmed_records":self.unconfirmed.len()})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn handoff_requires_matching_unconfirmed_identity_and_bytes() {
        let config = Config::production(crate::bench::Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
            "--system",
            "single-durable",
            "--window",
            "1",
            "--partitions",
            "1",
            "--groups",
            "1",
            "--group-members",
            "2",
            "--ack-window",
            "1",
            "--record-bytes",
            "32",
            "--group",
            "00000000-0000-0000-0000-000000000001",
        ]))
        .unwrap();
        let window = Window {
            start: 10_000_000_000,
            measured: 10_000_000_000,
            end: 11_000_000_000,
        };
        let incarnation = PartitionIncarnation::from_bytes([1; 16]);
        let mut state = State::new(&config, 0, window, vec![incarnation]).unwrap();
        let group = super::super::super::super::config::group(&config.args).unwrap();
        let mut payload = [7; 32];
        payload[..8].copy_from_slice(&window.start.to_be_bytes());
        let id = super::super::super::super::config::message_id(group, 0, 0);
        let record = Record {
            partition: 0,
            offset: 0,
            id,
            payload: &payload,
        };
        assert!(
            !state
                .observe(&config, group, record, window.start + 100)
                .unwrap()
        );
        assert!(
            state
                .observe(&config, group, record, window.start + 200)
                .unwrap()
        );
        assert!(!state.complete(Some(&[1])).unwrap());
        let mut corrupted = payload;
        corrupted[8] ^= 1;
        assert!(
            state
                .observe(
                    &config,
                    group,
                    Record {
                        payload: &corrupted,
                        ..record
                    },
                    window.start + 300
                )
                .is_err()
        );
        assert!(
            state
                .observe(
                    &config,
                    group,
                    Record {
                        id: MessageId::from_bytes([9; 16]),
                        ..record
                    },
                    window.start + 300
                )
                .is_err()
        );
        assert!(state.confirm(incarnation, 2).is_err());
        state.confirm(incarnation, 1).unwrap();
        assert!(state.complete(Some(&[1])).unwrap());
        assert!(
            state
                .observe(&config, group, record, window.start + 400)
                .is_err()
        );
        assert_eq!(state.oracle.rows()[0]["total"], 1);
    }
}
