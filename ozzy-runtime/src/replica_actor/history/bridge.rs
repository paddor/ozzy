//! Retired ancestry is proved through an overlapping, exact-tail-verified source.

use super::{ActorError, Duration, LogSource, Lookup, OpNumber, PendingIo, Prefix, ReplicaActor};
use super::{HistoryReason, TransferPurpose};

#[derive(Debug, Clone, Copy)]
pub(super) struct Bridge {
    source: LogSource,
    op: OpNumber,
    donor: LogSource,
    prefix: Option<Prefix>,
}

impl Lookup {
    /// Both lookups must verify their complete source lineage. A matching donor
    /// tail inside the selected lineage then proves the older prefix's ancestry.
    pub(super) fn verified(&mut self, source: LogSource, prefix: Prefix) -> Result<(), ActorError> {
        if let Some(bridge) = &mut self.bridge {
            if source == bridge.donor && prefix.op == bridge.op && bridge.prefix.is_none() {
                bridge.prefix = Some(prefix);
                return Ok(());
            }
            if source == bridge.source && prefix.op == bridge.donor.accepted.op {
                let bridge = self.bridge.take().expect("checked bridge");
                if prefix != bridge.donor.accepted {
                    // An older uncertain tail may legitimately diverge. It is
                    // not evidence for this prefix and cannot unlock selection.
                    self.retire(bridge.source, bridge.op);
                    return Ok(());
                }
                self.insert(
                    bridge.source,
                    bridge
                        .prefix
                        .ok_or_else(|| ActorError::history(HistoryReason::Lookup))?,
                )?;
                self.compatible = Some((bridge.source, bridge.donor));
                return Ok(());
            }
        }
        self.insert(source, prefix)
    }

    pub(in crate::replica_actor) fn compatible_local(
        &self,
        selected: LogSource,
        pinned: Option<LogSource>,
        staged: Prefix,
    ) -> Option<LogSource> {
        let (source, donor) = self.compatible?;
        (source == selected && Some(donor) == pinned && staged.op < donor.accepted.op)
            .then_some(donor)
    }
}

impl ReplicaActor {
    pub(super) fn bridge_or_retire(&mut self, source: LogSource, op: OpNumber, before: Prefix) {
        if let Some(bridge) = self.lookup.bridge.take() {
            // A donor or overlap lookup also expired. Preserve the original
            // missing ancestry; never recursively accumulate proof attempts.
            self.lookup.retire(bridge.source, bridge.op);
            return;
        }
        let sources = self.driver.history_sources();
        let donor = self
            .pinned
            .into_iter()
            .chain(sources.into_iter().flatten())
            .find(|donor| {
                *donor != source
                    && donor.accepted.op >= before.op
                    && donor.accepted.op <= source.accepted.op
                    && op <= donor.accepted.op
            });
        if let Some(donor) = donor {
            self.lookup.bridge = Some(Bridge {
                source,
                op,
                donor,
                prefix: None,
            });
        } else {
            self.lookup.retire(source, op);
        }
    }

    pub(in crate::replica_actor) fn schedule_bridge(
        &mut self,
        now: Duration,
    ) -> Result<bool, ActorError> {
        let Some(bridge) = self.lookup.bridge else {
            return Ok(false);
        };
        let (source, op) = if bridge.prefix.is_some() {
            if bridge.donor.accepted == bridge.source.accepted {
                self.lookup
                    .verified(bridge.source, bridge.source.accepted)?;
                return Ok(true);
            }
            (bridge.source, bridge.donor.accepted.op)
        } else {
            (bridge.donor, bridge.op)
        };
        if source.voter == self.local {
            if self.pinned != Some(source) {
                return Err(ActorError::history(HistoryReason::Lookup));
            }
            self.pending = Some(PendingIo::Position(
                self.journal.history_position(source, op)?,
            ));
        } else {
            self.start_transfer(
                self.driver.scope(),
                source,
                Prefix::GENESIS,
                TransferPurpose::Lookup { op, found: None },
                now,
            )?;
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_proto::NodeId;
    use ozzy_replication::{Digest, JournalGeneration};

    fn prefix(op: u64, byte: u8) -> Prefix {
        Prefix {
            op: OpNumber(op),
            digest: Digest::from_bytes([byte; 32]),
        }
    }

    fn sources() -> (LogSource, LogSource, Prefix) {
        let source = LogSource {
            voter: NodeId::from_bytes([1; 16]),
            generation: JournalGeneration(1),
            accepted: prefix(20, 20),
        };
        let donor = LogSource {
            voter: NodeId::from_bytes([2; 16]),
            accepted: prefix(19, 19),
            ..source
        };
        (source, donor, prefix(10, 10))
    }

    fn lookup(source: LogSource, donor: LogSource, prefix: Prefix) -> Lookup {
        Lookup {
            bridge: Some(Bridge {
                source,
                donor,
                op: prefix.op,
                prefix: None,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn retired_prefix_needs_both_anchored_lineages_before_installation_reuses_local_bytes() {
        let (source, donor, older) = sources();
        let mut lookup = lookup(source, donor, older);
        lookup.verified(donor, older).unwrap();
        assert_eq!(lookup.get(source, older.op), None);
        assert_eq!(lookup.compatible_local(source, Some(donor), older), None);
        lookup.verified(source, donor.accepted).unwrap();
        assert_eq!(lookup.get(source, older.op), Some(older.digest));
        assert_eq!(
            lookup.compatible_local(source, Some(donor), older),
            Some(donor)
        );
        assert_eq!(
            lookup.compatible_local(source, Some(donor), donor.accepted),
            None
        );
        let changed = LogSource {
            generation: JournalGeneration(2),
            ..donor
        };
        assert_eq!(lookup.compatible_local(source, Some(changed), older), None);
    }

    #[test]
    fn divergent_overlap_cannot_supply_ancestry_or_installation_bytes() {
        let (source, donor, older) = sources();
        let mut lookup = lookup(source, donor, older);
        lookup.verified(donor, older).unwrap();
        lookup
            .verified(source, prefix(donor.accepted.op.0, 99))
            .unwrap();
        assert_eq!(lookup.get(source, older.op), None);
        assert!(lookup.unavailable(source, older.op));
        assert_eq!(lookup.compatible_local(source, Some(donor), older), None);
    }

    #[test]
    fn scope_change_discards_partial_proof_and_reuse_authority() {
        let (source, donor, older) = sources();
        let mut lookup = lookup(source, donor, older);
        lookup.verified(donor, older).unwrap();
        lookup.reset_scope(ozzy_replication::Scope {
            group_id: ozzy_proto::GroupId::from_bytes([1; 16]),
            configuration_epoch: 1,
            configuration_digest: Digest::ZERO,
            view: 2,
        });
        assert!(lookup.bridge.is_none());
        assert!(lookup.compatible.is_none());
        assert_eq!(lookup.get(source, older.op), None);
    }
}
