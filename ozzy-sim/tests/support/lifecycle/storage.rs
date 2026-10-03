//! Production byte storage bound to the same driver actions as the atomic adapter.

use ozzy_journal_segment::simulation::{Journal, Recovered};
use ozzy_journal_segment::{DirectoryError, GroupIdentity, LogPosition};
use ozzy_proto::{StoreId, VolumeId};

use super::disk::DiskImage;
use super::*;

pub(super) fn image(recovered: Recovered, configuration: Configuration) -> DiskImage {
    let manifest = recovered.manifest;
    DiskImage {
        admitted: recovered.admitted,
        promised: Scope {
            view: manifest.promised_view,
            ..configuration.scope()
        },
        last_normal_view: manifest.last_normal_view,
        committed: if recovered.admitted {
            Prefix {
                op: OpNumber(manifest.committed.op_number),
                digest: manifest.committed.digest,
            }
        } else {
            Prefix::GENESIS
        },
        operations: recovered
            .operations
            .into_iter()
            .map(|operation| Operation {
                scope: Scope {
                    view: operation.original_view,
                    ..configuration.scope()
                },
                number: operation.op_number,
                previous: operation.previous_digest,
                kind: operation.kind,
                body: operation.body.into_owned(),
            })
            .collect(),
    }
}

fn position(prefix: Prefix) -> LogPosition {
    LogPosition {
        op_number: prefix.op.0,
        digest: prefix.digest,
    }
}

impl Replica {
    pub(super) fn enable_storage(&mut self, seed: u64) {
        assert!(self.accepted.is_empty());
        self.storage = Some(
            Journal::format(
                GroupIdentity {
                    group_id: self.configuration.scope().group_id,
                    replica_node_id: node(self.id),
                    volume_id: VolumeId::from_bytes([self.id as u8 + 21; 16]),
                    store_id: StoreId::from_bytes([self.id as u8 + 31; 16]),
                    store_generation: 1,
                },
                configuration_record_with_policy(self.configuration.policy())
                    .encode()
                    .to_vec(),
                1,
                self.generation,
                97 + seed as usize % 4096,
            )
            .unwrap(),
        );
        if self.configuration.policy() == QuorumPolicy::Replicated {
            self.storage
                .as_mut()
                .unwrap()
                .enable_memory_voting()
                .unwrap();
        }
    }

    pub(super) fn perform_storage(&mut self, action: &DiskAction) -> Result<(), DirectoryError> {
        let Some(storage) = &mut self.storage else {
            return Ok(());
        };
        match action {
            DiskAction::Write(ticket) => storage.append(
                &self.accepted[ticket.first().0 as usize - 1..ticket.through().0 as usize]
                    .iter()
                    .map(Operation::canonical)
                    .collect::<Vec<_>>(),
            )?,
            DiskAction::Sync(ticket) => storage.sync(ticket.through().0)?,
            DiskAction::Promise(ticket) => storage.publish_view(
                ticket.scope().view,
                ticket.log().last_normal_view,
                position(ticket.log().committed),
            )?,
            DiskAction::Install { ticket, operations } => storage.replace(
                ticket.generation(),
                ticket.scope().view,
                position(ticket.committed()),
                &operations
                    .iter()
                    .map(Operation::canonical)
                    .collect::<Vec<_>>(),
            )?,
            DiskAction::Activate(ticket) => storage.publish_view(
                ticket.scope().view,
                ticket.scope().view,
                position(ticket.through()),
            )?,
            DiskAction::Recovery(RecoveryDisk::Capture { .. }) => {
                storage.sync(tail(&self.buffered).op.0)?;
            }
            DiskAction::Recovery(RecoveryDisk::Stage { operations, .. }) => {
                storage.append(
                    &operations
                        .iter()
                        .map(Operation::canonical)
                        .collect::<Vec<_>>(),
                )?;
            }
            DiskAction::Recovery(RecoveryDisk::Publish(ticket)) => {
                storage.publish_recovery(ticket.scope().view, position(ticket.committed()))?;
            }
        }
        Ok(())
    }
}
