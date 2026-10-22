//! A partition's admin row: actor status, route, and committed facts.

use ozzy_proto::admin::{self, GroupCursor, GroupStatus, PartitionLog};
use ozzy_proto::{GroupId, directory::RouteState};
use ozzy_runtime::{replica_actor::PartitionStatus, replica_journal::PartitionFacts};

/// One partition's admin row from its actor status, route, and committed
/// facts.
pub(crate) fn partition_status(
    group: GroupId,
    status: &PartitionStatus,
    route: &RouteState,
    facts: Option<&PartitionFacts>,
) -> admin::PartitionStatus {
    let mut row = admin::PartitionStatus {
        group,
        state: admin::PartitionState::Electing,
        ready: route.leader.is_some(),
        disk_pending: false,
        view: route.view,
        leader: route.leader,
        accepted: 0,
        committed: 0,
        applied: 0,
        pending_operations: 0,
        pending_body_bytes: 0,
        validation_cycles: 0,
        reclaimed_bytes: 0,
        written: 0,
        durable: 0,
        leader_commit: 0,
        followers: Vec::new(),
        log: facts.map(log),
    };
    match status {
        PartitionStatus::Local(snapshot) => {
            row.state = admin::PartitionState::Local;
            row.accepted = snapshot.accepted.op.0;
            row.committed = snapshot.committed.op.0;
            row.applied = snapshot.applied.op.0;
            row.pending_operations = snapshot.pending_operations as u64;
            row.pending_body_bytes = snapshot.pending_body_bytes as u64;
            row.written = snapshot.journal.written.0;
            row.durable = snapshot.journal.durable.0;
            row.leader_commit = snapshot.committed.op.0;
        }
        PartitionStatus::Replicated(status) => {
            row.ready = status.application_ready;
            row.disk_pending = status.disk_pending;
            row.validation_cycles = status.maintenance.validation_cycles;
            row.reclaimed_bytes = status.maintenance.reclaimed_bytes;
            if let Some(normal) = status.normal {
                row.state = admin::PartitionState::Normal;
                row.accepted = normal.accepted.op.0;
                row.committed = normal.committed.op.0;
                row.applied = normal.applied.op.0;
                row.pending_operations = normal.pending_operations as u64;
                row.pending_body_bytes = normal.pending_body_bytes as u64;
                row.written = normal.journal.written.0;
                row.durable = normal.journal.durable.0;
                row.leader_commit = normal.known_commit.0;
                row.followers = normal
                    .backups
                    .iter()
                    .flatten()
                    .map(|(id, acknowledged)| (*id, acknowledged.0))
                    .collect();
            }
        }
    }
    row
}

fn saturate(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// The admin view of a partition's committed state.
fn log(facts: &PartitionFacts) -> PartitionLog {
    PartitionLog {
        first_offset: facts.retained_from.get(),
        next_offset: facts.next_offset.get(),
        producers: u32::try_from(facts.producers).unwrap_or(u32::MAX),
        retention_age_millis: facts.retention.max_age_millis,
        retention_bytes: facts.retention.max_bytes,
        policy_revision: facts.policy_revision,
        segments: facts
            .segments
            .map_or(0, |(count, _)| u32::try_from(count).unwrap_or(u32::MAX)),
        segment_bytes: facts.segments.map_or(0, |(_, bytes)| bytes),
        dedup_entries: facts.dedup.map_or(0, |(entries, _)| saturate(entries)),
        dedup_limit: facts
            .dedup
            .and_then(|(_, limit)| std::num::NonZeroU64::new(saturate(limit))),
        groups: facts
            .groups
            .iter()
            .map(|group| GroupStatus {
                id: group.group,
                name: group.name.clone(),
                revision: group.revision,
                members: group
                    .members
                    .iter()
                    .map(|member| (member.id, member.node))
                    .collect(),
            })
            .collect(),
        cursors: facts
            .sources
            .iter()
            .map(|source| GroupCursor {
                group: source.group,
                member: source.assignment.and_then(|assignment| assignment.member),
                offset: source.progress.map(ozzy_proto::Offset::get),
            })
            .collect(),
    }
}
