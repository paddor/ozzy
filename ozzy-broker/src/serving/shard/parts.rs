//! Steps the configured partitions and the runtime logs share on the way
//! from an opened journal to an actor on the shard.

use super::{Config, failure};
use crate::journals::OpenedRecovery;
use crate::{
    JournalConfig, OpenedPartition, PartitionJournal, ShardMemory, StartedPartition, StartupError,
};
use ozzy_proto::{GroupId, append::Policy};
use ozzy_runtime::{
    log::{Log, LogPort},
    replica_actor::{ActorIds, LogLane, PartitionActor},
};
use std::collections::BTreeMap;

/// The confirmation policy, append bound, and group of a journal.
pub(super) fn profile(config: &JournalConfig) -> (Policy, usize, GroupId) {
    match config {
        JournalConfig::Local(config) => (
            Policy::LocalDurable,
            config.append_limits.max_body_bytes,
            config.identity.group_id,
        ),
        JournalConfig::Replicated(config) => (
            Policy::QuorumDurable,
            config.append_limits.max_body_bytes,
            config.identity.group_id,
        ),
    }
}

/// The handle and port of `plan`'s log, bounded by its append bound.
pub(super) fn log_channel(plan: &PartitionJournal) -> Result<(Log, LogPort), StartupError> {
    let (_, body_bytes, group) = profile(&plan.config);
    let limits = super::super::config::log_limits(body_bytes);
    ozzy_runtime::log::log_channel(plan.incarnation, limits)
        .ok_or_else(|| failure(format!("invalid log limits for group {group}")))
}

/// The lane of `plan`'s log service on a serving `actor`.
pub(super) fn log_lane(
    actor: &mut PartitionActor,
    plan: &PartitionJournal,
) -> Result<LogLane, StartupError> {
    let (_, body_bytes, _) = profile(&plan.config);
    LogLane::new(actor, super::super::config::log_limits(body_bytes)).map_err(failure)
}

/// The actor of a journal that opened to serve, without links yet.
pub(super) fn serving_actor(
    opened: OpenedPartition,
    memory: &ShardMemory,
    config: &Config,
) -> Result<StartedPartition, StartupError> {
    opened.into_actor(
        &memory.data,
        &BTreeMap::new(),
        ActorIds::random(config.receipt_handles.clone()),
        config.journal_clock(),
    )
}

/// The actor of a journal that opened as a nonvoting copy, without links
/// yet.
pub(super) fn recovering_actor(
    opened: OpenedRecovery,
    memory: &ShardMemory,
    config: &Config,
) -> Result<PartitionActor, StartupError> {
    opened.into_actor(
        &memory.data,
        &BTreeMap::new(),
        ActorIds::random(config.receipt_handles.clone()),
        config.journal_clock(),
    )
}

/// Bind a serving actor's follower memory, let it publish, and prepare the
/// proposal that creates its partition.
pub(super) fn prepare_serving(
    started: &mut StartedPartition,
    memory: &ShardMemory,
) -> Result<(), StartupError> {
    started
        .actor
        .bind_receive_owner(&memory.replica)
        .map_err(failure)?;
    started.actor.enable_publication();
    started.prepare_partition()
}
