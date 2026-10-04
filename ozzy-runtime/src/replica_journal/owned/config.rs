use super::{JournalError, MAX_APPEND_OPERATIONS};
use ozzy_journal_segment::{
    AsyncJournalLimits, CanonicalRecoveryLimits, GroupIdentity, SegmentWriteMode,
};
use ozzy_replication::{ConfigurationRecord, JournalGeneration, PipelineLimits, QuorumPolicy};
use std::path::PathBuf;
use tokio::sync::Semaphore;

/// Storage policy for one local or replicated partition. Backend execution and admission
/// belong to the supplied shard lane, shared with its other partition actors.
#[derive(Debug, Clone)]
pub struct OwnedConfig<C = ConfigurationRecord> {
    /// Exact established directory, or absent directory for explicit formatting.
    pub root: PathBuf,
    /// Expected persistent group, broker, volume and store identity.
    pub identity: GroupIdentity,
    /// Exact persisted membership, order and confirmation policy.
    pub configuration: C,
    /// Bounded metadata, segment, transfer and directory work.
    pub limits: AsyncJournalLimits,
    /// Canonical replay, identity cache and pending-transition bounds.
    pub recovery: CanonicalRecoveryLimits,
    /// Maximum simultaneously leased APPEND/history arenas for this owner.
    pub append_buffers: usize,
    /// Count/body bounds for each arena, separate from the shared device budget.
    pub append_limits: PipelineLimits,
    /// Accepted data retained until physical completion. Independent of arenas
    /// and of the shared backend's per-device queue/admission limits.
    pub writeback: PipelineLimits,
    /// Physical group collection target; an indivisible larger operation fits
    /// under decoder and segment limits rather than being split.
    pub write_group_bytes: usize,
    /// Incremental reader indexes, payload residency, and detached read limits.
    pub reads: ozzy_journal_segment::AsyncPartitionReadLimits,
}

impl OwnedConfig {
    pub(super) fn validate(&self, generation: JournalGeneration) -> Result<(), JournalError> {
        let configuration = self.configuration.configuration();
        let mode = match configuration.policy() {
            QuorumPolicy::Replicated => SegmentWriteMode::Buffered,
            QuorumPolicy::Durable => SegmentWriteMode::DataSync,
        };
        if self.identity.group_id != configuration.scope().group_id
            || !configuration
                .voters()
                .contains(&self.identity.replica_node_id)
        {
            return Err(JournalError::Configuration);
        }
        self.validate_storage(generation, mode)
    }
}

impl<C> OwnedConfig<C> {
    pub(super) fn validate_storage(
        &self,
        generation: JournalGeneration,
        mode: SegmentWriteMode,
    ) -> Result<(), JournalError> {
        if self.limits.io.write_mode != mode
            || generation.0 == 0
            || self.append_buffers == 0
            || self.append_buffers > Semaphore::MAX_PERMITS
            || self.append_limits.max_operations == 0
            || self.append_limits.max_operations > MAX_APPEND_OPERATIONS
            || self.append_limits.max_operations > self.recovery.accepted_transitions
            || self.append_limits.max_body_bytes == 0
            || self.append_limits.max_body_bytes > isize::MAX as usize
            || self.limits.io.max_segment_bytes > isize::MAX as u64
            || self.writeback.max_operations == 0
            || self.writeback.max_operations > self.recovery.accepted_transitions
            || self.writeback.max_body_bytes == 0
            || self.writeback.max_body_bytes > isize::MAX as usize
            || self.write_group_bytes == 0
            || self.write_group_bytes > self.writeback.max_body_bytes
            || self.reads.concurrent_reads == 0
        {
            return Err(JournalError::Configuration);
        }
        Ok(())
    }
}
