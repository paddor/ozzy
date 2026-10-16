use super::{JournalError, MAX_APPEND_OPERATIONS, authority};
use ozzy_journal_segment::{
    CanonicalRecoveryLimits, CommitMode, GroupIdentity, JournalFormat, JournalLimits, Manifest,
    SegmentHeader, SegmentWriteMode,
};
use ozzy_replication::{
    ConfigurationRecord, Digest, JournalGeneration, PipelineLimits, QuorumPolicy,
};
use std::path::PathBuf;
use tokio::sync::Semaphore;

pub use ozzy_core::dedup::Limits as DedupLimits;

/// Size a partition's identity reservation for its live pipeline. Pending
/// identities stay charged until application, and a backup can hold its
/// whole retained capacity accepted but not yet applied.
pub fn dedup_limits(
    window_millis: u64,
    entries: usize,
    bytes: usize,
    max_records: usize,
    pipeline: PipelineLimits,
) -> DedupLimits {
    let operations = pipeline.backup().max_operations;
    let batch_records = max_records.min(entries);
    DedupLimits {
        window_millis,
        entries,
        batch_records,
        pending_records: entries.min(batch_records.saturating_mul(operations)),
        pending_operations: operations,
        bytes,
    }
}

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
    pub limits: JournalLimits,
    /// Canonical replay, identity cache and pending-transition bounds. A
    /// replicated owner admits a backup's retained window unapplied.
    pub recovery: CanonicalRecoveryLimits,
    /// Optional partition-local identity reservation, separate from core clones.
    /// Its window must exactly match the immutable canonical topic policy.
    pub deduplication: Option<DedupLimits>,
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
    pub reads: ozzy_journal_segment::PartitionReadLimits,
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

    /// Reject a stored manifest outside this configuration's supported profile.
    pub(super) fn validate_manifest(&self, manifest: &Manifest) -> Result<(), JournalError> {
        authority::validate_manifest(
            manifest,
            self.configuration
                .configuration()
                .scope()
                .configuration_epoch,
            self.limits.io.max_segment_bytes,
        )
    }

    /// Format of an absent store whose first segment holds `capacity` bytes.
    pub(super) fn journal_format(&self, capacity: u64) -> Result<JournalFormat, JournalError> {
        let first_segment =
            SegmentHeader::new(self.identity.group_id, 1, None, Digest::ZERO, capacity)
                .map_err(ozzy_journal_segment::DirectoryError::from)?;
        Ok(JournalFormat {
            identity: self.identity,
            configuration_epoch: self
                .configuration
                .configuration()
                .scope()
                .configuration_epoch,
            commit_mode: CommitMode::External,
            first_segment,
            configuration: self.configuration.encode().to_vec(),
        })
    }
}

impl<C> OwnedConfig<C> {
    pub(super) fn validate_storage(
        &self,
        generation: JournalGeneration,
        mode: SegmentWriteMode,
    ) -> Result<(), JournalError> {
        if let Some(limits) = self.deduplication {
            limits.validate()?;
            if limits.pending_operations < self.writeback.max_operations {
                return Err(JournalError::Configuration);
            }
        }
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
