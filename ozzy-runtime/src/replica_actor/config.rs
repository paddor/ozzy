//! Replica actor sessions, timing and hard work bounds, checked together.

use super::{ActorError, Duration, LinkSessionId, PipelineLimits, Timing, WireLimits};
use crate::replica_journal::InstallationConfig;
use crate::replica_transport::QueueLimits;

/// Maximum operations in one replica packet, independent of live-window capacity.
/// Keeps per-packet descriptor scratch bounded while journal groups can be larger.
pub const MAX_TRANSFER_OPERATIONS: usize = 64;

/// Flush triggers that leave room for later writes in the live pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncBatchTarget {
    /// Flush after this many complete canonical operations or validation attempts.
    pub operations: usize,
    /// Flush after this many complete canonical body bytes.
    pub body_bytes: usize,
}

impl SyncBatchTarget {
    /// Target half the live window, rounded up to at least one operation/byte.
    /// Windows larger than one can leave room for writes during an older barrier.
    /// This is a sizing starting point, not a measured optimum for every device.
    pub fn half_window(pipeline: PipelineLimits) -> Self {
        Self {
            operations: pipeline.max_operations.div_ceil(2).max(1),
            body_bytes: pipeline.max_body_bytes.div_ceil(2).max(1),
        }
    }
}

/// Initial sessions and hard work bounds for one replica actor.
#[derive(Debug, Clone, Copy)]
pub struct ActorConfig {
    /// Periodic bounded retention turns; absent for unlimited topic policies.
    pub retention_interval: Option<Duration>,
    /// Session with each voter in configuration order. The local entry is unused.
    /// Each remote entry must match that peer's binding for this connection.
    /// Zero means no established link. It permits startup, never wire admission.
    /// Shared scheduling replaces links through `ScheduledReplica::replace_session`.
    pub sessions: [LinkSessionId; 3],
    /// Protocol timeouts; storage startup happens before the actor clock starts.
    pub timing: Timing,
    /// Small receipt probe pacing, independent of durable/election deadlines.
    pub flow_probe: ozzy_replication::flow::ProbeTiming,
    /// Live core operation/body limits, including staged and validating receives.
    /// Background persistence may span multiple bounded journal commands.
    pub pipeline: PipelineLimits,
    /// Independently bounded applied packets retained for follower catch-up.
    /// Must cover at least one live window. Eviction never waits for followers.
    pub replay_cache: PipelineLimits,
    /// Maximum operations/body bytes in one PREPARE or history-transfer packet.
    /// Both bounds must be nonzero and no larger than the live pipeline. Producer
    /// proposals must fit one packet; backups can coalesce several packets into
    /// a larger journal write and sync group under the live bounds.
    /// Use a common transfer profile across voters; packet limits are not negotiated.
    /// Every retained canonical operation must fit without splitting its body.
    pub transfer: PipelineLimits,
    /// Group-flush triggers, independent of total live pipeline capacity.
    /// Both must be nonzero and no larger than the pipeline. One indivisible
    /// admitted write may cross a trigger, but never the hard pipeline limits.
    /// Backups coalesce staged work up to these targets while an older barrier
    /// runs. Its completion also releases partial groups without a new timer.
    pub sync_batch_target: SyncBatchTarget,
    /// Replicated-persisting only: maximum collection age from the first
    /// admitted write in a sync batch. Must be nonzero. Checked between journal
    /// actions on every voter; an expired batch syncs before accepting more
    /// queued work. An empty queue syncs immediately. Disk quorum publishes
    /// `DURABLE` once no write is in flight or a batch target fills. This is not a linger delay or an I/O timeout: an
    /// executing write/barrier cannot be interrupted and may exceed this age.
    pub sync_batch_max_age: Duration,
    /// Disk quorum only: publish `DURABLE` once no write is in flight and none
    /// completed for this long. A filled batch, due maintenance, or an election
    /// publishes sooner. Zero publishes as soon as no write is in flight.
    pub evidence_quiet: Duration,
    /// Startup-allocated producer lanes; no runtime sender registration.
    pub proposal_lanes: usize,
    /// Global outstanding proposal count, including queued and quorum-waiting work.
    pub proposal_capacity: usize,
    /// Independently reserved per-peer control queue.
    pub control: QueueLimits,
    /// Independently reserved per-peer bounded history queue.
    pub data: QueueLimits,
    /// Local selected-history staging policy.
    pub installation: InstallationConfig,
}

impl ActorConfig {
    pub(super) fn wire_limits(self) -> Result<WireLimits, ActorError> {
        let limits =
            WireLimits::for_transfer(self.transfer.max_operations, self.transfer.max_body_bytes)
                .ok_or(ActorError::Limits)?;
        let message_bytes = limits.message_bytes().ok_or(ActorError::Limits)?;
        if self
            .retention_interval
            .is_some_and(|interval| interval.is_zero())
            || self.pipeline.max_operations == 0
            || self.replay_cache.max_operations < self.pipeline.max_operations
            || self.replay_cache.max_operations > 65536
            || self.replay_cache.max_body_bytes < self.pipeline.max_body_bytes
            || u32::try_from(self.replay_cache.max_body_bytes).is_err()
            || self.replay_cache.max_body_bytes > isize::MAX as usize
            || self.transfer.max_operations == 0
            || self.transfer.max_operations > MAX_TRANSFER_OPERATIONS
            || self.transfer.max_operations > self.pipeline.max_operations
            || self.transfer.max_body_bytes == 0
            || self.transfer.max_body_bytes > self.pipeline.max_body_bytes
            || self.sync_batch_target.operations == 0
            || self.sync_batch_target.operations > self.pipeline.max_operations
            || self.sync_batch_target.body_bytes == 0
            || self.sync_batch_target.body_bytes > self.pipeline.max_body_bytes
            || self.sync_batch_max_age.is_zero()
            || self.pipeline.max_body_bytes == 0
            || u32::try_from(self.pipeline.max_body_bytes).is_err()
            || self.pipeline.max_body_bytes > isize::MAX as usize
            || self.data.message_bytes < message_bytes
            || self.control.message_bytes < 300
            || self.proposal_capacity == 0
            || self.proposal_capacity > crate::replica_journal::MAX_APPEND_OPERATIONS
            || self.proposal_lanes == 0
            || self.proposal_lanes > self.proposal_capacity
        {
            return Err(ActorError::Limits);
        }
        Ok(limits)
    }
}
