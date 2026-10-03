//! Deterministic replication with explicit network and storage evidence.
//!
//! This core implements normal operation, durable view-change initiation, and
//! log selection and installation for three fixed durable voters. Intact-disk
//! restart leaves the last installed normal view or resumes an unfinished promise,
//! never old primary authority. Lost-state recovery has a bounded fresh-quorum
//! admission, streamed validation, and wire codecs. The runtime journal worker
//! owns immutable donor pins and the segment store has a nonvoting publication
//! gate. Runtime receiving workers and OMQ recovery actors validate transferred
//! history before handing off to fenced restart, never directly to voting.
//! Payload validation, persistence, transport, and deterministic application are
//! separate adapters. Normal metadata storage is preallocated and byte-bounded.
#![forbid(unsafe_code)]

mod configuration_record;
pub mod driver;
pub mod flow;
mod installation;
pub mod local;
mod normal;
pub mod recovery;
mod view_change;
pub mod wire;

pub use configuration_record::{
    CONFIGURATION_RECORD_BYTES, ConfigurationRecord, ConfigurationRecordError, ConfiguredVoter,
};
pub use installation::{
    InstallOutcome, InstallTicket, InstallingView, RejectedInstallation, StartView,
};
pub use normal::{Admission, NormalReplica, ReplicaSnapshot, Status};
pub use ozzy_journal::operation::Digest;
pub use ozzy_journal::progress::{JournalGeneration, OpNumber, SyncTicket, WriteTicket};
pub use view_change::{
    DoViewChange, FrozenLog, LogSource, PromiseTicket, RecoveredState, SelectedView,
    StartViewChange, ViewChange, ViewChangeError,
};

use ozzy_journal::operation::{CanonicalOperation, logical_operation_digest_with_body_digest};
use ozzy_journal::progress::ProgressError;
use ozzy_proto::{GroupId, NodeId};

/// Exact canonical group prefix. Zero with the zero digest is genesis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Prefix {
    /// Last operation in the contiguous prefix.
    pub op: OpNumber,
    /// Canonical chain digest at that operation.
    pub digest: Digest,
}

impl Prefix {
    /// Empty group history.
    pub const GENESIS: Self = Self {
        op: OpNumber(0),
        digest: Digest::ZERO,
    };
}

/// Immutable identity and consensus generation carried by normal messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scope {
    /// Persistent group identity.
    pub group_id: GroupId,
    /// Fixed membership/configuration epoch.
    pub configuration_epoch: u64,
    /// Digest of the complete configured membership, principals, and policy.
    pub configuration_digest: Digest,
    /// Active primary view, not an operation's original view.
    pub view: u64,
}

/// Confirmation boundary for a group whose history is persisted to a journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QuorumPolicy {
    /// Confirmation requires synchronized history on the leader and one backup.
    Durable = 1,
    /// Confirmation requires retained history on the leader and one backup.
    /// Journal writes proceed independently; restart needs separate recovery evidence.
    Replicated = 2,
}

/// Three ordered, distinct voters with an immutable confirmation policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Configuration {
    scope: Scope,
    voters: [NodeId; 3],
    policy: QuorumPolicy,
}

impl Configuration {
    /// Validate an explicitly bootstrapped fixed configuration at view zero.
    ///
    /// `digest` must identify the full durable configuration, including its
    /// authenticated voter mapping and [`ozzy_journal::integrity::PROFILE`].
    /// Prefer [`ConfigurationRecord`] for that canonical binding. The digest is
    /// not computed from socket identities.
    pub fn new(
        group_id: GroupId,
        configuration_epoch: u64,
        digest: Digest,
        voters: [NodeId; 3],
    ) -> Result<Self, ReplicationError> {
        if group_id.as_bytes() == &[0; 16]
            || voters.iter().any(|voter| voter.as_bytes() == &[0; 16])
            || digest == Digest::ZERO
            || voters[0] == voters[1]
            || voters[0] == voters[2]
            || voters[1] == voters[2]
        {
            return Err(ReplicationError::InvalidConfiguration);
        }
        Ok(Self {
            scope: Scope {
                group_id,
                configuration_epoch,
                configuration_digest: digest,
                view: 0,
            },
            voters,
            policy: QuorumPolicy::Durable,
        })
    }

    /// Confirmation boundary bound into the canonical configuration digest.
    pub const fn policy(self) -> QuorumPolicy {
        self.policy
    }

    /// Native writer policy bound to this immutable group configuration.
    pub const fn append_policy(self) -> ozzy_proto::append::Policy {
        match self.policy {
            QuorumPolicy::Durable => ozzy_proto::append::Policy::QuorumDurable,
            QuorumPolicy::Replicated => ozzy_proto::append::Policy::QuorumReplicatedPersisting,
        }
    }

    /// Initial configuration scope. Later views require the view-change protocol.
    pub const fn scope(self) -> Scope {
        self.scope
    }

    /// Ordered persistent voter identities.
    pub const fn voters(&self) -> &[NodeId; 3] {
        &self.voters
    }

    /// Deterministic primary selection. Does not grant leadership authority.
    pub fn primary(&self, view: u64) -> NodeId {
        self.voters[(view % 3) as usize]
    }

    fn voter_index(&self, id: NodeId) -> Result<usize, ReplicationError> {
        self.voters
            .iter()
            .position(|&voter| voter == id)
            .ok_or(ReplicationError::UnknownVoter)
    }
}

/// Bounds on live prepare metadata/payloads and each streamed validation chunk.
/// Frozen recovered/installed history can exceed these bounds and stays on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineLimits {
    /// Maximum retained live descriptors, preallocated at bootstrap or restart.
    pub max_operations: usize,
    /// Maximum total canonical body bytes retained by the payload adapter.
    pub max_body_bytes: usize,
}

/// Metadata for one already validated canonical operation.
///
/// This value is not payload storage or proof of validation. The caller must
/// verify the body codec and deterministic application transition before using
/// it, and retain the exact body until application/replication releases it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedOperation {
    group_id: GroupId,
    configuration_epoch: u64,
    original_view: u64,
    prefix: Prefix,
    previous_digest: Digest,
    body_bytes: usize,
}

impl PreparedOperation {
    /// Reuse a verified body digest; hash only the small canonical envelope.
    ///
    /// The caller asserts `body_digest` was computed over `operation.body` and
    /// that both body and state transition passed their respective validators.
    pub fn from_verified(operation: &CanonicalOperation<'_>, body_digest: Digest) -> Self {
        Self {
            group_id: operation.group_id,
            configuration_epoch: operation.configuration_epoch,
            original_view: operation.original_view,
            prefix: Prefix {
                op: OpNumber(operation.op_number),
                digest: logical_operation_digest_with_body_digest(operation, body_digest),
            },
            previous_digest: operation.previous_digest,
            body_bytes: operation.body.len(),
        }
    }

    /// Canonical position and digest after this operation.
    pub const fn prefix(self) -> Prefix {
        self.prefix
    }

    /// Canonical body bytes charged against pipeline capacity.
    pub const fn body_bytes(self) -> usize {
        self.body_bytes
    }
}

/// Cumulative durable `PREPARE_OK`. Sender identity comes from authentication.
///
/// There is deliberately no written/received evidence variant in this initial
/// disk-quorum core. A transport adapter must reject weaker wire evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrepareOk {
    /// Group/configuration/current-view fence.
    pub scope: Scope,
    /// Highest matching contiguous prefix covered by a completed disk barrier.
    pub durable: Prefix,
}

/// Cumulative retained-memory vote for a background-persisting group.
/// This type cannot be passed to the disk-quorum acknowledgment entry point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedPrepareOk {
    /// Exact group, immutable policy/configuration, and current view.
    pub scope: Scope,
    /// Complete validated prefix whose payload remains owned by this broker.
    pub retained: Prefix,
}

/// Primary's cumulative commit announcement, also usable as an idle heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commit {
    /// Group/configuration/current-view fence.
    pub scope: Scope,
    /// Highest quorum-committed canonical operation.
    pub committed: Prefix,
}

/// Rejected replication event. Capacity and missing history are recoverable;
/// conflicting current lineage faults the normal core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReplicationError {
    /// A vote's evidence does not match the group's immutable confirmation policy.
    #[error("replication confirmation policy mismatch")]
    PolicyMismatch,
    /// Configuration needs nonzero group/voter IDs, distinct voters, and a digest.
    #[error("invalid fixed replication configuration")]
    InvalidConfiguration,
    /// Both pipeline bounds must be nonzero and operation count representable.
    #[error("invalid replication pipeline limits")]
    InvalidLimits,
    /// Message sender is not an authenticated configured voter.
    #[error("unknown replica voter")]
    UnknownVoter,
    /// This role cannot perform the requested action.
    #[error("action requires the current primary or a backup")]
    WrongRole,
    /// Group/configuration/view does not match this normal generation.
    #[error("replication message scope mismatch")]
    ScopeMismatch,
    /// Normal operation was fenced or faulted and must not resume in place.
    #[error("replica is not in normal operation")]
    NotNormal,
    /// Caller must backpressure rather than overrun payload/metadata capacity.
    #[error("replication pipeline capacity exhausted")]
    Capacity,
    /// One physical admission must include at least one operation.
    #[error("empty prepare group")]
    EmptyPrepare,
    /// Receiver needs an earlier prefix or an appropriately bounded retry.
    #[error("prepare or evidence leaves a local history gap")]
    HistoryGap,
    /// Metadata was released or is in a disk-backed installed tail; consult the log.
    #[error("operation metadata is outside the retained pipeline")]
    HistoryUnavailable,
    /// Same active lineage names incompatible canonical bytes.
    #[error("conflicting canonical replication lineage")]
    ConflictingHistory,
    /// Application completion may not outrun quorum/local-durability evidence.
    #[error("application completion exceeds committed prefix")]
    ApplyBeyondCommit,
    /// Installed primary must reconfirm and apply the selected tail first.
    #[error("selected view tail has not reached quorum and application")]
    ActivationPending,
    /// Existing journal ticket/generation contract rejected the completion.
    #[error(transparent)]
    Journal(#[from] ProgressError),
}
