//! Source ownership changes share the ordinary canonical proposal boundary.

use super::{JournalError, ProposalBuffer};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, Offset, OperationId, PartitionIncarnation, group::Start,
};

/// A committed coordinator intent, relayed to its source partition owner by the
/// member that activates it. The source allocates the assignment epoch and
/// fences older plan revisions; it never trusts an epoch chosen by a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupAssignRequest {
    /// Expected source leader generation.
    pub authority: ozzy_proto::append::Authority,
    /// Group incarnation; ordinary membership churn preserves it.
    pub group: ConsumerGroupId,
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Monotonically increasing coordinator plan revision.
    pub plan_revision: u64,
    /// New owner, or no owner to revoke processing.
    pub member: Option<ConsumerMemberId>,
    /// First assignment only: the group's immutable creation selector.
    pub initial: Start,
    /// Internal identity for a newly required canonical transition.
    pub operation: OperationId,
}

/// Activation coordinates, usable only after `ProposalOutcome::Committed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupAssignState {
    /// Group incarnation.
    pub group: ConsumerGroupId,
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Newly confirmed processing epoch.
    pub epoch: u64,
    /// Coordinator plan revision confirmed at this source.
    pub plan_revision: u64,
    /// Confirmed owner; absent for a revocation.
    pub member: Option<ConsumerMemberId>,
    /// Progress captured with the assignment, unaffected by later acknowledgments.
    pub cursor: Offset,
    /// Confirmed retention floor when this response was prepared.
    pub retained_from: Offset,
    /// Confirmed exclusive record end when this response was prepared.
    pub end: Offset,
    /// Required source confirmation policy.
    pub policy: ozzy_proto::append::Policy,
}

/// A rejected source ownership change admits no canonical operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum GroupAssignError {
    /// Stale or foreign source leader coordinates.
    #[error("group assignment authority is stale or foreign")]
    Authority,
    /// The requested boundary differs from the source policy.
    #[error("group assignment policy does not match the source")]
    Policy,
    /// Invalid identity, revision, or epoch arithmetic.
    #[error("invalid group assignment request")]
    Invalid,
    /// A newer plan revision already reached this source.
    #[error("group assignment intent is fenced")]
    Fenced,
    /// A repeated plan revision names another owner.
    #[error("group assignment plan conflicts with accepted ownership")]
    Conflict,
    /// The source does not exist.
    #[error("unknown group assignment partition")]
    UnknownPartition,
    /// A first assignment selects a position outside confirmed retained history.
    #[error("initial group cursor {cursor:?} is outside {retained_from:?}..={end:?}")]
    InitialPosition {
        /// Requested first cursor.
        cursor: Offset,
        /// Confirmed earliest retained position.
        retained_from: Offset,
        /// Confirmed exclusive record end.
        end: Offset,
    },
}

#[derive(Debug)]
pub(super) struct Session {
    pub request: GroupAssignRequest,
    pub state: Option<GroupAssignState>,
}

impl ProposalBuffer {
    /// Prepare a bounded ownership change. The creation selector never replaces
    /// existing progress; the source captures its accepted image and epoch.
    pub fn prepare_group_assignment(
        &mut self,
        request: GroupAssignRequest,
        policy: ozzy_proto::append::Policy,
    ) -> Result<(), JournalError> {
        self.0.prepare_group_assignment(request, policy)
    }

    /// Captured activation coordinates. Preparation never authorizes delivery.
    pub fn group_assignment_state(&self) -> Option<GroupAssignState> {
        self.0.group_assignment.as_ref().and_then(|s| s.state)
    }
}
