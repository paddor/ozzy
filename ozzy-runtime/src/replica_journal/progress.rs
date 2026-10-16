//! Group processing cursors use the ordinary canonical proposal boundary.

use super::{JournalError, ProposalBuffer};
pub use ozzy_proto::progress::Assignment as GroupAssignment;
use ozzy_proto::{Offset, OperationId};

/// Query or advance a cumulative, exclusive processing cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupProgressRequest {
    /// Expected source leader generation.
    pub authority: ozzy_proto::append::Authority,
    /// Member and source assignment being acknowledged.
    pub assignment: GroupAssignment,
    /// Requested exclusive offset; absent queries confirmed progress.
    pub cursor: Option<Offset>,
    /// Internal identity for a newly required canonical transition.
    pub operation: OperationId,
}

/// Captured response coordinates, usable only after proposal confirmation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupProgressState {
    /// Exact processing ownership checked before returning these coordinates.
    pub assignment: GroupAssignment,
    /// Cumulative exclusive progress covered by the returned proposal prefix.
    pub cursor: Offset,
    /// Confirmed earliest retained record at validation.
    pub retained_from: Offset,
    /// Confirmed exclusive record end at validation.
    pub end: Offset,
    /// Required source confirmation policy.
    pub policy: ozzy_proto::append::Policy,
}

/// A rejected group request admits no canonical operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GroupProgressError {
    /// The binding names another group, configuration epoch, or source view.
    #[error("group progress authority is stale or foreign")]
    Authority,
    /// The binding must match the immutable source confirmation policy.
    #[error("group progress policy does not match the source")]
    Policy,
    /// Group, member, partition, operation, or epoch is invalid.
    #[error("invalid group progress binding")]
    InvalidBinding,
    /// This source partition does not exist in the selected image.
    #[error("unknown group progress partition")]
    UnknownPartition,
    /// The source assignment is missing, revoked, or belongs to another member.
    #[error("group progress assignment is fenced")]
    Fenced,
    /// Confirmed progress cannot be resumed from retained history.
    #[error("group progress {cursor:?} precedes retained history {retained_from:?}")]
    HistoryExpired {
        /// Unavailable exclusive resume offset.
        cursor: Offset,
        /// Confirmed earliest retained offset.
        retained_from: Offset,
    },
    /// A request attempts to acknowledge records beyond the confirmed end.
    #[error("group progress {cursor:?} exceeds confirmed record end {end:?}")]
    BeyondCommit {
        /// Requested exclusive processing offset.
        cursor: Offset,
        /// Confirmed exclusive record end.
        end: Offset,
    },
    /// A joined update needs validation of its own to report its result.
    #[error("joined group progress needs individual validation")]
    Batch,
}

#[derive(Debug)]
pub(super) struct Session {
    pub request: GroupProgressRequest,
    pub state: Option<GroupProgressState>,
}

impl ProposalBuffer {
    /// Prepare a bounded source-partition group request. The caller authenticates
    /// the link; the owner validates member, assignment, authority, and policy.
    /// Submit through the existing actor to wait for its confirmation boundary.
    pub fn prepare_group_progress(
        &mut self,
        request: GroupProgressRequest,
        policy: ozzy_proto::append::Policy,
    ) -> Result<(), JournalError> {
        self.0.prepare_group_progress(request, policy)
    }

    /// Captured cumulative coordinates. They authorize a response only after
    /// `ProposalOutcome::Committed`; preparation and transport receipt do not.
    pub fn group_progress_state(&self) -> Option<GroupProgressState> {
        self.0
            .group_progress
            .as_ref()
            .and_then(|session| session.state)
    }

    /// Join another prepared progress update as one more operation of this
    /// proposal, so one validation and replication round confirms both. The
    /// rider keeps its own arena and request. Declines, leaving both
    /// unchanged, for queries, a different authority or policy, a cursor
    /// already joined, or a full proposal.
    pub fn join_group_progress(&mut self, rider: &Self) -> Result<bool, JournalError> {
        self.0.join_group_progress(&rider.0)
    }

    /// Validated coordinates of the `index`th joined update, after the first.
    pub fn group_progress_rider_state(&self, index: usize) -> Option<GroupProgressState> {
        self.0
            .progress_riders
            .get(index)
            .and_then(|session| session.state)
    }

    /// Record coordinates validated for this update in the proposal it joined.
    /// Like the proposal's own, they authorize a reply only after commit.
    pub fn adopt_group_progress_state(
        &mut self,
        state: GroupProgressState,
    ) -> Result<(), JournalError> {
        let session = self
            .0
            .group_progress
            .as_mut()
            .filter(|session| session.request.assignment == state.assignment)
            .ok_or(JournalError::AppendMismatch)?;
        session.state = Some(state);
        Ok(())
    }

    /// This proposal's first progress update and its policy, to prepare it
    /// again on its own.
    pub fn group_progress_request(
        &self,
    ) -> Option<(GroupProgressRequest, ozzy_proto::append::Policy)> {
        Some((
            self.0.group_progress.as_ref()?.request,
            self.0.proposal_policy?,
        ))
    }
}
