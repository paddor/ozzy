use super::{AppendBuffer, JournalError, body};
use crate::replica_journal::{GroupProgressError, GroupProgressRequest, progress::Session};
use ozzy_journal::operation::{
    OperationBody, OperationLimits, Progress, ProgressAssignment, ProgressOwner,
    append_operation_body, append_operation_body_to,
};
use ozzy_proto::{Offset, append::Policy};

/// One joined progress update's operation in a rewritten proposal.
#[derive(Debug, Clone, Copy)]
pub(in crate::replica_journal) enum JoinedProgress {
    /// Repeats an accepted cursor; the proposal's commit covers it.
    Repeat,
    /// Advances the cursor from its expected prior.
    Advance { expected: Option<Offset> },
}

impl AppendBuffer {
    pub(in crate::replica_journal) fn prepare_group_progress(
        &mut self,
        request: GroupProgressRequest,
        policy: Policy,
    ) -> Result<(), JournalError> {
        if !self.is_empty() {
            return Err(JournalError::AppendMismatch);
        }
        let assignment = request.assignment;
        if assignment.group.as_bytes() == &[0; 16]
            || assignment.partition.as_bytes() == &[0; 16]
            || assignment.member.as_bytes() == &[0; 16]
            || assignment.epoch == 0
            || request.operation.as_bytes() == &[0; 16]
            || request.authority.group_id.as_bytes() == &[0; 16]
            || request.authority.config_epoch == 0
        {
            return Err(GroupProgressError::InvalidBinding.into());
        }
        // Reserve the largest optional-prior shape before owner validation.
        // The prior cursor is replaced from canonical state, never from a client.
        self.push_typed_body(&progress_body(request, Some(Offset::ZERO)))?;
        self.group_progress = Some(Box::new(Session {
            request,
            state: None,
        }));
        self.proposal_authority = Some(request.authority);
        self.proposal_policy = Some(policy);
        Ok(())
    }

    /// Append another prepared progress update as one more operation of this
    /// proposal; each operation keeps its own session for its reply. Declines,
    /// leaving both buffers unchanged, for a query, a different authority or
    /// policy, a cursor already present, or a proposal that is full.
    pub(in crate::replica_journal) fn join_group_progress(
        &mut self,
        rider: &Self,
    ) -> Result<bool, JournalError> {
        let (Some(primary), Some(session)) = (&self.group_progress, &rider.group_progress) else {
            return Err(JournalError::AppendMismatch);
        };
        let key = |request: GroupProgressRequest| {
            (request.assignment.group, request.assignment.partition)
        };
        if rider.len() != 1
            || !rider.progress_riders.is_empty()
            || primary.request.cursor.is_none()
            || session.request.cursor.is_none()
            || self.proposal_authority != rider.proposal_authority
            || self.proposal_policy != rider.proposal_policy
            || self
                .progress_sessions()
                .any(|joined| key(joined.request) == key(session.request))
            || self.len() >= self.limits.max_operations
            || self.body_bytes() + rider.body_bytes() > self.limits.max_body_bytes
        {
            return Ok(false);
        }
        let request = session.request;
        self.push_typed_body(&progress_body(request, Some(Offset::ZERO)))?;
        self.progress_riders.push(Session {
            request,
            state: None,
        });
        Ok(true)
    }

    /// The first progress update, then each joined one, in operation order.
    pub(in crate::replica_journal) fn progress_sessions(&self) -> impl Iterator<Item = &Session> {
        self.group_progress
            .as_deref()
            .into_iter()
            .chain(&self.progress_riders)
    }

    pub(in crate::replica_journal) fn progress_sessions_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut Session> {
        self.group_progress
            .as_deref_mut()
            .into_iter()
            .chain(&mut self.progress_riders)
    }

    /// Rewrite the joined progress operations: one per advancing update with
    /// its expected prior, none for a repeated cursor.
    pub(in crate::replica_journal) fn assign_group_progress_batch(
        &mut self,
        joined: &[JoinedProgress],
    ) -> Result<(), JournalError> {
        let requests = self
            .progress_sessions()
            .map(|session| session.request)
            .collect::<smallvec::SmallVec<[_; 8]>>();
        if requests.len() != joined.len() || self.len() > requests.len() {
            return Err(JournalError::AppendMismatch);
        }
        self.entries.clear();
        self.bodies.mutable()?.clear();
        for (request, joined) in requests.into_iter().zip(joined) {
            if let JoinedProgress::Advance { expected } = *joined {
                self.push_typed_body(&progress_body(request, expected))?;
            }
        }
        self.validated = false;
        self.prepared.clear();
        self.body_digests.clear();
        Ok(())
    }

    pub(in crate::replica_journal) fn assign_group_progress(
        &mut self,
        expected: Option<Offset>,
    ) -> Result<(), JournalError> {
        let request = self
            .group_progress
            .as_ref()
            .ok_or(JournalError::AppendMismatch)?
            .request;
        self.replace_control_body(&progress_body(request, expected))
    }

    pub(super) fn replace_control_body(
        &mut self,
        value: &OperationBody<'_>,
    ) -> Result<(), JournalError> {
        if self.len() != 1 || self.entries[0].envelope.kind != value.kind() {
            return Err(JournalError::AppendMismatch);
        }
        let limits = OperationLimits {
            max_body_bytes: self.limits.max_body_bytes,
            ..OperationLimits::default()
        };
        let mut size = body::EncodedSize::default();
        append_operation_body_to(&mut size, value, limits)?;
        self.bodies
            .reserve(size.0.saturating_sub(self.bodies.len()))?;
        let bytes = self.bodies.mutable()?;
        bytes.clear();
        let range = append_operation_body(bytes, value, limits)?;
        self.entries[0].body = range;
        self.entries[0].digest = None;
        self.validated = false;
        self.prepared.clear();
        self.body_digests.clear();
        Ok(())
    }
}

fn progress_body(
    request: GroupProgressRequest,
    expected: Option<Offset>,
) -> OperationBody<'static> {
    OperationBody::Progress(Progress {
        owner: ProgressOwner::ConsumerGroup(request.assignment.group),
        partition: request.assignment.partition,
        assignment: Some(ProgressAssignment {
            epoch: request.assignment.epoch,
            member: request.assignment.member,
        }),
        expected_progress: expected,
        new_progress: request.cursor.unwrap_or(Offset::ZERO),
        operation_id: request.operation,
    })
}
