use super::{AppendBuffer, JournalError};
use crate::replica_journal::{GroupAssignError, GroupAssignRequest, assignment::Session};
use ozzy_journal::operation::{Assign, OperationBody};
use ozzy_proto::{Offset, append::Policy};

impl AppendBuffer {
    pub(in crate::replica_journal) fn prepare_group_assignment(
        &mut self,
        request: GroupAssignRequest,
        policy: Policy,
    ) -> Result<(), JournalError> {
        if !self.is_empty() {
            return Err(JournalError::AppendMismatch);
        }
        if request.group.as_bytes() == &[0; 16]
            || request.partition.as_bytes() == &[0; 16]
            || request
                .member
                .is_some_and(|member| member.as_bytes() == &[0; 16])
            || request.operation.as_bytes() == &[0; 16]
            || request.authority.group_id.as_bytes() == &[0; 16]
            || request.authority.config_epoch == 0
            || request.plan_revision == 0
        {
            return Err(GroupAssignError::Invalid.into());
        }
        self.push_typed_body(&assignment_body(request, 0, Offset::ZERO))?;
        self.group_assignment = Some(Box::new(Session {
            request,
            state: None,
        }));
        self.proposal_authority = Some(request.authority);
        self.proposal_policy = Some(policy);
        Ok(())
    }

    /// The source fills the fence it observed and the cursor it captured;
    /// neither comes from the client.
    pub(in crate::replica_journal) fn assign_group_cursor(
        &mut self,
        expected_epoch: u64,
        cursor: Offset,
    ) -> Result<(), JournalError> {
        let request = self
            .group_assignment
            .as_ref()
            .ok_or(JournalError::AppendMismatch)?
            .request;
        self.replace_control_body(&assignment_body(request, expected_epoch, cursor))
    }
}

fn assignment_body(
    request: GroupAssignRequest,
    expected_epoch: u64,
    cursor: Offset,
) -> OperationBody<'static> {
    OperationBody::Assign(Assign {
        consumer_group_id: request.group,
        partition: request.partition,
        expected_assignment_epoch: expected_epoch,
        new_assignment_epoch: expected_epoch.saturating_add(1),
        plan_revision: request.plan_revision,
        resume_cursor: cursor,
        new_member: request.member,
        operation_id: request.operation,
    })
}
