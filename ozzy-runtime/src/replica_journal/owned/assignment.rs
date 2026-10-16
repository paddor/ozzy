use super::{AppendBuffer, JournalError, OwnedJournal};
use crate::replica_journal::{GroupAssignError, GroupAssignRequest, GroupAssignState};
use ozzy_core::state::{AssignmentState, CanonicalState};
use ozzy_journal::operation::ProgressOwner;
use ozzy_proto::{Offset, group::Start};
use ozzy_replication::{Prefix, driver::ValidationTicket};

impl OwnedJournal {
    pub(super) fn prepare_group_assignment(
        &self,
        ticket: ValidationTicket,
        buffer: &mut AppendBuffer,
    ) -> Result<Option<Prefix>, JournalError> {
        let session = buffer
            .group_assignment
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?;
        session.state = None;
        let request = session.request;
        if buffer.len() != 1 || buffer.is_producer() || buffer.is_partition_creation() {
            return Err(JournalError::AppendMismatch);
        }
        let images = self.images()?;
        let source = images
            .committed()
            .partition(request.partition)
            .ok_or(GroupAssignError::UnknownPartition)?;
        let retained_from = images
            .speculative()
            .partition(request.partition)
            .ok_or(GroupAssignError::UnknownPartition)?
            .retained_from;
        let current = images
            .speculative()
            .assignment(request.group, request.partition);
        let (epoch, cursor, resolved) = if let Some(current) =
            current.filter(|current| current.plan_revision == request.plan_revision)
        {
            if current.member != request.member {
                return Err(GroupAssignError::Conflict.into());
            }
            // A lost reply must return the cursor captured by that assignment,
            // even if its new owner has already accepted newer progress.
            (
                current.epoch,
                current.resume_cursor,
                Some(ticket.accepted()),
            )
        } else {
            let (expected, epoch, cursor) = next_epoch(
                images.speculative(),
                request,
                current,
                retained_from,
                source.next_offset,
            )?;
            buffer.assign_group_cursor(expected, cursor)?;
            (epoch, cursor, None)
        };
        buffer
            .group_assignment
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?
            .state = Some(GroupAssignState {
            group: request.group,
            partition: request.partition,
            epoch,
            plan_revision: request.plan_revision,
            member: request.member,
            cursor,
            retained_from: source.retained_from,
            end: source.next_offset,
            policy: self.configuration.append_policy(),
        });
        Ok(resolved)
    }
}

/// A newer plan revision replaces the `current` epoch with the next one. Its
/// cursor is the group's progress, or the creation selector without progress.
fn next_epoch(
    speculative: &CanonicalState,
    request: GroupAssignRequest,
    current: Option<AssignmentState>,
    retained_from: Offset,
    end: Offset,
) -> Result<(u64, u64, Offset), GroupAssignError> {
    if current.is_some_and(|current| current.plan_revision >= request.plan_revision) {
        return Err(GroupAssignError::Fenced);
    }
    let expected = current.map_or(0, |current| current.epoch);
    let epoch = expected.checked_add(1).ok_or(GroupAssignError::Invalid)?;
    let progress = speculative.progress(
        ProgressOwner::ConsumerGroup(request.group),
        request.partition,
    );
    let cursor = match progress {
        Some(cursor) => cursor,
        None => initial_cursor(request.initial, retained_from, end)?,
    };
    Ok((expected, epoch, cursor))
}

/// Where a group with no progress on a partition starts reading it.
fn initial_cursor(
    start: Start,
    retained_from: Offset,
    end: Offset,
) -> Result<Offset, GroupAssignError> {
    let cursor = match start {
        Start::Earliest => retained_from,
        Start::Latest => end,
        Start::Offset(cursor) => cursor,
    };
    if cursor < retained_from || cursor > end {
        return Err(GroupAssignError::InitialPosition {
            cursor,
            retained_from,
            end,
        });
    }
    Ok(cursor)
}
