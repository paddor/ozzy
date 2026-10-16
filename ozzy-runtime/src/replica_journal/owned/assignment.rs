use super::{AppendBuffer, JournalError, OwnedJournal};
use crate::replica_journal::{GroupAssignError, GroupAssignState};
use ozzy_journal::operation::ProgressOwner;
use ozzy_proto::group::Start;
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
            if current.is_some_and(|current| current.plan_revision >= request.plan_revision) {
                return Err(GroupAssignError::Fenced.into());
            }
            let expected = current.map_or(0, |current| current.epoch);
            let epoch = expected.checked_add(1).ok_or(GroupAssignError::Invalid)?;
            let cursor = if let Some(cursor) = images.speculative().progress(
                ProgressOwner::ConsumerGroup(request.group),
                request.partition,
            ) {
                cursor
            } else {
                let cursor = match request.initial {
                    Start::Earliest => retained_from,
                    Start::Latest => source.next_offset,
                    Start::Offset(cursor) => cursor,
                };
                if cursor < retained_from || cursor > source.next_offset {
                    return Err(GroupAssignError::InitialPosition {
                        cursor,
                        retained_from,
                        end: source.next_offset,
                    }
                    .into());
                }
                cursor
            };
            buffer.assign_group_cursor(expected, cursor)?;
            (epoch, cursor, None)
        };
        buffer
            .group_assignment
            .as_mut()
            .expect("validated assignment")
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
