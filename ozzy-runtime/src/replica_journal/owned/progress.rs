use super::{AppendBuffer, JournalError, OwnedJournal};
use crate::replica_journal::append::JoinedProgress;
use crate::replica_journal::{
    GroupAssignment, GroupProgressError, GroupProgressRequest, GroupProgressState,
};
use ozzy_core::state::{CanonicalPartition, CanonicalState};
use ozzy_journal::operation::ProgressOwner;
use ozzy_proto::{Offset, append::Policy};
use ozzy_replication::{Prefix, driver::ValidationTicket};

/// Where one cursor update stands against its partition.
enum Update {
    /// An accepted cursor already covers it.
    Repeat(Offset),
    /// It moves the cursor from `prior`.
    Advance { prior: Option<Offset> },
    /// It names an offset past the partition's end.
    Beyond,
    /// It names an offset retention already removed.
    Expired,
}

fn classify(prior: Option<Offset>, cursor: Offset, partition: &CanonicalPartition) -> Update {
    if cursor > partition.next_offset {
        Update::Beyond
    } else if let Some(prior) = prior.filter(|&prior| prior >= cursor) {
        Update::Repeat(prior)
    } else if cursor < partition.retained_from {
        Update::Expired
    } else {
        Update::Advance { prior }
    }
}

fn progress_state(
    assignment: GroupAssignment,
    cursor: Offset,
    partition: &CanonicalPartition,
    policy: Policy,
) -> GroupProgressState {
    GroupProgressState {
        assignment,
        cursor,
        retained_from: partition.retained_from,
        end: partition.next_offset,
        policy,
    }
}

impl OwnedJournal {
    // Out of line: inlined into the shared proposal path, this rare branch
    // raised every APPEND's p99 at 100k records/s from 15 to 21 ms.
    #[inline(never)]
    pub(super) fn prepare_group_progress(
        &self,
        ticket: ValidationTicket,
        buffer: &mut AppendBuffer,
    ) -> Result<Option<Prefix>, JournalError> {
        if !buffer.progress_riders.is_empty() {
            return self.prepare_group_progress_batch(ticket, buffer);
        }
        let session = buffer
            .group_progress
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?;
        session.state = None;
        let request = session.request;
        if buffer.len() != 1 || buffer.is_producer() || buffer.is_partition_creation() {
            return Err(JournalError::AppendMismatch);
        }
        let images = self.images()?;
        validate_assignment(images.speculative(), request.assignment)?;
        let partition = images
            .committed()
            .partition(request.assignment.partition)
            .ok_or(GroupProgressError::UnknownPartition)?;
        let (cursor, resolved) = match request.cursor {
            Some(cursor) => {
                let prior = images.speculative().progress(
                    ProgressOwner::ConsumerGroup(request.assignment.group),
                    request.assignment.partition,
                );
                match classify(prior, cursor, partition) {
                    Update::Beyond => {
                        let end = partition.next_offset;
                        return Err(GroupProgressError::BeyondCommit { cursor, end }.into());
                    }
                    Update::Expired => return Err(expired(cursor, partition)),
                    // Cumulative retries cover existing accepted work. The actor waits
                    // for confirmation and application of this exact accepted prefix.
                    Update::Repeat(prior) => (prior, Some(ticket.accepted())),
                    Update::Advance { prior } => {
                        buffer.assign_group_progress(prior)?;
                        (cursor, None)
                    }
                }
            }
            // Query only confirmed application state, even with newer accepted
            // progress. It neither waits for unrelated work nor changes the log.
            None => (
                confirmed_cursor(images.committed(), request, partition)?,
                Some(self.applied),
            ),
        };
        let policy = self.configuration.append_policy();
        buffer
            .group_progress
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?
            .state = Some(progress_state(
            request.assignment,
            cursor,
            partition,
            policy,
        ));
        Ok(resolved)
    }

    /// Validate every joined update on its own assignment, then rewrite one
    /// operation per fresh cursor with its expected prior. An update that
    /// repeats an accepted cursor needs no operation: the proposal's commit
    /// covers that cursor's accepted prefix too, and when every update
    /// repeats one, the batch resolves through the accepted prefix. Any
    /// invalid update fails the proposal; the intake then submits its
    /// updates singly for individual results.
    fn prepare_group_progress_batch(
        &self,
        ticket: ValidationTicket,
        buffer: &mut AppendBuffer,
    ) -> Result<Option<Prefix>, JournalError> {
        if buffer.len() > 1 + buffer.progress_riders.len()
            || buffer.is_producer()
            || buffer.is_partition_creation()
        {
            return Err(JournalError::AppendMismatch);
        }
        let images = self.images()?;
        let committed = images.committed();
        let policy = self.configuration.append_policy();
        let mut expected = smallvec::SmallVec::<[_; 8]>::new();
        for session in buffer.progress_sessions_mut() {
            session.state = None;
            let request = session.request;
            let cursor = request.cursor.ok_or(GroupProgressError::Batch)?;
            validate_assignment(images.speculative(), request.assignment)?;
            let partition = committed
                .partition(request.assignment.partition)
                .ok_or(GroupProgressError::UnknownPartition)?;
            let prior = images.speculative().progress(
                ProgressOwner::ConsumerGroup(request.assignment.group),
                request.assignment.partition,
            );
            let (cursor, joined) = match classify(prior, cursor, partition) {
                Update::Beyond | Update::Expired => return Err(GroupProgressError::Batch.into()),
                Update::Repeat(prior) => (prior, JoinedProgress::Repeat),
                Update::Advance { prior } => (cursor, JoinedProgress::Advance { expected: prior }),
            };
            expected.push(joined);
            session.state = Some(progress_state(
                request.assignment,
                cursor,
                partition,
                policy,
            ));
        }
        if expected
            .iter()
            .all(|joined| matches!(joined, JoinedProgress::Repeat))
        {
            return Ok(Some(ticket.accepted()));
        }
        buffer.assign_group_progress_batch(&expected)?;
        Ok(None)
    }
}

/// The confirmed cursor a progress query answers with.
fn confirmed_cursor(
    committed: &CanonicalState,
    request: GroupProgressRequest,
    partition: &CanonicalPartition,
) -> Result<Offset, JournalError> {
    validate_assignment(committed, request.assignment)?;
    let cursor = committed
        .progress(
            ProgressOwner::ConsumerGroup(request.assignment.group),
            request.assignment.partition,
        )
        .ok_or(GroupProgressError::Fenced)?;
    if cursor < partition.retained_from {
        return Err(expired(cursor, partition));
    }
    Ok(cursor)
}

fn expired(cursor: Offset, partition: &CanonicalPartition) -> JournalError {
    GroupProgressError::HistoryExpired {
        cursor,
        retained_from: partition.retained_from,
    }
    .into()
}

fn validate_assignment(
    state: &CanonicalState,
    assignment: GroupAssignment,
) -> Result<(), GroupProgressError> {
    if state.partition(assignment.partition).is_none() {
        return Err(GroupProgressError::UnknownPartition);
    }
    if !state
        .assignment(assignment.group, assignment.partition)
        .is_some_and(|current| {
            current.epoch == assignment.epoch && current.member == Some(assignment.member)
        })
    {
        return Err(GroupProgressError::Fenced);
    }
    Ok(())
}
