use super::{Action, Command, Job, State};
use crate::replica_journal::commands::{finish, finish_read, read_fault};
use crate::replica_journal::{JournalError, ProposalValidation, Turn, TurnResult};

#[expect(
    clippy::too_many_lines,
    reason = "exhaustive shared command protocol; detached work lives in jobs"
)]
pub(super) async fn run(
    mut state: Box<State>,
    command: Command,
    timestamp: u64,
) -> (Box<State>, Option<Job>, bool) {
    let Command {
        action,
        _permit: permit,
    } = command;
    let mut job = None;
    let failed = match action {
        Action::Promise { ticket, done } => {
            finish(done, state.owner.persist_promise(ticket).await, permit)
        }
        Action::Validate {
            ticket,
            buffer,
            done,
        } => finish_read(
            done,
            state.owner.validate_append(ticket, buffer).await,
            permit,
        ),
        Action::Propose {
            ticket,
            buffer,
            done,
        } => {
            let result = state.owner.propose_append(ticket, buffer, timestamp).await;
            let failed = matches!(&result, ProposalValidation::Rejected { reason, .. } if read_fault(reason));
            drop(permit);
            let _ = done.send(Ok(result));
            failed
        }
        Action::Admit {
            ticket,
            validated,
            done,
        } => finish(done, state.owner.admit_append(ticket, validated), permit),
        Action::Apply { ticket, done } => finish(done, state.owner.apply(ticket), permit),
        Action::Turn { turn, done } => {
            let result = Box::pin(turn_work(&mut state, *turn, timestamp))
                .await
                .map(Box::new);
            let failed = crate::replica_journal::turn::faulted(&result);
            drop(permit);
            let _ = done.send(result);
            failed
        }
        Action::Sync { ticket, done } => finish(done, state.owner.sync(ticket).await, permit),
        Action::BeginPipelinedSync { ticket, done } => {
            let result = super::jobs::sync(&mut state, ticket, done, permit);
            match result {
                Ok(work) => {
                    job = Some(work);
                    false
                }
                Err(()) => true,
            }
        }
        Action::FinishPipelinedSync { ready, done } => {
            let result = if state.sync == Some(ready.0) && !state.sync_busy {
                state.sync = None;
                Ok(ready.0.ticket)
            } else {
                Err(JournalError::CompletionMismatch)
            };
            finish(done, result, permit)
        }
        Action::ReadPartition {
            read_permit,
            cursor,
            limits,
            buffer,
            done,
        } => match state.owner.prepare_read(cursor, limits, buffer) {
            Ok(read) => {
                job = Some(super::jobs::read(read, done, permit, read_permit));
                false
            }
            Err(error) => done.finish(Err(error), permit),
        },
        Action::ValidateStorage {
            ticket,
            budget,
            done,
        } => match state.owner.prepare_storage_validation(ticket, budget).await {
            Ok(work) => {
                job = Some(super::jobs::validate(work, done, permit));
                false
            }
            Err(error) => finish_read(done, Err(error), permit),
        },
        Action::CaptureHistory { source, done } => {
            finish_read(done, state.owner.capture_history(source), permit)
        }
        Action::ReleaseHistory { source, done } => {
            finish_read(done, state.owner.release_history(source), permit)
        }
        Action::HistoryPosition { source, op, done } => {
            finish_read(done, state.owner.history_position(source, op).await, permit)
        }
        Action::FetchHistory {
            request,
            buffer,
            done,
        } => finish_read(
            done,
            state.owner.fetch_history(request, buffer).await,
            permit,
        ),
        Action::FetchReplication {
            ticket,
            predecessor,
            limits,
            buffer,
            done,
        } => {
            let result = state.next_request().and_then(|id| {
                state
                    .owner
                    .prepare_replay(ticket, predecessor, limits, buffer, id)
            });
            match result {
                Ok(work) => {
                    job = Some(super::jobs::replay(work, done, permit));
                    false
                }
                Err(error) => finish_read(done, Err(error), permit),
            }
        }
        Action::ReplicationPositions {
            ticket,
            requested,
            done,
        } => finish_read(
            done,
            state.owner.replication_positions(ticket, requested).await,
            permit,
        ),
        Action::BeginInstall {
            ticket,
            config,
            done,
        } => finish(
            done,
            state.owner.begin_installation(ticket, config).await,
            permit,
        ),
        Action::InstallChunk {
            ticket,
            buffer,
            done,
        } => finish(
            done,
            state.owner.install_chunk(ticket, buffer).await,
            permit,
        ),
        Action::FinishInstall { ticket, done } => finish(
            done,
            Box::pin(state.owner.finish_installation(ticket)).await,
            permit,
        ),
        Action::AbortInstall { ticket, done } => finish(
            done,
            Box::pin(state.owner.abort_installation(ticket)).await,
            permit,
        ),
        Action::Activate { ticket, done } => {
            finish(done, state.owner.activate_installed(ticket).await, permit)
        }
        Action::Recovery(action) => {
            use crate::replica_journal::recovery::RecoveryAction;
            match action {
                RecoveryAction::Pin {
                    requester,
                    response,
                    done,
                } => finish_read(
                    done,
                    state.owner.pin_recovery(requester, response).await,
                    permit,
                ),
                RecoveryAction::Release { pin, done } => {
                    finish_read(done, state.owner.release_recovery(pin), permit)
                }
                RecoveryAction::Fetch {
                    pin,
                    request,
                    buffer,
                    done,
                } => match state.owner.prepare_recovery_read(pin, request, buffer) {
                    Ok(work) => {
                        job = Some(super::jobs::donation(work, done, permit));
                        false
                    }
                    Err(error) => finish_read(done, Err(error), permit),
                },
            }
        }
        Action::CleanupOrphans {
            ticket,
            budget,
            done,
        } => {
            let result = super::jobs::cleanup(&mut state, ticket, budget, false).await;
            finish_read(done, result, permit)
        }
        Action::CleanupMetadata {
            ticket,
            budget,
            done,
        } => {
            let result = super::jobs::cleanup(&mut state, ticket, budget, true).await;
            finish_read(done, result, permit)
        }
        // Only a nonvoting recovery handle exposes receiving commands.
        Action::Receiving(_) => true,
    };
    let failed = failed || state.owner.is_faulted();
    (state, job, failed)
}

async fn turn_work(
    state: &mut State,
    turn: Turn,
    timestamp: u64,
) -> Result<TurnResult, JournalError> {
    let admitted = turn
        .admit
        .map(|(ticket, validated)| state.owner.admit_append(ticket, validated))
        .transpose()?;
    if admitted.is_some()
        && (turn
            .propose
            .as_ref()
            .is_some_and(|(_, buffer)| state.owner.validation_may_read(&buffer.0))
            || turn
                .validate
                .as_ref()
                .is_some_and(|(_, buffer)| state.owner.validation_may_read(buffer)))
    {
        return Ok(TurnResult {
            admitted,
            proposal: None,
            validated: None,
            applied: None,
            deferred: Some(Box::new(Turn {
                propose: turn.propose,
                validate: turn.validate,
                apply: turn.apply,
                ..Default::default()
            })),
        });
    }
    let proposal = match turn.propose {
        Some((ticket, buffer)) => Some(state.owner.propose_append(ticket, buffer, timestamp).await),
        None => None,
    };
    let validated = match turn.validate {
        Some((ticket, buffer)) => Some(state.owner.validate_append(ticket, buffer).await),
        None => None,
    };
    let applied = turn
        .apply
        .map(|ticket| state.owner.apply(ticket))
        .transpose()?;
    Ok(TurnResult {
        admitted,
        proposal,
        validated,
        applied,
        deferred: None,
    })
}
