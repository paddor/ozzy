use super::{Command, Job, State};
use crate::replica_journal::commands::{
    Action, AppendAction, MaintenanceAction, Reply, fault_log, finish, finish_read, read_fault,
};
use crate::replica_journal::history::HistoryAction;
use crate::replica_journal::install::InstallAction;
use crate::replica_journal::read::ReadAction;
use crate::replica_journal::recovery::RecoveryAction;
use crate::replica_journal::{
    JournalError, OwnedReplayRequest, OwnedSemaphorePermit, ProposalValidation, Turn, TurnResult,
};

/// Detached file work a command left behind, and whether it fenced the owner.
struct Outcome {
    job: Option<Job>,
    failed: bool,
}

impl Outcome {
    fn done(failed: bool) -> Self {
        Self { job: None, failed }
    }

    fn job(job: Job) -> Self {
        Self {
            job: Some(job),
            failed: false,
        }
    }
}

/// Detach prepared file work, or fail the command with its preparation error.
fn detach<W, T>(
    prepared: Result<W, JournalError>,
    done: Reply<T>,
    permit: OwnedSemaphorePermit,
    start: impl FnOnce(W, Reply<T>, OwnedSemaphorePermit) -> Job,
) -> Outcome {
    match prepared {
        Ok(work) => Outcome::job(start(work, done, permit)),
        Err(error) => Outcome::done(finish_read(done, Err(error), permit)),
    }
}

/// Run one exhaustive owner command. Detached work continues as a job.
pub(super) async fn run(
    mut state: Box<State>,
    command: Command,
    timestamp: u64,
) -> (Box<State>, Option<Job>, bool) {
    let Command {
        action,
        _permit: permit,
    } = command;
    let outcome = match action {
        Action::Read(action) => read(&mut state, action, permit).await,
        Action::Append(action) => append(&mut state, action, permit, timestamp).await,
        Action::Maintenance(action) => maintenance(&mut state, action, permit, timestamp).await,
        Action::History(action) => history(&mut state, action, permit).await,
        Action::Install(action) => install(&mut state, action, permit).await,
        Action::Recovery(action) => recovery(&mut state, action, permit).await,
    };
    let failed = outcome.failed || state.owner.is_faulted();
    (state, outcome.job, failed)
}

/// Partition reads continue as detached file jobs once prepared.
async fn read(state: &mut State, action: ReadAction, permit: OwnedSemaphorePermit) -> Outcome {
    match action {
        ReadAction::Seek {
            ticket,
            partition,
            start,
            done,
        } => {
            let prepared = state.owner.prepare_seek(ticket, partition, start).await;
            detach(prepared, done, permit, super::jobs::seek)
        }
        ReadAction::ReadPartition {
            read_permit,
            cursor,
            limits,
            buffer,
            done,
        } => match state.owner.prepare_read(cursor, limits, buffer) {
            Ok(read) => Outcome::job(super::jobs::read(read, done, permit, read_permit)),
            Err(error) => Outcome::done(done.finish(Err(error), permit)),
        },
    }
}

/// Writer-path commands: validation, proposals, admission, and syncs.
async fn append(
    state: &mut State,
    action: AppendAction,
    permit: OwnedSemaphorePermit,
    timestamp: u64,
) -> Outcome {
    let failed = match action {
        AppendAction::Promise { ticket, done } => {
            finish(done, state.owner.persist_promise(ticket).await, permit)
        }
        AppendAction::Validate {
            ticket,
            buffer,
            done,
        } => finish_read(
            done,
            state.owner.validate_append(ticket, buffer).await,
            permit,
        ),
        AppendAction::Propose {
            ticket,
            buffer,
            done,
        } => {
            let result = state.owner.propose_append(ticket, buffer, timestamp).await;
            reply_proposal(result, done, permit)
        }
        AppendAction::Admit {
            ticket,
            validated,
            done,
        } => finish(done, state.owner.admit_append(ticket, validated), permit),
        AppendAction::Apply { ticket, done } => finish(done, state.owner.apply(ticket), permit),
        AppendAction::Turn { turn, done } => {
            let result = Box::pin(turn_work(state, *turn, timestamp)).await;
            reply_turn(result.map(Box::new), done, permit)
        }
        AppendAction::Sync { ticket, done } => finish(done, state.owner.sync(ticket).await, permit),
        AppendAction::BeginPipelinedSync { ticket, done } => {
            return super::jobs::sync(state, ticket, done, permit)
                .map_or_else(|()| Outcome::done(true), Outcome::job);
        }
        AppendAction::FinishPipelinedSync { ready, done } => {
            finish(done, finish_pipelined_sync(state, &ready), permit)
        }
    };
    Outcome::done(failed)
}

/// Reply to a proposal. Only a storage fault among its rejections fences the owner.
fn reply_proposal(
    result: ProposalValidation,
    done: Reply<ProposalValidation>,
    permit: OwnedSemaphorePermit,
) -> bool {
    let failed =
        matches!(&result, ProposalValidation::Rejected { reason, .. } if read_fault(reason));
    if failed && let ProposalValidation::Rejected { reason, .. } = &result {
        fault_log(reason);
    }
    drop(permit);
    let _ = done.send(Ok(result));
    failed
}

fn reply_turn(
    result: Result<Box<TurnResult>, JournalError>,
    done: Reply<Box<TurnResult>>,
    permit: OwnedSemaphorePermit,
) -> bool {
    let fault = crate::replica_journal::turn::fault(&result);
    if let Some(error) = fault {
        fault_log(error);
    }
    let failed = fault.is_some();
    drop(permit);
    let _ = done.send(result);
    failed
}

fn finish_pipelined_sync(
    state: &mut State,
    ready: &crate::replica_journal::ReadyJournalSync,
) -> Result<ozzy_replication::SyncTicket, JournalError> {
    if state.sync == Some(ready.0) && !state.sync_busy {
        state.sync = None;
        Ok(ready.0.ticket)
    } else {
        Err(JournalError::CompletionMismatch)
    }
}

async fn maintenance(
    state: &mut State,
    action: MaintenanceAction,
    permit: OwnedSemaphorePermit,
    timestamp: u64,
) -> Outcome {
    let failed = match action {
        MaintenanceAction::Retention {
            ticket,
            seed,
            leader,
            done,
        } => {
            let turn = state.owner.retention_turn(ticket, timestamp, seed, leader);
            finish_read(done, turn.await, permit)
        }
        MaintenanceAction::RetainIdentities {
            ticket,
            leader,
            done,
        } => finish_read(
            done,
            state.owner.retain_identities_turn(ticket, leader),
            permit,
        ),
        MaintenanceAction::Release { ticket, done } => {
            return release(state, ticket, done, permit, timestamp).await;
        }
        MaintenanceAction::ValidateStorage {
            ticket,
            budget,
            done,
        } => {
            let prepared = state.owner.prepare_storage_validation(ticket, budget).await;
            return detach(prepared, done, permit, super::jobs::validate);
        }
        MaintenanceAction::CleanupOrphans {
            ticket,
            budget,
            done,
        } => {
            let result = super::jobs::cleanup(state, ticket, budget, false).await;
            finish_read(done, result, permit)
        }
        MaintenanceAction::CleanupMetadata {
            ticket,
            budget,
            done,
        } => {
            let result = super::jobs::cleanup(state, ticket, budget, true).await;
            finish_read(done, result, permit)
        }
    };
    Outcome::done(failed)
}

/// Pick due held records on the owner, then read them as a detached job.
async fn release(
    state: &mut State,
    ticket: ozzy_replication::driver::ValidationTicket,
    done: Reply<crate::replica_journal::ReleaseTurn>,
    permit: OwnedSemaphorePermit,
    timestamp: u64,
) -> Outcome {
    use crate::replica_journal::owned::release::ReleaseStep;
    let turn = match state.owner.prepare_release(ticket, timestamp).await {
        Ok(ReleaseStep::Read(read)) => {
            return Outcome::job(super::jobs::release(*read, done, permit));
        }
        Ok(ReleaseStep::Idle) => Ok(crate::replica_journal::ReleaseTurn::default()),
        Err(error) => Err(error),
    };
    Outcome::done(finish_read(done, turn, permit))
}

async fn history(
    state: &mut State,
    action: HistoryAction,
    permit: OwnedSemaphorePermit,
) -> Outcome {
    let failed = match action {
        HistoryAction::Capture { source, done } => {
            finish_read(done, state.owner.capture_history(source), permit)
        }
        HistoryAction::Release { source, done } => {
            finish_read(done, state.owner.release_history(source), permit)
        }
        HistoryAction::Position { source, op, done } => {
            finish_read(done, state.owner.history_position(source, op).await, permit)
        }
        HistoryAction::Fetch {
            request,
            buffer,
            done,
        } => {
            let fetched = state.owner.fetch_history(request, buffer).await;
            finish_read(done, fetched, permit)
        }
        HistoryAction::FetchReplication {
            ticket,
            predecessor,
            limits,
            buffer,
            done,
        } => {
            let prepared = state.next_request().and_then(|request_id| {
                let request = OwnedReplayRequest {
                    predecessor,
                    limits,
                    request_id,
                };
                state.owner.prepare_replay(ticket, request, buffer)
            });
            return detach(prepared, done, permit, super::jobs::replay);
        }
        HistoryAction::ReplicationPositions {
            ticket,
            requested,
            done,
        } => {
            let positions = state.owner.replication_positions(ticket, requested).await;
            finish_read(done, positions, permit)
        }
    };
    Outcome::done(failed)
}

async fn install(
    state: &mut State,
    action: InstallAction,
    permit: OwnedSemaphorePermit,
) -> Outcome {
    let failed = match action {
        InstallAction::Begin {
            ticket,
            config,
            done,
        } => finish(
            done,
            state.owner.begin_installation(ticket, config).await,
            permit,
        ),
        InstallAction::Chunk {
            ticket,
            buffer,
            done,
        } => finish(
            done,
            state.owner.install_chunk(ticket, buffer).await,
            permit,
        ),
        InstallAction::Finish { ticket, done } => finish(
            done,
            Box::pin(state.owner.finish_installation(ticket)).await,
            permit,
        ),
        InstallAction::Abort { ticket, done } => finish(
            done,
            Box::pin(state.owner.abort_installation(ticket)).await,
            permit,
        ),
        InstallAction::Activate { ticket, done } => {
            finish(done, state.owner.activate_installed(ticket).await, permit)
        }
    };
    Outcome::done(failed)
}

async fn recovery(
    state: &mut State,
    action: RecoveryAction,
    permit: OwnedSemaphorePermit,
) -> Outcome {
    match action {
        RecoveryAction::Checkpoint { pin, request, done } => {
            let prepared = state.owner.prepare_checkpoint_read(pin, request);
            detach(prepared, done, permit, super::jobs::checkpoint)
        }
        RecoveryAction::Pin {
            requester,
            response,
            done,
        } => {
            let pinned = state.owner.pin_recovery(requester, response).await;
            Outcome::done(finish_read(done, pinned, permit))
        }
        RecoveryAction::Release { pin, done } => {
            Outcome::done(finish_read(done, state.owner.release_recovery(pin), permit))
        }
        RecoveryAction::Fetch {
            pin,
            request,
            buffer,
            done,
        } => {
            let prepared = state.owner.prepare_recovery_read(pin, request, buffer);
            detach(prepared, done, permit, super::jobs::donation)
        }
    }
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
        Some((ticket, buffer)) => Some(state.owner.validate_received(ticket, buffer).await),
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
