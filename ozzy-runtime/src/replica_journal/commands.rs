//! Bounded typed command admission. Rejected payloads remain owned by the caller.

use super::ReadyJournalSync;
use super::receiving::ReceiveAction;
use super::recovery::RecoveryAction;
use super::{
    AppendBuffer, JournalCompletion, JournalError, OwnedSemaphorePermit, PromiseTicket,
    ReplicaJournal, SubmitError, ValidatedAppend, completion,
};
use super::{ProposalBuffer, ProposalValidation};
use ozzy_core::state::{CanonicalImagesError, IdentityIndexError, StateError};
use ozzy_replication::driver::ValidationTicket;
use ozzy_replication::{SyncTicket, WriteTicket};

/// An unsubmitted value returned intact for retry. No disk action was admitted.
#[derive(Debug)]
pub struct Rejected<T> {
    /// Backpressure or closed admission, never a persistence outcome.
    pub reason: SubmitError,
    /// Original payload/plan; retain it without rebuilding on queue backpressure.
    pub value: T,
}

pub(super) type Reply<T> = completion::Sender<Result<T, JournalError>>;

#[derive(Debug)]
pub(super) struct Command<A = Action> {
    pub action: A,
    pub(super) _permit: OwnedSemaphorePermit,
}

/// What a command needs from the normal owner before it may start.
pub(super) enum Gate<'a> {
    /// Starts on any available owner.
    Free,
    /// Waits until every submitted write settled.
    Settled,
    /// Waits for settled writes when validating this suffix may read storage.
    Validate(&'a AppendBuffer),
    /// Waits for writeback room, unless every write settled.
    Admit(&'a mut ValidatedAppend),
    /// Combines the gates of a turn's parts.
    Turn(&'a mut super::Turn),
}

/// One owner command, by family.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "bounded command slots retain their arenas inline without another allocation"
)]
pub(super) enum Action {
    Read(super::read::ReadAction),
    Append(AppendAction),
    Maintenance(MaintenanceAction),
    History(super::history::HistoryAction),
    Install(super::install::InstallAction),
    Recovery(RecoveryAction),
}

/// Writer-path commands: validation, proposals, admission, and syncs.
#[derive(Debug)]
pub(super) enum AppendAction {
    Promise {
        ticket: PromiseTicket,
        done: completion::Sender<Result<PromiseTicket, JournalError>>,
    },
    Validate {
        ticket: ValidationTicket,
        buffer: AppendBuffer,
        done: completion::Sender<Result<ValidatedAppend, JournalError>>,
    },
    Propose {
        ticket: ValidationTicket,
        buffer: ProposalBuffer,
        done: completion::Sender<Result<ProposalValidation, JournalError>>,
    },
    Admit {
        ticket: WriteTicket,
        validated: ValidatedAppend,
        done: completion::Sender<Result<super::AdmittedAppend, JournalError>>,
    },
    Sync {
        ticket: SyncTicket,
        done: completion::Sender<Result<SyncTicket, JournalError>>,
    },
    BeginPipelinedSync {
        ticket: SyncTicket,
        done: completion::Sender<Result<ReadyJournalSync, JournalError>>,
    },
    FinishPipelinedSync {
        ready: ReadyJournalSync,
        done: completion::Sender<Result<SyncTicket, JournalError>>,
    },
    Apply {
        ticket: ValidationTicket,
        done: completion::Sender<Result<ValidationTicket, JournalError>>,
    },
    Turn {
        turn: Box<super::Turn>,
        done: completion::Sender<Result<Box<super::TurnResult>, JournalError>>,
    },
}

/// Retention, identity expiry, cleanup, and validation turns on the owner.
#[derive(Debug)]
pub(super) enum MaintenanceAction {
    Retention {
        ticket: ozzy_replication::driver::ValidationTicket,
        seed: ozzy_proto::OperationId,
        leader: bool,
        done: completion::Sender<Result<super::RetentionTurn, JournalError>>,
    },
    RetainIdentities {
        ticket: ozzy_replication::driver::ValidationTicket,
        leader: bool,
        done: completion::Sender<Result<super::IdentityRetention, JournalError>>,
    },
    Release {
        ticket: ValidationTicket,
        done: completion::Sender<Result<super::ReleaseTurn, JournalError>>,
    },
    LoadHeld {
        ticket: ValidationTicket,
        done: completion::Sender<Result<super::ReleaseTurn, JournalError>>,
    },
    CleanupOrphans {
        ticket: ValidationTicket,
        budget: ozzy_journal_segment::MaintenanceBudget,
        done: completion::Sender<Result<crate::replica_journal::OwnedCleanedStorage, JournalError>>,
    },
    CleanupMetadata {
        ticket: ValidationTicket,
        budget: ozzy_journal_segment::MaintenanceBudget,
        done: completion::Sender<Result<crate::replica_journal::OwnedCleanedStorage, JournalError>>,
    },
    ValidateStorage {
        ticket: ValidationTicket,
        budget: ozzy_journal_segment::StorageValidationBudget,
        done: completion::Sender<Result<super::ValidatedStorage, JournalError>>,
    },
}

pub(super) fn finish_read<T>(
    done: completion::Sender<Result<T, JournalError>>,
    result: Result<T, JournalError>,
    permit: OwnedSemaphorePermit,
) -> bool {
    let faulted = result.as_ref().err().is_some_and(read_fault);
    if faulted && let Err(error) = &result {
        fault_log(error);
    }
    // Release command admission before waking the actor. Payload leases remain
    // charged until their completion/owner drops them.
    drop(permit);
    let _ = done.send(result);
    faulted
}

/// Name the error that fences this journal in the bounded diagnostic log,
/// so a faulted worker reports its cause and not only the fence.
pub(super) fn fault_log(error: &JournalError) {
    crate::profiling::log(
        crate::profiling::Level::Error,
        &format!("replica journal fault: {error}"),
    );
}

/// Log a failed result as the journal fault it causes. Returns whether it failed.
pub(super) fn logged_fault<T>(result: &Result<T, JournalError>) -> bool {
    if let Err(error) = result {
        fault_log(error);
    }
    result.is_err()
}

pub(super) fn read_fault(error: &JournalError) -> bool {
    use ozzy_journal_segment::HistoryError;
    matches!(
        error,
        JournalError::CheckpointBytes(_)
            | JournalError::Checkpoint(_)
            | JournalError::Directory(_)
            | JournalError::Index(_)
            | JournalError::IdentityHandoff(_)
            | JournalError::Faulted
            | JournalError::Images(CanonicalImagesError::State(StateError::IdentityIndex(
                IdentityIndexError::LookupUnavailable
            )))
            | JournalError::History(
                HistoryError::Io(_)
                    | HistoryError::Codec(_)
                    | HistoryError::Operation(_)
                    | HistoryError::Journal(_)
                    | HistoryError::Source
            )
    )
}

pub(super) fn finish<T>(
    done: completion::Sender<Result<T, JournalError>>,
    result: Result<T, JournalError>,
    permit: OwnedSemaphorePermit,
) -> bool {
    let faulted = logged_fault(&result);
    drop(permit);
    // Caller cancellation does not cancel admitted effects or panic the worker.
    let _ = done.send(result);
    faulted
}

impl ReplicaJournal {
    /// Install validated acceptance and queue its physical write on the dedicated
    /// writer. The returned write completion covers the exact write ticket; the
    /// group policy decides whether it counts before or after confirmation.
    #[expect(
        clippy::result_large_err,
        reason = "return the caller-owned arena on backpressure"
    )]
    pub fn admit_append(
        &mut self,
        ticket: WriteTicket,
        validated: ValidatedAppend,
    ) -> Result<JournalCompletion<super::AdmittedAppend>, Rejected<ValidatedAppend>> {
        self.submit(
            validated,
            |validated| Gate::Admit(validated),
            |validated, done| {
                Action::Append(AppendAction::Admit {
                    ticket,
                    validated,
                    done,
                })
            },
        )
    }

    /// Allocate a bounded body-only proposal arena from the same worker lease pool.
    /// Reserve during startup; rejected validation and completed writes allow reuse.
    pub fn lease_proposal_buffer(&self) -> Result<ProposalBuffer, SubmitError> {
        self.lease_append_buffer().map(ProposalBuffer)
    }

    /// Assign consensus coordinates and validate on the current primary's worker.
    /// No write or state mutation occurs. Original bodies survive rejection without
    /// releasing their arena. Native producer requests get worker-assigned offsets
    /// and timestamps, or exact retry resolution against complete written history.
    /// Body-only inputs retain their already assigned application coordinates.
    /// Recheck the returned validation ticket before admission or transmission.
    #[expect(
        clippy::result_large_err,
        reason = "return the reusable arena on backpressure"
    )]
    pub fn propose_append(
        &mut self,
        ticket: ValidationTicket,
        buffer: ProposalBuffer,
    ) -> Result<JournalCompletion<ProposalValidation>, Rejected<ProposalBuffer>> {
        self.submit(
            buffer,
            |buffer| Gate::Validate(&buffer.0),
            |buffer, done| {
                Action::Append(AppendAction::Propose {
                    ticket,
                    buffer,
                    done,
                })
            },
        )
    }

    /// Allocate one bounded payload arena. Call during setup, then reuse completions.
    /// The lease remains charged through queued work and actor/network retention.
    pub fn lease_append_buffer(&self) -> Result<AppendBuffer, SubmitError> {
        self.lease_append_buffer_with_limits(self.append_limits)
    }

    // The actor validates public requests before reaching this internal allocator.
    // Narrower arenas still consume one permit from the same worker lease pool.
    pub(crate) fn lease_append_buffer_with_limits(
        &self,
        limits: ozzy_replication::PipelineLimits,
    ) -> Result<AppendBuffer, SubmitError> {
        assert!(
            limits.max_operations > 0 && limits.max_operations <= self.append_limits.max_operations
        );
        assert!(
            limits.max_body_bytes > 0 && limits.max_body_bytes <= self.append_limits.max_body_bytes
        );
        if self.execution.is_closed() {
            return Err(SubmitError::Stopped);
        }
        let lease = self
            .buffers
            .clone()
            .try_acquire_owned()
            .map_err(|_| SubmitError::Full)?;
        Ok(AppendBuffer::new_with_memory(
            self.buffer_generation,
            limits,
            lease,
            self.append_memory.clone(),
        ))
    }

    /// Validate a fresh contiguous suffix against an exact driver image on the worker.
    /// No write or application mutation occurs. Recheck the returned ticket with
    /// `ReplicaDriver::prepare_validated` before sending PREPARE or submitting a write.
    /// Rejected validation drops its arena and releases its lease; queue rejection
    /// returns the arena unchanged. Decoding and hashing never run on this caller.
    #[expect(
        clippy::result_large_err,
        reason = "return arena ownership without allocating on backpressure"
    )]
    pub fn validate_append(
        &mut self,
        ticket: ValidationTicket,
        buffer: AppendBuffer,
    ) -> Result<JournalCompletion<ValidatedAppend>, Rejected<AppendBuffer>> {
        self.submit(
            buffer,
            |buffer| Gate::Validate(buffer),
            |buffer, done| {
                Action::Append(AppendAction::Validate {
                    ticket,
                    buffer,
                    done,
                })
            },
        )
    }

    /// Group barrier covering a core-captured written prefix. Later queued writes
    /// may also reach disk, but completion acknowledges only the supplied ticket.
    pub fn sync(
        &mut self,
        ticket: SyncTicket,
    ) -> Result<JournalCompletion<SyncTicket>, SubmitError> {
        self.command(Gate::Settled, |done| {
            Action::Append(AppendAction::Sync { ticket, done })
        })
    }

    /// Publish one group's recovery evidence on the assigned disk executor.
    ///
    /// Segment writes already completed through `O_DSYNC`. Recovery evidence
    /// publication remains ordered on the journal owner. Call
    /// `finish_pipelined_sync` to consume the exact core ticket. Only one may
    /// remain outstanding; later writes cannot widen it. The command permit
    /// remains charged through publication. Abandoned notifications never
    /// cancel admitted physical work.
    pub fn begin_pipelined_sync(
        &mut self,
        ticket: SyncTicket,
    ) -> Result<JournalCompletion<ReadyJournalSync>, SubmitError> {
        self.command(Gate::Free, |done| {
            Action::Append(AppendAction::BeginPipelinedSync { ticket, done })
        })
    }

    /// Consume the worker's completed publication and report its captured ticket.
    ///
    /// Backpressure returns the opaque notification intact for retry. Installation
    /// never widens the ticket to later writes or roll barriers. A foreign/stale
    /// notification or failed publication fences the worker. Metadata/view
    /// changes and another sync require this completion first.
    pub fn finish_pipelined_sync(
        &mut self,
        ready: ReadyJournalSync,
    ) -> Result<JournalCompletion<SyncTicket>, Rejected<ReadyJournalSync>> {
        self.submit(
            ready,
            |_| Gate::Free,
            |ready, done| Action::Append(AppendAction::FinishPipelinedSync { ready, done }),
        )
    }

    /// Apply through a driver-validated quorum commit, without a metadata barrier.
    /// Capture `begin_validation` after quorum advancement. Only after completion
    /// may the actor call `apply_through(ticket.committed())` on its still-live core.
    /// This does not prove an external consumer processed any record.
    pub fn apply_committed(
        &mut self,
        ticket: ValidationTicket,
    ) -> Result<JournalCompletion<ValidationTicket>, SubmitError> {
        self.command(Gate::Free, |done| {
            Action::Append(AppendAction::Apply { ticket, done })
        })
    }

    /// Start a command on the normal owner once `gate` allows it. Rejection
    /// returns `value` intact; `make` runs only for an admitted command.
    pub(super) fn submit<T, R>(
        &mut self,
        mut value: T,
        gate: impl FnOnce(&mut T) -> Gate<'_>,
        make: impl FnOnce(T, Reply<R>) -> Action,
    ) -> Result<JournalCompletion<R>, Rejected<T>> {
        let permit = match self.permit() {
            Ok(permit) => permit,
            Err(reason) => return Err(Rejected { reason, value }),
        };
        match self.execution.admit(gate(&mut value)) {
            Ok(admitted) => {
                let (command, completion) = bind(value, permit, make);
                admitted.start(command);
                Ok(completion)
            }
            Err(reason) => Err(Rejected { reason, value }),
        }
    }

    /// Start a command whose inputs need not survive rejection.
    pub(super) fn command<R>(
        &mut self,
        gate: Gate<'static>,
        make: impl FnOnce(Reply<R>) -> Action,
    ) -> Result<JournalCompletion<R>, SubmitError> {
        self.submit((), |()| gate, |(), done| make(done))
            .map_err(|rejected| rejected.reason)
    }

    /// Start transfer work on a nonvoting recovery owner. Rejection returns
    /// `value` intact.
    pub(super) fn submit_receive<T, R>(
        &mut self,
        value: T,
        make: impl FnOnce(T, Reply<R>) -> ReceiveAction,
    ) -> Result<JournalCompletion<R>, Rejected<T>> {
        let permit = match self.permit() {
            Ok(permit) => permit,
            Err(reason) => return Err(Rejected { reason, value }),
        };
        match self.execution.admit_receive() {
            Ok(admitted) => {
                let (command, completion) = bind(value, permit, make);
                admitted.start(command);
                Ok(completion)
            }
            Err(reason) => Err(Rejected { reason, value }),
        }
    }

    /// Start transfer work whose inputs need not survive rejection.
    pub(super) fn receive<R>(
        &mut self,
        make: impl FnOnce(Reply<R>) -> ReceiveAction,
    ) -> Result<JournalCompletion<R>, SubmitError> {
        self.submit_receive((), |(), done| make(done))
            .map_err(|rejected| rejected.reason)
    }

    fn permit(&self) -> Result<OwnedSemaphorePermit, SubmitError> {
        if self.execution.is_closed() {
            return Err(SubmitError::Stopped);
        }
        self.capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| SubmitError::Full)
    }
}

/// Build an admitted command and the completion its caller observes.
fn bind<T, R, A>(
    value: T,
    permit: OwnedSemaphorePermit,
    make: impl FnOnce(T, Reply<R>) -> A,
) -> (Command<A>, JournalCompletion<R>) {
    let (done, receiver) = completion::channel();
    let command = Command {
        action: make(value, done),
        _permit: permit,
    };
    (command, JournalCompletion { receiver })
}
