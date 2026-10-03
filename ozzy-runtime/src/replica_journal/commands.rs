//! Bounded typed command admission. Rejected payloads remain owned by the caller.

use super::ReadyJournalSync;
use super::recovery::RecoveryAction;
use super::{
    AppendBuffer, JournalCompletion, JournalError, OwnedSemaphorePermit, PromiseTicket,
    ReplicaJournal, SubmitError, ValidatedAppend, completion, mpsc,
};
use super::{
    FetchedHistory, HistoryPosition, ProposalBuffer, ProposalValidation, ReplicationPositions,
};
use super::{InstallationConfig, InstalledChunk, InstalledJournal};
use ozzy_core::state::{CanonicalImagesError, IdentityIndexError, StateError};
use ozzy_replication::driver::{ActivationTicket, ValidationTicket};
use ozzy_replication::wire::FetchOps;
use ozzy_replication::{InstallTicket, LogSource, OpNumber, Prefix};
use ozzy_replication::{SyncTicket, WriteTicket};

/// An unsubmitted value returned intact for retry. No disk action was admitted.
#[derive(Debug)]
pub struct Rejected<T> {
    /// Backpressure or closed admission, never a persistence outcome.
    pub reason: SubmitError,
    /// Original payload/plan; retain it without rebuilding on queue backpressure.
    pub value: T,
}

#[derive(Debug)]
pub(super) struct Command {
    pub action: Action,
    pub(super) _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(super) enum Action {
    CleanupOrphans {
        ticket: ValidationTicket,
        budget: ozzy_journal_segment::MaintenanceBudget,
        done: completion::Sender<Result<ozzy_journal_segment::OrphanCleanupStep, JournalError>>,
    },
    CleanupMetadata {
        ticket: ValidationTicket,
        budget: ozzy_journal_segment::MaintenanceBudget,
        done: completion::Sender<Result<ozzy_journal_segment::MetadataCleanupStep, JournalError>>,
    },
    ValidateStorage {
        ticket: ValidationTicket,
        budget: ozzy_journal_segment::StorageValidationBudget,
        done: completion::Sender<Result<super::ValidatedStorage, JournalError>>,
    },
    OpenReader {
        ticket: ValidationTicket,
        partition: ozzy_proto::PartitionIncarnation,
        /// None selects the applied end.
        from: Option<ozzy_proto::Offset>,
        done: completion::Sender<Result<super::PartitionReadCursor, JournalError>>,
    },
    ReadPartition {
        read_permit: OwnedSemaphorePermit,
        cursor: super::PartitionReadCursor,
        limits: super::PartitionReadLimits,
        buffer: super::PartitionReadLease,
        done: super::read::delivery::ReadReply,
    },
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
    CaptureHistory {
        source: LogSource,
        done: completion::Sender<Result<LogSource, JournalError>>,
    },
    Recovery(RecoveryAction),
    Receiving(super::receiving::ReceiveAction),
    ReleaseHistory {
        source: LogSource,
        done: completion::Sender<Result<LogSource, JournalError>>,
    },
    HistoryPosition {
        source: LogSource,
        op: OpNumber,
        done: completion::Sender<Result<HistoryPosition, JournalError>>,
    },
    FetchHistory {
        request: FetchOps,
        buffer: AppendBuffer,
        done: completion::Sender<Result<FetchedHistory, JournalError>>,
    },
    FetchReplication {
        ticket: ValidationTicket,
        predecessor: Prefix,
        limits: ozzy_replication::PipelineLimits,
        buffer: AppendBuffer,
        done: completion::Sender<Result<FetchedHistory, JournalError>>,
    },
    ReplicationPositions {
        ticket: ValidationTicket,
        requested: [OpNumber; 2],
        done: completion::Sender<Result<ReplicationPositions, JournalError>>,
    },
    BeginInstall {
        ticket: InstallTicket,
        config: InstallationConfig,
        done: completion::Sender<Result<InstallTicket, JournalError>>,
    },
    InstallChunk {
        ticket: InstallTicket,
        buffer: AppendBuffer,
        done: completion::Sender<Result<InstalledChunk, JournalError>>,
    },
    FinishInstall {
        ticket: InstallTicket,
        done: completion::Sender<Result<InstalledJournal, JournalError>>,
    },
    AbortInstall {
        ticket: InstallTicket,
        done: completion::Sender<Result<InstallTicket, JournalError>>,
    },
    Activate {
        ticket: ActivationTicket,
        done: completion::Sender<Result<ActivationTicket, JournalError>>,
    },
}

pub(super) fn finish_read<T>(
    done: completion::Sender<Result<T, JournalError>>,
    result: Result<T, JournalError>,
    permit: OwnedSemaphorePermit,
) -> bool {
    let faulted = result.as_ref().err().is_some_and(read_fault);
    // Release command admission before waking the actor. Payload leases remain
    // charged until their completion/owner drops them.
    drop(permit);
    let _ = done.send(result);
    faulted
}

pub(super) fn read_fault(error: &JournalError) -> bool {
    use ozzy_journal_segment::HistoryError;
    matches!(
        error,
        JournalError::Directory(_)
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
    let faulted = result.is_err();
    drop(permit);
    // Caller cancellation does not cancel admitted effects or panic the worker.
    let _ = done.send(result);
    faulted
}

impl<E> ReplicaJournal<E> {
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
            |validated, done| Action::Admit {
                ticket,
                validated,
                done,
            },
            |action| {
                let Action::Admit { validated, .. } = action else {
                    unreachable!("same command")
                };
                validated
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
            |buffer, done| Action::Propose {
                ticket,
                buffer,
                done,
            },
            |action| match action {
                Action::Propose { buffer, .. } => buffer,
                _ => unreachable!("submission preserves command kind"),
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
        if self
            .sender
            .as_ref()
            .is_none_or(mpsc::NotifiedSender::is_disconnected)
        {
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
            |buffer, done| Action::Validate {
                ticket,
                buffer,
                done,
            },
            |action| match action {
                Action::Validate { buffer, .. } => buffer,
                _ => unreachable!("submission preserves command kind"),
            },
        )
    }

    /// Group barrier covering a core-captured written prefix. Later queued writes
    /// may also reach disk, but completion acknowledges only the supplied ticket.
    pub fn sync(
        &mut self,
        ticket: SyncTicket,
    ) -> Result<JournalCompletion<SyncTicket>, SubmitError> {
        self.submit(
            ticket,
            |ticket, done| Action::Sync { ticket, done },
            |action| match action {
                Action::Sync { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
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
        self.submit(
            ticket,
            |ticket, done| Action::BeginPipelinedSync { ticket, done },
            |action| match action {
                Action::BeginPipelinedSync { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
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
            |ready, done| Action::FinishPipelinedSync { ready, done },
            |action| match action {
                Action::FinishPipelinedSync { ready, .. } => ready,
                _ => unreachable!("submission preserves command kind"),
            },
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
        self.submit(
            ticket,
            |ticket, done| Action::Apply { ticket, done },
            |action| match action {
                Action::Apply { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    pub(super) fn submit<T, R>(
        &mut self,
        value: T,
        make: impl FnOnce(T, completion::Sender<Result<R, JournalError>>) -> Action,
        recover: impl FnOnce(Action) -> T,
    ) -> Result<JournalCompletion<R>, Rejected<T>> {
        let Some(sender) = self
            .sender
            .as_mut()
            .filter(|sender| !sender.is_disconnected())
        else {
            return Err(Rejected {
                reason: SubmitError::Stopped,
                value,
            });
        };
        let Ok(permit) = self.capacity.clone().try_acquire_owned() else {
            return Err(Rejected {
                reason: SubmitError::Full,
                value,
            });
        };
        let (done, receiver) = completion::channel();
        let command = Command {
            action: make(value, done),
            _permit: permit,
        };
        if let Err(error) = sender.try_send(command) {
            let (reason, command) = match error {
                mpsc::TrySendError::Full(command) => (SubmitError::Full, command),
                mpsc::TrySendError::Disconnected(command) => (SubmitError::Stopped, command),
            };
            return Err(Rejected {
                reason,
                value: recover(command.action),
            });
        }
        Ok(JournalCompletion { receiver })
    }
}
