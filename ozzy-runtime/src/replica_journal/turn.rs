//! One worker command per actor turn when validation stays in memory. A
//! validation that reads storage follows admission in a separate command.

use ozzy_replication::WriteTicket;
use ozzy_replication::driver::ValidationTicket;

use super::commands::{Action, AppendAction, Gate, Rejected};
use super::{
    AdmittedAppend, AppendBuffer, JournalCompletion, JournalError, ProposalBuffer,
    ProposalValidation, ReplicaJournal, ValidatedAppend,
};

/// Work for one actor turn, executed in this order: admission of operations
/// the core already accepted, validation of the next proposal against the
/// image that admission leaves, then application through a committed prefix.
///
/// Capture the proposal ticket after the core admitted `admit`, so it names
/// the accepted end the worker holds when validation runs. Application runs
/// last, so it cannot move the image under that validation. A slow validation
/// is returned to the actor for a later command without holding the admitted
/// write's completion.
#[derive(Debug, Default)]
pub struct Turn {
    /// Install and write queueing of operations the core already admitted.
    pub admit: Option<(WriteTicket, ValidatedAppend)>,
    /// Next proposal and its ticket, captured after the core admitted `admit`.
    pub propose: Option<(ValidationTicket, ProposalBuffer)>,
    /// Next received suffix and its ticket, captured the same way.
    pub validate: Option<(ValidationTicket, AppendBuffer)>,
    /// Application through this ticket's committed prefix.
    pub apply: Option<ValidationTicket>,
}

impl Turn {
    /// True when the turn carries no work.
    pub fn is_empty(&self) -> bool {
        self.admit.is_none()
            && self.propose.is_none()
            && self.validate.is_none()
            && self.apply.is_none()
    }
}

/// Results of one turn. A failed admission or application fails the whole
/// turn and fences the worker; a rejected proposal does not.
#[derive(Debug)]
pub struct TurnResult {
    /// Admission result, when the turn carried one. Its write completes later.
    pub admitted: Option<AdmittedAppend>,
    /// Proposal validation, when the turn carried one.
    pub proposal: Option<ProposalValidation>,
    /// Received-suffix validation, when the turn carried one.
    pub validated: Option<ReceivedValidation>,
    /// Applied ticket, when the turn carried one.
    pub applied: Option<ValidationTicket>,
    /// Slow validation returned for a following command after admission.
    pub deferred: Option<Box<Turn>>,
}

/// Received suffix validation retains its bounded arena under memory pressure.
#[derive(Debug)]
pub enum ReceivedValidation {
    /// Recheck the authority ticket before admitting these operations.
    Ready(ValidatedAppend),
    /// No operations were admitted. Retry after charged backing is released.
    Deferred {
        /// Authority that owned the staged suffix before preparation stopped.
        validation: ValidationTicket,
        /// Unadmitted canonical bytes, still within the original receive window.
        buffer: AppendBuffer,
    },
    /// Invalid input or a storage failure; no arena remains in flight.
    Rejected(JournalError),
}

impl ReplicaJournal {
    /// Submit one actor turn. Backpressure returns every part unchanged.
    pub fn turn(
        &mut self,
        turn: Box<Turn>,
    ) -> Result<JournalCompletion<Box<TurnResult>>, Rejected<Box<Turn>>> {
        debug_assert!(!turn.is_empty());
        self.submit(
            turn,
            |turn| Gate::Turn(turn),
            |turn, done| Action::Append(AppendAction::Turn { turn, done }),
        )
    }
}

/// A turn faults the worker when admission or application failed, or when
/// its proposal failed on storage rather than on validation. Returns the
/// error that fences the worker.
pub(super) fn fault(result: &Result<Box<TurnResult>, JournalError>) -> Option<&JournalError> {
    match result {
        Err(error) => Some(error),
        Ok(result) => match (&result.proposal, &result.validated) {
            (Some(ProposalValidation::Rejected { reason, .. }), _)
            | (_, Some(ReceivedValidation::Rejected(reason)))
                if super::commands::read_fault(reason) =>
            {
                Some(reason)
            }
            _ => None,
        },
    }
}
