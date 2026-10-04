//! Cold, fail-closed history evidence. No backtrace or hot-path allocation.
use super::{ActorError, ReplicaActor};
use crate::replica_journal::PinnedRecovery;
use ozzy_proto::NodeId;
use ozzy_replication::{JournalGeneration, Scope, wire::FetchOps};
use std::{fmt, panic::Location};

/// The invariant domain that rejected history; the source site identifies its check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HistoryReason {
    /// An admitted operation has no available owner buffer.
    BufferUnavailable,
    /// A completed donor pin contradicts its owner slot.
    DonorPin,
    /// A history lookup or remembered prefix is inconsistent.
    Lookup,
    /// History transfer does not match the selected evidence.
    Transfer,
    /// View installation or source release contradicts its ticket.
    ViewInstallation,
    /// Retention maintenance contradicts applied journal progress.
    Retention,
    /// A follower receive window contradicts its retained records.
    ReceiveWindow,
    /// Proposal publication or completion contradicts its admitted window.
    ProposalWindow,
    /// Nonvoting recovery lacks the selected ticket or checkpoint evidence.
    Recovery,
}

/// Specific history failure, with immutable source location and cold owner evidence.
#[derive(Debug)]
pub struct HistoryFailure {
    reason: HistoryReason,
    site: &'static Location<'static>,
    completed_pin: Option<PinnedRecovery>,
    evidence: Option<Box<Evidence>>,
}

#[derive(Debug)]
struct Evidence {
    local: NodeId,
    scope: Scope,
    generation: JournalGeneration,
    journal_scope: Scope,
    normal: Option<ozzy_replication::ReplicaSnapshot>,
    transfer: Option<FetchOps>,
    donor_responses: [Option<ozzy_replication::recovery::RecoveryResponse>; 3],
    donor_pins: [Option<PinnedRecovery>; 3],
}

impl HistoryFailure {
    /// Invariant domain; never permission to weaken confirmation or recovery.
    pub const fn reason(&self) -> HistoryReason {
        self.reason
    }

    /// Exact source check in the measured or deployed worker revision.
    pub const fn site(&self) -> &'static Location<'static> {
        self.site
    }
}

impl fmt::Display for HistoryFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "replica history does not satisfy selected authority: {:?} at {}",
            self.reason, self.site
        )?;
        if let Some(pin) = self.completed_pin {
            write!(f, "; completed_pin={pin:?}")?;
        }
        if let Some(evidence) = &self.evidence {
            write!(
                f,
                "; local={:?} scope={:?} generation={:?} journal_scope={:?} normal={:?} transfer={:?} donor_responses={:?} donor_pins={:?}",
                evidence.local,
                evidence.scope,
                evidence.generation,
                evidence.journal_scope,
                evidence.normal,
                evidence.transfer,
                evidence.donor_responses,
                evidence.donor_pins
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for HistoryFailure {}

impl ActorError {
    #[cold]
    #[track_caller]
    pub(super) fn history(reason: HistoryReason) -> Self {
        Self::History(Box::new(HistoryFailure {
            reason,
            site: Location::caller(),
            completed_pin: None,
            evidence: None,
        }))
    }

    #[cold]
    #[track_caller]
    pub(super) fn donor_pin(pin: &PinnedRecovery) -> Self {
        let mut error = Self::history(HistoryReason::DonorPin);
        if let Self::History(failure) = &mut error {
            failure.completed_pin = Some(*pin);
        }
        error
    }
}

impl ReplicaActor {
    #[cold]
    pub(super) fn diagnose(&self, mut error: ActorError) -> ActorError {
        if let ActorError::History(failure) = &mut error {
            let (donor_responses, donor_pins) = self
                .donors
                .as_ref()
                .map_or(([None; 3], [None; 3]), |donors| donors.evidence());
            failure.evidence = Some(Box::new(Evidence {
                local: self.local,
                scope: self.driver.scope(),
                generation: self.journal.generation(),
                journal_scope: self.journal_scope,
                normal: self
                    .driver
                    .normal()
                    .map(ozzy_replication::NormalReplica::snapshot),
                transfer: self.transfer.map(|transfer| transfer.request),
                donor_responses,
                donor_pins,
            }));
        }
        error
    }
}
