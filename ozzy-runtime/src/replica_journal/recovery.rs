//! Nonvoting recovery source ownership, independent of election/replay pins.

use ozzy_proto::NodeId;
use ozzy_replication::LogSource;
use ozzy_replication::recovery::RecoveryResponse;
use ozzy_replication::wire::FetchOps;

use super::commands::Action;
use super::{
    AppendBuffer, FetchedHistory, JournalCompletion, JournalError, Rejected, ReplicaJournal,
    SubmitError, completion,
};

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "bounded command slots retain leased arenas inline without per-command boxes"
)]
pub(super) enum RecoveryAction {
    Pin {
        requester: NodeId,
        response: RecoveryResponse,
        done: completion::Sender<Result<PinnedRecovery, JournalError>>,
    },
    Release {
        pin: PinnedRecovery,
        done: completion::Sender<Result<PinnedRecovery, JournalError>>,
    },
    Fetch {
        pin: PinnedRecovery,
        request: FetchOps,
        buffer: AppendBuffer,
        done: completion::Sender<Result<FetchedHistory, JournalError>>,
    },
}

/// Exact donor snapshot retained on a worker for one recovering voter.
///
/// Explicitly release after transfer or abandonment. Dropping a completion does
/// not release the pin; retrying the same request recovers this token. A durable
/// view change invalidates all recovery pins. This token is not a normal vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinnedRecovery {
    pub(super) requester: NodeId,
    pub(super) response: RecoveryResponse,
    pub(super) donor: NodeId,
}

impl PinnedRecovery {
    /// Configured recovering voter, distinct from this donor.
    pub const fn requester(self) -> NodeId {
        self.requester
    }

    /// Immutable nonce/view-scoped response whose history is retained.
    pub const fn response(self) -> RecoveryResponse {
        self.response
    }

    /// Exact source for independently correlated `FETCH_OPS` exchanges.
    pub fn source(self) -> LogSource {
        let log = self
            .response
            .primary
            .expect("only primary responses own a pin");
        LogSource {
            voter: self.donor,
            generation: log.generation,
            accepted: log.accepted,
        }
    }
}

impl ReplicaJournal {
    /// Queue synchronization and capture of a primary's exact accepted snapshot.
    ///
    /// Obtain `response` from the activated normal core before submitting. Its
    /// writes must already be submitted, but later writes need not be stopped.
    /// The worker synchronizes its submitted prefix and pins only the requested
    /// export range. Completion supplies no normal sync/quorum evidence. Recheck
    /// the live role/view before advertising it. One pin per other voter is held;
    /// changing an occupied slot requires an exact release first.
    pub fn pin_recovery(
        &mut self,
        requester: NodeId,
        response: RecoveryResponse,
    ) -> Result<JournalCompletion<PinnedRecovery>, SubmitError> {
        self.submit(
            (requester, response),
            |(requester, response), done| {
                Action::Recovery(RecoveryAction::Pin {
                    requester,
                    response,
                    done,
                })
            },
            |action| match action {
                Action::Recovery(RecoveryAction::Pin {
                    requester,
                    response,
                    ..
                }) => (requester, response),
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Retire only the matching donor nonce/view/source, never a newer pin.
    pub fn release_recovery(
        &mut self,
        pin: PinnedRecovery,
    ) -> Result<JournalCompletion<PinnedRecovery>, SubmitError> {
        self.submit(
            pin,
            |pin, done| Action::Recovery(RecoveryAction::Release { pin, done }),
            |action| match action {
                Action::Recovery(RecoveryAction::Release { pin, .. }) => pin,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Read one bounded chunk from an exact retained donor pin into a leased arena.
    /// New link request IDs are allowed; source, scope, and pin ownership must match.
    #[expect(
        clippy::result_large_err,
        reason = "return the leased arena on backpressure"
    )]
    pub fn fetch_recovery(
        &mut self,
        pin: PinnedRecovery,
        request: FetchOps,
        buffer: AppendBuffer,
    ) -> Result<JournalCompletion<FetchedHistory>, Rejected<AppendBuffer>> {
        self.submit(
            buffer,
            |buffer, done| {
                Action::Recovery(RecoveryAction::Fetch {
                    pin,
                    request,
                    buffer,
                    done,
                })
            },
            |action| match action {
                Action::Recovery(RecoveryAction::Fetch { buffer, .. }) => buffer,
                _ => unreachable!("submission preserves command kind"),
            },
        )
    }
}
