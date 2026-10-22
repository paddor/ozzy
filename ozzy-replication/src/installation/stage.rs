//! Admission and staging: a promised view change becomes an installing view.

use super::{
    InstallState, InstallTicket, InstallingView, ReceivedStart, RejectedInstallation, StartView,
};
use crate::{
    Digest, JournalGeneration, LogSource, Prefix, PreparedOperation, PromiseTicket,
    ReplicationError, ViewChange, ViewChangeError, VoterIndex,
};

/// Installation inputs checked before the fenced role moves.
#[derive(Debug, Clone, Copy)]
struct Admitted {
    ticket: InstallTicket,
    start: StartView,
    promise: PromiseTicket,
    body_bytes: usize,
}

/// Where the selected tail after the commit floor comes from.
#[derive(Debug, Clone, Copy)]
enum Suffix<'a> {
    /// Supplied whole with this call and already validated.
    Supplied(&'a [PreparedOperation]),
    /// Stays in the journal and is validated later in bounded chunks.
    Streamed,
}

impl ViewChange {
    /// Stage primary installation whose selected tail remains in the journal.
    ///
    /// Feed `InstallingView::validate_suffix` bounded chunks before completion.
    /// Durable publication still installs the complete selected log atomically.
    /// After activation, a whole-tail new-view ACK and ordered application are
    /// required before fresh appends; old operations consume no live ring slots.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns owned fenced role"
    )]
    pub fn begin_primary_install(
        self,
        generation: JournalGeneration,
    ) -> Result<InstallingView, RejectedInstallation> {
        let admitted = self.admit_primary(generation);
        self.stage(&admitted, Suffix::Streamed)
    }

    /// Stage bounded-stream backup installation after authenticated `START_VIEW`.
    ///
    /// Same promise and primary checks as `install_backup`. Staging streams the
    /// source from the local commit, so it and an announced commit above it
    /// need no lookup; one below it is looked up in local history.
    /// Every selected byte must be validated and durably published before the
    /// backup can ACK. Fresh prepares also wait for whole-tail commit/application.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns owned fenced role"
    )]
    pub fn begin_backup_install(
        mut self,
        received: ReceivedStart,
        generation: JournalGeneration,
        mut lookup: impl FnMut(LogSource, Prefix) -> Option<Digest>,
    ) -> Result<InstallingView, RejectedInstallation> {
        let admitted = self.admit_backup(received, generation, &mut lookup, Suffix::Streamed);
        self.stage(&admitted, Suffix::Streamed)
    }

    /// Consume the selected primary's old role and stage atomic installation.
    ///
    /// `suffix` is the verified selected tail strictly after `ticket.committed`,
    /// not the full retained WAL. Transfer/application/storage independently
    /// validate earlier history and preserve the protected old committed prefix.
    /// Staging checks operation/byte bounds and reuses the old descriptor arena.
    /// Rejection returns the fenced role so missing data can be retried.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns protocol ownership without allocation"
    )]
    pub fn install_primary(
        self,
        generation: JournalGeneration,
        suffix: &[PreparedOperation],
    ) -> Result<InstallingView, RejectedInstallation> {
        let admitted = self
            .admit_primary(generation)
            .and_then(|admitted| self.validate_suffix(&admitted, suffix));
        self.stage(&admitted, Suffix::Supplied(suffix))
    }

    /// Consume a backup's fenced role after an authenticated `START_VIEW`.
    ///
    /// Unlike primary selection, this needs no local first/second-phase quorum:
    /// the current primary's installed descriptor supplies protocol authority.
    /// It still requires this backup's completed view promise and verified
    /// compatibility with its own possibly higher committed prefix.
    /// `lookup` has the same pinned, nonblocking lineage contract as `select`.
    /// `suffix` starts after the greater of local and announced commit knowledge.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns protocol ownership without allocation"
    )]
    pub fn install_backup(
        mut self,
        received: ReceivedStart,
        generation: JournalGeneration,
        suffix: &[PreparedOperation],
        mut lookup: impl FnMut(LogSource, Prefix) -> Option<Digest>,
    ) -> Result<InstallingView, RejectedInstallation> {
        let admitted = self
            .admit_backup(received, generation, &mut lookup, Suffix::Supplied(suffix))
            .and_then(|admitted| self.validate_suffix(&admitted, suffix));
        self.stage(&admitted, Suffix::Supplied(suffix))
    }

    fn admit_primary(&self, generation: JournalGeneration) -> Result<Admitted, ViewChangeError> {
        let promise = self.require_promised()?;
        self.require_primary()?;
        let selected = self.selected.ok_or(ViewChangeError::SelectionRequired)?;
        Ok(Admitted {
            ticket: self.install_ticket(
                generation,
                selected.source(),
                selected.committed(),
                selected.retained(),
            )?,
            start: StartView {
                scope: selected.scope(),
                generation,
                accepted: selected.source().accepted,
                committed: selected.committed(),
            },
            promise,
            body_bytes: 0,
        })
    }

    fn admit_backup(
        &mut self,
        ReceivedStart { from, start }: ReceivedStart,
        generation: JournalGeneration,
        lookup: &mut impl FnMut(LogSource, Prefix) -> Option<Digest>,
        suffix: Suffix<'_>,
    ) -> Result<Admitted, ViewChangeError> {
        self.require_scope(start.scope)?;
        let promise = self.require_promised()?;
        let voter = self.configuration.voter_index(from)?;
        if voter != VoterIndex::primary(start.scope.view) || voter == self.local {
            return Err(ReplicationError::WrongRole.into());
        }
        if !crate::view_change::valid_prefix(start.accepted)
            || !crate::view_change::valid_prefix(start.committed)
            || start.committed.op > start.accepted.op
        {
            return Err(ViewChangeError::InvalidReport);
        }
        let source = LogSource {
            voter: from,
            generation: start.generation,
            accepted: start.accepted,
        };
        let local_commit = self.normal.snapshot().committed;
        if let Suffix::Streamed = suffix {
            // Staging streams the source from the local commit to its exact
            // tail and checks the digest at the announced commit on the way.
            // That chain proves both lie in the source; a lookup first would
            // move the same suffix twice. An announced commit below the local
            // one is checked in the local history the installation keeps.
            self.require_streamed(source, local_commit)?;
            match start.committed.op.cmp(&local_commit.op) {
                std::cmp::Ordering::Greater => {
                    self.require_streamed(source, start.committed)?;
                }
                std::cmp::Ordering::Equal => {
                    if start.committed != local_commit {
                        return Err(self.conflict());
                    }
                }
                std::cmp::Ordering::Less => {
                    let local = LogSource {
                        voter: self.configuration.voter(self.local),
                        generation: promise.generation(),
                        accepted: promise.log().accepted,
                    };
                    self.verify_prefix(local, start.committed, lookup)?;
                }
            }
        } else {
            self.verify_prefix(source, local_commit, lookup)?;
            self.verify_prefix(source, start.committed, lookup)?;
        }
        let committed = if local_commit.op > start.committed.op {
            local_commit
        } else {
            start.committed
        };
        Ok(Admitted {
            ticket: self.install_ticket(generation, source, committed, local_commit)?,
            start,
            promise,
            body_bytes: 0,
        })
    }

    fn validate_suffix(
        &self,
        admitted: &Admitted,
        suffix: &[PreparedOperation],
    ) -> Result<Admitted, ViewChangeError> {
        let body_bytes = self.normal.validate_install(&admitted.ticket, suffix)?;
        Ok(Admitted {
            body_bytes,
            ..*admitted
        })
    }

    fn install_ticket(
        &self,
        generation: JournalGeneration,
        source: LogSource,
        committed: Prefix,
        retained: Prefix,
    ) -> Result<InstallTicket, ViewChangeError> {
        let previous = self.normal.snapshot();
        if generation == previous.journal.generation {
            return Err(ViewChangeError::ReusedGeneration);
        }
        Ok(InstallTicket {
            scope: self.scope(),
            previous_generation: previous.journal.generation,
            generation,
            source,
            protected_committed: previous.committed,
            retained: if retained.op > previous.committed.op {
                retained
            } else {
                previous.committed
            },
            committed,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "rejection returns protocol ownership without allocation"
    )]
    fn stage(
        self,
        admitted: &Result<Admitted, ViewChangeError>,
        suffix: Suffix<'_>,
    ) -> Result<InstallingView, RejectedInstallation> {
        let admitted = match *admitted {
            Ok(admitted) => admitted,
            Err(error) => {
                return Err(RejectedInstallation {
                    view_change: self,
                    error,
                });
            }
        };
        let ticket = admitted.ticket;
        let (operations, validated_through) = match suffix {
            Suffix::Supplied(operations) => (operations, ticket.accepted()),
            Suffix::Streamed => (&[][..], ticket.committed()),
        };
        let mut normal =
            self.normal
                .stage_install(&ticket, operations, admitted.start, admitted.body_bytes);
        if let Suffix::Streamed = suffix {
            normal.retain_installed_tail_on_disk();
        }
        Ok(InstallingView {
            ticket,
            previous_promise: admitted.promise,
            state: InstallState::Pending(normal),
            configuration: self.configuration,
            requested_view: ticket.scope.view,
            validated_through,
        })
    }
}
