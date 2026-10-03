//! Keep protocol ownership and newer-view fences alive across asynchronous installation.

use std::time::Duration;

use ozzy_proto::NodeId;

use super::{Action, DriverError, ReplicaDriver, Role};
use crate::wire::Control;
use crate::{
    Commit, Digest, InstallOutcome, InstallTicket, InstallingView, JournalGeneration, LogSource,
    OpNumber, Prefix, PreparedOperation, ReplicationError, Scope, StartView,
};

/// Whole-tail commit authority captured before worker-side image activation.
/// This is not application completion. Recheck it after I/O before releasing admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivationTicket {
    local: NodeId,
    scope: Scope,
    generation: JournalGeneration,
    through: Prefix,
    applied: Prefix,
}

impl ActivationTicket {
    /// Local configured voter whose application image may be activated.
    pub const fn local(self) -> NodeId {
        self.local
    }
    /// Exact installed configuration and view.
    pub const fn scope(self) -> Scope {
        self.scope
    }
    /// Installed writer incarnation; old generations cannot activate it.
    pub const fn generation(self) -> JournalGeneration {
        self.generation
    }
    /// Entire selected tail, committed and locally durable.
    pub const fn through(self) -> Prefix {
        self.through
    }
    /// Previously completed application boundary.
    pub const fn applied(self) -> Prefix {
        self.applied
    }
}

impl ReplicaDriver {
    pub(super) fn activation_action(&mut self) -> Result<Option<Action>, DriverError> {
        if self.activation_pending == 0 {
            return Ok(None);
        }
        let start = self
            .normal()
            .expect("normal role")
            .start_view()?
            .expect("activation response requires an installed view");
        let voter = self.activation_pending.trailing_zeros() as usize;
        self.activation_pending &= !(1 << voter);
        Ok(Some(Action::Send {
            to: self.configuration.voters()[voter],
            message: Control::Commit(Commit {
                scope: start.scope,
                committed: start.accepted,
            }),
        }))
    }

    /// Authorize worker-side activation of an installed, whole-tail committed image.
    /// The primary additionally requires a backup's new-view durable ACK, even
    /// when the selected tail was already known committed before installation.
    pub fn begin_activation(&self) -> Result<ActivationTicket, DriverError> {
        let normal = self.normal().ok_or(ReplicationError::NotNormal)?;
        let through = normal.activation_prefix()?;
        let snapshot = normal.snapshot();
        Ok(ActivationTicket {
            local: self.local,
            scope: snapshot.scope,
            generation: snapshot.journal.generation,
            through,
            applied: snapshot.applied,
        })
    }

    /// Report completed image activation only while its captured authority is current.
    /// A newer view or changed image rejects a late completion without restoring voting.
    pub fn complete_activation(&mut self, ticket: ActivationTicket) -> Result<(), DriverError> {
        if self.begin_activation().ok() != Some(ticket) {
            return Err(DriverError::StaleValidation);
        }
        self.apply_through(ticket.through)
    }

    /// Inspect the exact pending selected-log action without granting normal authority.
    pub fn installation_ticket(&self) -> Option<InstallTicket> {
        match self.role.as_ref().expect("owned role") {
            Role::Installing(installing) => Some(installing.ticket()),
            Role::Normal(_) | Role::ViewChanging(_) => None,
        }
    }

    /// Begin primary installation after verified quorum selection. The adapter
    /// supplies a fresh writer incarnation and performs bounded staging, sync,
    /// publication, and application recovery on the storage worker.
    /// Rejection retains the original fenced role and any missing-history state.
    pub fn begin_primary_install(
        &mut self,
        generation: JournalGeneration,
    ) -> Result<InstallTicket, DriverError> {
        self.begin_install(None, generation, |_, _| None)
    }

    /// Begin backup publication after an authenticated current-primary descriptor
    /// and this backup's completed durable promise. History lookup must be pinned
    /// and nonblocking; missing evidence retains the fenced role for later retry.
    pub fn begin_backup_install(
        &mut self,
        from: NodeId,
        start: StartView,
        generation: JournalGeneration,
        lookup: impl FnMut(LogSource, OpNumber) -> Option<Digest>,
    ) -> Result<InstallTicket, DriverError> {
        self.begin_install(Some((from, start)), generation, lookup)
    }

    fn begin_install(
        &mut self,
        backup: Option<(NodeId, StartView)>,
        generation: JournalGeneration,
        lookup: impl FnMut(LogSource, OpNumber) -> Option<Digest>,
    ) -> Result<InstallTicket, DriverError> {
        self.changing()?;
        let Role::ViewChanging(changing) = self.role.take().expect("owned role") else {
            unreachable!("view-changing checked");
        };
        let admitted = match backup {
            Some((from, start)) => changing.begin_backup_install(from, start, generation, lookup),
            None => changing.begin_primary_install(generation),
        };
        match admitted {
            Ok(installing) => {
                let ticket = installing.ticket();
                self.role = Some(Role::Installing(installing));
                self.election_deadline = self.now + self.election_delay;
                Ok(ticket)
            }
            Err(rejected) => {
                let error = rejected.error();
                self.role = Some(Role::ViewChanging(rejected.into_view_change()));
                Err(error.into())
            }
        }
    }

    /// Validate the next bounded selected suffix chunk, independently of its disk
    /// completion. The adapter must validate bodies/application transitions and
    /// stage those same bytes; this alone is never durability or commit evidence.
    pub fn validate_install_suffix(
        &mut self,
        operations: &[PreparedOperation],
    ) -> Result<(), DriverError> {
        Ok(self.installing_mut()?.validate_suffix(operations)?)
    }

    /// Finish actual selected-log publication and committed application recovery.
    /// A newer observed/requested view suppresses intermediate normal activation.
    /// A current installed primary still needs whole-tail quorum and application
    /// before fresh appends. Failed or stale completions retain phase ownership.
    pub fn complete_installation(
        &mut self,
        ticket: InstallTicket,
        applied: Prefix,
        now: Duration,
    ) -> Result<(), DriverError> {
        self.observe_time(now)?;
        let outcome = self.installing_mut()?.complete(ticket, applied)?;
        let changing = matches!(outcome, InstallOutcome::ViewChanging(_));
        self.role = Some(match outcome {
            InstallOutcome::Normal(normal) => Role::Normal(normal),
            InstallOutcome::ViewChanging(changing) => Role::ViewChanging(changing),
        });
        self.contact_deadline = now + self.timing.primary_timeout;
        self.persistence_deadline = now + self.timing.primary_timeout;
        self.progress_deadline = now + self.progress_timeout();
        self.election_deadline = now + self.election_delay;
        self.retry_at = now;
        self.start_pending = changing;
        self.report_pending = changing;
        self.commit_pending = false;
        self.activation_pending = 0;
        Ok(())
    }

    /// Resume election after the worker successfully aborted unpublished staging.
    /// All admitted staging I/O must have settled; publication must not have begun.
    /// A later requested view is required. The old promise/history remain durable,
    /// but no normal authority or staged bytes survive this transition.
    pub fn complete_abandon(
        &mut self,
        ticket: InstallTicket,
        now: Duration,
    ) -> Result<(), DriverError> {
        self.observe_time(now)?;
        let changing = self.installing_mut()?.complete_abandon(ticket)?;
        self.role = Some(Role::ViewChanging(changing));
        self.election_deadline = now + self.election_delay;
        self.retry_at = now;
        self.start_pending = true;
        self.report_pending = true;
        self.commit_pending = false;
        self.activation_pending = 0;
        Ok(())
    }

    fn installing_mut(&mut self) -> Result<&mut InstallingView, DriverError> {
        match self.role.as_mut().expect("owned role") {
            Role::Installing(installing) => Ok(installing),
            Role::Normal(_) | Role::ViewChanging(_) => Err(ReplicationError::WrongRole.into()),
        }
    }
}
