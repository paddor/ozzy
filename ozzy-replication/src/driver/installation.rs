//! Keep protocol ownership and newer-view fences alive across asynchronous installation.

use std::time::Duration;

use ozzy_proto::NodeId;

use super::{Action, DriverError, ReplicaDriver, Role};
use crate::wire::Control;
use crate::{
    Commit, Digest, InstallOutcome, InstallTicket, InstallingView, JournalGeneration, LogSource,
    Prefix, PreparedOperation, ReceivedStart, RejectedInstallation, ReplicationError, Scope,
    StartView, ViewChangeError, VoterSet,
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

/// Evidence that a stopping primary stored every operation it admitted in
/// its view. Kept across a clean stop, it lets the restarted primary resume
/// that view instead of leaving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanStop {
    /// The view this copy leads.
    pub view: u64,
    /// Its whole log, durable when it stopped.
    pub accepted: Prefix,
    /// What a quorum held of that log. The journal learns of a commit
    /// late, so its own record of it may trail this.
    pub committed: Prefix,
}

impl ReplicaDriver {
    /// Clean-stop evidence of a normal primary whose whole log is durable.
    /// Valid only if the caller admits and sends nothing afterwards.
    pub fn clean_stop(&self) -> Option<CleanStop> {
        let snapshot = self.normal()?.snapshot();
        (snapshot.status == crate::Status::Normal
            && self.leads(snapshot.scope.view)
            && !snapshot.journal.faulted
            && snapshot.journal.durable == snapshot.accepted.op)
            .then_some(CleanStop {
                view: snapshot.scope.view,
                accepted: snapshot.accepted,
                committed: snapshot.committed,
            })
    }

    /// Send one pending new-view activation commit. A voter set only while an
    /// installed view exists, so a missing view drops the pending set.
    pub(super) fn activation_action(&mut self) -> Result<Option<Action>, DriverError> {
        if self.activation_pending.is_empty() {
            return Ok(None);
        }
        let Some(start) = self.require_normal()?.start_view()? else {
            self.activation_pending = VoterSet::EMPTY;
            return Ok(None);
        };
        let Some(voter) = self.activation_pending.pop_first() else {
            return Ok(None);
        };
        Ok(Some(Action::Send {
            to: self.configuration.voter(voter),
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
            local: self.configuration.voter(self.local),
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
        match &self.role {
            Role::Installing(installing) => Some(installing.ticket()),
            Role::Normal(_) | Role::ViewChanging(_) | Role::Fenced(_) => None,
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
        lookup: impl FnMut(LogSource, Prefix) -> Option<Digest>,
    ) -> Result<InstallTicket, DriverError> {
        self.begin_install(Some(ReceivedStart { from, start }), generation, lookup)
    }

    /// Whether this restarted backup may still rejoin the view it last
    /// installed. Until the adapter tries, or the election deadline passes,
    /// `poll` promises no later view.
    pub fn rejoin_pending(&self) -> bool {
        matches!(&self.role, Role::ViewChanging(changing) if changing.rejoinable())
    }

    /// Give up rejoining. The ordinary election of the next view runs.
    pub fn decline_rejoin(&mut self) {
        if let Role::ViewChanging(changing) = &mut self.role {
            changing.decline_rejoin();
        }
    }

    /// Begin a restarted backup's return to its installed view from its own
    /// pinned log. The adapter supplies a fresh writer incarnation and runs
    /// the same staging, publication and application recovery as any
    /// installation. A rejection ends the attempt: the ordinary election of
    /// the next view follows.
    pub fn begin_rejoin_install(
        &mut self,
        generation: JournalGeneration,
    ) -> Result<InstallTicket, DriverError> {
        self.changing()?;
        let Role::ViewChanging(changing) = self.take_role() else {
            return Err(ViewChangeError::Faulted.into());
        };
        self.admit_install(changing.begin_rejoin_install(generation))
    }

    fn begin_install(
        &mut self,
        backup: Option<ReceivedStart>,
        generation: JournalGeneration,
        lookup: impl FnMut(LogSource, Prefix) -> Option<Digest>,
    ) -> Result<InstallTicket, DriverError> {
        self.changing()?;
        let Role::ViewChanging(changing) = self.take_role() else {
            return Err(ViewChangeError::Faulted.into());
        };
        let admitted = match backup {
            Some(received) => changing.begin_backup_install(received, generation, lookup),
            None => changing.begin_primary_install(generation),
        };
        self.admit_install(admitted)
    }

    fn admit_install(
        &mut self,
        admitted: Result<InstallingView, RejectedInstallation>,
    ) -> Result<InstallTicket, DriverError> {
        match admitted {
            Ok(installing) => {
                let ticket = installing.ticket();
                self.role = Role::Installing(installing);
                self.timers.election_deadline = self.now.saturating_add(self.timers.election_delay);
                // A verified selection or admitted START_VIEW is newer
                // evidence than a timeout this voter raised before it.
                // The installation deadline raises suspicion again.
                self.withdraw_exit();
                Ok(ticket)
            }
            Err(rejected) => {
                let error = rejected.error();
                self.role = Role::ViewChanging(rejected.into_view_change());
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

    /// Note progress of this view's election or installation: a looked-up
    /// batch, a staged chunk, or a local promise or installation I/O still
    /// running. A lagging copy moves and replays its whole missed suffix in
    /// the view change, and a slow disk's promise can take as long, which can
    /// outlast any election timeout, so each advance pushes the local
    /// election deadline out by one election delay. Progress of the view
    /// being installed also withdraws this voter's own suspicion of it, so a
    /// timeout raised before or during the installation cannot join a peer's
    /// timeout while the installation advances. Peer suspicion still counts;
    /// two other voters can leave the view without this one.
    pub fn history_progress(&mut self, now: Duration) -> Result<(), DriverError> {
        self.observe_time(now)?;
        if matches!(self.role, Role::ViewChanging(_) | Role::Installing(_)) {
            self.timers.election_deadline = self
                .timers
                .election_deadline
                .max(now.saturating_add(self.timers.election_delay));
        }
        if let Role::Installing(installing) = &self.role
            && installing.requested_scope() == installing.ticket().scope()
        {
            self.withdraw_exit();
        }
        Ok(())
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
        self.role = match outcome {
            InstallOutcome::Normal(normal) => Role::Normal(normal),
            InstallOutcome::ViewChanging(changing) => Role::ViewChanging(changing),
        };
        self.restart_timeouts(now);
        self.timers.retry_at = now;
        self.start_pending = changing;
        self.report_pending = changing;
        self.commit_pending = false;
        self.activation_pending = VoterSet::EMPTY;
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
        self.role = Role::ViewChanging(changing);
        self.timers.election_deadline = now.saturating_add(self.timers.election_delay);
        self.timers.retry_at = now;
        self.start_pending = true;
        self.report_pending = true;
        self.commit_pending = false;
        self.activation_pending = VoterSet::EMPTY;
        Ok(())
    }

    fn installing_mut(&mut self) -> Result<&mut InstallingView, DriverError> {
        match &mut self.role {
            Role::Installing(installing) => Ok(installing),
            Role::Normal(_) | Role::ViewChanging(_) => Err(ReplicationError::WrongRole.into()),
            Role::Fenced(_) => Err(ViewChangeError::Faulted.into()),
        }
    }
}
