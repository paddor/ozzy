//! Disk-gated election selection and generation installation.

use crate::replica_actor::HistoryReason::ViewInstallation;
use crate::replica_actor::require_normal;
use crate::replica_journal::FetchedHistory;
use ozzy_replication::{Prefix, ViewChangeError};

use super::history::TransferPurpose;
use super::io::{Completed, FetchPurpose};
use super::maintenance::{Maintenance, MaintenanceTurn};
use super::{ActorError, DriverError, Duration, PendingIo, ReplicaActor};

impl ReplicaActor {
    /// Start the next owner command in authority order: ready installs,
    /// donor releases, due maintenance, durability, installation, pins, then
    /// normal or election work.
    pub(super) fn schedule(&mut self, now: Duration) -> Result<(), ActorError> {
        if self.pending.is_some() || self.journal.available_command_slots() == 0 {
            return Ok(());
        }
        // Core acceptance already owns these bytes. Install them before any
        // later validation or election action changes the journal image.
        if self.work.ready.is_some() {
            return self.send_ready_turn(now);
        }
        if self.schedule_donor_release()? {
            return Ok(());
        }
        // The captured read owns the transfer arena. Normal appends use their
        // own arenas; election/installation waits for its fenced return.
        if self.pending_replay.is_some() && !self.application_ready() {
            return Ok(());
        }
        if self.schedule_release(now)? {
            return Ok(());
        }
        let hold = match self.schedule_due_maintenance(now)? {
            MaintenanceTurn::Stop => return Ok(()),
            MaintenanceTurn::Hold => true,
            MaintenanceTurn::Continue => false,
        };
        if self.schedule_durability()? {
            return Ok(());
        }
        // Installation publishes and selects storage; a cleanup job beside
        // writes finishes first.
        if self.journal.cleaning() {
            return Ok(());
        }
        if let Some(ticket) = self.driver.installation_ticket() {
            return self.schedule_installation(&ticket);
        }
        if self.schedule_pin()? {
            return Ok(());
        }
        if self.driver.normal().is_some() {
            return self.schedule_activation(now, hold);
        }
        if self.transfer.is_some() || self.schedule_bridge()? {
            return Ok(());
        }
        self.select_or_install()
    }

    /// Give the oldest due maintenance task a turn once admitted writes
    /// drain.
    fn schedule_due_maintenance(&mut self, now: Duration) -> Result<MaintenanceTurn, ActorError> {
        // A slow maintenance step must not repeatedly win over queued writers.
        let foreground_turn = std::mem::take(&mut self.maintenance.foreground_due);
        let Some(kind) = self
            .due_maintenance(now)
            .filter(|_| !foreground_turn && self.application_ready())
        else {
            return Ok(MaintenanceTurn::Continue);
        };
        if self.pending_sync.is_some() || !self.pending_persistence.is_empty() {
            return Ok(MaintenanceTurn::Stop);
        }
        if self.work.needs_sync {
            self.schedule_sync()?;
            return Ok(MaintenanceTurn::Stop);
        }
        // A waiting proposal goes first, in this round. Deferring it to the
        // next round would ask for one that does nothing while the proposal
        // cannot move yet.
        if kind == Maintenance::Retention && self.work.proposals.blocks_maintenance() {
            return Ok(MaintenanceTurn::Continue);
        }
        self.maintenance_round(kind, now)?;
        Ok(if self.pending.is_some() {
            MaintenanceTurn::Stop
        } else if kind != Maintenance::Retention {
            MaintenanceTurn::Continue
        } else if self.maintenance.foreground_due {
            // A waiting proposal goes first on the next round. No journal
            // command completes to start it, so ask for it now.
            self.more_work = true;
            MaintenanceTurn::Stop
        } else {
            MaintenanceTurn::Hold
        })
    }

    /// Persist what election evidence needs before a promise. `true` when
    /// scheduling stops here.
    fn schedule_durability(&mut self) -> Result<bool, ActorError> {
        if (self.pending_sync.is_some() || !self.pending_persistence.is_empty())
            && !self.application_ready()
        {
            return Ok(true);
        }
        // Unsynchronized writes are not durable election evidence. After the
        // background lane drains, synchronize the old view before its promise.
        if (self.driver.election_needs_sync() && self.pending_sync.is_none())
            || (self.work.needs_sync && self.driver.normal().is_none())
        {
            self.schedule_sync()?;
            return Ok(true);
        }
        if self.promise.is_some() && !self.journal.settled() {
            return Ok(true);
        }
        if let Some(ticket) = self.promise.take() {
            self.pending = Some(PendingIo::Promise(self.journal.persist_promise(ticket)?));
            return Ok(true);
        }
        Ok(false)
    }

    fn schedule_installation(
        &mut self,
        ticket: &ozzy_replication::InstallTicket,
    ) -> Result<(), ActorError> {
        let Some(staged) = self.staged else {
            return Ok(());
        };
        if self.driver.scope().view > ticket.scope().view {
            // No disk command remains, and publication has not been submitted.
            // Stop fetching the abandoned source; retain old durable promises.
            self.transfer = None;
            self.pending = Some(PendingIo::Abort(self.journal.abort_installation(*ticket)?));
        } else if staged == ticket.accepted() {
            self.pending = Some(PendingIo::Finish(
                self.journal.finish_installation(*ticket)?,
            ));
        } else if self.transfer.is_none() && !self.schedule_bridge()? {
            // A bridge proves a retired staging start first.
            let source = self
                .lookup
                .compatible_local(ticket.source(), self.pinned, staged)
                .unwrap_or(ticket.source());
            self.start_transfer(ticket.scope(), source, staged, TransferPurpose::Install)?;
        }
        Ok(())
    }

    /// Drop state of older scopes and pin the wanted election source. `true`
    /// when a pin command started.
    fn schedule_pin(&mut self) -> Result<bool, ActorError> {
        self.lookup.reset_scope(self.driver.scope());
        if self
            .transfer
            .is_some_and(|transfer| transfer.request.scope != self.driver.scope())
        {
            self.transfer = None;
        }
        let Some((scope, source)) = self.wanted_pin else {
            return Ok(false);
        };
        if scope != self.driver.scope() {
            self.wanted_pin = None;
            return Ok(false);
        }
        if self.pinned == Some(source) {
            return Ok(false);
        }
        self.pending = Some(if let Some(old) = self.pinned {
            PendingIo::Release(self.journal.release_history(old)?)
        } else {
            PendingIo::Capture(self.journal.capture_history(source)?)
        });
        Ok(true)
    }

    fn schedule_activation(&mut self, now: Duration, hold: bool) -> Result<(), ActorError> {
        let snapshot = require_normal(&self.driver)?.snapshot();
        if let Ok(ticket) = self.driver.begin_activation() {
            // Already active normal cores need no repeated manifest publication.
            if self.activated != Some((snapshot.scope, snapshot.journal.generation)) {
                self.pending = Some(PendingIo::Activate(
                    self.journal.activate_installed(ticket)?,
                ));
            }
        }
        if self.pending.is_none() && self.application_ready() {
            self.schedule_normal(now, hold)?;
        }
        Ok(())
    }

    fn select_or_install(&mut self) -> Result<(), ActorError> {
        let generation = self.ids.generation()?;
        let mut missing = None;
        let lookup = &self.lookup;
        let mut get = |source, expected: Prefix| {
            lookup.get(source, expected.op).or_else(|| {
                missing = Some((source, expected));
                None
            })
        };
        let admitted = if self.configuration.primary(self.driver.scope().view) == self.local {
            self.driver
                .select(&mut get)
                .and_then(|_| self.driver.begin_primary_install(generation))
        } else if let Some((from, start)) = self.start {
            if start.scope != self.driver.scope() {
                self.start = None;
                return Ok(());
            }
            self.driver
                .begin_backup_install(from, start, generation, &mut get)
        } else {
            return Ok(());
        };
        match admitted {
            Ok(ticket) => {
                self.start = None;
                self.staged = None;
                self.pending = Some(PendingIo::Begin(
                    self.journal
                        .begin_installation(ticket, self.config.installation)?,
                ));
            }
            Err(DriverError::ViewChange(ViewChangeError::HistoryMissing)) => {
                let (source, expected) =
                    missing.ok_or_else(|| ActorError::history(ViewInstallation))?;
                if !self.lookup.unavailable(source, expected.op) {
                    self.lookup_history(source, expected.op, Some(expected), ViewInstallation)?;
                }
            }
            Err(DriverError::ViewChange(
                ViewChangeError::PromiseRequired
                | ViewChangeError::PromisePending
                | ViewChangeError::StartQuorumMissing
                | ViewChangeError::ReportQuorumMissing
                | ViewChangeError::StoragePending,
            )) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub(super) fn complete(
        &mut self,
        completed: Completed,
        now: Duration,
    ) -> Result<(), ActorError> {
        match completed {
            // Diagnostic progress has no replication authority.
            Completed::OrphanCleanup(step) => {
                self.maintenance
                    .complete_orphan_cleanup(step.reclaimed_bytes);
            }
            Completed::MetadataCleanup(step) => {
                self.maintenance
                    .complete_metadata_cleanup(step.reclaimed_bytes);
            }
            Completed::StorageValidation(checked) => {
                self.maintenance.complete_validation(checked.step);
                // Diagnostic work has no authority to alter a newer actor view.
                debug_assert_eq!(checked.step.generation, checked.ticket.generation());
            }
            Completed::RetiredLookup(positions, to, request) => {
                self.complete_retired_lookup(&positions, to, request)?;
            }
            Completed::Retention(turn) => self.complete_retention(turn, now),
            Completed::IdentityRetention(turn) => self.complete_identity_retention(turn, now),
            Completed::RecoveryCheckpoint(chunk, to) => {
                self.complete_recovery_checkpoint(to, chunk)?;
            }
            Completed::RecoveryPin(pin) => self.complete_recovery_pin(&pin, false)?,
            Completed::RecoveryRelease(pin) => self.complete_recovery_pin(&pin, true)?,
            Completed::Fetch(fetched, FetchPurpose::Recovery(to, nonce)) => {
                self.complete_recovery_fetch(to, nonce, fetched)?;
            }
            Completed::Fetch(fetched, FetchPurpose::Serve(to)) => self.send_ops(to, fetched)?,
            Completed::FlowPositions(positions, voter, request) => {
                self.complete_flow_verification(&positions, voter, request, now)?;
            }
            completed => self.complete_election(completed, now)?,
        }
        Ok(())
    }

    /// Promise, history pin, lookup, and installation completions; the rest
    /// belong to normal work.
    fn complete_election(&mut self, completed: Completed, now: Duration) -> Result<(), ActorError> {
        match completed {
            Completed::Promise(ticket) => {
                if let Some(donors) = &mut self.donors {
                    donors.clear_pins();
                }
                self.journal_scope = ticket.scope();
                self.driver.complete_promise(ticket)?;
            }
            Completed::Capture(source) => self.pinned = Some(source),
            Completed::Release(source) => {
                if self.pinned != Some(source) {
                    return Err(ActorError::history(ViewInstallation));
                }
                self.pinned = None;
            }
            Completed::Position(position) => self.complete_position(&position)?,
            Completed::Fetch(fetched, FetchPurpose::Install) => {
                self.complete_install_fetch(fetched)?;
            }
            Completed::Begin(ticket) => self.staged = Some(ticket.retained()),
            Completed::Chunk(chunk) => {
                if !chunk.prepared().is_empty() {
                    self.driver.validate_install_suffix(chunk.prepared())?;
                }
                self.driver.history_progress(now)?;
                self.staged = Some(chunk.end());
                self.recycle(chunk.into_buffer())?;
            }
            Completed::Finish(installed) => {
                self.activated = None;
                self.journal_scope = installed.ticket().scope();
                self.driver
                    .complete_installation(installed.ticket(), installed.applied(), now)?;
                self.staged = None;
                self.ack_at = now;
            }
            Completed::Abort(ticket) => {
                self.driver.complete_abandon(ticket, now)?;
                self.staged = None;
                self.activated = None;
            }
            Completed::Activate(ticket) => match self.driver.complete_activation(ticket) {
                Ok(()) => self.activated = Some((ticket.scope(), ticket.generation())),
                Err(DriverError::StaleValidation) => {}
                Err(error) => return Err(error.into()),
            },
            completed => self.complete_normal(completed, now)?,
        }
        Ok(())
    }

    fn complete_install_fetch(&mut self, fetched: FetchedHistory) -> Result<(), ActorError> {
        let ticket = self
            .driver
            .installation_ticket()
            .ok_or_else(|| ActorError::history(ViewInstallation))?;
        let request = self
            .transfer
            .take()
            .ok_or_else(|| ActorError::history(ViewInstallation))?
            .request;
        // The local source holds every staged predecessor it installs from.
        if fetched.request() != request || fetched.retired_predecessor().is_some() {
            return Err(ActorError::history(ViewInstallation));
        }
        let buffer = fetched.into_buffer();
        if self.driver.scope().view > ticket.scope().view {
            self.recycle(buffer)?;
        } else {
            self.pending = Some(PendingIo::Chunk(
                self.journal
                    .install_chunk(ticket, buffer)
                    .map_err(|rejected| rejected.reason)?,
            ));
        }
        Ok(())
    }
}
