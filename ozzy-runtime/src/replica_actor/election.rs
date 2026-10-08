//! Disk-gated election selection and generation installation.

use crate::replica_actor::HistoryReason::ViewInstallation;
use crate::replica_journal::FetchedHistory;
use ozzy_replication::ViewChangeError;

use super::history::TransferPurpose;
use super::io::{Completed, FetchPurpose};
use super::{ActorError, DriverError, Duration, PendingIo, Prefix, ReplicaActor};

impl ReplicaActor {
    #[expect(
        clippy::too_many_lines,
        reason = "ordered authority and storage scheduling decisions"
    )]
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
        // A slow maintenance step must not repeatedly win over queued writers.
        let foreground_turn = std::mem::take(&mut self.foreground_turn_due);
        let due = [
            self.storage_validation.map(|(_, next, _)| next),
            self.metadata_cleanup.map(|(_, next, _)| next),
            self.orphan_cleanup.map(|(_, next, _)| next),
            self.retention_at,
        ]
        .into_iter()
        .enumerate()
        .filter_map(|(kind, next)| next.filter(|next| now >= *next).map(|next| (kind, next)))
        .min_by_key(|(_, next)| *next)
        .map(|(kind, _)| kind);
        if let Some(kind) = due.filter(|_| !foreground_turn && self.application_ready()) {
            // Drain admitted writes, then give the oldest due maintenance task a turn.
            if self.pending_sync.is_some() || !self.pending_persistence.is_empty() {
                return Ok(());
            }
            if self.work.needs_sync {
                self.schedule_sync()?;
                return Ok(());
            }
            match kind {
                0 => self.storage_validation_round(now)?,
                1 => self.metadata_cleanup_round(now)?,
                2 => self.orphan_cleanup_round(now)?,
                3 => self.retention_round(now)?,
                _ => unreachable!("three maintenance tasks"),
            }
            if self.pending.is_some() || kind == 3 {
                return Ok(());
            }
        }
        if (self.pending_sync.is_some() || !self.pending_persistence.is_empty())
            && !self.application_ready()
        {
            return Ok(());
        }
        // Buffered normal writes are not durable election evidence. After the
        // background lane drains, synchronize the old view before its promise.
        if self.driver.election_needs_sync() && self.pending_sync.is_none() {
            self.schedule_sync()?;
            return Ok(());
        }
        if self.work.needs_sync && self.driver.normal().is_none() {
            self.schedule_sync()?;
            return Ok(());
        }
        if self.promise.is_some() && !self.journal.settled() {
            return Ok(());
        }
        if let Some(ticket) = self.promise.take() {
            self.pending = Some(PendingIo::Promise(self.journal.persist_promise(ticket)?));
            return Ok(());
        }
        if let Some(ticket) = self.driver.installation_ticket() {
            if let Some(staged) = self.staged {
                if self.driver.scope().view > ticket.scope().view {
                    // No disk command remains, and publication has not been submitted.
                    // Stop fetching the abandoned source; retain old durable promises.
                    self.transfer = None;
                    self.pending = Some(PendingIo::Abort(self.journal.abort_installation(ticket)?));
                } else if staged == ticket.accepted() {
                    self.pending =
                        Some(PendingIo::Finish(self.journal.finish_installation(ticket)?));
                } else if self.transfer.is_none() {
                    self.start_transfer(
                        ticket.scope(),
                        self.lookup
                            .compatible_local(ticket.source(), self.pinned, staged)
                            .unwrap_or(ticket.source()),
                        staged,
                        TransferPurpose::Install,
                        now,
                    )?;
                }
            }
            return Ok(());
        }
        self.lookup.reset_scope(self.driver.scope());
        if self
            .transfer
            .is_some_and(|transfer| transfer.request.scope != self.driver.scope())
        {
            self.transfer = None;
        }
        if let Some((scope, source)) = self.wanted_pin {
            if scope != self.driver.scope() {
                self.wanted_pin = None;
            } else if self.pinned != Some(source) {
                self.pending = Some(if let Some(old) = self.pinned {
                    PendingIo::Release(self.journal.release_history(old)?)
                } else {
                    PendingIo::Capture(self.journal.capture_history(source)?)
                });
                return Ok(());
            }
        }
        if let Some(normal) = self.driver.normal() {
            let snapshot = normal.snapshot();
            if let Ok(ticket) = self.driver.begin_activation() {
                // Already active normal cores need no repeated manifest publication.
                if self.activated != Some((snapshot.scope, snapshot.journal.generation)) {
                    self.pending = Some(PendingIo::Activate(
                        self.journal.activate_installed(ticket)?,
                    ));
                }
            }
            if self.pending.is_none() && self.application_ready() {
                self.schedule_normal(now)?;
            }
            return Ok(());
        }
        if self.transfer.is_some() {
            return Ok(());
        }
        if self.schedule_bridge(now)? {
            return Ok(());
        }
        self.select_or_install(now)
    }

    fn select_or_install(&mut self, now: Duration) -> Result<(), ActorError> {
        let generation = self.ids.generation()?;
        let mut missing = None;
        let lookup = &self.lookup;
        let mut get = |source, op| {
            lookup.get(source, op).or_else(|| {
                missing = Some((source, op));
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
                let (source, op) = missing.ok_or_else(|| ActorError::history(ViewInstallation))?;
                if self.lookup.unavailable(source, op) {
                    return Ok(());
                }
                if source.voter == self.local {
                    if self.pinned != Some(source) {
                        return Err(ActorError::history(ViewInstallation));
                    }
                    self.pending = Some(PendingIo::Position(
                        self.journal.history_position(source, op)?,
                    ));
                } else {
                    self.start_transfer(
                        self.driver.scope(),
                        source,
                        Prefix::GENESIS,
                        TransferPurpose::Lookup { op, found: None },
                        now,
                    )?;
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
            Completed::OrphanCleanup(step) => {
                self.foreground_turn_due = true;
                self.maintenance_status.orphan_steps =
                    self.maintenance_status.orphan_steps.saturating_add(1);
                self.maintenance_status
                    .observe_reclaimed(step.reclaimed_bytes);
            }
            Completed::MetadataCleanup(step) => {
                self.foreground_turn_due = true;
                self.maintenance_status.metadata_steps =
                    self.maintenance_status.metadata_steps.saturating_add(1);
                self.maintenance_status
                    .observe_reclaimed(step.reclaimed_bytes);
            } // Diagnostic progress has no replication authority.
            Completed::StorageValidation(checked) => {
                self.foreground_turn_due = true;
                self.maintenance_status.observe_validation(checked.step);
                // Diagnostic work has no authority to alter a newer actor view.
                debug_assert_eq!(checked.step.generation, checked.ticket.generation());
            }
            Completed::RetiredLookup(positions, to, request) => {
                self.complete_retired_lookup(&positions, to, request)?;
            }
            Completed::Retention(turn) => self.complete_retention(turn, now)?,
            Completed::RecoveryCheckpoint(chunk, to) => {
                self.complete_recovery_checkpoint(to, chunk)?;
            }
            Completed::RecoveryPin(pin) => self.complete_recovery_pin(&pin, false)?,
            Completed::RecoveryRelease(pin) => self.complete_recovery_pin(&pin, true)?,
            Completed::Fetch(fetched, FetchPurpose::Recovery(to, nonce)) => {
                self.complete_recovery_fetch(to, nonce, fetched)?;
            }
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
            Completed::Fetch(fetched, FetchPurpose::Serve(to)) => self.send_ops(to, fetched)?,
            Completed::Fetch(fetched, FetchPurpose::Install) => {
                self.complete_install_fetch(fetched)?;
            }
            Completed::Begin(ticket) => self.staged = Some(ticket.protected_committed()),
            Completed::Chunk(chunk) => {
                if !chunk.prepared().is_empty() {
                    self.driver.validate_install_suffix(chunk.prepared())?;
                }
                self.staged = Some(chunk.end());
                self.recycle(chunk.into_buffer());
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
            Completed::FlowPositions(positions, voter, request) => {
                self.complete_flow_verification(&positions, voter, request, now)?;
            }
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
        if fetched.request() != request {
            return Err(ActorError::history(ViewInstallation));
        }
        let buffer = fetched.into_buffer();
        if self.driver.scope().view > ticket.scope().view {
            self.recycle(buffer);
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
