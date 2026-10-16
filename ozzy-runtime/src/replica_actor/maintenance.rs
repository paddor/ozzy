//! Bounded maintenance admission after foreground writes settle.

use super::periodic::Periodic;
use super::retention_turns::{RetentionDue, RetentionStep};
use super::{ActorError, Duration, MaintenanceStatus, PendingIo, ReplicaActor};
use crate::replica_actor::HistoryReason;
use ozzy_journal_segment::{MaintenanceBudget, StorageValidationBudget};

/// Periodic journal turns that run only between foreground writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Maintenance {
    StorageValidation,
    MetadataCleanup,
    OrphanCleanup,
    Retention,
}

/// The actor's maintenance schedule and the work it completed.
#[derive(Debug)]
pub(super) struct Turns {
    pub(super) status: MaintenanceStatus,
    /// A maintenance step just finished; the next command goes to writers.
    pub(super) foreground_due: bool,
    pub(super) storage_validation: Option<Periodic<StorageValidationBudget>>,
    pub(super) metadata_cleanup: Option<Periodic<MaintenanceBudget>>,
    pub(super) orphan_cleanup: Option<Periodic<MaintenanceBudget>>,
    pub(super) retention: RetentionDue,
}

impl Turns {
    /// Only retention runs until other turns are configured.
    pub(super) fn new(retention_interval: Option<Duration>) -> Self {
        Self {
            status: MaintenanceStatus::default(),
            foreground_due: false,
            storage_validation: None,
            metadata_cleanup: None,
            orphan_cleanup: None,
            retention: RetentionDue::new(retention_interval),
        }
    }

    /// Whether any maintenance turn is due at `now`.
    pub(super) fn due(&self, now: Duration) -> bool {
        self.retention.due(now)
            || self
                .storage_validation
                .as_ref()
                .is_some_and(|turn| now >= turn.next())
            || self
                .metadata_cleanup
                .as_ref()
                .is_some_and(|turn| now >= turn.next())
            || self
                .orphan_cleanup
                .as_ref()
                .is_some_and(|turn| now >= turn.next())
    }

    pub(super) fn complete_orphan_cleanup(&mut self, reclaimed_bytes: u64) {
        self.foreground_due = true;
        self.status.orphan_steps = self.status.orphan_steps.saturating_add(1);
        self.status.observe_reclaimed(reclaimed_bytes);
    }

    pub(super) fn complete_metadata_cleanup(&mut self, reclaimed_bytes: u64) {
        self.foreground_due = true;
        self.status.metadata_steps = self.status.metadata_steps.saturating_add(1);
        self.status.observe_reclaimed(reclaimed_bytes);
    }

    pub(super) fn complete_validation(
        &mut self,
        step: ozzy_journal_segment::StorageValidationStep,
    ) {
        self.foreground_due = true;
        self.status.observe_validation(step);
    }
}

impl ReplicaActor {
    /// The oldest due maintenance turn.
    pub(super) fn due_maintenance(&self, now: Duration) -> Option<Maintenance> {
        [
            (
                Maintenance::StorageValidation,
                self.maintenance
                    .storage_validation
                    .as_ref()
                    .map(Periodic::next),
            ),
            // Explicit cleanup waits for a retention cleanup job.
            (
                Maintenance::MetadataCleanup,
                self.maintenance
                    .metadata_cleanup
                    .as_ref()
                    .map(Periodic::next)
                    .filter(|_| !self.journal.cleaning()),
            ),
            (
                Maintenance::OrphanCleanup,
                self.maintenance
                    .orphan_cleanup
                    .as_ref()
                    .map(Periodic::next)
                    .filter(|_| !self.journal.cleaning()),
            ),
            // A retirement's file work runs beside writes; the next turn
            // waits for its installation instead of draining for nothing.
            // A turn's proposal must also leave validation first.
            (
                Maintenance::Retention,
                self.maintenance.retention.at().filter(|_| {
                    !self.journal.retention_busy() && !self.work.proposals.holds_maintenance()
                }),
            ),
        ]
        .into_iter()
        .filter_map(|(kind, next)| next.filter(|next| now >= *next).map(|next| (kind, next)))
        .min_by_key(|(_, next)| *next)
        .map(|(kind, _)| kind)
    }

    /// Give one due turn to the journal once admitted writes are settled.
    pub(super) fn maintenance_round(
        &mut self,
        kind: Maintenance,
        now: Duration,
    ) -> Result<(), ActorError> {
        match kind {
            Maintenance::StorageValidation => self.periodic_round(
                now,
                |actor| &mut actor.maintenance.storage_validation,
                |journal, ticket, budget| {
                    Ok(PendingIo::StorageValidation(
                        journal.validate_storage_with_budget(ticket, budget)?,
                    ))
                },
            ),
            Maintenance::MetadataCleanup => self.periodic_round(
                now,
                |actor| &mut actor.maintenance.metadata_cleanup,
                |journal, ticket, budget| {
                    Ok(PendingIo::MetadataCleanup(
                        journal.cleanup_metadata(ticket, budget)?,
                    ))
                },
            ),
            Maintenance::OrphanCleanup => self.periodic_round(
                now,
                |actor| &mut actor.maintenance.orphan_cleanup,
                |journal, ticket, budget| {
                    Ok(PendingIo::OrphanCleanup(
                        journal.cleanup_orphans(ticket, budget)?,
                    ))
                },
            ),
            Maintenance::Retention => self.retention_round(now),
        }
    }

    /// Start a due periodic turn once maintenance may run, and schedule the next.
    fn periodic_round<B: Copy>(
        &mut self,
        now: Duration,
        schedule: fn(&mut Self) -> &mut Option<Periodic<B>>,
        start: impl FnOnce(
            &mut crate::replica_journal::ReplicaJournal,
            ozzy_replication::driver::ValidationTicket,
            B,
        ) -> Result<PendingIo, ActorError>,
    ) -> Result<(), ActorError> {
        let Some(periodic) = *schedule(self) else {
            return Ok(());
        };
        if !self.maintenance_ready(now, periodic.next()) {
            return Ok(());
        }
        let ticket = self.driver.begin_validation()?;
        self.pending = Some(start(&mut self.journal, ticket, periodic.budget)?);
        if let Some(periodic) = schedule(self) {
            periodic.restart(now);
        }
        Ok(())
    }

    pub(super) fn complete_retention(
        &mut self,
        turn: crate::replica_journal::RetentionTurn,
        now: Duration,
    ) {
        if let Some(source) = turn.released {
            if self.pinned == Some(source) {
                self.pinned = None;
            }
            if self.wanted_pin.is_some_and(|(_, wanted)| wanted == source) {
                self.wanted_pin = None;
            }
        }
        self.maintenance.foreground_due = true;
        self.maintenance
            .retention
            .complete_turn(turn, now, &mut self.work.proposals);
    }

    pub(super) fn complete_identity_retention(
        &mut self,
        turn: crate::replica_journal::IdentityRetention,
        now: Duration,
    ) {
        self.maintenance.foreground_due = true;
        self.maintenance
            .retention
            .complete_identities(turn, now, &mut self.work.proposals);
    }

    /// Enable periodic byte-bounded storage checks between foreground disk actions.
    ///
    /// Overdue checks drain admitted writes before allowing another write group.
    /// Default steps read at most 256 KiB and yield between decoding units after
    /// 2 ms. A filesystem call or group decode can overrun that cooperative target.
    /// Corruption stops the actor through its ordinary journal-failure path.
    /// Disabled by default; this does not enable automatic quarantine or repair.
    pub fn with_storage_validation(self, interval: Duration) -> Result<Self, ActorError> {
        self.with_storage_validation_budget(
            interval,
            ozzy_journal_segment::StorageValidationBudget::default(),
        )
    }

    /// Reserve an overdue maintenance turn after current writes synchronize.
    /// Byte limits apply to segment reads; time limits yield between decoding units.
    pub fn with_storage_validation_budget(
        mut self,
        interval: Duration,
        budget: ozzy_journal_segment::StorageValidationBudget,
    ) -> Result<Self, ActorError> {
        if interval.is_zero() || !budget.is_valid() {
            return Err(ActorError::Limits);
        }
        self.maintenance.storage_validation = Some(Periodic::new(interval, budget));
        Ok(self)
    }

    /// Reserve periodic device turns to reclaim unselected artifacts. Default: disabled.
    pub fn with_orphan_cleanup(
        mut self,
        interval: Duration,
        budget: ozzy_journal_segment::MaintenanceBudget,
    ) -> Result<Self, ActorError> {
        if interval.is_zero() || budget.max_entries == 0 {
            return Err(ActorError::Limits);
        }
        self.maintenance.orphan_cleanup = Some(Periodic::new(interval, budget));
        Ok(self)
    }

    /// Reserve periodic device turns to reclaim old manifests. Default: disabled.
    pub fn with_metadata_cleanup(
        mut self,
        interval: Duration,
        budget: ozzy_journal_segment::MaintenanceBudget,
    ) -> Result<Self, ActorError> {
        if interval.is_zero() || budget.max_entries == 0 {
            return Err(ActorError::Limits);
        }
        self.maintenance.metadata_cleanup = Some(Periodic::new(interval, budget));
        Ok(self)
    }

    fn maintenance_ready(&self, now: Duration, next: Duration) -> bool {
        now >= next
            && self.pending.is_none()
            && self.pending_sync.is_none()
            && self.pending_persistence.is_empty()
            && !self.work.needs_sync
            && self.application_ready()
            && self.journal.available_command_slots() != 0
    }
}

impl ReplicaActor {
    pub(super) fn retention_round(&mut self, now: Duration) -> Result<(), ActorError> {
        let Some(next) = self.maintenance.retention.at() else {
            return Ok(());
        };
        if !self.maintenance_ready(now, next) {
            return Ok(());
        }
        let snapshot = self
            .driver
            .normal()
            .ok_or_else(|| ActorError::history(HistoryReason::Retention))?
            .snapshot();
        if snapshot.applied != snapshot.committed {
            self.pending = Some(PendingIo::Apply(
                self.journal
                    .apply_committed(self.driver.begin_validation()?)?,
            ));
            return Ok(());
        }
        if self.work.proposals.blocks_maintenance() {
            self.maintenance.foreground_due = true;
            return Ok(());
        }
        let leader = self.configuration.primary(snapshot.scope.view) == self.local;
        let settled = snapshot.accepted == snapshot.applied && self.pending_replay.is_none();
        let step = self
            .maintenance
            .retention
            .start(now, settled, self.pending_replay.is_none());
        match step {
            // Identity expiry owed by the last turn runs now, between
            // foreground turns, instead of draining the core for a full turn.
            Some(RetentionStep::Identities) => {
                self.maintenance.foreground_due = true;
                crate::profiling::event(crate::profiling::Event::IdentityRetention);
                let ticket = self.driver.begin_validation()?;
                self.pending = Some(PendingIo::IdentityRetention(
                    self.journal.retain_identities(ticket, leader)?,
                ));
            }
            Some(RetentionStep::Turn) => {
                let seed = ozzy_proto::OperationId::from_bytes(*self.ids.request()?.as_bytes());
                crate::profiling::event(crate::profiling::Event::RetentionTurn);
                let ticket = self.driver.begin_validation()?;
                self.pending = Some(PendingIo::Retention(
                    self.journal.retention_turn(ticket, seed, leader)?,
                ));
            }
            None => {}
        }
        Ok(())
    }
}
