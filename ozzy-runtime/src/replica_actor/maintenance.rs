//! Bounded maintenance admission after foreground writes settle.

use super::{ActorError, Duration, PendingIo, ReplicaActor};
use crate::replica_actor::HistoryReason;

/// Periodic journal turns that run only between foreground writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Maintenance {
    StorageValidation,
    MetadataCleanup,
    OrphanCleanup,
    Retention,
}

impl ReplicaActor {
    /// The oldest due maintenance turn.
    pub(super) fn due_maintenance(&self, now: Duration) -> Option<Maintenance> {
        [
            (
                Maintenance::StorageValidation,
                self.storage_validation.map(|(_, next, _)| next),
            ),
            (
                Maintenance::MetadataCleanup,
                self.metadata_cleanup.map(|(_, next, _)| next),
            ),
            (
                Maintenance::OrphanCleanup,
                self.orphan_cleanup.map(|(_, next, _)| next),
            ),
            // A retirement's file work runs beside writes; the next turn
            // waits for its installation instead of draining for nothing.
            (
                Maintenance::Retention,
                self.retention_at.filter(|_| !self.journal.retiring()),
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
            Maintenance::StorageValidation => self.storage_validation_round(now),
            Maintenance::MetadataCleanup => self.metadata_cleanup_round(now),
            Maintenance::OrphanCleanup => self.orphan_cleanup_round(now),
            Maintenance::Retention => self.retention_round(now),
        }
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
        self.foreground_turn_due = true;
        if turn.expired_identities != 0 {
            self.work.proposals.release_capacity();
        }
        if !turn.enabled {
            self.retention_at = None;
        } else if turn.more_work {
            // Identity expiry continues without draining the core again;
            // record retention repeats its settled turn.
            self.identity_retention_due = turn.expired_identities != 0;
            self.retention_at = self.config.retention_interval.map(|_| now);
        }
        if let Some(buffer) = turn.proposal {
            self.work.proposals.set_maintenance(buffer);
        }
    }

    pub(super) fn complete_identity_retention(
        &mut self,
        turn: crate::replica_journal::IdentityRetention,
        now: Duration,
    ) {
        self.foreground_turn_due = true;
        if turn.expired != 0 {
            self.work.proposals.release_capacity();
        }
        if turn.more_work {
            self.identity_retention_due = true;
            self.retention_at = self.config.retention_interval.map(|_| now);
        }
        if let Some(buffer) = turn.proposal {
            self.work.proposals.set_maintenance(buffer);
        }
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
        self.storage_validation = Some((interval, interval, budget));
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
        self.orphan_cleanup = Some((interval, interval, budget));
        Ok(self)
    }

    pub(super) fn orphan_cleanup_round(&mut self, now: Duration) -> Result<(), ActorError> {
        let Some((interval, next, budget)) = self.orphan_cleanup else {
            return Ok(());
        };
        if !self.maintenance_ready(now, next) {
            return Ok(());
        }
        let ticket = self.driver.begin_validation()?;
        self.pending = Some(PendingIo::OrphanCleanup(
            self.journal.cleanup_orphans(ticket, budget)?,
        ));
        self.orphan_cleanup = Some((interval, now.saturating_add(interval), budget));
        Ok(())
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
        self.metadata_cleanup = Some((interval, interval, budget));
        Ok(self)
    }

    pub(super) fn metadata_cleanup_round(&mut self, now: Duration) -> Result<(), ActorError> {
        let Some((interval, next, budget)) = self.metadata_cleanup else {
            return Ok(());
        };
        if !self.maintenance_ready(now, next) {
            return Ok(());
        }
        let ticket = self.driver.begin_validation()?;
        self.pending = Some(PendingIo::MetadataCleanup(
            self.journal.cleanup_metadata(ticket, budget)?,
        ));
        self.metadata_cleanup = Some((interval, now.saturating_add(interval), budget));
        Ok(())
    }

    pub(super) fn storage_validation_round(&mut self, now: Duration) -> Result<(), ActorError> {
        let Some((interval, next, budget)) = self.storage_validation else {
            return Ok(());
        };
        if !self.maintenance_ready(now, next) {
            return Ok(());
        }
        let ticket = self.driver.begin_validation()?;
        self.pending = Some(PendingIo::StorageValidation(
            self.journal.validate_storage_with_budget(ticket, budget)?,
        ));
        self.storage_validation = Some((interval, now.saturating_add(interval), budget));
        Ok(())
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
        let Some(next) = self.retention_at else {
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
            self.foreground_turn_due = true;
            return Ok(());
        }
        let leader = self.configuration.primary(snapshot.scope.view) == self.local;
        if snapshot.accepted != snapshot.applied || self.pending_replay.is_some() {
            // Identity expiry owed by the last turn runs now, between
            // foreground turns, instead of draining the core for a full turn.
            if self.identity_retention_due && self.pending_replay.is_none() {
                self.identity_retention_due = false;
                self.foreground_turn_due = true;
                crate::profiling::event(crate::profiling::Event::IdentityRetention);
                self.pending = Some(PendingIo::IdentityRetention(
                    self.journal
                        .retain_identities(self.driver.begin_validation()?, leader)?,
                ));
                self.retention_at = self
                    .config
                    .retention_interval
                    .map(|interval| now.saturating_add(interval));
            }
            return Ok(());
        }
        let seed = ozzy_proto::OperationId::from_bytes(*self.ids.request()?.as_bytes());
        crate::profiling::event(crate::profiling::Event::RetentionTurn);
        self.identity_retention_due = false;
        self.pending = Some(PendingIo::Retention(self.journal.retention_turn(
            self.driver.begin_validation()?,
            seed,
            leader,
        )?));
        self.retention_at = self
            .config
            .retention_interval
            .map(|interval| now.saturating_add(interval));
        Ok(())
    }
}
