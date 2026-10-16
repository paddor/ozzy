//! Observability snapshots of a replica actor. None grants authority.

use super::{ReplicaSnapshot, Scope};
use std::cell::Cell;
use std::rc::Rc;

/// Completed device maintenance work. Observability only; never storage authority.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceStatus {
    /// Successfully completed validation commands.
    pub validation_steps: u64,
    /// Captured segment lists fully checked.
    pub validation_cycles: u64,
    /// Segment bytes read by completed validation commands.
    pub validation_read_bytes: u64,
    /// Successfully completed metadata cleanup commands.
    pub metadata_steps: u64,
    /// Successfully completed orphan cleanup commands.
    pub orphan_steps: u64,
    /// File bytes removed by completed cleanup commands.
    pub reclaimed_bytes: u64,
}

impl MaintenanceStatus {
    pub(super) fn observe_reclaimed(&mut self, bytes: u64) {
        self.reclaimed_bytes = self.reclaimed_bytes.saturating_add(bytes);
    }

    pub(super) fn observe_validation(&mut self, step: ozzy_journal_segment::StorageValidationStep) {
        self.validation_steps = self.validation_steps.saturating_add(1);
        self.validation_cycles = self
            .validation_cycles
            .saturating_add(u64::from(step.remaining_segments == 0));
        self.validation_read_bytes = self
            .validation_read_bytes
            .saturating_add(step.checked_bytes);
    }
}

/// One partition's latest status, shared by its actor and scheduler and handed
/// on through recovery replacements. Owned by the shard thread.
pub(super) type SharedStatus = Rc<Cell<ActorStatus>>;

/// Observability only. A snapshot is never permission to write or emit an ACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorStatus {
    /// Cumulative completed maintenance work since actor creation.
    pub maintenance: MaintenanceStatus,
    /// Latest proposed view, including uninstalled election state.
    pub scope: Scope,
    /// Current normal-core evidence, absent during view change or installation.
    pub normal: Option<ReplicaSnapshot>,
    /// Whether a real journal action is outstanding.
    pub disk_pending: bool,
    /// Core readiness plus successful activation of this exact worker generation.
    pub application_ready: bool,
    /// True while the actor loop is running.
    pub running: bool,
}

impl ActorStatus {
    /// Status of an actor that has not started running.
    pub(super) fn initial(scope: Scope) -> Self {
        Self {
            maintenance: MaintenanceStatus::default(),
            scope,
            normal: None,
            disk_pending: false,
            application_ready: false,
            running: false,
        }
    }
}
