//! Optional process-wide counters for active versus persisted record reads.
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) static RESIDENT_SELECTIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static PERSISTED_LOADS: AtomicU64 = AtomicU64::new(0);

/// Cumulative operation-batch selections from resident active/predecessor data.
pub fn resident_selections() -> u64 {
    RESIDENT_SELECTIONS.load(Ordering::Relaxed)
}

/// Cumulative indexed operation loads from persisted bytes, including failures.
/// Startup recovery and separate storage validation scans are not reader loads.
pub fn persisted_loads() -> u64 {
    PERSISTED_LOADS.load(Ordering::Relaxed)
}
