//! Bounded async index cleanup progress.

/// Progress includes directory visits, file removals, and deferred live sources.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OrphanCleanupStep {
    /// Recognized and unrecognized directory entries visited.
    pub work_units: usize,
    /// Derived index files removed in this turn.
    pub removed_files: usize,
    /// Selected or captured sources preserved.
    pub deferred_sources: usize,
    /// Logical bytes removed, excluding filesystem allocation overhead.
    pub reclaimed_bytes: u64,
    /// The bounded directory snapshot was fully visited.
    pub complete: bool,
}
