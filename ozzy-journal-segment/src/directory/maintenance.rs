//! Bounded storage maintenance and selected checkpoint sources.

/// Bound objects removed per maintenance turn. File futures yield independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceBudget {
    /// Maximum removable objects; directory scans obey the journal listing limits.
    pub max_entries: usize,
}

impl Default for MaintenanceBudget {
    fn default() -> Self {
        Self { max_entries: 32 }
    }
}
