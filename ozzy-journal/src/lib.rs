//! Canonical journal operations, progress, integrity, and work limits.
#![forbid(unsafe_code)]

pub mod integrity;
pub mod operation;
pub mod progress;
pub mod work;

/// Bounds applied to one storage read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadLimits {
    /// Maximum records returned.
    pub max_records: usize,
    /// Maximum combined payload bytes returned.
    pub max_bytes: usize,
}
