//! Canonical journal operations, progress, integrity, and work limits.
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::indexing_slicing)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

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
