//! Deterministic storage adapter using production writers, decoders, and publishers.
//!
//! Models one bounded active segment, immutable metadata, and nonvoting staging.
//! It does not model OS locks, directories on a device, or checkpoints.
//! Protocol authority belongs to the caller's production replication core.

pub(crate) mod io;
mod journal;

pub use journal::{Journal, Recovered};

/// Offline damage to bytes that a successful file barrier previously preserved.
#[derive(Debug, Clone, Copy)]
pub enum Damage {
    /// Flip one byte at an explicit offset.
    Flip(usize),
    /// Replace the suffix beginning at this offset with zeros.
    ZeroSuffix(usize),
    /// Remove the suffix beginning at this offset.
    Truncate(usize),
    /// Lose the selected directory entry.
    Missing,
}
