//! Deterministic storage adapter over the async journal and a simulated device.
//!
//! Runs the production metadata, segment, suffix and recovery paths on the
//! `ozzy_io` simulation controller. It does not model checkpoints or rolls.
//! Protocol authority belongs to the caller's production replication core.

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
