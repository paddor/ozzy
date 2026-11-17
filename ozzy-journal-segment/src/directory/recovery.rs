//! Persistent nonvoting configuration marker for explicit lost-store recovery.

use crate::{CanonicalStateRecoveryError, CurrentReference, LogPosition};
use ozzy_journal::progress::JournalGeneration;
use std::io;

use super::{DirectoryError, validate_configuration_length};

const MAGIC: &[u8; 8] = b"OZYRECOV";
const MARKER_BYTES: usize = 64;

pub(crate) mod repair;
pub use repair::{RepairRange, RepairSource, SealedRepairLimits};

/// Exact synchronized image authorized by an external nonvoting recovery core.
///
/// These fields assert consensus authority; storage only checks their physical
/// consequences. A stale publication may not change a newer selected image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPublication {
    /// Complete selected metadata generation, including its digest.
    pub current: CurrentReference,
    /// Fresh local writer incarnation for the installed replacement.
    pub generation: JournalGeneration,
    /// Both the durable promise and last installed normal view.
    pub view: u64,
    /// Full selected accepted history, including uncommitted operations.
    pub accepted: LogPosition,
    /// Primary snapshot's known commit floor, never inferred from sync.
    pub committed: LogPosition,
}

/// A replacement could not be safely admitted as intact disk state.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryPublicationError {
    /// The claimed publication does not match the complete stable local image.
    #[error("recovery publication does not match the synchronized selected image")]
    ImageMismatch,
    /// The exact marker, configuration, metadata, or files could not be published.
    #[error(transparent)]
    Directory(#[from] DirectoryError),
    /// Full private application-history validation failed before publication.
    #[error(transparent)]
    Canonical(#[from] CanonicalStateRecoveryError),
    /// File creation, synchronization, or rename failed; reopen to resolve state.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Whether the store at `root` was left in the middle of a nonvoting
/// recovery: its `CONFIGURATION` holds the marker in place of the group's
/// configuration. Reads the file directly, for a start that has not yet
/// opened a file backend.
///
/// # Errors
///
/// Fails when the file cannot be read. A missing file is no marker.
pub fn recovery_unfinished(root: &std::path::Path) -> io::Result<bool> {
    use io::Read;
    let mut head = [0; MAGIC.len()];
    let mut file = match std::fs::File::open(root.join(super::CONFIGURATION_FILE)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let mut read = 0;
    while read < head.len() {
        match file.read(&mut head[read..])? {
            0 => return Ok(false),
            bytes => read += bytes,
        }
    }
    Ok(&head == MAGIC)
}

pub(crate) fn recovery_marker(configuration: &[u8]) -> Result<[u8; MARKER_BYTES], DirectoryError> {
    validate_configuration_length(configuration.len())?;
    // Reserve this namespace so a marker cannot itself be the intended config.
    if configuration.starts_with(MAGIC) {
        return Err(DirectoryError::ConfigurationMismatch);
    }
    let mut marker = [0; MARKER_BYTES];
    marker[..8].copy_from_slice(MAGIC);
    marker[8..10].copy_from_slice(&2_u16.to_be_bytes());
    marker[10..12].copy_from_slice(&(MARKER_BYTES as u16).to_be_bytes());
    marker[12..16].copy_from_slice(&(configuration.len() as u32).to_be_bytes());
    marker[16..48].copy_from_slice(
        ozzy_journal::integrity::hash("ozzy unfinished replica configuration v1", configuration)
            .as_bytes(),
    );
    Ok(marker)
}
