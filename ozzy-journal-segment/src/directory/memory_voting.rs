//! Startup/shutdown evidence for votes that may precede local persistence.

use super::{DirectoryError, evidence};

pub(crate) const NAME: &str = "MEMORY_VOTING";
pub(crate) const TEMPORARY: &str = ".MEMORY_VOTING.tmp";
pub(crate) const BYTES: usize = 256;
const CONTEXT: &str = "ozzy memory voting restart evidence";
#[derive(Clone, Copy)]
pub(crate) enum VoteState {
    Drained = 1,
    Running = 2,
}

/// Shared exact evidence encoding for filesystem and byte-model publication.
pub(crate) fn encode(
    manifest: &crate::Manifest,
    configuration: &[u8],
    accepted: crate::LogPosition,
    state: VoteState,
) -> Result<[u8; BYTES], DirectoryError> {
    let mut bytes = [0; BYTES];
    bytes[..8].copy_from_slice(b"OZYMEM\0\0");
    bytes[8] = state as u8;
    // Store, broker, volume, manifest, active segment, and exact accepted prefix.
    bytes[16..176].copy_from_slice(&evidence::encode(manifest, accepted)?[..160]);
    bytes[176..208].copy_from_slice(
        ozzy_journal::integrity::hash("ozzy memory voting configuration", configuration).as_bytes(),
    );
    bytes[208..216].copy_from_slice(&manifest.promised_view.to_be_bytes());
    bytes[216..224].copy_from_slice(&manifest.last_normal_view.to_be_bytes());
    let checksum = ozzy_journal::integrity::hash(CONTEXT, &bytes[..224]);
    bytes[224..].copy_from_slice(checksum.as_bytes());
    Ok(bytes)
}
