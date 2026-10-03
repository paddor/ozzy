//! Persistent nonvoting configuration marker for explicit lost-store recovery.

use crate::{
    CanonicalRecoveryCandidate, CanonicalRecoveryLimits, CanonicalStateRecoveryError, CommitMode,
    CurrentReference, LogPosition, OpenGroupJournal,
};
use ozzy_journal::progress::JournalGeneration;
use std::path::Path;
use std::{fs, io};

use super::{
    DirectoryError, GroupDirectory, GroupIdentity, MetadataLimits, SegmentHeader,
    validate_configuration_length,
};

const MAGIC: &[u8; 8] = b"OZYRECOV";
const MARKER_BYTES: usize = 64;

mod quarantine;
pub(crate) mod repair;
pub use repair::{RepairRange, SealedRepair, SealedRepairLimits};
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationPhase {
    Validated,
    ConfigurationSynced,
    ConfigurationRenamed,
    DirectorySynced,
}

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

impl OpenGroupJournal {
    /// Validate and atomically replace an unfinished marker with the real configuration.
    ///
    /// Requires an externally authorized exact full-WAL image with synchronized
    /// data and selected metadata. Replays application history privately before
    /// publishing configuration, preserving any accepted-only tail. The lock is
    /// retained throughout. Errors consume ownership; reopen to distinguish an
    /// incomplete marker from a fully published image after uncertain I/O.
    /// Reopen with a fresh writer incarnation before retrying. Temporary names
    /// include that incarnation so an interrupted write cannot block a new one.
    ///
    /// Success permits only fenced intact restart, not bootstrap or same-view
    /// voting. The returned candidate is still private and has no quorum authority.
    /// Run all of this blocking work on the storage worker.
    pub fn publish_recovered_configuration(
        self,
        configuration: &[u8],
        publication: RecoveryPublication,
        limits: CanonicalRecoveryLimits,
    ) -> Result<(Self, CanonicalRecoveryCandidate), RecoveryPublicationError> {
        self.publish_recovered_configuration_observing(configuration, publication, limits, |_| {
            Ok(())
        })
    }

    fn publish_recovered_configuration_observing(
        self,
        configuration: &[u8],
        publication: RecoveryPublication,
        limits: CanonicalRecoveryLimits,
        completed: impl FnMut(PublicationPhase) -> io::Result<()>,
    ) -> Result<(Self, CanonicalRecoveryCandidate), RecoveryPublicationError> {
        self.publish_configuration_observing(configuration, publication, limits, false, completed)
    }

    fn publish_configuration_observing(
        mut self,
        configuration: &[u8],
        publication: RecoveryPublication,
        limits: CanonicalRecoveryLimits,
        physical_repair: bool,
        mut completed: impl FnMut(PublicationPhase) -> io::Result<()>,
    ) -> Result<(Self, CanonicalRecoveryCandidate), RecoveryPublicationError> {
        let marker = recovery_marker(configuration)?;
        let manifest = self.directory().manifest();
        if self.directory().configuration() != Some(marker.as_slice()) {
            return Err(DirectoryError::ConfigurationMismatch.into());
        }
        if self.directory().current() != publication.current
            || self.writer().written_position().generation() != publication.generation
            || self.writer().is_faulted()
            || self.writer().written_position() != self.writer().durable_position()
            || self.buffered_roll_pending()
            || manifest.commit_mode != CommitMode::External
            || manifest.promised_view != publication.view
            || (!physical_repair && manifest.last_normal_view != publication.view)
            || manifest.accepted != publication.accepted
            || manifest.committed != publication.committed
            || self.accepted_position()? != publication.accepted
        {
            return Err(RecoveryPublicationError::ImageMismatch);
        }
        let candidate = self.recover_canonical_candidate(limits)?;
        let root = &self.directory.root;
        super::require_exact_contents(&root.join(super::CONFIGURATION_FILE), &marker)?;
        completed(PublicationPhase::Validated)?;
        let temporary = root.join(format!(
            ".CONFIGURATION.recovered.{}.tmp",
            publication.generation.0
        ));
        match fs::symlink_metadata(&temporary) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                super::write_new_synced(&temporary, configuration)?;
            }
            Err(error) => return Err(error.into()),
            Ok(_) => {
                super::require_exact_contents(&temporary, configuration)?;
                super::open_regular_file(&temporary, "recovered configuration")?.sync_all()?;
            }
        }
        completed(PublicationPhase::ConfigurationSynced)?;
        fs::rename(&temporary, root.join(super::CONFIGURATION_FILE))?;
        completed(PublicationPhase::ConfigurationRenamed)?;
        super::sync_directory(root)?;
        completed(PublicationPhase::DirectorySynced)?;
        self.directory.configuration = Some(configuration.into());
        Ok((self, candidate))
    }
}

impl GroupDirectory {
    pub(crate) fn require_recovering(&self, configuration: &[u8]) -> Result<(), DirectoryError> {
        let marker = recovery_marker(configuration)?;
        if self.configuration() != Some(marker.as_slice()) {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        super::require_exact_contents(&self.root.join(super::CONFIGURATION_FILE), &marker)
    }

    /// Explicitly create an absent replacement store without a valid voter configuration.
    ///
    /// The stored marker is bound to the intended configuration but never equals
    /// it. Ordinary configured open rejects this store even after a crash. The
    /// caller must independently obtain fresh recovery authority; formatting is
    /// not bootstrap, a vote, or permission to use previously forgotten state.
    /// All filesystem work belongs on a blocking storage worker.
    pub fn format_recovering(
        root: impl AsRef<Path>,
        identity: GroupIdentity,
        configuration_epoch: u64,
        first_segment: &SegmentHeader,
        configuration: &[u8],
    ) -> Result<Self, DirectoryError> {
        let marker = recovery_marker(configuration)?;
        Self::format_new_with_durable_evidence(
            root,
            identity,
            configuration_epoch,
            first_segment,
            &marker,
        )
    }

    /// Open only an unfinished replacement with the exact intended configuration.
    ///
    /// This resynchronizes the existing marker, not a valid voting configuration.
    /// Obtain a fresh recovery exchange before resuming transfer/publication.
    /// Missing or changed markers fail closed; completed stores use ordinary
    /// configured open and its fenced intact-restart path instead.
    pub fn open_recovering(
        root: impl AsRef<Path>,
        identity: GroupIdentity,
        limits: MetadataLimits,
        configuration: &[u8],
    ) -> Result<Self, DirectoryError> {
        let directory = Self::inspect_recovering(root, identity, limits, configuration)?;
        super::open_regular_file(
            &directory.root.join(super::CONFIGURATION_FILE),
            "configuration",
        )?
        .sync_all()?;
        super::sync_directory(&directory.root)?;
        Ok(directory)
    }

    /// Inspect exact nonvoting metadata without writing or synchronizing files.
    /// Holds the existing directory lock until dropped. This is an offline
    /// startup preflight, not journal recovery or permission to resume voting.
    /// Payload/header damage remains eligible for explicit recovery repair.
    pub fn inspect_recovering(
        root: impl AsRef<Path>,
        identity: GroupIdentity,
        limits: MetadataLimits,
        configuration: &[u8],
    ) -> Result<Self, DirectoryError> {
        let marker = recovery_marker(configuration)?;
        let directory = Self::open_metadata(root.as_ref(), identity, limits, false)?;
        if directory.configuration() != Some(marker.as_slice()) {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        Ok(directory)
    }

    /// Validate authority metadata while permitting damaged payload/header files.
    /// The caller must quarantine this directory before staging any repair.
    pub fn open_for_repair(
        root: impl AsRef<Path>,
        identity: GroupIdentity,
        limits: MetadataLimits,
        configuration: &[u8],
    ) -> Result<Self, DirectoryError> {
        let directory = Self::open_metadata(root.as_ref(), identity, limits, false)?;
        if directory.configuration() != Some(configuration) {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        Ok(directory)
    }
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
