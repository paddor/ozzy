//! Metadata-only access and persistent nonvoting recovery. No damaged payload
//! is scanned, overwritten or made eligible to vote by opening this directory.

use super::{Candidate, Format, Journal, Limits};
use crate::directory::{DATA_DIRECTORIES, recovery::recovery_marker, segment_name};
use crate::{
    AsyncSegmentStart, AsyncSegmentWriter, CanonicalRecoveryLimits, ChainPosition, CommitMode,
    CurrentReference, Digest, DirectoryError, GroupIdentity, LogPosition, Manifest,
    RecoveryPublication, RecoveryPublicationError, SegmentHeader, SegmentReference, WriterError,
    async_files::Access, async_metadata,
};
use ozzy_io::{Local, Operation};
use ozzy_journal::progress::JournalGeneration;
use std::{
    io,
    path::{Path, PathBuf},
};
mod repair;
pub use repair::Repair;

/// Locked authority metadata, not a recovered journal or consensus authority.
/// A selected checkpoint remains strict even when segment bytes are damaged.
#[derive(Debug)]
pub struct RecoveryDirectory {
    directory: async_metadata::Directory,
    access: Access,
    manifest: Manifest,
    current: CurrentReference,
    configuration: Vec<u8>,
    limits: Limits,
}

impl RecoveryDirectory {
    /// Release inspected metadata handles before another owner opens the store.
    pub async fn close(self) -> Result<(), DirectoryError> {
        drop(self.access);
        self.directory.close().await
    }

    /// Filesystem directory bound to this owner.
    pub fn root(&self) -> &Path {
        self.directory.root()
    }
    /// Exact validated metadata manifest held by this object.
    pub const fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    /// Validated reference selecting the current manifest generation.
    pub const fn current(&self) -> CurrentReference {
        self.current
    }

    /// Validate all authority metadata without trusting segment payloads.
    pub async fn open_for_repair(
        root: PathBuf,
        io: Local,
        identity: GroupIdentity,
        configuration: &[u8],
        limits: Limits,
    ) -> Result<Self, DirectoryError> {
        recovery_marker(configuration)?;
        Self::open_exact(root, io, identity, configuration, limits).await
    }

    /// Inspect only an explicitly nonvoting directory without changing it.
    pub async fn open_recovering(
        root: PathBuf,
        io: Local,
        identity: GroupIdentity,
        configuration: &[u8],
        limits: Limits,
    ) -> Result<Self, DirectoryError> {
        Self::open_exact(root, io, identity, &recovery_marker(configuration)?, limits).await
    }

    async fn open_exact(
        root: PathBuf,
        io: Local,
        identity: GroupIdentity,
        configuration: &[u8],
        limits: Limits,
    ) -> Result<Self, DirectoryError> {
        limits.validate(&io, &root)?;
        let mut directory =
            async_metadata::Directory::open_existing(root.clone(), io, limits.metadata_io())
                .await?;
        let selected = directory
            .read_selected(identity, limits.metadata, Some(configuration))
            .await?;
        if selected.manifest.commit_mode != CommitMode::External
            || !selected.manifest.durable_evidence
        {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        let access = directory.access();
        for name in DATA_DIRECTORIES {
            let handle = access.open_directory(root.join(name)).await?;
            access.done(Operation::Close { handle }).await?;
        }
        super::opening::selected_checkpoint(&access, &root, &selected.manifest, limits).await?;
        Ok(Self {
            directory,
            access,
            manifest: selected.manifest,
            current: selected.current,
            configuration: configuration.to_vec(),
            limits,
        })
    }

    /// Remove voting eligibility durably before starting replacement. Selected
    /// metadata and all old segment files remain untouched for inspection.
    pub async fn quarantine_for_recovery(
        mut self,
        configuration: &[u8],
    ) -> Result<Self, DirectoryError> {
        if self.configuration != configuration {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        self.require_selected(configuration).await?;
        let marker = recovery_marker(configuration)?;
        self.directory
            .replace("CONFIGURATION", ".CONFIGURATION.quarantine.tmp", &marker)
            .await?;
        self.configuration = marker.to_vec();
        Ok(self)
    }

    async fn require_selected(&mut self, configuration: &[u8]) -> Result<(), DirectoryError> {
        let selected = self
            .directory
            .read_selected(
                self.manifest.identity,
                self.limits.metadata,
                Some(configuration),
            )
            .await?;
        if selected.current != self.current || selected.manifest != self.manifest {
            return Err(DirectoryError::CurrentMismatch);
        }
        Ok(())
    }

    /// Create a fresh private generation without reading damaged segment bytes.
    /// Old checkpoint and segment generations remain unselected and untouched.
    /// Fresh external authority must provide state plus its required history.
    pub async fn recover_nonvoting(
        mut self,
        configuration: &[u8],
        generation: JournalGeneration,
        max_orphan_probes: usize,
    ) -> Result<Journal, DirectoryError> {
        let marker = recovery_marker(configuration)?;
        if self.configuration != marker {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        self.require_selected(&marker).await?;
        let previous = self
            .manifest
            .segments
            .last()
            .ok_or(DirectoryError::MissingActiveSegment)?;
        let capacity = previous.capacity;
        let mut segment_id = previous.segment_id;
        for _ in 0..max_orphan_probes {
            segment_id = segment_id
                .checked_add(1)
                .ok_or(DirectoryError::SegmentIdExhausted)?;
            let header = SegmentHeader::new(
                self.manifest.identity.group_id,
                segment_id,
                None,
                Digest::ZERO,
                capacity,
            )?;
            let writer = AsyncSegmentWriter::create(
                self.root().join(segment_name(segment_id)),
                self.access.io.clone(),
                self.access.protection.clone(),
                header,
                AsyncSegmentStart {
                    generation,
                    first_group_number: 1,
                    initial_chain: ChainPosition::GENESIS,
                },
                self.limits.io,
            )
            .await;
            let writer = match writer {
                Ok(writer) => writer,
                Err(WriterError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            self.access
                .sync_directory(self.root().join("segments"))
                .await?;
            let mut next = self.manifest.clone();
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(DirectoryError::ManifestGeneration)?;
            next.parent_generation = self.manifest.generation;
            next.accepted = LogPosition::GENESIS;
            next.committed = LogPosition::GENESIS;
            next.checkpoint = None;
            next.segments = vec![SegmentReference {
                segment_id,
                file_generation: 0,
                first_group_number: 1,
                first_chain: ChainPosition::GENESIS,
                capacity,
                sealed: None,
            }];
            let (manifest, manifest_digest) = self
                .directory
                .publish_manifest(&self.manifest, next, self.limits.metadata)
                .await?;
            let current = CurrentReference {
                group_id: manifest.identity.group_id,
                store_id: manifest.identity.store_id,
                generation: manifest.generation,
                manifest_digest,
            };
            self.directory.select_current(current).await?;
            let evidence = Some(self.directory.restore_evidence(&manifest).await?);
            return Ok(Journal {
                directory: self.directory,
                access: self.access,
                manifest,
                current,
                configuration: Some(self.configuration),
                writer,
                evidence,
                checkpoint: None,
                limits: self.limits,
                interrupted: false,
                pins: std::sync::Arc::default(),
            });
        }
        Err(DirectoryError::RollProbeLimit {
            limit: max_orphan_probes,
        })
    }
}

impl Journal {
    /// Format an absent replacement with a marker, never a voting configuration.
    pub async fn format_recovering(
        root: PathBuf,
        io: Local,
        mut spec: Format,
        generation: JournalGeneration,
        limits: Limits,
    ) -> Result<Self, DirectoryError> {
        if spec.commit_mode != CommitMode::External {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        spec.configuration = recovery_marker(&spec.configuration)?.to_vec();
        Self::format(root, io, spec, generation, limits).await
    }

    /// Validate the exact externally authorized stable image before replacing
    /// its nonvoting marker. The returned tail remains private until activation.
    /// Errors after publication starts fence this journal until explicit reopen.
    pub async fn publish_recovered_configuration(
        &mut self,
        configuration: &[u8],
        publication: RecoveryPublication,
        limits: CanonicalRecoveryLimits,
    ) -> Result<Candidate, RecoveryPublicationError> {
        self.publish_configuration(configuration, publication, limits, false)
            .await
    }

    async fn publish_configuration(
        &mut self,
        configuration: &[u8],
        publication: RecoveryPublication,
        limits: CanonicalRecoveryLimits,
        physical_repair: bool,
    ) -> Result<Candidate, RecoveryPublicationError> {
        self.healthy()?;
        let marker = recovery_marker(configuration)?;
        if self.configuration.as_deref() != Some(marker.as_slice()) {
            return Err(DirectoryError::ConfigurationMismatch.into());
        }
        if self.current != publication.current
            || self.writer.written_position().generation() != publication.generation
            || self.writer.written_position() != self.writer.durable_position()
            || self.manifest.commit_mode != CommitMode::External
            || self.manifest.promised_view != publication.view
            || (!physical_repair && self.manifest.last_normal_view != publication.view)
            || self.manifest.accepted != publication.accepted
            || self.manifest.committed != publication.committed
            || self.accepted_position()? != publication.accepted
        {
            return Err(RecoveryPublicationError::ImageMismatch);
        }
        self.validate_authority_files().await?;
        let candidate = self.recover_canonical_candidate(limits).await?;
        let temporary = format!(".CONFIGURATION.recovered.{}.tmp", publication.generation.0);
        self.interrupted = true;
        self.directory
            .install_immutable(&temporary, configuration)
            .await?;
        self.access
            .done(Operation::Rename {
                source: self.root().join(&temporary),
                destination: self.root().join("CONFIGURATION"),
            })
            .await?;
        self.access
            .sync_directory(self.root().to_path_buf())
            .await?;
        self.configuration = Some(configuration.to_vec());
        self.interrupted = false;
        Ok(candidate)
    }
}
