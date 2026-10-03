use super::{Format, Journal, Limits, scan_contains_position, validate_operation_bodies};
use crate::directory::{
    DATA_DIRECTORIES, DirectoryError, GROUP_CONFIGURATION_MAX_BYTES, evidence,
    is_checkpoint_staging_name, is_index_staging_name, position_before, segment_name,
    segment_reference_name, validate_configuration_length, validate_initial_segment,
    validate_segment_header,
};
use crate::{
    AsyncSegmentStart, AsyncSegmentWriter, CanonicalRecoveryRequirements, ChainPosition,
    CommitMode, CurrentReference, GroupIdentity, LogPosition, Manifest, SegmentReference,
    TailState, async_files::Access, async_metadata, checkpoint::asynchronous::Checkpoint,
    checkpoint_name, decode_segment_header, encode_group_identity, encode_manifest_with_limits,
    manifest_digest, scan_segment_async,
};
use ozzy_io::{FileKind, Local, OpenMode, Operation};
use ozzy_journal::progress::JournalGeneration;
use std::{
    io,
    path::{Path, PathBuf},
};

/// Locked, validated authority metadata before mutable recovery starts.
/// Adapters may reject unsupported histories without touching segment bytes.
#[derive(Debug)]
pub struct Opening {
    directory: async_metadata::Directory,
    selected: async_metadata::Selected,
    limits: Limits,
}

impl Opening {
    pub const fn manifest(&self) -> &Manifest {
        &self.selected.manifest
    }

    /// Recover under the same directory lock and exact selected metadata.
    pub async fn recover(self, generation: JournalGeneration) -> Result<Journal, DirectoryError> {
        Journal::recover_selected(self.directory, self.selected, generation, self.limits).await
    }
}

impl Limits {
    pub(super) fn validate(self, io: &Local, root: &Path) -> Result<(), DirectoryError> {
        self.validate_backend(io.admission().limits(), io.shard(), root)
    }

    /// Pure startup check against a shared backend lane. Does not open files.
    pub fn validate_backend(
        self,
        limits: ozzy_io::Limits,
        shard: usize,
        root: &Path,
    ) -> Result<(), DirectoryError> {
        self.io.validate_backend(limits, shard)?;
        crate::codec::validate_decode_limits(self.decode)?;
        crate::checkpoint::validate_limits(self.checkpoint)?;
        let listing = Operation::ReadDirectory {
            path: root.join("staging"),
            max_entries: self.directory_entries,
            max_name_bytes: self.directory_name_bytes,
        };
        let listing_bytes = listing
            .retained_bytes()?
            .checked_add(size_of::<Operation>() + size_of::<ozzy_io::Handle>())
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        if self.directory_entries == 0
            || self.directory_name_bytes == 0
            || self.io.chunk_bytes > self.metadata_io().max_file_bytes
            || self.metadata.max_segments == 0
            || self.metadata.max_manifest_bytes
                < crate::MANIFEST_HEADER_BYTES + crate::SEGMENT_REFERENCE_BYTES
            || listing_bytes > limits.share(shard, ozzy_io::Class::Progress).bytes
        {
            return Err(
                io::Error::new(io::ErrorKind::InvalidInput, "invalid journal I/O limits").into(),
            );
        }
        Ok(())
    }

    pub(super) fn metadata_io(self) -> async_metadata::Limits {
        async_metadata::Limits {
            max_file_bytes: self
                .metadata
                .max_manifest_bytes
                .max(evidence::FILE_BYTES)
                .max(GROUP_CONFIGURATION_MAX_BYTES),
            chunk_bytes: self.io.chunk_bytes,
        }
    }
}

impl Journal {
    /// Format only an absent directory below an existing parent. Every failure
    /// leaves its artifacts for explicit inspection; this never retries format
    /// against a partial or established store.
    pub async fn format(
        root: PathBuf,
        io: Local,
        spec: Format,
        generation: JournalGeneration,
        limits: Limits,
    ) -> Result<Self, DirectoryError> {
        limits.validate(&io, &root)?;
        spec.identity.validate()?;
        validate_initial_segment(spec.identity, &spec.first_segment)?;
        validate_configuration_length(spec.configuration.len())?;
        let parent = root
            .parent()
            .ok_or(DirectoryError::MissingParent)?
            .to_path_buf();
        if spec.first_segment.capacity() > limits.io.max_segment_bytes {
            return Err(DirectoryError::SegmentMismatch(
                spec.first_segment.segment_id(),
            ));
        }
        let manifest = initial_manifest(&spec);
        let bytes = encode_manifest_with_limits(&manifest, limits.metadata)?;
        let access = Access {
            io: io.clone(),
            protection: None,
        };
        let directory = access.open_directory(parent.clone()).await?;
        access.done(Operation::Close { handle: directory }).await?;
        access
            .done(Operation::CreateDirectory { path: root.clone() })
            .await
            .map_err(|error| {
                if error.kind() == io::ErrorKind::AlreadyExists {
                    DirectoryError::StoreAlreadyExists
                } else {
                    error.into()
                }
            })?;
        let mut directory =
            async_metadata::Directory::open(root.clone(), io.clone(), limits.metadata_io()).await?;
        let access = directory.access();
        for name in DATA_DIRECTORIES {
            access
                .done(Operation::CreateDirectory {
                    path: root.join(name),
                })
                .await?;
        }
        directory
            .install_immutable("identity", &encode_group_identity(spec.identity)?)
            .await?;
        directory
            .install_immutable("CONFIGURATION", &spec.configuration)
            .await?;
        let writer = AsyncSegmentWriter::create(
            root.join(segment_name(spec.first_segment.segment_id())),
            io,
            access.protection.clone(),
            spec.first_segment,
            AsyncSegmentStart {
                generation,
                first_group_number: 1,
                initial_chain: ChainPosition::GENESIS,
            },
            limits.io,
        )
        .await?;
        access.sync_directory(root.join("segments")).await?;
        directory
            .replace(
                evidence::NAME,
                ".DURABLE.tmp",
                &evidence::image(&manifest, LogPosition::GENESIS)?,
            )
            .await?;
        directory.install_immutable("MANIFEST.1", &bytes).await?;
        let current = CurrentReference {
            group_id: spec.identity.group_id,
            store_id: spec.identity.store_id,
            generation: 1,
            manifest_digest: manifest_digest(&bytes, limits.metadata)?,
        };
        directory.select_current(current).await?;
        access.sync_directory(parent).await?;
        let evidence = Some(directory.load_evidence(&manifest).await?);
        Ok(Self {
            directory,
            access,
            manifest,
            current,
            configuration: Some(spec.configuration),
            writer,
            evidence,
            checkpoint: None,
            limits,
            interrupted: false,
            pins: std::sync::Arc::default(),
        })
    }

    /// Validate the exact selected store and protected history before reopening
    /// for writes. A missing or damaged established store is never formatted.
    pub async fn open(
        root: PathBuf,
        io: Local,
        identity: GroupIdentity,
        configuration: Option<&[u8]>,
        generation: JournalGeneration,
        limits: Limits,
    ) -> Result<Self, DirectoryError> {
        Self::prepare_open(root, io, identity, configuration, limits)
            .await?
            .recover(generation)
            .await
    }

    /// Open authority files without recovering or changing segment history.
    /// The returned value retains the lock through caller validation and recovery.
    pub async fn prepare_open(
        root: PathBuf,
        io: Local,
        identity: GroupIdentity,
        configuration: Option<&[u8]>,
        limits: Limits,
    ) -> Result<Opening, DirectoryError> {
        limits.validate(&io, &root)?;
        let mut directory = async_metadata::Directory::open_existing(
            root.clone(),
            io.clone(),
            limits.metadata_io(),
        )
        .await?;
        let selected = directory
            .read_selected(identity, limits.metadata, configuration)
            .await?;
        Ok(Opening {
            directory,
            selected,
            limits,
        })
    }

    pub(super) async fn recover_selected(
        mut directory: async_metadata::Directory,
        selected: async_metadata::Selected,
        generation: JournalGeneration,
        limits: Limits,
    ) -> Result<Self, DirectoryError> {
        let root = directory.root().to_path_buf();
        let access = directory.access();
        for name in DATA_DIRECTORIES {
            let handle = access.open_directory(root.join(name)).await?;
            access.done(Operation::Close { handle }).await?;
        }
        validate_headers(&access, &root, &selected.manifest, limits).await?;
        let checkpoint = selected_checkpoint(&access, &root, &selected.manifest, limits).await?;
        cleanup_staging(&access, &root, limits).await?;
        let evidence = if selected.manifest.durable_evidence {
            Some(directory.restore_evidence(&selected.manifest).await?)
        } else {
            None
        };
        let protected = [
            selected.protected.following_chain()?,
            selected.manifest.committed.following_chain()?,
        ];
        let seen = validate_sealed(&access, &root, &selected.manifest, protected, limits).await?;
        let active = *selected
            .manifest
            .segments
            .last()
            .ok_or(DirectoryError::MissingActiveSegment)?;
        let mut writer = AsyncSegmentWriter::recover(
            root.join(segment_reference_name(active)),
            access.io.clone(),
            access.protection.clone(),
            AsyncSegmentStart {
                generation,
                first_group_number: active.first_group_number,
                initial_chain: active.first_chain,
            },
            limits.io,
            limits.decode,
            Some(CanonicalRecoveryRequirements {
                operation_limits: limits.operations,
                protected: std::array::from_fn(|i| (!seen[i]).then_some(protected[i])),
                configuration_epoch: Some(selected.manifest.configuration_epoch),
                promised_view: Some(selected.manifest.promised_view),
                discard_damaged_tail: true,
            }),
        )
        .await?;
        writer.restore_allocation().await?;
        let recovered = position_before(writer.durable_position().next_chain())?;
        let mut journal = Self {
            directory,
            access,
            manifest: selected.manifest,
            current: selected.current,
            configuration: selected.configuration,
            writer,
            evidence,
            checkpoint,
            limits,
            interrupted: false,
            pins: std::sync::Arc::default(),
        };
        let committed = if journal.manifest.commit_mode == CommitMode::LocalDurable {
            recovered
        } else {
            journal.manifest.committed
        };
        if journal.manifest.accepted != recovered || journal.manifest.committed != committed {
            let mut next = journal.next_manifest()?;
            next.accepted = recovered;
            next.committed = committed;
            journal.install_selected(next).await?;
        }
        Ok(journal)
    }
}

fn initial_manifest(spec: &Format) -> Manifest {
    Manifest {
        generation: 1,
        parent_generation: 0,
        identity: spec.identity,
        configuration_epoch: spec.configuration_epoch,
        commit_mode: spec.commit_mode,
        durable_evidence: true,
        promised_view: 0,
        last_normal_view: 0,
        accepted: LogPosition::GENESIS,
        committed: LogPosition::GENESIS,
        checkpoint: None,
        segments: vec![SegmentReference {
            segment_id: spec.first_segment.segment_id(),
            file_generation: 0,
            first_group_number: 1,
            first_chain: ChainPosition::GENESIS,
            capacity: spec.first_segment.capacity(),
            sealed: None,
        }],
    }
}

pub(super) async fn selected_checkpoint(
    access: &Access,
    root: &Path,
    manifest: &Manifest,
    limits: Limits,
) -> Result<Option<Checkpoint>, DirectoryError> {
    let Some(reference) = manifest.checkpoint else {
        return Ok(None);
    };
    let checkpoint = Checkpoint::open(
        access.clone(),
        root.join("checkpoints")
            .join(checkpoint_name(reference.checkpoint_id)),
        (manifest.identity.group_id, manifest.identity.store_id),
        limits.checkpoint,
        limits.io.chunk_bytes,
        (limits.directory_entries, limits.directory_name_bytes),
    )
    .await?;
    if checkpoint.manifest.checkpoint_id != reference.checkpoint_id
        || checkpoint.manifest.position != reference.position
        || checkpoint.digest != reference.manifest_digest
        || checkpoint.manifest.configuration_epoch != manifest.configuration_epoch
    {
        return Err(DirectoryError::CheckpointMismatch);
    }
    Ok(Some(checkpoint))
}

pub(super) async fn validate_headers(
    access: &Access,
    root: &Path,
    manifest: &Manifest,
    limits: Limits,
) -> Result<(), DirectoryError> {
    let mut previous = None;
    for reference in &manifest.segments {
        if reference.capacity > limits.io.max_segment_bytes {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id));
        }
        let handle = access
            .open(
                root.join(segment_reference_name(reference)),
                OpenMode::Read,
                false,
                false,
            )
            .await?;
        let length = access.length(&handle).await?;
        let bytes = access
            .read_range(
                &handle,
                0,
                crate::SEGMENT_HEADER_BYTES,
                limits.io.chunk_bytes,
            )
            .await?;
        access.done(Operation::Close { handle }).await?;
        let header = decode_segment_header(&bytes)?;
        validate_segment_header(manifest, previous, reference, &header, length)?;
        previous = Some(reference);
    }
    Ok(())
}

async fn validate_sealed(
    access: &Access,
    root: &Path,
    manifest: &Manifest,
    protected: [ChainPosition; 2],
    limits: Limits,
) -> Result<[bool; 2], DirectoryError> {
    let mut seen = [false; 2];
    for pair in manifest.segments.windows(2) {
        let reference = pair[0];
        let sealed = reference
            .sealed
            .ok_or(DirectoryError::SegmentMismatch(reference.segment_id))?;
        let bytes = access
            .read_file(
                root.join(segment_reference_name(reference)),
                reference.capacity as usize,
                limits.io.chunk_bytes,
            )
            .await?;
        let scan = scan_segment_async(
            &bytes,
            reference.first_group_number,
            reference.first_chain,
            limits.decode,
        )
        .await?;
        validate_operation_bodies(
            &scan,
            limits.operations,
            manifest.configuration_epoch,
            manifest.promised_view,
        )
        .await?;
        if scan.valid_bytes != sealed.valid_bytes
            || scan.digest != sealed.digest
            || scan.next_chain != pair[1].first_chain
            || matches!(scan.tail, TailState::Truncated { .. })
        {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id));
        }
        for (found, protected) in seen.iter_mut().zip(protected) {
            *found |= scan_contains_position(&scan, reference.first_chain, protected).await;
        }
    }
    Ok(seen)
}

async fn cleanup_staging(
    access: &Access,
    root: &Path,
    limits: Limits,
) -> Result<(), DirectoryError> {
    let staging = root.join("staging");
    let entries = access
        .list(
            staging.clone(),
            limits.directory_entries,
            limits.directory_name_bytes,
        )
        .await?;
    let mut changed = false;
    for entry in entries {
        if !is_checkpoint_staging_name(&entry.name) && !is_index_staging_name(&entry.name) {
            continue;
        }
        let path = staging.join(entry.name);
        if entry.kind == FileKind::Directory {
            let children = access
                .list(
                    path.clone(),
                    limits.directory_entries,
                    limits.directory_name_bytes,
                )
                .await?;
            for child in children {
                if child.kind != FileKind::File {
                    return Err(DirectoryError::NotRegularFile("staging artifact"));
                }
                access
                    .done(Operation::RemoveFile {
                        path: path.join(child.name),
                    })
                    .await?;
            }
            access.done(Operation::RemoveDirectory { path }).await?;
        } else {
            access.done(Operation::RemoveFile { path }).await?;
        }
        changed = true;
    }
    if changed {
        access.sync_directory(staging).await?;
    }
    Ok(())
}
