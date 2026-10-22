//! Formatting: create an absent store with its genesis segment and manifest.

use super::{Format, Journal, Limits};
use crate::directory::{
    CONFIGURATION_FILE, DATA_DIRECTORIES, DirectoryError, IDENTITY_FILE, evidence, manifest_name,
    segment_name, validate_configuration_length, validate_initial_segment,
};
use crate::{
    ChainPosition, CurrentReference, LogPosition, Manifest, SegmentReference, SegmentStart,
    SegmentWriter, async_files::Access, async_metadata, encode_group_identity,
    encode_manifest_with_limits, manifest_digest,
};
use ozzy_io::{Local, Operation};
use ozzy_journal::progress::JournalGeneration;
use std::{
    io,
    path::{Path, PathBuf},
};

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
        spec.validate()?;
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
        let mut directory = create_store(&root, &parent, &io, limits).await?;
        let access = directory.access();
        directory
            .install_immutable(IDENTITY_FILE, &encode_group_identity(spec.identity)?)
            .await?;
        directory
            .install_immutable(CONFIGURATION_FILE, &spec.configuration)
            .await?;
        let writer = SegmentWriter::create(
            access.segment_file(root.join(segment_name(spec.first_segment.segment_id()))),
            spec.first_segment,
            SegmentStart {
                generation,
                first_group_number: 1,
                initial_chain: ChainPosition::GENESIS,
            },
            limits.io,
        )
        .await?;
        access.sync_directory(root.join("segments")).await?;
        let current = select_initial(&mut directory, &manifest, &bytes, limits).await?;
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
            cleaning: std::rc::Rc::default(),
            pins: std::rc::Rc::default(),
            validated_indexes: std::cell::RefCell::default(),
            pack_layouts: std::cell::RefCell::default(),
            active_prefix: None,
        })
    }
}

impl Format {
    /// Check the identity, genesis segment, and configuration length.
    fn validate(&self) -> Result<(), DirectoryError> {
        self.identity.validate()?;
        validate_initial_segment(self.identity, &self.first_segment)?;
        validate_configuration_length(self.configuration.len())
    }
}

/// Create the absent store directory and its data directories below an
/// existing parent. An existing store is never reused.
async fn create_store(
    root: &Path,
    parent: &Path,
    io: &Local,
    limits: Limits,
) -> Result<async_metadata::Directory, DirectoryError> {
    let access = Access {
        io: io.clone(),
        protection: None,
        readers: std::rc::Rc::default(),
    };
    let directory = access.open_directory(parent.to_path_buf()).await?;
    access.done(Operation::Close { handle: directory }).await?;
    access
        .done(Operation::CreateDirectory {
            path: root.to_path_buf(),
        })
        .await
        .map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                DirectoryError::StoreAlreadyExists
            } else {
                error.into()
            }
        })?;
    let directory =
        async_metadata::Directory::open(root.to_path_buf(), io.clone(), limits.metadata_io())
            .await?;
    let access = directory.access();
    for name in DATA_DIRECTORIES {
        access
            .done(Operation::CreateDirectory {
                path: root.join(name),
            })
            .await?;
    }
    Ok(directory)
}

/// Publish genesis evidence and the first manifest, then select it as
/// CURRENT.
async fn select_initial(
    directory: &mut async_metadata::Directory,
    manifest: &Manifest,
    bytes: &[u8],
    limits: Limits,
) -> Result<CurrentReference, DirectoryError> {
    directory
        .replace(
            evidence::NAME,
            ".DURABLE.tmp",
            &evidence::image(manifest, LogPosition::GENESIS)?,
        )
        .await?;
    directory
        .install_immutable(&manifest_name(1), bytes)
        .await?;
    let current = CurrentReference {
        group_id: manifest.identity.group_id,
        store_id: manifest.identity.store_id,
        generation: 1,
        manifest_digest: manifest_digest(bytes, limits.metadata)?,
    };
    directory.select_current(current).await?;
    Ok(current)
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
