//! Offline cross-volume group relocation with an explicit placement switch.

use std::convert::Infallible;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{
    CheckpointLimits, CurrentReference, GroupDirectory, GroupIdentity, OpenGroupJournal,
    PlacementDirectory, PlacementEntry, SegmentReference, VolumeDirectory, checkpoint_name,
    encode_current, encode_group_identity, encode_manifest_with_limits, manifest_digest,
    open_checkpoint,
};

const LOCK_FILE: &str = "group.lock";
const IDENTITY_FILE: &str = "identity";
const CURRENT_FILE: &str = "CURRENT";
const DATA_DIRECTORIES: [&str; 4] = ["segments", "checkpoints", "indexes", "staging"];

/// Fully synchronized destination copy awaiting a node placement switch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelocationArtifact {
    source_root: PathBuf,
    destination_root: PathBuf,
    source: GroupIdentity,
    destination: GroupIdentity,
    manifest_generation: u64,
    manifest_digest: crate::Digest,
    configuration: Option<Vec<u8>>,
}

/// Quiescent source ownership retained across the placement commit point.
#[derive(Debug)]
pub struct PreparedRelocation {
    source: OpenGroupJournal,
    destination: OpenGroupJournal,
    artifact: RelocationArtifact,
    checkpoint_limits: CheckpointLimits,
}

impl PreparedRelocation {
    pub const fn artifact(&self) -> &RelocationArtifact {
        &self.artifact
    }

    pub(crate) fn validate_destination(&self) -> Result<(), RelocationError> {
        validate_open_destination(&self.destination, &self.artifact, self.checkpoint_limits)
    }
}

impl RelocationArtifact {
    pub fn source_root(&self) -> &Path {
        &self.source_root
    }

    pub fn destination_root(&self) -> &Path {
        &self.destination_root
    }

    pub const fn source_identity(&self) -> GroupIdentity {
        self.source
    }

    pub const fn destination_identity(&self) -> GroupIdentity {
        self.destination
    }

    pub const fn manifest_generation(&self) -> u64 {
        self.manifest_generation
    }

    pub const fn manifest_digest(&self) -> crate::Digest {
        self.manifest_digest
    }

    pub const fn destination_placement(&self) -> PlacementEntry {
        PlacementEntry {
            group_id: self.destination.group_id,
            replica_node_id: self.destination.replica_node_id,
            volume_id: self.destination.volume_id,
            store_id: self.destination.store_id,
            store_generation: self.destination.store_generation,
        }
    }
}

/// Source tree moved out of its authoritative group path after placement switch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredGroup {
    root: PathBuf,
    destination: PlacementEntry,
}

impl RetiredGroup {
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Explicitly delete one already-retired source after rechecking placement.
    pub fn remove(self, placement: &PlacementDirectory) -> Result<(), RelocationError> {
        require_selected_destination(placement, self.destination)?;
        require_directory(&self.root, "retired group")?;
        let parent = self.root.parent().ok_or(RelocationError::InvalidLayout)?;
        fs::remove_dir_all(&self.root)?;
        sync_directory(parent)
    }
}

/// Copy the selected source generation into a new destination store generation.
///
/// Source must be quiescent and fully synchronized. Local commit progress must
/// already be published. The destination volume is locked and explicitly
/// formatted; no source content is modified.
pub fn prepare_relocation(
    source: OpenGroupJournal,
    destination: &VolumeDirectory,
    checkpoint_limits: CheckpointLimits,
) -> Result<PreparedRelocation, RelocationError> {
    if source.writer().written_position() != source.writer().durable_position() {
        return Err(RelocationError::SourceNotDurable);
    }
    if source.directory().manifest().accepted != source.accepted_position()?
        || source.directory().manifest().committed != source.committed_position()?
    {
        return Err(RelocationError::SourceProgressUnpublished);
    }
    let source_identity = source.directory().identity();
    let configuration = source_configuration(&source)?;
    if source_identity.volume_id == destination.identity().volume_id {
        return Err(RelocationError::SameVolume);
    }
    let destination_identity = GroupIdentity {
        volume_id: destination.identity().volume_id,
        store_generation: source_identity
            .store_generation
            .checked_add(1)
            .ok_or(RelocationError::GenerationExhausted)?,
        ..source_identity
    };
    let destination_root = destination.group_root(source_identity.group_id);
    let volume_staging = destination.root().join("staging");
    require_directory(&volume_staging, "destination volume staging")?;
    let mut destination_manifest = source.directory().manifest().clone();
    destination_manifest.identity = destination_identity;
    let limits = source.directory().metadata_limits();
    let manifest_bytes = encode_manifest_with_limits(&destination_manifest, limits)?;
    let destination_manifest_digest = manifest_digest(&manifest_bytes, limits)?;
    let artifact = RelocationArtifact {
        source_root: source.directory().root().to_path_buf(),
        destination_root: destination_root.clone(),
        source: source_identity,
        destination: destination_identity,
        manifest_generation: destination_manifest.generation,
        manifest_digest: destination_manifest_digest,
        configuration,
    };
    match fs::symlink_metadata(&destination_root) {
        Ok(_) => {
            let prepared = finish_prepared_relocation(source, artifact, checkpoint_limits)?;
            synchronize_destination_publication(destination)?;
            return Ok(prepared);
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let temporary = volume_staging.join(format!(
        ".relocate-{}-{}.tmp",
        id_name(source_identity.group_id.as_bytes()),
        destination_identity.store_generation
    ));
    match fs::symlink_metadata(&temporary) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            fs::remove_dir_all(&temporary)?;
            sync_directory(&volume_staging)?;
        }
        Ok(_) => return Err(RelocationError::StagingConflict),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::create_dir(&temporary)?;
    for directory in DATA_DIRECTORIES {
        fs::create_dir(temporary.join(directory))?;
    }
    write_new_synced(&temporary.join(LOCK_FILE), &[])?;
    write_new_synced(
        &temporary.join(IDENTITY_FILE),
        &encode_group_identity(destination_identity)?,
    )?;
    write_authority(
        &temporary,
        artifact.configuration.as_deref(),
        &destination_manifest,
    )?;
    copy_segments(&source, &temporary)?;
    copy_selected_checkpoint(&source, &temporary, checkpoint_limits)?;

    write_new_synced(
        &temporary.join(format!("MANIFEST.{}", destination_manifest.generation)),
        &manifest_bytes,
    )?;
    write_new_synced(
        &temporary.join(CURRENT_FILE),
        &encode_current(CurrentReference {
            group_id: destination_identity.group_id,
            store_id: destination_identity.store_id,
            generation: destination_manifest.generation,
            manifest_digest: destination_manifest_digest,
        })?,
    )?;
    for directory in DATA_DIRECTORIES {
        sync_directory(&temporary.join(directory))?;
    }
    sync_directory(&temporary)?;
    fs::rename(&temporary, &destination_root)?;
    sync_directory(&destination.root().join("groups"))?;
    sync_directory(&volume_staging)?;
    sync_directory(destination.root())?;

    finish_prepared_relocation(source, artifact, checkpoint_limits)
}

fn write_authority(
    temporary: &Path,
    configuration: Option<&[u8]>,
    destination_manifest: &crate::Manifest,
) -> Result<(), RelocationError> {
    if let Some(configuration) = configuration {
        write_new_synced(
            &temporary.join(crate::directory::CONFIGURATION_FILE),
            configuration,
        )?;
    }
    if destination_manifest.durable_evidence {
        write_new_synced(
            &temporary.join("DURABLE"),
            &crate::directory::evidence::image(
                destination_manifest,
                destination_manifest.accepted,
            )?,
        )?;
    }
    Ok(())
}

fn finish_prepared_relocation(
    source: OpenGroupJournal,
    artifact: RelocationArtifact,
    checkpoint_limits: CheckpointLimits,
) -> Result<PreparedRelocation, RelocationError> {
    let destination = open_destination(
        &artifact,
        source.directory().metadata_limits(),
        source.writer().written_position().generation(),
        source.decode_limits,
        source.operation_limits,
        checkpoint_limits,
    )?;
    Ok(PreparedRelocation {
        source,
        destination,
        artifact,
        checkpoint_limits,
    })
}

fn source_configuration(source: &OpenGroupJournal) -> Result<Option<Vec<u8>>, RelocationError> {
    let configuration = crate::directory::read_configuration(source.directory().root())?;
    if configuration.as_deref() != source.directory().configuration() {
        return Err(RelocationError::SourceMismatch);
    }
    Ok(configuration)
}

fn synchronize_destination_publication(
    destination: &VolumeDirectory,
) -> Result<(), RelocationError> {
    sync_directory(&destination.root().join("groups"))?;
    sync_directory(&destination.root().join("staging"))?;
    sync_directory(destination.root())
}

/// Move the old source out of `groups/` after durable placement selection.
pub fn retire_relocated_source(
    prepared: PreparedRelocation,
    placement: &PlacementDirectory,
) -> Result<RetiredGroup, RelocationError> {
    let PreparedRelocation {
        source,
        destination,
        artifact,
        checkpoint_limits,
    } = prepared;
    if source.directory().identity() != artifact.source
        || source.directory().root() != artifact.source_root
        || source.any_artifact_pinned()?
    {
        return Err(RelocationError::SourceMismatch);
    }
    require_selected_destination(placement, artifact.destination_placement())?;
    validate_open_destination(&destination, &artifact, checkpoint_limits)?;

    let groups = artifact
        .source_root
        .parent()
        .ok_or(RelocationError::InvalidLayout)?;
    if groups.file_name().and_then(|name| name.to_str()) != Some("groups") {
        return Err(RelocationError::InvalidLayout);
    }
    let volume_root = groups.parent().ok_or(RelocationError::InvalidLayout)?;
    let staging = volume_root.join("staging");
    require_directory(&staging, "source volume staging")?;
    let retired_root = staging.join(format!(
        ".retired-{}-{}",
        id_name(artifact.source.group_id.as_bytes()),
        artifact.source.store_generation
    ));
    match fs::symlink_metadata(&retired_root) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
        Ok(_) => return Err(RelocationError::RetiredExists),
    }
    fs::rename(&artifact.source_root, &retired_root)?;
    sync_directory(groups)?;
    sync_directory(&staging)?;
    sync_directory(volume_root)?;
    drop(source);
    drop(destination);
    Ok(RetiredGroup {
        root: retired_root,
        destination: artifact.destination_placement(),
    })
}

fn open_destination(
    artifact: &RelocationArtifact,
    limits: crate::MetadataLimits,
    writer_generation: ozzy_journal::progress::JournalGeneration,
    decode_limits: crate::DecodeLimits,
    operation_limits: ozzy_journal::operation::OperationLimits,
    checkpoint_limits: CheckpointLimits,
) -> Result<OpenGroupJournal, RelocationError> {
    let validated = GroupDirectory::open(&artifact.destination_root, artifact.destination, limits)?;
    if validated.current().manifest_digest != artifact.manifest_digest
        || validated.current().generation != artifact.manifest_generation
        || validated.configuration() != artifact.configuration.as_deref()
    {
        return Err(RelocationError::DestinationMismatch);
    }
    let recovered = validated.recover(writer_generation, decode_limits, operation_limits)?;
    if recovered.directory().current().manifest_digest != artifact.manifest_digest
        || recovered.directory().current().generation != artifact.manifest_generation
    {
        return Err(RelocationError::DestinationMismatch);
    }
    validate_open_destination(&recovered, artifact, checkpoint_limits)?;
    Ok(recovered)
}

fn validate_open_destination(
    destination: &OpenGroupJournal,
    artifact: &RelocationArtifact,
    checkpoint_limits: CheckpointLimits,
) -> Result<(), RelocationError> {
    if destination.directory().identity() != artifact.destination
        || destination.directory().root() != artifact.destination_root
        || destination.directory().current().manifest_digest != artifact.manifest_digest
        || destination.directory().current().generation != artifact.manifest_generation
        || destination.directory().configuration() != artifact.configuration.as_deref()
    {
        return Err(RelocationError::DestinationMismatch);
    }
    if crate::directory::read_configuration(destination.directory().root())?.as_deref()
        != artifact.configuration.as_deref()
    {
        return Err(RelocationError::DestinationMismatch);
    }
    match destination.replay_retained_accepted(|_| Ok::<(), Infallible>(())) {
        Ok(()) => {}
        Err(crate::ReplayError::Journal(error)) => return Err(error.into()),
        Err(crate::ReplayError::Visitor(error)) => match error {},
    }
    destination.pin_selected_checkpoint(checkpoint_limits)?;
    Ok(())
}

fn copy_segments(source: &OpenGroupJournal, temporary: &Path) -> Result<(), RelocationError> {
    for reference in &source.directory().manifest().segments {
        let source_path = source
            .directory()
            .root()
            .join("segments")
            .join(segment_name(*reference));
        let destination_path = temporary.join("segments").join(segment_name(*reference));
        copy_segment(&source_path, &destination_path, *reference)?;
    }
    sync_directory(&temporary.join("segments"))
}

fn copy_selected_checkpoint(
    source: &OpenGroupJournal,
    temporary: &Path,
    limits: CheckpointLimits,
) -> Result<(), RelocationError> {
    let Some(reference) = source.directory().manifest().checkpoint else {
        return Ok(());
    };
    let source_checkpoint = source
        .directory()
        .root()
        .join("checkpoints")
        .join(checkpoint_name(reference.checkpoint_id));
    let image = open_checkpoint(
        &source_checkpoint,
        source.directory().identity().group_id,
        source.directory().identity().store_id,
        limits,
    )?;
    if image.manifest_digest() != reference.manifest_digest
        || image.manifest().position != reference.position
    {
        return Err(RelocationError::SourceMismatch);
    }
    let destination_checkpoint = temporary
        .join("checkpoints")
        .join(checkpoint_name(reference.checkpoint_id));
    fs::create_dir(&destination_checkpoint)?;
    copy_regular_synced(
        &source_checkpoint.join("manifest"),
        &destination_checkpoint.join("manifest"),
    )?;
    for chunk in &image.manifest().chunks {
        let name = format!("CHUNK.{:08x}", chunk.ordinal);
        copy_regular_synced(
            &source_checkpoint.join(&name),
            &destination_checkpoint.join(name),
        )?;
    }
    sync_directory(&destination_checkpoint)?;
    sync_directory(&temporary.join("checkpoints"))
}

fn copy_segment(
    source: &Path,
    destination: &Path,
    reference: SegmentReference,
) -> Result<(), RelocationError> {
    require_regular_file(source, "source segment")?;
    let mut input = File::open(source)?;
    let length = input.metadata()?.len();
    if length > reference.capacity
        || reference
            .sealed
            .is_some_and(|sealed| length < sealed.valid_bytes)
    {
        return Err(RelocationError::SourceMismatch);
    }
    let mut output = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(destination)?;
    allocate_segment(&output, reference.capacity)?;
    let copied = io::copy(&mut input, &mut output)?;
    if copied != length {
        return Err(RelocationError::SourceMismatch);
    }
    output.sync_all()?;
    Ok(())
}

fn copy_regular_synced(source: &Path, destination: &Path) -> Result<(), RelocationError> {
    require_regular_file(source, "relocation source")?;
    let mut input = File::open(source)?;
    let expected = input.metadata()?.len();
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    if io::copy(&mut input, &mut output)? != expected {
        return Err(RelocationError::SourceMismatch);
    }
    output.sync_all()?;
    Ok(())
}

fn require_selected_destination(
    placement: &PlacementDirectory,
    expected: PlacementEntry,
) -> Result<(), RelocationError> {
    if placement.resolve(expected.group_id) == Some(expected) {
        Ok(())
    } else {
        Err(RelocationError::PlacementMismatch)
    }
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), RelocationError> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn require_regular_file(path: &Path, object: &'static str) -> Result<(), RelocationError> {
    if fs::symlink_metadata(path)?.file_type().is_file() {
        Ok(())
    } else {
        Err(RelocationError::NotRegularFile(object))
    }
}

fn require_directory(path: &Path, object: &'static str) -> Result<(), RelocationError> {
    if fs::symlink_metadata(path)?.file_type().is_dir() {
        Ok(())
    } else {
        Err(RelocationError::NotDirectory(object))
    }
}

fn sync_directory(path: &Path) -> Result<(), RelocationError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn segment_name(reference: SegmentReference) -> String {
    reference.file_name()
}

fn id_name(bytes: &[u8; 16]) -> String {
    let mut output = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing into String is infallible");
    }
    output
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn allocate_segment(file: &File, capacity: u64) -> Result<(), RelocationError> {
    rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, capacity)
        .map_err(io::Error::from)?;
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn allocate_segment(_file: &File, _capacity: u64) -> Result<(), RelocationError> {
    Err(RelocationError::AllocationUnsupported)
}

/// Relocation copy, selection, or retirement failure.
#[derive(Debug, Error)]
pub enum RelocationError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Directory(#[from] crate::DirectoryError),
    #[error(transparent)]
    Metadata(#[from] crate::MetadataError),
    #[error(transparent)]
    Checkpoint(#[from] crate::CheckpointError),
    #[error(transparent)]
    Retention(#[from] crate::RetentionError),
    #[error("source journal contains unsynchronized writes")]
    SourceNotDurable,
    #[error("source local progress must be published before relocation")]
    SourceProgressUnpublished,
    #[error("source and destination name the same volume")]
    SameVolume,
    #[error("store generation is exhausted")]
    GenerationExhausted,
    #[error("destination relocation staging path is not a directory")]
    StagingConflict,
    #[error("relocation source does not match frozen artifact")]
    SourceMismatch,
    #[error("relocation destination does not validate against frozen artifact")]
    DestinationMismatch,
    #[error("selected placement is not the relocation destination")]
    PlacementMismatch,
    #[error("source or destination volume layout is invalid")]
    InvalidLayout,
    #[error("retired source path already exists")]
    RetiredExists,
    #[error("{0} is not a regular file")]
    NotRegularFile(&'static str),
    #[error("{0} is not a directory")]
    NotDirectory(&'static str),
    #[error("physical segment allocation is unsupported on this platform")]
    AllocationUnsupported,
}
