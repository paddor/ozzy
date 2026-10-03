//! Durable node-local group-to-volume placement catalog.

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::store_lock::StoreLock;
use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_journal::operation::OperationLimits;
use ozzy_journal::progress::JournalGeneration;
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};
use thiserror::Error;

use crate::{
    DecodeLimits, Digest, GroupIdentity, MetadataLimits, OpenGroupJournal, PreparedRelocation,
    VolumeError, VolumeSet,
};

pub const PLACEMENT_HEADER_BYTES: usize = 256;
pub const PLACEMENT_ENTRY_BYTES: usize = 80;
pub const PLACEMENT_CURRENT_BYTES: usize = 128;

const CATALOG_MAGIC: &[u8; 8] = b"OZYPLC01";
const CURRENT_MAGIC: &[u8; 8] = b"OZYPCUR1";
const VERSION: u16 = 2;
const CATALOG_HASH_CONTEXT: &str = "ozzy journal placement catalog v1";
const CURRENT_HASH_CONTEXT: &str = "ozzy journal placement current v1";
const CATALOG_DIGEST_START: usize = 64;
const CATALOG_DIGEST_END: usize = 96;
const CURRENT_DIGEST_START: usize = 72;
const CURRENT_DIGEST_END: usize = 104;
const LOCK_FILE: &str = "placement.lock";
const CURRENT_FILE: &str = "CURRENT";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistencePhase {
    CatalogTemporarySynced,
    CatalogLinked,
    CatalogDirectorySynced,
    CurrentTemporarySynced,
    CurrentRenamed,
    CurrentDirectorySynced,
}

trait PersistenceObserver {
    fn completed(&mut self, phase: PersistencePhase) -> io::Result<()>;
}

#[derive(Debug, Default)]
struct NoopObserver;

impl PersistenceObserver for NoopObserver {
    fn completed(&mut self, _phase: PersistencePhase) -> io::Result<()> {
        Ok(())
    }
}

/// One selected writable local copy. Paths never enter this durable value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementEntry {
    pub group_id: GroupId,
    pub replica_node_id: NodeId,
    pub volume_id: VolumeId,
    pub store_id: StoreId,
    pub store_generation: u64,
}

/// One immutable complete node placement generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementCatalog {
    pub generation: u64,
    pub parent_generation: u64,
    pub node_id: NodeId,
    pub entries: Vec<PlacementEntry>,
}

/// Small replaceable selector for one immutable placement generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementCurrent {
    pub node_id: NodeId,
    pub generation: u64,
    pub catalog_digest: Digest,
}

/// Result of reclaiming placement catalogs not selected by `CURRENT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementCleanup {
    pub removed_catalog_generations: Vec<u64>,
    pub removed_temporary_files: Vec<String>,
    pub reclaimed_bytes: u64,
}

/// Decode/allocation bounds for node-local placement state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementLimits {
    pub max_catalog_bytes: usize,
    pub max_entries: usize,
}

impl Default for PlacementLimits {
    fn default() -> Self {
        Self {
            max_catalog_bytes: 16 * 1024 * 1024,
            max_entries: 65_536,
        }
    }
}

/// Exclusively owned placement root selected through durable `CURRENT`.
#[derive(Debug)]
pub struct PlacementDirectory {
    root: PathBuf,
    _lock: StoreLock,
    catalog: PlacementCatalog,
    current: PlacementCurrent,
    limits: PlacementLimits,
}

impl PlacementDirectory {
    /// Format one absent placement root below an existing persistent state dir.
    pub fn format_new(
        root: impl AsRef<Path>,
        node_id: NodeId,
        entries: Vec<PlacementEntry>,
        limits: PlacementLimits,
    ) -> Result<Self, PlacementError> {
        let root = root.as_ref();
        let parent = root.parent().ok_or(PlacementError::MissingParent)?;
        require_directory(parent, "placement parent")?;
        match fs::symlink_metadata(root) {
            Ok(_) => return Err(PlacementError::AlreadyExists),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        require_nonzero("node", node_id.as_bytes())?;
        fs::create_dir(root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(root.join(LOCK_FILE))?;
        let lock = acquire_lock(lock)?;
        let catalog = PlacementCatalog {
            generation: 1,
            parent_generation: 0,
            node_id,
            entries,
        };
        let bytes = encode_placement_catalog(&catalog, limits)?;
        let digest = placement_catalog_digest(&bytes, limits)?;
        write_new_synced(&root.join(catalog_name(1)), &bytes)?;
        let current = PlacementCurrent {
            node_id,
            generation: 1,
            catalog_digest: digest,
        };
        write_new_synced(
            &root.join(CURRENT_FILE),
            &encode_placement_current(current)?,
        )?;
        sync_directory(root)?;
        sync_directory(parent)?;
        Ok(Self {
            root: root.to_path_buf(),
            _lock: lock,
            catalog,
            current,
            limits,
        })
    }

    /// Open exactly `CURRENT`; missing/corrupt catalogs never trigger scanning.
    pub fn open(
        root: impl AsRef<Path>,
        expected_node_id: NodeId,
        limits: PlacementLimits,
    ) -> Result<Self, PlacementError> {
        require_nonzero("node", expected_node_id.as_bytes())?;
        let root = root.as_ref();
        require_directory(root, "placement root")?;
        require_regular_file(&root.join(LOCK_FILE), "placement lock")?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join(LOCK_FILE))?;
        let lock = acquire_lock(lock)?;
        let current = decode_placement_current(&read_exact_file(
            &root.join(CURRENT_FILE),
            PLACEMENT_CURRENT_BYTES,
        )?)?;
        if current.node_id != expected_node_id {
            return Err(PlacementError::IdentityMismatch);
        }
        let bytes = read_limited_file(
            &root.join(catalog_name(current.generation)),
            limits.max_catalog_bytes,
        )?;
        if placement_catalog_digest(&bytes, limits)? != current.catalog_digest {
            return Err(PlacementError::CurrentMismatch);
        }
        let catalog = decode_placement_catalog(&bytes, limits)?;
        if catalog.node_id != expected_node_id || catalog.generation != current.generation {
            return Err(PlacementError::CurrentMismatch);
        }
        Ok(Self {
            root: root.to_path_buf(),
            _lock: lock,
            catalog,
            current,
            limits,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub const fn current(&self) -> PlacementCurrent {
        self.current
    }

    pub const fn catalog(&self) -> &PlacementCatalog {
        &self.catalog
    }

    pub fn resolve(&self, group_id: GroupId) -> Option<PlacementEntry> {
        self.catalog
            .entries
            .binary_search_by(|entry| entry.group_id.as_bytes().cmp(group_id.as_bytes()))
            .ok()
            .map(|index| self.catalog.entries[index])
    }

    /// Reject any placement whose configured volume is absent or swapped.
    pub fn validate_volumes(&self, volumes: &VolumeSet) -> Result<(), PlacementError> {
        if self
            .catalog
            .entries
            .iter()
            .any(|entry| volumes.get(entry.volume_id).is_none())
        {
            Err(PlacementError::MissingVolume)
        } else {
            Ok(())
        }
    }

    /// Open exactly the group copy selected by this catalog generation.
    pub fn open_group(
        &self,
        volumes: &VolumeSet,
        group_id: GroupId,
        metadata_limits: MetadataLimits,
        writer_generation: JournalGeneration,
        decode_limits: DecodeLimits,
        operation_limits: OperationLimits,
    ) -> Result<OpenGroupJournal, PlacementError> {
        let entry = self
            .resolve(group_id)
            .ok_or(PlacementError::GroupNotPlaced)?;
        let volume = volumes
            .get(entry.volume_id)
            .ok_or(PlacementError::MissingVolume)?;
        Ok(volume.open_group(
            GroupIdentity {
                group_id: entry.group_id,
                replica_node_id: entry.replica_node_id,
                volume_id: entry.volume_id,
                store_id: entry.store_id,
                store_generation: entry.store_generation,
            },
            metadata_limits,
            writer_generation,
            decode_limits,
            operation_limits,
        )?)
    }

    /// Install one immutable successor and replace `CURRENT` last.
    ///
    /// Consuming ownership fences ambiguous I/O outcomes. Reopen resolves them.
    pub fn install(self, entries: Vec<PlacementEntry>) -> Result<Self, PlacementError> {
        self.install_observing(entries, &mut NoopObserver)
    }

    fn install_observing(
        mut self,
        entries: Vec<PlacementEntry>,
        observer: &mut impl PersistenceObserver,
    ) -> Result<Self, PlacementError> {
        let generation = self
            .catalog
            .generation
            .checked_add(1)
            .ok_or(PlacementError::InvalidGeneration)?;
        let mut next = PlacementCatalog {
            generation,
            parent_generation: self.catalog.generation,
            node_id: self.catalog.node_id,
            entries,
        };
        let bytes = loop {
            let bytes = encode_placement_catalog(&next, self.limits)?;
            let name = catalog_name(next.generation);
            let final_path = self.root.join(&name);
            if path_exists(&final_path)? {
                match require_exact(&final_path, &bytes) {
                    Ok(()) => break bytes,
                    Err(PlacementError::ImmutableConflict | PlacementError::LengthMismatch) => {}
                    Err(error) => return Err(error),
                }
            } else {
                let temporary = self.root.join(format!(".{name}.tmp"));
                if !path_exists(&temporary)? {
                    break bytes;
                }
                match require_exact(&temporary, &bytes) {
                    Ok(()) => break bytes,
                    Err(PlacementError::ImmutableConflict | PlacementError::LengthMismatch) => {}
                    Err(error) => return Err(error),
                }
            }
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(PlacementError::InvalidGeneration)?;
        };
        let digest = placement_catalog_digest(&bytes, self.limits)?;
        install_immutable(&self.root, &catalog_name(next.generation), &bytes, observer)?;
        let current = PlacementCurrent {
            node_id: next.node_id,
            generation: next.generation,
            catalog_digest: digest,
        };
        replace_current(
            &self.root,
            next.generation,
            &encode_placement_current(current)?,
            observer,
        )?;
        self.catalog = next;
        self.current = current;
        Ok(self)
    }

    /// Select a fully synchronized relocation destination in one successor.
    pub fn install_relocation(self, prepared: &PreparedRelocation) -> Result<Self, PlacementError> {
        let artifact = prepared.artifact();
        let source = artifact.source_identity();
        let destination = artifact.destination_placement();
        if self.resolve(source.group_id) == Some(destination) {
            prepared
                .validate_destination()
                .map_err(PlacementError::RelocationDestination)?;
            return Ok(self);
        }
        let expected_source = PlacementEntry {
            group_id: source.group_id,
            replica_node_id: source.replica_node_id,
            volume_id: source.volume_id,
            store_id: source.store_id,
            store_generation: source.store_generation,
        };
        if self.resolve(source.group_id) != Some(expected_source) {
            return Err(PlacementError::RelocationSourceMismatch);
        }
        prepared
            .validate_destination()
            .map_err(PlacementError::RelocationDestination)?;
        let mut entries = self.catalog.entries.clone();
        let index = entries
            .binary_search_by(|entry| entry.group_id.as_bytes().cmp(source.group_id.as_bytes()))
            .map_err(|_| PlacementError::RelocationSourceMismatch)?;
        entries[index] = destination;
        self.install(entries)
    }

    /// Delete catalogs not selected by `CURRENT` and exact stale temp names.
    pub fn reclaim_unreferenced_metadata(&self) -> Result<PlacementCleanup, PlacementError> {
        let mut catalogs = Vec::new();
        let mut temporaries = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(generation) = parse_catalog_name(&name) {
                if generation != self.current.generation {
                    catalogs.push((generation, entry.path()));
                }
            } else if parse_catalog_temporary_name(&name).is_some()
                || parse_current_temporary_name(&name).is_some()
            {
                temporaries.push((name.to_string_lossy().into_owned(), entry.path()));
            }
        }
        catalogs.sort_unstable_by_key(|(generation, _)| *generation);
        temporaries.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        let mut removed_catalog_generations = Vec::new();
        let mut removed_temporary_files = Vec::new();
        let mut reclaimed_bytes = 0_u64;
        for (generation, path) in catalogs {
            reclaimed_bytes = reclaimed_bytes
                .checked_add(remove_regular_file(&path)?)
                .ok_or(PlacementError::LengthOverflow)?;
            removed_catalog_generations.push(generation);
        }
        for (name, path) in temporaries {
            reclaimed_bytes = reclaimed_bytes
                .checked_add(remove_regular_file(&path)?)
                .ok_or(PlacementError::LengthOverflow)?;
            removed_temporary_files.push(name);
        }
        if !removed_catalog_generations.is_empty() || !removed_temporary_files.is_empty() {
            sync_directory(&self.root)?;
        }
        Ok(PlacementCleanup {
            removed_catalog_generations,
            removed_temporary_files,
            reclaimed_bytes,
        })
    }
}

pub fn encode_placement_catalog(
    catalog: &PlacementCatalog,
    limits: PlacementLimits,
) -> Result<Vec<u8>, PlacementError> {
    validate_catalog(catalog, limits)?;
    let entry_bytes = catalog
        .entries
        .len()
        .checked_mul(PLACEMENT_ENTRY_BYTES)
        .ok_or(PlacementError::LengthOverflow)?;
    let file_bytes = PLACEMENT_HEADER_BYTES
        .checked_add(entry_bytes)
        .ok_or(PlacementError::LengthOverflow)?;
    enforce_limit(
        "placement catalog bytes",
        file_bytes,
        limits.max_catalog_bytes,
    )?;
    let mut output = vec![0_u8; file_bytes];
    output[..8].copy_from_slice(CATALOG_MAGIC);
    put_u16(&mut output, 8, VERSION);
    put_u16(&mut output, 10, PLACEMENT_HEADER_BYTES as u16);
    put_u64(&mut output, 16, usize_to_u64(file_bytes)?);
    put_u64(&mut output, 24, catalog.generation);
    put_u64(&mut output, 32, catalog.parent_generation);
    output[40..56].copy_from_slice(catalog.node_id.as_bytes());
    put_u32(
        &mut output,
        56,
        u32::try_from(catalog.entries.len()).map_err(|_| PlacementError::LengthOverflow)?,
    );
    put_u32(&mut output, 60, PLACEMENT_ENTRY_BYTES as u32);
    for (entry, target) in catalog.entries.iter().zip(
        output[PLACEMENT_HEADER_BYTES..]
            .as_chunks_mut::<PLACEMENT_ENTRY_BYTES>()
            .0,
    ) {
        encode_entry(*entry, target);
    }
    let digest = catalog_digest(&output);
    output[CATALOG_DIGEST_START..CATALOG_DIGEST_END].copy_from_slice(digest.as_bytes());
    Ok(output)
}

pub fn decode_placement_catalog(
    input: &[u8],
    limits: PlacementLimits,
) -> Result<PlacementCatalog, PlacementError> {
    validate_limits(limits)?;
    enforce_limit(
        "placement catalog bytes",
        input.len(),
        limits.max_catalog_bytes,
    )?;
    if input.len() < PLACEMENT_HEADER_BYTES {
        return Err(PlacementError::Truncated);
    }
    let header = &input[..PLACEMENT_HEADER_BYTES];
    if &header[..8] != CATALOG_MAGIC {
        return Err(PlacementError::WrongMagic("placement catalog"));
    }
    require_version(read_u16(header, 8))?;
    if read_u16(header, 10) != PLACEMENT_HEADER_BYTES as u16
        || read_u32(header, 12) != 0
        || read_u32(header, 60) != PLACEMENT_ENTRY_BYTES as u32
        || header[96..].iter().any(|byte| *byte != 0)
    {
        return Err(PlacementError::UnsupportedFields);
    }
    let file_bytes =
        usize::try_from(read_u64(header, 16)).map_err(|_| PlacementError::LengthOverflow)?;
    let count = read_u32(header, 56) as usize;
    enforce_limit("placement entry count", count, limits.max_entries)?;
    let expected_bytes = PLACEMENT_HEADER_BYTES
        .checked_add(
            count
                .checked_mul(PLACEMENT_ENTRY_BYTES)
                .ok_or(PlacementError::LengthOverflow)?,
        )
        .ok_or(PlacementError::LengthOverflow)?;
    if file_bytes != input.len() || file_bytes != expected_bytes {
        return Err(PlacementError::LengthMismatch);
    }
    if catalog_digest(input).as_bytes() != &header[CATALOG_DIGEST_START..CATALOG_DIGEST_END] {
        return Err(PlacementError::DigestMismatch("placement catalog"));
    }
    let node_id = NodeId::from_bytes(array_16(header, 40));
    let entries = input[PLACEMENT_HEADER_BYTES..]
        .as_chunks::<PLACEMENT_ENTRY_BYTES>()
        .0
        .iter()
        .map(decode_entry)
        .collect::<Result<Vec<_>, _>>()?;
    let catalog = PlacementCatalog {
        generation: read_u64(header, 24),
        parent_generation: read_u64(header, 32),
        node_id,
        entries,
    };
    validate_catalog(&catalog, limits)?;
    Ok(catalog)
}

pub fn placement_catalog_digest(
    input: &[u8],
    limits: PlacementLimits,
) -> Result<Digest, PlacementError> {
    decode_placement_catalog_shape(input, limits)?;
    Ok(catalog_digest(input))
}

pub fn encode_placement_current(
    current: PlacementCurrent,
) -> Result<[u8; PLACEMENT_CURRENT_BYTES], PlacementError> {
    validate_current(current)?;
    let mut output = [0_u8; PLACEMENT_CURRENT_BYTES];
    output[..8].copy_from_slice(CURRENT_MAGIC);
    put_u16(&mut output, 8, VERSION);
    put_u16(&mut output, 10, PLACEMENT_CURRENT_BYTES as u16);
    output[16..32].copy_from_slice(current.node_id.as_bytes());
    put_u64(&mut output, 32, current.generation);
    output[40..72].copy_from_slice(current.catalog_digest.as_bytes());
    let digest = current_digest(&output);
    output[CURRENT_DIGEST_START..CURRENT_DIGEST_END].copy_from_slice(digest.as_bytes());
    Ok(output)
}

pub fn decode_placement_current(input: &[u8]) -> Result<PlacementCurrent, PlacementError> {
    if input.len() != PLACEMENT_CURRENT_BYTES {
        return Err(PlacementError::LengthMismatch);
    }
    if &input[..8] != CURRENT_MAGIC {
        return Err(PlacementError::WrongMagic("placement CURRENT"));
    }
    require_version(read_u16(input, 8))?;
    if read_u16(input, 10) != PLACEMENT_CURRENT_BYTES as u16
        || read_u32(input, 12) != 0
        || input[104..].iter().any(|byte| *byte != 0)
    {
        return Err(PlacementError::UnsupportedFields);
    }
    if current_digest(input).as_bytes() != &input[CURRENT_DIGEST_START..CURRENT_DIGEST_END] {
        return Err(PlacementError::DigestMismatch("placement CURRENT"));
    }
    let current = PlacementCurrent {
        node_id: NodeId::from_bytes(array_16(input, 16)),
        generation: read_u64(input, 32),
        catalog_digest: digest_at(input, 40),
    };
    validate_current(current)?;
    Ok(current)
}

fn validate_catalog(
    catalog: &PlacementCatalog,
    limits: PlacementLimits,
) -> Result<(), PlacementError> {
    validate_limits(limits)?;
    require_nonzero("node", catalog.node_id.as_bytes())?;
    if catalog.generation == 0
        || (catalog.generation == 1 && catalog.parent_generation != 0)
        || (catalog.generation > 1
            && (catalog.parent_generation == 0 || catalog.parent_generation >= catalog.generation))
    {
        return Err(PlacementError::InvalidGeneration);
    }
    enforce_limit(
        "placement entry count",
        catalog.entries.len(),
        limits.max_entries,
    )?;
    for entry in &catalog.entries {
        validate_entry(*entry, catalog.node_id)?;
    }
    if catalog
        .entries
        .windows(2)
        .any(|pair| pair[0].group_id.as_bytes() >= pair[1].group_id.as_bytes())
    {
        return Err(PlacementError::UnsortedOrDuplicate);
    }
    Ok(())
}

fn validate_entry(entry: PlacementEntry, node_id: NodeId) -> Result<(), PlacementError> {
    for (kind, bytes) in [
        ("group", entry.group_id.as_bytes()),
        ("replica node", entry.replica_node_id.as_bytes()),
        ("volume", entry.volume_id.as_bytes()),
        ("store", entry.store_id.as_bytes()),
    ] {
        require_nonzero(kind, bytes)?;
    }
    if entry.replica_node_id != node_id || entry.store_generation == 0 {
        return Err(PlacementError::InvalidEntry);
    }
    Ok(())
}

fn validate_current(current: PlacementCurrent) -> Result<(), PlacementError> {
    require_nonzero("node", current.node_id.as_bytes())?;
    if current.generation == 0 || current.catalog_digest == Digest::ZERO {
        return Err(PlacementError::InvalidCurrent);
    }
    Ok(())
}

fn validate_limits(limits: PlacementLimits) -> Result<(), PlacementError> {
    if limits.max_catalog_bytes < PLACEMENT_HEADER_BYTES {
        Err(PlacementError::InvalidLimits)
    } else {
        Ok(())
    }
}

fn encode_entry(entry: PlacementEntry, output: &mut [u8; PLACEMENT_ENTRY_BYTES]) {
    output[..16].copy_from_slice(entry.group_id.as_bytes());
    output[16..32].copy_from_slice(entry.replica_node_id.as_bytes());
    output[32..48].copy_from_slice(entry.volume_id.as_bytes());
    output[48..64].copy_from_slice(entry.store_id.as_bytes());
    put_u64(output, 64, entry.store_generation);
}

fn decode_entry(input: &[u8; PLACEMENT_ENTRY_BYTES]) -> Result<PlacementEntry, PlacementError> {
    if read_u32(input, 72) != 0 || read_u32(input, 76) != 0 {
        return Err(PlacementError::UnsupportedFields);
    }
    Ok(PlacementEntry {
        group_id: GroupId::from_bytes(array_16(input, 0)),
        replica_node_id: NodeId::from_bytes(array_16(input, 16)),
        volume_id: VolumeId::from_bytes(array_16(input, 32)),
        store_id: StoreId::from_bytes(array_16(input, 48)),
        store_generation: read_u64(input, 64),
    })
}

fn decode_placement_catalog_shape(
    input: &[u8],
    limits: PlacementLimits,
) -> Result<(), PlacementError> {
    validate_limits(limits)?;
    enforce_limit(
        "placement catalog bytes",
        input.len(),
        limits.max_catalog_bytes,
    )?;
    if input.len() < PLACEMENT_HEADER_BYTES {
        return Err(PlacementError::Truncated);
    }
    let count = read_u32(input, 56) as usize;
    enforce_limit("placement entry count", count, limits.max_entries)?;
    let expected = PLACEMENT_HEADER_BYTES
        .checked_add(
            count
                .checked_mul(PLACEMENT_ENTRY_BYTES)
                .ok_or(PlacementError::LengthOverflow)?,
        )
        .ok_or(PlacementError::LengthOverflow)?;
    if expected != input.len() || read_u64(input, 16) != usize_to_u64(expected)? {
        return Err(PlacementError::LengthMismatch);
    }
    Ok(())
}

fn catalog_digest(input: &[u8]) -> Digest {
    hash_zeroed(
        CATALOG_HASH_CONTEXT,
        input,
        CATALOG_DIGEST_START,
        CATALOG_DIGEST_END,
    )
}

fn current_digest(input: &[u8]) -> Digest {
    hash_zeroed(
        CURRENT_HASH_CONTEXT,
        input,
        CURRENT_DIGEST_START,
        CURRENT_DIGEST_END,
    )
}

fn hash_zeroed(context: &str, input: &[u8], start: usize, end: usize) -> Digest {
    debug_assert_eq!(end - start, 32);
    let mut hasher = Hasher::new(context);
    hasher.update(&input[..start]);
    hasher.update(&[0_u8; 32]);
    hasher.update(&input[end..]);
    hasher.finish()
}

fn install_immutable(
    root: &Path,
    name: &str,
    bytes: &[u8],
    observer: &mut impl PersistenceObserver,
) -> Result<(), PlacementError> {
    let final_path = root.join(name);
    match fs::symlink_metadata(&final_path) {
        Ok(_) => {
            require_exact(&final_path, bytes)?;
            sync_directory(root)?;
            observer.completed(PersistencePhase::CatalogDirectorySynced)?;
            return Ok(());
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let temporary = root.join(format!(".{name}.tmp"));
    prepare_temporary(&temporary, bytes)?;
    observer.completed(PersistencePhase::CatalogTemporarySynced)?;
    match fs::hard_link(&temporary, &final_path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            require_exact(&final_path, bytes)?;
        }
        Err(error) => return Err(error.into()),
    }
    observer.completed(PersistencePhase::CatalogLinked)?;
    match fs::remove_file(temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    sync_directory(root)?;
    observer.completed(PersistencePhase::CatalogDirectorySynced)?;
    Ok(())
}

fn replace_current(
    root: &Path,
    generation: u64,
    bytes: &[u8; PLACEMENT_CURRENT_BYTES],
    observer: &mut impl PersistenceObserver,
) -> Result<(), PlacementError> {
    let temporary = root.join(format!(".CURRENT.{generation}.tmp"));
    prepare_temporary(&temporary, bytes)?;
    observer.completed(PersistencePhase::CurrentTemporarySynced)?;
    fs::rename(temporary, root.join(CURRENT_FILE))?;
    observer.completed(PersistencePhase::CurrentRenamed)?;
    sync_directory(root)?;
    observer.completed(PersistencePhase::CurrentDirectorySynced)?;
    Ok(())
}

/// Write and synchronize an unselected temporary. An interrupted attempt can
/// leave its name with no, some, or other bytes. Nothing refers to that name,
/// so it is removed and created again. The old file is never written: an
/// interrupted link may share it with an installed catalog.
fn prepare_temporary(path: &Path, bytes: &[u8]) -> Result<(), PlacementError> {
    let create = || -> Result<(), PlacementError> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    };
    match create() {
        Err(PlacementError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {
            match require_exact(path, bytes) {
                Ok(()) => Ok(File::open(path)?.sync_all()?),
                Err(PlacementError::ImmutableConflict | PlacementError::LengthMismatch) => {
                    fs::remove_file(path)?;
                    create()
                }
                Err(error) => Err(error),
            }
        }
        result => result,
    }
}

fn path_exists(path: &Path) -> Result<bool, PlacementError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn require_exact(path: &Path, expected: &[u8]) -> Result<(), PlacementError> {
    if read_exact_file(path, expected.len())? == expected {
        Ok(())
    } else {
        Err(PlacementError::ImmutableConflict)
    }
}

fn remove_regular_file(path: &Path) -> Result<u64, PlacementError> {
    require_regular_file(path, "unreferenced placement metadata")?;
    let bytes = fs::metadata(path)?.len();
    fs::remove_file(path)?;
    Ok(bytes)
}

fn read_exact_file(path: &Path, expected: usize) -> Result<Vec<u8>, PlacementError> {
    require_regular_file(path, "placement file")?;
    let mut file = File::open(path)?;
    if usize::try_from(file.metadata()?.len()).ok() != Some(expected) {
        return Err(PlacementError::LengthMismatch);
    }
    let mut bytes = vec![0_u8; expected];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_limited_file(path: &Path, limit: usize) -> Result<Vec<u8>, PlacementError> {
    require_regular_file(path, "placement catalog")?;
    let file = File::open(path)?;
    let length =
        usize::try_from(file.metadata()?.len()).map_err(|_| PlacementError::LengthOverflow)?;
    enforce_limit("placement catalog bytes", length, limit)?;
    let mut bytes = Vec::with_capacity(length);
    file.take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    enforce_limit("placement catalog bytes", bytes.len(), limit)?;
    Ok(bytes)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), PlacementError> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn acquire_lock(file: File) -> Result<StoreLock, PlacementError> {
    match StoreLock::acquire(file) {
        Ok(lock) => Ok(lock),
        Err(TryLockError::WouldBlock) => Err(PlacementError::Locked),
        Err(TryLockError::Error(error)) => Err(error.into()),
    }
}

fn require_regular_file(path: &Path, object: &'static str) -> Result<(), PlacementError> {
    if fs::symlink_metadata(path)?.file_type().is_file() {
        Ok(())
    } else {
        Err(PlacementError::NotRegularFile(object))
    }
}

fn require_directory(path: &Path, object: &'static str) -> Result<(), PlacementError> {
    if fs::symlink_metadata(path)?.file_type().is_dir() {
        Ok(())
    } else {
        Err(PlacementError::NotDirectory(object))
    }
}

fn sync_directory(path: &Path) -> Result<(), PlacementError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn require_nonzero(kind: &'static str, bytes: &[u8]) -> Result<(), PlacementError> {
    if bytes.iter().all(|byte| *byte == 0) {
        Err(PlacementError::ZeroIdentity(kind))
    } else {
        Ok(())
    }
}

fn enforce_limit(kind: &'static str, actual: usize, limit: usize) -> Result<(), PlacementError> {
    if actual > limit {
        Err(PlacementError::LimitExceeded {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

fn catalog_name(generation: u64) -> String {
    format!("CATALOG.{generation}")
}

fn parse_catalog_name(name: &OsStr) -> Option<u64> {
    parse_decimal_name(name.to_str()?.strip_prefix("CATALOG.")?)
}

fn parse_catalog_temporary_name(name: &OsStr) -> Option<u64> {
    parse_decimal_name(
        name.to_str()?
            .strip_prefix(".CATALOG.")?
            .strip_suffix(".tmp")?,
    )
}

fn parse_current_temporary_name(name: &OsStr) -> Option<u64> {
    parse_decimal_name(
        name.to_str()?
            .strip_prefix(".CURRENT.")?
            .strip_suffix(".tmp")?,
    )
}

fn parse_decimal_name(name: &str) -> Option<u64> {
    let value: u64 = name.parse().ok()?;
    (name == value.to_string()).then_some(value)
}

fn usize_to_u64(value: usize) -> Result<u64, PlacementError> {
    u64::try_from(value).map_err(|_| PlacementError::LengthOverflow)
}

fn array_16(input: &[u8], offset: usize) -> [u8; 16] {
    input[offset..offset + 16]
        .try_into()
        .expect("validated fixed-width field")
}

fn digest_at(input: &[u8], offset: usize) -> Digest {
    Digest::from_bytes(
        input[offset..offset + 32]
            .try_into()
            .expect("validated fixed-width digest"),
    )
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

fn read_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(
        input[offset..offset + 2]
            .try_into()
            .expect("fixed-width field"),
    )
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        input[offset..offset + 4]
            .try_into()
            .expect("fixed-width field"),
    )
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        input[offset..offset + 8]
            .try_into()
            .expect("fixed-width field"),
    )
}

fn require_version(version: u16) -> Result<(), PlacementError> {
    if version == VERSION {
        Ok(())
    } else {
        Err(PlacementError::UnsupportedVersion(version))
    }
}

/// Placement codec, ownership, or publication failure.
#[derive(Debug, Error)]
pub enum PlacementError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Volume(#[from] VolumeError),
    #[error("placement root parent is missing")]
    MissingParent,
    #[error("placement root already exists")]
    AlreadyExists,
    #[error("placement root is already owned")]
    Locked,
    #[error("{0} is not a regular file")]
    NotRegularFile(&'static str),
    #[error("{0} is not a directory")]
    NotDirectory(&'static str),
    #[error("wrong {0} magic")]
    WrongMagic(&'static str),
    #[error("unsupported placement format version {0}")]
    UnsupportedVersion(u16),
    #[error("unsupported placement flags, widths, or reserved fields")]
    UnsupportedFields,
    #[error("{0} identity is zero")]
    ZeroIdentity(&'static str),
    #[error("placement generation or parent is invalid")]
    InvalidGeneration,
    #[error("placement entry is invalid for this node")]
    InvalidEntry,
    #[error("placement entries are unsorted or duplicated")]
    UnsortedOrDuplicate,
    #[error("placement metadata length is invalid")]
    LengthMismatch,
    #[error("placement metadata is truncated")]
    Truncated,
    #[error("{0} digest mismatch")]
    DigestMismatch(&'static str),
    #[error("placement CURRENT does not identify selected catalog")]
    CurrentMismatch,
    #[error("configured node identity does not match placement state")]
    IdentityMismatch,
    #[error("placement CURRENT is invalid")]
    InvalidCurrent,
    #[error("placement resource limits are invalid")]
    InvalidLimits,
    #[error("placement references an unavailable configured volume")]
    MissingVolume,
    #[error("group is absent from selected placement catalog")]
    GroupNotPlaced,
    #[error("relocation source is not the currently selected store generation")]
    RelocationSourceMismatch,
    #[error("relocation destination failed validation")]
    RelocationDestination(#[source] crate::RelocationError),
    #[error("immutable placement generation contains different bytes")]
    ImmutableConflict,
    #[error("placement integer or length arithmetic overflow")]
    LengthOverflow,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    LimitExceeded {
        kind: &'static str,
        actual: usize,
        limit: usize,
    },
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug)]
    struct FailAfter(PersistencePhase);

    impl PersistenceObserver for FailAfter {
        fn completed(&mut self, phase: PersistencePhase) -> io::Result<()> {
            if phase == self.0 {
                Err(io::Error::other(format!("injected after {phase:?}")))
            } else {
                Ok(())
            }
        }
    }

    fn node() -> NodeId {
        NodeId::from_bytes([0x11; 16])
    }

    fn entry(store_generation: u64) -> PlacementEntry {
        PlacementEntry {
            group_id: GroupId::from_bytes([0x21; 16]),
            replica_node_id: node(),
            volume_id: VolumeId::from_bytes([0x31; 16]),
            store_id: StoreId::from_bytes([0x41; 16]),
            store_generation,
        }
    }

    #[test]
    fn every_install_persistence_cut_reopens_old_or_new_catalog() {
        let phases = [
            PersistencePhase::CatalogTemporarySynced,
            PersistencePhase::CatalogLinked,
            PersistencePhase::CatalogDirectorySynced,
            PersistencePhase::CurrentTemporarySynced,
            PersistencePhase::CurrentRenamed,
            PersistencePhase::CurrentDirectorySynced,
        ];
        for phase in phases {
            let temporary = TempDir::new().unwrap();
            let root = temporary.path().join("placement");
            let placement = PlacementDirectory::format_new(
                &root,
                node(),
                vec![entry(1)],
                PlacementLimits::default(),
            )
            .unwrap();
            assert!(matches!(
                placement.install_observing(vec![entry(2)], &mut FailAfter(phase)),
                Err(PlacementError::Io(_))
            ));

            let reopened =
                PlacementDirectory::open(&root, node(), PlacementLimits::default()).unwrap();
            let selected_new = matches!(
                phase,
                PersistencePhase::CurrentRenamed | PersistencePhase::CurrentDirectorySynced
            );
            assert_eq!(
                reopened.current().generation,
                if selected_new { 2 } else { 1 }
            );
            assert_eq!(
                reopened.resolve(entry(1).group_id),
                Some(entry(if selected_new { 2 } else { 1 }))
            );
            if !selected_new {
                let retried = reopened.install(vec![entry(2)]).unwrap();
                assert_eq!(retried.current().generation, 2);
                assert_eq!(retried.resolve(entry(1).group_id), Some(entry(2)));
            }
        }
    }
}
