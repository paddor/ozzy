pub(crate) mod asynchronous;
pub(crate) mod evidence;
pub use evidence::{CompletedDurableProgress, PreparedDurableProgress};
mod indexed_append;
mod maintenance;
pub(crate) mod memory_voting;
mod owned_write;
#[cfg(target_os = "linux")]
pub use owned_write::JournalAio;
pub(crate) use owned_write::start_writeback_range;
pub use owned_write::{
    CompletedJournalWrite, JournalGroupEncoder, JournalGroupEncoding, JournalWriteEvent,
    JournalWritePipeline, JournalWriteback, PendingJournalWrite, PreencodedJournalGroup,
    PreparedJournalWrite, SharedJournalOperation,
};
mod orphans;
pub use maintenance::{MaintenanceBudget, MetadataCleanup, MetadataCleanupStep};
pub use orphans::{OrphanCleanup, OrphanCleanupStep};
pub(crate) mod publication;
mod read_capture;
mod sealed_source;
pub(crate) use sealed_source::sealed_source;
pub(crate) mod recovery;
mod roll;
pub use recovery::{
    RecoveryPublication, RecoveryPublicationError, RepairRange, SealedRepair, SealedRepairLimits,
};
pub use roll::{
    CompletedJournalRoll, PendingJournalRoll, PreparedJournalRoll, PreparedSegment,
    SegmentPreparation,
};
#[cfg(test)]
mod progress_tests;

use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ozzy_journal::operation::{OperationCodecError, OperationLimits, validate_operation_body};
use ozzy_journal::progress::JournalGeneration;
use ozzy_proto::CheckpointId;
use thiserror::Error;

use crate::index_builder::IndexPublication;
use crate::retention::{PinRegistry, scan_is_below_floors};
use crate::store_lock::StoreLock;
use crate::{
    ActiveSegmentIndex, CURRENT_BYTES, CanonicalRecoveryRequirements, ChainPosition,
    CheckpointError, CheckpointImage, CheckpointLimits, CheckpointPin, CheckpointPlan,
    CheckpointReference, CheckpointSpec, CodecError, CommitMode, CurrentReference, DecodeLimits,
    DecodedOperation, GROUP_IDENTITY_BYTES, GroupIdentity, IndexBuildError, IndexBuildLimits,
    IndexCatalogError, IndexSource, JournalIndexBoundary, JournalIndexError, JournalIndexSnapshot,
    LogPosition, Manifest, MetadataError, MetadataLimits, RetentionError, RetentionFloors,
    RetentionResult, RetentionScanBudget, RetiredPrefix, SEGMENT_HEADER_BYTES, SegmentHeader,
    SegmentIndex, SegmentIndexCatalog, SegmentReference, SegmentWriteMode, SegmentWriter,
    TailState, UnreferencedCheckpointCleanup, UnreferencedMetadataCleanup,
    UnreferencedSegmentCleanup, WriterError, WriterPosition, checkpoint_name, decode_current,
    decode_group_identity, decode_manifest, decode_segment_header, encode_current,
    encode_group_identity, encode_manifest_with_limits, encode_segment_header, manifest_digest,
    open_checkpoint, open_segment_index, scan_segment, segment_index_name,
};

const LOCK_FILE: &str = "group.lock";
const IDENTITY_FILE: &str = "identity";
const CURRENT_FILE: &str = "CURRENT";
pub(crate) const CONFIGURATION_FILE: &str = "CONFIGURATION";
/// Maximum immutable adapter configuration bytes stored beside a group identity.
pub const GROUP_CONFIGURATION_MAX_BYTES: usize = 4096;
const DATA_DIRECTORIES: [&str; 4] = ["segments", "checkpoints", "indexes", "staging"];

/// Exclusively owned, validated metadata root for one local group replica.
#[derive(Debug)]
pub struct GroupDirectory {
    pub(crate) root: PathBuf,
    // Pins share this guard so ownership outlives detached readers.
    pub(crate) lock: Arc<StoreLock>,
    // Detached readers and this owner share publication serialization; the OS
    // ownership lease alone does not serialize threads inside this process.
    pub(crate) index_publication_lock: Arc<Mutex<()>>,
    pub(crate) identity: GroupIdentity,
    pub(crate) manifest: Manifest,
    pub(crate) current: CurrentReference,
    pub(crate) limits: MetadataLimits,
    configuration: Option<Box<[u8]>>,
    pub(crate) data_sync: bool,
    /// Write jobs use a separate `O_DIRECT` descriptor for the active segment.
    pub(crate) direct: bool,
}

/// Recovered active writer coupled to its exclusive group-directory lock.
#[derive(Debug)]
pub struct OpenGroupJournal {
    pub(crate) directory: GroupDirectory,
    pub(crate) writer: SegmentWriter<Arc<File>>,
    /// Open `DURABLE` and its last written records, when the store keeps evidence.
    pub(crate) evidence: Option<Box<evidence::Evidence>>,
    pub(crate) decode_limits: DecodeLimits,
    pub(crate) operation_limits: OperationLimits,
    pub(crate) pins: Arc<PinRegistry>,
}

/// Frozen roll work that may be published by a separate blocking I/O thread.
///
/// The paired journal already owns the successor writer. Dropping this value
/// leaves `CURRENT` selecting the predecessor generation; reopening therefore
/// ignores the unpublished successor and recovers the old active segment.
#[derive(Debug)]
pub struct BufferedRollPublication {
    root: PathBuf,
    lock: Arc<StoreLock>,
    expected_current: CurrentReference,
    next_manifest: Manifest,
    manifest_bytes: Vec<u8>,
    manifest_digest: crate::Digest,
    predecessor: Arc<File>,
    /// Appends may already be in the successor; only its header is synchronized.
    successor: File,
    successor_header: [u8; SEGMENT_HEADER_BYTES],
}

/// Evidence that a buffered roll's successor manifest is selected durably.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedBufferedRoll {
    previous: CurrentReference,
    current: CurrentReference,
}

#[derive(Debug, Clone, Copy)]
struct RollBoundary {
    active: SegmentReference,
    valid_bytes: u64,
    digest: crate::Digest,
    next_group_number: u64,
    next_chain: ChainPosition,
    writer_generation: JournalGeneration,
}

#[derive(Debug, Clone, Copy)]
struct RollSegmentPreparation<'a> {
    root: &'a Path,
    header: &'a SegmentHeader,
    generation: JournalGeneration,
    first_group_number: u64,
    first_chain: ChainPosition,
    decode_limits: DecodeLimits,
    operation_limits: OperationLimits,
    pub(crate) data_sync: bool,
    pub(crate) direct: bool,
}

#[derive(Debug, Clone, Copy)]
struct OperationEnvelope<'a> {
    kind: crate::OperationKind,
    body: &'a [u8],
    op_number: u64,
    configuration_epoch: u64,
    original_view: u64,
}

#[derive(Debug)]
struct ReclaimCandidate {
    segment_id: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistencePhase {
    PredecessorSynced,
    SegmentCreated,
    SegmentAllocated,
    SegmentHeaderSynced,
    SegmentsDirectorySynced,
    ManifestTemporarySynced,
    ManifestLinked,
    ManifestDirectorySynced,
    CurrentTemporarySynced,
    CurrentRenamed,
    CurrentDirectorySynced,
    EvidenceCopiesSynced,
}

pub(crate) trait PersistenceObserver {
    fn completed(&mut self, phase: PersistencePhase) -> io::Result<()>;
}

#[derive(Debug, Default)]
pub(crate) struct NoopObserver;

impl PersistenceObserver for NoopObserver {
    fn completed(&mut self, _phase: PersistencePhase) -> io::Result<()> {
        Ok(())
    }
}

impl BufferedRollPublication {
    /// Synchronize the frozen predecessor and the successor header, then select
    /// the already-prepared manifest. This performs blocking storage barriers.
    pub fn publish(self) -> Result<PublishedBufferedRoll, DirectoryError> {
        self.publish_observing(&mut NoopObserver)
    }

    fn publish_observing(
        self,
        observer: &mut impl PersistenceObserver,
    ) -> Result<PublishedBufferedRoll, DirectoryError> {
        let Self {
            root,
            lock,
            expected_current,
            next_manifest,
            manifest_bytes,
            manifest_digest,
            predecessor,
            successor,
            successor_header,
        } = self;
        // Retain the group lock for the entire detached publication.
        let _lock = lock;
        predecessor.sync_data()?;
        observer.completed(PersistencePhase::PredecessorSynced)?;
        // Preparation synchronized the allocation. A whole-file barrier here
        // would also wait for every append already made to the successor.
        write_synchronized(&successor, 0, &successor_header)?;
        observer.completed(PersistencePhase::SegmentHeaderSynced)?;
        sync_directory(&root.join("segments"))?;
        observer.completed(PersistencePhase::SegmentsDirectorySynced)?;

        require_exact_contents(&root.join(CURRENT_FILE), &encode_current(expected_current)?)?;
        validate_segment_headers(&root, &next_manifest)?;
        install_immutable(
            &root,
            &manifest_name(next_manifest.generation),
            &manifest_bytes,
            observer,
        )?;
        let current = CurrentReference {
            group_id: next_manifest.identity.group_id,
            store_id: next_manifest.identity.store_id,
            generation: next_manifest.generation,
            manifest_digest,
        };
        replace_current(
            &root,
            next_manifest.generation,
            &encode_current(current)?,
            observer,
        )?;
        Ok(PublishedBufferedRoll {
            previous: expected_current,
            current,
        })
    }
}

/// One accepted operation borrowed from a bounded replay segment buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayedOperation<'a> {
    pub operation: &'a DecodedOperation<'a>,
    pub committed: bool,
}

impl GroupDirectory {
    /// Select active-segment I/O before recovery. The default is Linux `O_DSYNC`.
    /// Buffered owners must explicitly select `Buffered` on each reopen.
    /// Metadata publication and recovery retain their independent barriers.
    pub fn with_write_mode(mut self, mode: SegmentWriteMode) -> Result<Self, DirectoryError> {
        let enabled = mode == SegmentWriteMode::DataSync;
        if enabled && !cfg!(any(target_os = "linux", target_os = "android")) {
            return Err(
                io::Error::new(io::ErrorKind::Unsupported, "O_DSYNC requires Linux").into(),
            );
        }
        self.data_sync = enabled;
        Ok(self)
    }

    /// Write groups through a separate `O_DIRECT` descriptor for every active
    /// segment, bypassing the page cache. Requires Linux and a file system that
    /// accepts 4 KiB-aligned direct I/O; opening the journal fails otherwise.
    #[must_use]
    pub const fn with_direct_io(mut self, enabled: bool) -> Self {
        self.direct = enabled;
        self
    }

    /// Explicitly format a new externally committed group with immutable configuration.
    ///
    /// The adapter must first validate the complete configuration, including its
    /// group, epoch, membership, principals, and policy. The journal treats these
    /// bounded bytes as opaque. This performs blocking file/directory barriers;
    /// use a storage worker. A failed format never permits automatic replacement.
    pub fn format_new_with_configuration(
        root: impl AsRef<Path>,
        identity: GroupIdentity,
        configuration_epoch: u64,
        first_segment: &SegmentHeader,
        configuration: &[u8],
    ) -> Result<Self, DirectoryError> {
        validate_configuration_length(configuration.len())?;
        let mut directory = Self::format_new(root, identity, configuration_epoch, first_segment)?;
        write_new_synced(&directory.root.join(CONFIGURATION_FILE), configuration)?;
        sync_directory(&directory.root)?;
        directory.configuration = Some(configuration.into());
        Ok(directory)
    }

    /// Open with exact immutable configuration under the journal's exclusive lock.
    ///
    /// Missing, changed, oversized, or non-regular configuration fails closed.
    /// No configuration file is created or repaired. Successful open resynchronizes
    /// the matching file and directory before a caller can use it for voting.
    /// Configuration syntax/semantics and intact-voter admission remain adapter work.
    pub fn open_with_configuration(
        root: impl AsRef<Path>,
        expected_identity: GroupIdentity,
        limits: MetadataLimits,
        expected_configuration: &[u8],
    ) -> Result<Self, DirectoryError> {
        validate_configuration_length(expected_configuration.len())?;
        let directory = Self::open(root, expected_identity, limits)?;
        if directory.configuration() != Some(expected_configuration) {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        open_regular_file(&directory.root.join(CONFIGURATION_FILE), CONFIGURATION_FILE)?
            .sync_all()?;
        sync_directory(&directory.root)?;
        Ok(directory)
    }

    /// Optional exact immutable adapter bytes. Presence alone grants no voter authority.
    pub fn configuration(&self) -> Option<&[u8]> {
        self.configuration.as_deref()
    }

    /// Explicitly format one absent group directory below an existing volume root.
    ///
    /// This never creates parents. A failed format leaves evidence for explicit
    /// inspection or removal; a later call never treats it as a new store.
    pub fn format_new(
        root: impl AsRef<Path>,
        identity: GroupIdentity,
        configuration_epoch: u64,
        first_segment: &SegmentHeader,
    ) -> Result<Self, DirectoryError> {
        Self::format_new_with_commit_mode(
            root,
            identity,
            configuration_epoch,
            CommitMode::External,
            first_segment,
        )
    }

    /// Format with an explicit persisted recovery/commit authority.
    pub fn format_new_with_commit_mode(
        root: impl AsRef<Path>,
        identity: GroupIdentity,
        configuration_epoch: u64,
        commit_mode: CommitMode,
        first_segment: &SegmentHeader,
    ) -> Result<Self, DirectoryError> {
        identity.validate()?;
        validate_initial_segment(identity, first_segment)?;
        let root = root.as_ref();
        let parent = root.parent().ok_or(DirectoryError::MissingParent)?;
        require_directory(parent, "volume root")?;
        match fs::symlink_metadata(root) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => return Err(DirectoryError::StoreAlreadyExists),
        }

        fs::create_dir(root)?;
        let lock_path = root.join(LOCK_FILE);
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(lock_path)?;
        let lock = acquire_lock(lock)?;

        for name in DATA_DIRECTORIES {
            fs::create_dir(root.join(name))?;
        }
        let identity_bytes = encode_group_identity(identity)?;
        write_new_synced(&root.join(IDENTITY_FILE), &identity_bytes)?;

        let segment_path = root.join(segment_name(first_segment.segment_id()));
        write_new_segment_synced(&segment_path, first_segment)?;
        sync_directory(&root.join("segments"))?;

        let manifest = Manifest {
            generation: 1,
            parent_generation: 0,
            identity,
            configuration_epoch,
            commit_mode,
            durable_evidence: false,
            promised_view: 0,
            last_normal_view: 0,
            accepted: LogPosition::GENESIS,
            committed: LogPosition::GENESIS,
            checkpoint: None,
            segments: vec![SegmentReference {
                segment_id: first_segment.segment_id(),
                file_generation: 0,
                first_group_number: 1,
                first_chain: ChainPosition::GENESIS,
                capacity: first_segment.capacity(),
                sealed: None,
            }],
        };
        let limits = MetadataLimits::default();
        let manifest_bytes = encode_manifest_with_limits(&manifest, limits)?;
        let manifest_digest = manifest_digest(&manifest_bytes, limits)?;
        write_new_synced(
            &root.join(manifest_name(manifest.generation)),
            &manifest_bytes,
        )?;
        let current = CurrentReference {
            group_id: identity.group_id,
            store_id: identity.store_id,
            generation: manifest.generation,
            manifest_digest,
        };
        write_new_synced(&root.join(CURRENT_FILE), &encode_current(current)?)?;
        sync_directory(root)?;
        sync_directory(parent)?;

        Ok(Self {
            root: root.to_path_buf(),
            lock: Arc::new(lock),
            index_publication_lock: Arc::new(Mutex::new(())),
            identity,
            manifest,
            current,
            limits,
            configuration: None,
            data_sync: true,
            direct: false,
        })
    }

    /// Open exactly the generation selected by `CURRENT` and hold its lock.
    ///
    /// No missing path is created and no older generation is selected. Journal
    /// scanning remains a separate recovery step with an explicit crash policy.
    pub fn open(
        root: impl AsRef<Path>,
        expected_identity: GroupIdentity,
        limits: MetadataLimits,
    ) -> Result<Self, DirectoryError> {
        Self::open_metadata(root.as_ref(), expected_identity, limits, true)
    }

    fn open_metadata(
        root: &Path,
        expected_identity: GroupIdentity,
        limits: MetadataLimits,
        check_segments: bool,
    ) -> Result<Self, DirectoryError> {
        expected_identity.validate()?;
        require_directory(root, "group root")?;
        for name in DATA_DIRECTORIES {
            require_directory(&root.join(name), name)?;
        }
        let lock_path = root.join(LOCK_FILE);
        require_regular_file(&lock_path, LOCK_FILE)?;
        let lock = OpenOptions::new().read(true).write(true).open(lock_path)?;
        let lock = acquire_lock(lock)?;

        let identity_bytes = read_exact_file(
            &root.join(IDENTITY_FILE),
            GROUP_IDENTITY_BYTES,
            IDENTITY_FILE,
        )?;
        let identity = decode_group_identity(&identity_bytes)?;
        if identity != expected_identity {
            return Err(DirectoryError::IdentityMismatch);
        }

        let current_bytes = read_exact_file(&root.join(CURRENT_FILE), CURRENT_BYTES, CURRENT_FILE)?;
        let current = decode_current(&current_bytes)?;
        if current.group_id != identity.group_id || current.store_id != identity.store_id {
            return Err(DirectoryError::CurrentMismatch);
        }
        let manifest_path = root.join(manifest_name(current.generation));
        let manifest_bytes =
            read_limited_file(&manifest_path, limits.max_manifest_bytes, "manifest")?;
        if manifest_digest(&manifest_bytes, limits)? != current.manifest_digest {
            return Err(DirectoryError::CurrentMismatch);
        }
        let manifest = decode_manifest(&manifest_bytes, limits)?;
        if manifest.generation != current.generation || manifest.identity != identity {
            return Err(DirectoryError::CurrentMismatch);
        }
        if manifest.durable_evidence {
            evidence::protected(root, &manifest)?;
        }
        if check_segments {
            validate_segment_headers(root, &manifest)?;
        }
        validate_selected_checkpoint(root, &manifest, CheckpointLimits::default())?;
        let configuration = read_configuration(root)?.map(Vec::into_boxed_slice);

        Ok(Self {
            root: root.to_path_buf(),
            lock: Arc::new(lock),
            index_publication_lock: Arc::new(Mutex::new(())),
            identity,
            manifest,
            current,
            limits,
            configuration,
            data_sync: true,
            direct: false,
        })
    }

    /// Install one immutable successor manifest, then atomically replace `CURRENT`.
    fn install_manifest(
        &mut self,
        next: Manifest,
        observer: &mut impl PersistenceObserver,
    ) -> Result<(), DirectoryError> {
        if self.current.generation != self.manifest.generation {
            return Err(DirectoryError::RollPublicationPending);
        }
        let (next, manifest_bytes, digest) = self.prepare_manifest_install(next)?;
        validate_segment_headers(&self.root, &next)?;
        install_immutable(
            &self.root,
            &manifest_name(next.generation),
            &manifest_bytes,
            observer,
        )?;
        let current = CurrentReference {
            group_id: self.identity.group_id,
            store_id: self.identity.store_id,
            generation: next.generation,
            manifest_digest: digest,
        };
        replace_current(
            &self.root,
            next.generation,
            &encode_current(current)?,
            observer,
        )?;
        self.manifest = next;
        self.current = current;
        Ok(())
    }

    fn prepare_manifest_install(
        &self,
        next: Manifest,
    ) -> Result<(Manifest, Vec<u8>, crate::Digest), DirectoryError> {
        publication::prepare_manifest(
            &mut publication::Filesystem(&self.root),
            &self.manifest,
            next,
            self.limits,
        )
    }

    pub(crate) fn install_selected_manifest(
        &mut self,
        next: Manifest,
    ) -> Result<(), DirectoryError> {
        self.install_manifest(next, &mut NoopObserver)
    }

    /// Validate every retained segment, repair only active EOF tail, then open writer.
    ///
    /// Consuming `self` keeps its exclusive lock coupled to returned writer.
    pub fn recover(
        mut self,
        writer_generation: JournalGeneration,
        limits: DecodeLimits,
        operation_limits: OperationLimits,
    ) -> Result<OpenGroupJournal, DirectoryError> {
        // Repair admission can open authority metadata without trusting segment
        // headers. No path from that handle may bypass ordinary recovery checks.
        validate_segment_headers(&self.root, &self.manifest)?;
        cleanup_abandoned_group_staging(&self.root)?;
        evidence::restore(&self.root, &self.manifest)?;
        let references = self.manifest.segments.clone();
        let protected = [
            evidence::protected(&self.root, &self.manifest)?.following_chain()?,
            self.manifest.committed.following_chain()?,
        ];
        let mut protected_seen = [false; 2];

        for pair in references.windows(2) {
            let reference = pair[0];
            let next = pair[1];
            let sealed = reference
                .sealed
                .ok_or(DirectoryError::SegmentMismatch(reference.segment_id))?;
            let path = self.root.join(segment_reference_name(reference));
            let capacity = usize::try_from(reference.capacity)
                .map_err(|_| DirectoryError::SegmentMismatch(reference.segment_id))?;
            let image = read_limited_file(&path, capacity, "segment")?;
            let scan = scan_segment(
                &image,
                reference.first_group_number,
                reference.first_chain,
                limits,
            )?;
            validate_operation_bodies(
                &scan,
                operation_limits,
                self.manifest.configuration_epoch,
                self.manifest.promised_view,
            )?;
            if scan.valid_bytes != sealed.valid_bytes
                || scan.digest != sealed.digest
                || scan.next_chain != next.first_chain
                || matches!(scan.tail, TailState::Truncated { .. })
            {
                return Err(DirectoryError::SegmentMismatch(reference.segment_id));
            }
            for (seen, position) in protected_seen.iter_mut().zip(protected) {
                *seen |= scan_contains_position(&scan, reference.first_chain, position);
            }
        }

        let active = *references
            .last()
            .ok_or(DirectoryError::MissingActiveSegment)?;
        let path = self.root.join(segment_reference_name(active));
        require_regular_file(&path, "active segment")?;
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let allocation = file.try_clone()?;
        let protected =
            std::array::from_fn(|index| (!protected_seen[index]).then_some(protected[index]));
        let mut writer = SegmentWriter::recover_canonical(
            std::sync::Arc::new(file),
            writer_generation,
            active.first_group_number,
            active.first_chain,
            limits,
            active.capacity,
            CanonicalRecoveryRequirements {
                operation_limits,
                protected,
                configuration_epoch: Some(self.manifest.configuration_epoch),
                promised_view: Some(self.manifest.promised_view),
                discard_damaged_tail: true,
            },
        )?;
        // Restore allocation only after recovery validated the protected history.
        // An EOF-truncated crash tail may have shortened the file during repair.
        if allocation.metadata()?.len() != active.capacity {
            allocate_segment(&allocation, active.capacity)?;
            allocation.sync_data()?;
        }
        self.configure_recovered_writer(&mut writer, &path, &allocation)?;
        let recovered = position_before(writer.durable_position().next_chain())?;
        let (accepted, committed) = match self.manifest.commit_mode {
            CommitMode::LocalDurable => (recovered, recovered),
            CommitMode::External => (recovered, self.manifest.committed),
        };
        if self.manifest.accepted != accepted || self.manifest.committed != committed {
            let mut next = self.manifest.clone();
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(DirectoryError::ManifestGeneration)?;
            next.parent_generation = self.manifest.generation;
            next.accepted = accepted;
            next.committed = committed;
            self.install_manifest(next, &mut NoopObserver)?;
        }
        let evidence = evidence::Evidence::open(&self.root, &self.manifest)?;
        Ok(OpenGroupJournal {
            directory: self,
            writer,
            evidence,
            decode_limits: limits,
            operation_limits,
            pins: Arc::new(PinRegistry::default()),
        })
    }

    /// Apply the configured write mode and direct I/O to a recovered writer.
    fn configure_recovered_writer(
        &self,
        writer: &mut SegmentWriter<Arc<File>>,
        path: &Path,
        file: &File,
    ) -> Result<(), DirectoryError> {
        if self.data_sync {
            writer.set_write_mode(path, SegmentWriteMode::DataSync)?;
        }
        if self.direct {
            writer.set_direct(path, true)?;
            // Recovery read the segment through the page cache. Drop those
            // pages so direct writes need not invalidate them.
            #[cfg(target_os = "linux")]
            rustix::fs::fadvise(file, 0, None, rustix::fs::Advice::DontNeed)
                .map_err(io::Error::from)?;
        }
        let _ = file;
        Ok(())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub const fn identity(&self) -> GroupIdentity {
        self.identity
    }

    pub const fn current(&self) -> CurrentReference {
        self.current
    }

    pub const fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Path of the exact physical file selected for a logical segment.
    pub fn segment_path(&self, segment_id: u64) -> Result<PathBuf, DirectoryError> {
        let index = self
            .manifest
            .segments
            .binary_search_by_key(&segment_id, |r| r.segment_id)
            .map_err(|_| DirectoryError::SegmentMismatch(segment_id))?;
        let reference = &self.manifest.segments[index];
        Ok(self.root.join(segment_reference_name(reference)))
    }

    pub const fn metadata_limits(&self) -> MetadataLimits {
        self.limits
    }
}

impl OpenGroupJournal {
    /// Configure active and future segment writes on the blocking journal owner.
    /// Switching to `O_DSYNC` first covers any previously buffered bytes.
    pub fn set_write_mode(&mut self, mode: SegmentWriteMode) -> Result<(), DirectoryError> {
        self.require_roll_published()?;
        self.writer
            .validate_sync_position(self.writer.begin_sync())?;
        self.writer
            .set_write_mode(&self.active_segment_path(), mode)?;
        self.directory.data_sync = mode == SegmentWriteMode::DataSync;
        Ok(())
    }

    /// Reread exact selected authority files. Run on the storage worker.
    /// Work is bounded by the configured manifest/configuration limits.
    pub fn validate_authority_files(&self) -> Result<(), DirectoryError> {
        self.require_roll_published()?;
        let directory = &self.directory;
        require_exact_contents(
            &directory.root.join(IDENTITY_FILE),
            &encode_group_identity(directory.identity)?,
        )?;
        require_exact_contents(
            &directory.root.join(CURRENT_FILE),
            &encode_current(directory.current)?,
        )?;
        let manifest = read_limited_file(
            &directory
                .root
                .join(manifest_name(directory.current.generation)),
            directory.limits.max_manifest_bytes,
            "selected manifest",
        )?;
        if manifest_digest(&manifest, directory.limits)? != directory.current.manifest_digest {
            return Err(DirectoryError::CurrentMismatch);
        }
        if let Some(configuration) = directory.configuration() {
            require_exact_contents(&directory.root.join(CONFIGURATION_FILE), configuration)?;
        }
        evidence::protected(&directory.root, &directory.manifest)?;
        Ok(())
    }
    pub const fn directory(&self) -> &GroupDirectory {
        &self.directory
    }

    pub const fn writer(&self) -> &SegmentWriter<Arc<File>> {
        &self.writer
    }

    /// Zero and synchronize the active segment's unused remainder before any
    /// further write. See `SegmentWriter::zero_remainder`.
    pub fn zero_active_remainder(&mut self, zeros: &[u8]) -> Result<(), DirectoryError> {
        Ok(self.writer.zero_remainder(zeros)?)
    }

    pub(crate) fn active_segment_path(&self) -> PathBuf {
        let id = self.writer.header().segment_id();
        self.directory.root.join(
            self.directory
                .manifest
                .segments
                .last()
                .filter(|reference| reference.segment_id == id)
                .map_or_else(|| segment_name(id), segment_reference_name),
        )
    }

    /// Whether writes have switched to a successor not yet selected by `CURRENT`.
    pub const fn buffered_roll_pending(&self) -> bool {
        self.directory.current.generation != self.directory.manifest.generation
    }

    /// Canonical body limits fixed when this journal was recovered.
    pub const fn operation_limits(&self) -> OperationLimits {
        self.operation_limits
    }

    /// Physical and decoded byte limits fixed when this journal was recovered.
    pub const fn decode_limits(&self) -> DecodeLimits {
        self.decode_limits
    }

    /// Largest uncompressed canonical body that fits an empty segment and all
    /// decoder bounds. Includes entry, seal and physical alignment overhead.
    pub fn append_body_limit(&self) -> usize {
        let usable = self
            .writer
            .header()
            .capacity()
            .saturating_sub(crate::SEGMENT_HEADER_BYTES as u64);
        let aligned =
            usable / crate::WRITE_GROUP_ALIGNMENT as u64 * crate::WRITE_GROUP_ALIGNMENT as u64;
        let physical =
            aligned.saturating_sub((crate::ENTRY_HEADER_BYTES + crate::GROUP_SEAL_BYTES) as u64);
        usize::try_from(physical)
            .unwrap_or(usize::MAX)
            .min(self.operation_limits.max_body_bytes)
            .min(
                self.decode_limits
                    .max_entry_bytes
                    .saturating_sub(crate::ENTRY_HEADER_BYTES),
            )
            .min(self.decode_limits.max_decoded_body_bytes)
            .min(self.decode_limits.max_group_decoded_body_bytes)
            .min(self.decode_limits.max_segment_decoded_body_bytes)
    }

    /// Complete local write prefix, without durability or commit evidence.
    pub fn written_position(&self) -> Result<LogPosition, DirectoryError> {
        position_before(self.writer.written_position().next_chain())
    }

    /// Complete physical prefix admitted under the configured commit authority.
    pub fn accepted_position(&self) -> Result<LogPosition, DirectoryError> {
        if self.directory.manifest.commit_mode == CommitMode::LocalDurable {
            position_before(self.writer.written_position().next_chain())
        } else {
            position_before(self.writer.durable_position().next_chain())
        }
    }

    /// Prefix safe to expose under the configured commit authority.
    pub fn committed_position(&self) -> Result<LogPosition, DirectoryError> {
        self.require_roll_published()?;
        if self.directory.manifest.commit_mode == CommitMode::LocalDurable {
            position_before(self.writer.durable_position().next_chain())
        } else {
            Ok(self.directory.manifest.committed)
        }
    }

    pub(crate) fn any_artifact_pinned(&self) -> Result<bool, RetentionError> {
        self.pins.any_pinned()
    }

    /// Open and pin the exact checkpoint selected by the current manifest.
    pub fn pin_selected_checkpoint(
        &self,
        limits: CheckpointLimits,
    ) -> Result<Option<CheckpointPin>, DirectoryError> {
        let Some(reference) = self.directory.manifest.checkpoint else {
            return Ok(None);
        };
        let image = open_checkpoint(
            self.directory
                .root
                .join("checkpoints")
                .join(checkpoint_name(reference.checkpoint_id)),
            self.directory.identity.group_id,
            self.directory.identity.store_id,
            limits,
        )?;
        if image.manifest_digest() != reference.manifest_digest
            || image.manifest().position != reference.position
        {
            return Err(DirectoryError::CheckpointMismatch);
        }
        Ok(Some(self.pins.acquire_checkpoint(
            image,
            limits,
            Arc::clone(&self.directory.lock),
        )?))
    }

    /// Delete segment generations absent from the selected manifest.
    ///
    /// This is safe to retry after interrupted trim or suffix cleanup. Indexes
    /// are made non-durable before their source segment is unlinked. Live pins
    /// defer deletion without weakening the selected manifest.
    pub fn reclaim_unreferenced_segments(
        &self,
    ) -> Result<UnreferencedSegmentCleanup, DirectoryError> {
        let referenced = self
            .directory
            .manifest
            .segments
            .iter()
            .map(|reference| reference.segment_id)
            .collect::<HashSet<_>>();
        let selected_files = self
            .directory
            .manifest
            .segments
            .iter()
            .map(|reference| reference.file_name())
            .collect::<HashSet<_>>();
        let segment_directory = self.directory.root.join("segments");
        let mut candidates = Vec::new();
        for entry in fs::read_dir(&segment_directory)? {
            let entry = entry?;
            let Some(segment_id) = parse_segment_name(&entry.file_name()) else {
                continue;
            };
            if !selected_files.contains(entry.file_name().to_string_lossy().as_ref()) {
                candidates.push((segment_id, entry.path()));
            }
        }
        candidates.sort_unstable_by_key(|(segment_id, _)| *segment_id);

        let mut removed_segment_ids = Vec::new();
        let mut pinned_segment_ids = Vec::new();
        let mut reclaimed_bytes = 0_u64;
        for (segment_id, segment_path) in candidates {
            if self.pins.is_pinned(segment_id)? {
                pinned_segment_ids.push(segment_id);
                continue;
            }
            let bytes = if referenced.contains(&segment_id) {
                let bytes = remove_regular_file(&segment_path, "obsolete segment incarnation")?;
                sync_directory(&segment_directory)?;
                bytes
            } else {
                remove_unreferenced_segment(&self.directory.root, segment_id, &segment_path)?
            };
            reclaimed_bytes = reclaimed_bytes
                .checked_add(bytes)
                .ok_or(RetentionError::LengthOverflow)?;
            removed_segment_ids.push(segment_id);
        }
        removed_segment_ids.dedup();
        pinned_segment_ids.dedup();
        Ok(UnreferencedSegmentCleanup {
            removed_segment_ids,
            pinned_segment_ids,
            reclaimed_bytes,
        })
    }

    /// Delete exact checkpoint IDs absent from the selected manifest.
    ///
    /// Live snapshot/transfer pins defer deletion. Selected checkpoint bytes
    /// are never inferred from directory recency or contents.
    pub fn reclaim_unreferenced_checkpoints(
        &self,
    ) -> Result<UnreferencedCheckpointCleanup, DirectoryError> {
        let selected = self
            .directory
            .manifest
            .checkpoint
            .map(|reference| reference.checkpoint_id);
        let checkpoints = self.directory.root.join("checkpoints");
        let mut candidates = Vec::new();
        for entry in fs::read_dir(&checkpoints)? {
            let entry = entry?;
            let Some(checkpoint_id) = parse_checkpoint_name(&entry.file_name()) else {
                continue;
            };
            if Some(checkpoint_id) != selected {
                candidates.push((checkpoint_id, entry.path()));
            }
        }
        candidates.sort_unstable_by_key(|(checkpoint_id, _)| *checkpoint_id.as_bytes());

        let mut removed_checkpoint_ids = Vec::new();
        let mut pinned_checkpoint_ids = Vec::new();
        let mut reclaimed_bytes = 0_u64;
        for (checkpoint_id, path) in candidates {
            if self.pins.checkpoint_is_pinned(checkpoint_id)? {
                pinned_checkpoint_ids.push(checkpoint_id);
                continue;
            }
            reclaimed_bytes = reclaimed_bytes
                .checked_add(remove_unreferenced_checkpoint(&path)?)
                .ok_or(RetentionError::LengthOverflow)?;
            sync_directory(&checkpoints)?;
            removed_checkpoint_ids.push(checkpoint_id);
        }
        Ok(UnreferencedCheckpointCleanup {
            removed_checkpoint_ids,
            pinned_checkpoint_ids,
            reclaimed_bytes,
        })
    }

    /// Delete manifests outside current, selected-checkpoint, and live build sources.
    pub fn reclaim_unreferenced_metadata(
        &self,
        limits: CheckpointLimits,
    ) -> Result<UnreferencedMetadataCleanup, DirectoryError> {
        self.require_roll_published()?;
        self.validate_authority_files()?;
        let checkpoint_source = self.checkpoint_source_generation(limits)?;
        let current = self.directory.current.generation;
        let mut manifests = Vec::new();
        let mut temporaries = Vec::new();
        for entry in fs::read_dir(&self.directory.root)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(generation) = parse_manifest_name(&name) {
                if generation != current
                    && Some(generation) != checkpoint_source
                    && !self.pins.metadata_is_pinned(generation)?
                {
                    manifests.push((generation, entry.path()));
                }
            } else if parse_manifest_temporary_name(&name).is_some()
                || parse_current_temporary_name(&name).is_some()
            {
                temporaries.push((name.to_string_lossy().into_owned(), entry.path()));
            }
        }
        manifests.sort_unstable_by_key(|(generation, _)| *generation);
        temporaries.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        let mut removed_manifest_generations = Vec::new();
        let mut removed_temporary_files = Vec::new();
        let mut reclaimed_bytes = 0_u64;
        for (generation, path) in manifests {
            reclaimed_bytes = reclaimed_bytes
                .checked_add(remove_regular_file(&path, "unreferenced manifest")?)
                .ok_or(RetentionError::LengthOverflow)?;
            removed_manifest_generations.push(generation);
        }
        for (name, path) in temporaries {
            reclaimed_bytes = reclaimed_bytes
                .checked_add(remove_regular_file(
                    &path,
                    "unreferenced metadata temporary",
                )?)
                .ok_or(RetentionError::LengthOverflow)?;
            removed_temporary_files.push(name);
        }
        if !removed_manifest_generations.is_empty() || !removed_temporary_files.is_empty() {
            sync_directory(&self.directory.root)?;
        }
        Ok(UnreferencedMetadataCleanup {
            removed_manifest_generations,
            removed_temporary_files,
            reclaimed_bytes,
        })
    }

    /// Validate and append one complete physical group without claiming durability.
    pub fn append(
        &mut self,
        operations: &[crate::CanonicalOperation<'_>],
    ) -> Result<WriterPosition, DirectoryError> {
        self.append_with_body_encoding(operations, crate::BodyEncoding::Raw)
    }

    /// Validate and append with one independently decodable body policy.
    pub fn append_with_body_encoding(
        &mut self,
        operations: &[crate::CanonicalOperation<'_>],
        encoding: crate::BodyEncoding,
    ) -> Result<WriterPosition, DirectoryError> {
        for operation in operations {
            validate_operation(
                OperationEnvelope {
                    kind: operation.kind,
                    body: operation.body,
                    op_number: operation.op_number,
                    configuration_epoch: operation.configuration_epoch,
                    original_view: operation.original_view,
                },
                self.operation_limits,
                self.directory.manifest.configuration_epoch,
                self.directory.manifest.promised_view,
            )?;
        }
        self.require_segment_decoded_capacity(operations)?;
        Ok(self
            .writer
            .append_with_body_encoding(operations, encoding)?)
    }

    fn require_segment_decoded_capacity(
        &self,
        operations: &[crate::CanonicalOperation<'_>],
    ) -> Result<(), DirectoryError> {
        let additional = operations.iter().try_fold(0_usize, |total, operation| {
            total.checked_add(operation.body.len())
        });
        self.require_additional_decoded_capacity(additional)
    }

    pub(crate) fn require_additional_decoded_capacity(
        &self,
        additional: Option<usize>,
    ) -> Result<(), DirectoryError> {
        let actual = additional.and_then(|additional| {
            self.writer
                .written_position()
                .decoded_body_bytes()
                .checked_add(additional)
        });
        let Some(actual) = actual else {
            return Err(CodecError::LengthOverflow.into());
        };
        let limit = self.decode_limits.max_segment_decoded_body_bytes;
        if actual > limit {
            return Err(CodecError::SegmentDecodedBodyLimit { actual, limit }.into());
        }
        Ok(())
    }

    /// Freeze the exact complete prefix covered by a later synchronization.
    pub const fn begin_sync(&self) -> WriterPosition {
        self.writer.begin_sync()
    }

    /// Reserve the active writer's reusable physical-group buffer.
    pub fn reserve_encode_buffer(
        &mut self,
        capacity: usize,
        encoding: crate::BodyEncoding,
    ) -> Result<(), DirectoryError> {
        self.writer.reserve_encode_buffer(capacity, encoding)?;
        Ok(())
    }

    /// Synchronize through one position previously returned by this writer.
    pub fn sync_through(
        &mut self,
        position: WriterPosition,
    ) -> Result<WriterPosition, DirectoryError> {
        self.require_roll_published()?;
        Ok(self.writer.sync_through(position)?)
    }

    fn require_roll_published(&self) -> Result<(), DirectoryError> {
        if self.buffered_roll_pending() {
            Err(DirectoryError::RollPublicationPending)
        } else {
            Ok(())
        }
    }

    /// Replay the selected accepted lineage without materializing the full log.
    ///
    /// Each callback borrow lives only for the current segment buffer. Complete
    /// physical operations beyond the manifest's accepted position are omitted.
    pub fn replay_accepted<E>(
        &self,
        visit: impl FnMut(ReplayedOperation<'_>) -> Result<(), E>,
    ) -> Result<(), ReplayError<E>>
    where
        E: std::error::Error + 'static,
    {
        let replay_start = self
            .directory
            .manifest
            .checkpoint
            .map_or(LogPosition::GENESIS, |checkpoint| checkpoint.position);
        self.replay_from(replay_start, visit)
    }

    /// Replay all retained accepted operations, including checkpoint-covered data.
    ///
    /// Used to reconstruct retained payload/result indexes. Canonical state must
    /// still start from the selected checkpoint, not reapply this older prefix.
    pub fn replay_retained_accepted<E>(
        &self,
        visit: impl FnMut(ReplayedOperation<'_>) -> Result<(), E>,
    ) -> Result<(), ReplayError<E>>
    where
        E: std::error::Error + 'static,
    {
        let first = self
            .directory
            .manifest
            .segments
            .first()
            .expect("validated manifest retains an active segment");
        let replay_start = position_before(first.first_chain).map_err(ReplayError::Journal)?;
        self.replay_from(replay_start, visit)
    }

    fn replay_from<E>(
        &self,
        replay_start: LogPosition,
        mut visit: impl FnMut(ReplayedOperation<'_>) -> Result<(), E>,
    ) -> Result<(), ReplayError<E>>
    where
        E: std::error::Error + 'static,
    {
        let accepted = self.accepted_position().map_err(ReplayError::Journal)?;
        let committed = self.committed_position().map_err(ReplayError::Journal)?;
        let mut replayed = replay_start;
        for reference in &self.directory.manifest.segments {
            let capacity = usize::try_from(reference.capacity).map_err(|_| {
                ReplayError::Journal(DirectoryError::SegmentMismatch(reference.segment_id))
            })?;
            let image = read_limited_file(
                &self.directory.root.join(segment_reference_name(reference)),
                capacity,
                "segment",
            )
            .map_err(ReplayError::Journal)?;
            let scan = scan_segment(
                &image,
                reference.first_group_number,
                reference.first_chain,
                self.decode_limits,
            )
            .map_err(DirectoryError::from)
            .map_err(ReplayError::Journal)?;
            validate_operation_bodies(
                &scan,
                self.operation_limits,
                self.directory.manifest.configuration_epoch,
                self.directory.manifest.promised_view,
            )
            .map_err(ReplayError::Journal)?;
            validate_replay_scan(reference, &scan, &self.writer).map_err(ReplayError::Journal)?;

            for operation in scan.groups.iter().flat_map(|group| &group.operations) {
                if operation.op_number <= replay_start.op_number {
                    continue;
                }
                if operation.op_number > accepted.op_number {
                    break;
                }
                visit(ReplayedOperation {
                    operation,
                    committed: operation.op_number <= committed.op_number,
                })
                .map_err(ReplayError::Visitor)?;
                replayed = LogPosition {
                    op_number: operation.op_number,
                    digest: operation.digest,
                };
            }
        }
        if replayed != accepted {
            return Err(ReplayError::Journal(DirectoryError::PositionMismatch(
                accepted.op_number,
            )));
        }
        Ok(())
    }

    /// Freeze an exact committed checkpoint build target for background work.
    pub fn checkpoint_plan(
        &self,
        checkpoint_id: CheckpointId,
        state_schema_digest: crate::Digest,
        chunk_bytes: usize,
    ) -> Result<CheckpointPlan, DirectoryError> {
        let position = self.committed_position()?;
        if position == LogPosition::GENESIS {
            return Err(DirectoryError::CheckpointAtGenesis);
        }
        if self.directory.manifest.committed != position {
            return Err(DirectoryError::LocalProgressUnpublished);
        }
        let lease = self.pins.acquire_checkpoint_lease(
            checkpoint_id,
            self.directory.manifest.generation,
            Arc::clone(&self.directory.lock),
        )?;
        Ok(CheckpointPlan::new(
            self.directory.root.join("checkpoints"),
            self.directory.root.join("staging"),
            CheckpointSpec {
                group_id: self.directory.identity.group_id,
                store_id: self.directory.identity.store_id,
                checkpoint_id,
                position,
                configuration_epoch: self.directory.manifest.configuration_epoch,
                source_manifest_generation: self.directory.manifest.generation,
                source_manifest_digest: self.directory.current.manifest_digest,
                state_schema_digest,
                chunk_bytes,
            },
            lease,
        ))
    }

    /// Publish the synchronized accepted/committed prefix for maintenance use.
    ///
    /// This also freezes an exact relocation/backup source generation. Disk
    /// replica replies use `publish_durable_progress` after their data barrier.
    pub fn publish_progress(mut self) -> Result<Self, DirectoryError> {
        self.publish_manifest_progress(&mut NoopObserver)?;
        Ok(self)
    }

    /// Persist the exact synchronized prefix before using it as voting evidence.
    ///
    /// Later written bytes are excluded. Any ambiguous publication error fences
    /// the writer; reopen is required even if some metadata reached the device.
    pub fn publish_durable_progress(&mut self) -> Result<(), DirectoryError> {
        self.publish_durable_progress_observing(&mut NoopObserver)
    }

    fn publish_durable_progress_observing(
        &mut self,
        observer: &mut impl PersistenceObserver,
    ) -> Result<(), DirectoryError> {
        let result = if self.directory.manifest.durable_evidence {
            self.publish_fixed_evidence(observer)
        } else {
            self.publish_manifest_progress(observer)
        };
        if result.is_err() {
            self.writer.fence();
        }
        result
    }

    fn publish_manifest_progress(
        &mut self,
        observer: &mut impl PersistenceObserver,
    ) -> Result<(), DirectoryError> {
        if self.writer.is_faulted() {
            return Err(WriterError::Faulted.into());
        }
        let accepted = self.accepted_position()?;
        let committed = self.committed_position()?;
        if self.directory.manifest.accepted == accepted
            && self.directory.manifest.committed == committed
        {
            return Ok(());
        }
        let mut next = self.directory.manifest.clone();
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(DirectoryError::ManifestGeneration)?;
        next.parent_generation = self.directory.manifest.generation;
        next.accepted = accepted;
        next.committed = committed;
        validate_progress_successor(&self.directory, &self.writer, &next)?;
        self.directory.install_manifest(next, observer)?;
        Ok(())
    }

    /// Select one already durable checkpoint through a successor group manifest.
    ///
    /// The consuming receiver fences ambiguous manifest publication outcomes.
    pub fn install_checkpoint(
        mut self,
        checkpoint_id: CheckpointId,
        limits: CheckpointLimits,
    ) -> Result<Self, DirectoryError> {
        let checkpoint = open_checkpoint(
            self.directory
                .root
                .join("checkpoints")
                .join(checkpoint_name(checkpoint_id)),
            self.directory.identity.group_id,
            self.directory.identity.store_id,
            limits,
        )?;
        if checkpoint.manifest().checkpoint_id != checkpoint_id {
            return Err(DirectoryError::CheckpointMismatch);
        }
        validate_checkpoint_install(
            &self.directory,
            &self.writer,
            &checkpoint,
            self.decode_limits,
            self.operation_limits,
        )?;
        let mut next = self.directory.manifest.clone();
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(DirectoryError::ManifestGeneration)?;
        next.parent_generation = self.directory.manifest.generation;
        next.checkpoint = Some(CheckpointReference {
            checkpoint_id,
            position: checkpoint.manifest().position,
            manifest_digest: checkpoint.manifest_digest(),
        });
        self.directory.install_manifest(next, &mut NoopObserver)?;
        Ok(self)
    }

    /// Remove the longest sealed prefix covered by checkpoint and trim floors.
    ///
    /// `floors` must come from the committed state encoded by the selected
    /// checkpoint. The manifest drops references before files are unlinked.
    /// Consuming ownership fences every ambiguous publication/deletion result.
    pub fn trim_sealed_prefix(
        mut self,
        floors: &RetentionFloors,
    ) -> Result<(Self, RetentionResult), DirectoryError> {
        let checkpoint = self
            .directory
            .manifest
            .checkpoint
            .ok_or(DirectoryError::RetentionRequiresCheckpoint)?;
        validate_selected_checkpoint(
            &self.directory.root,
            &self.directory.manifest,
            CheckpointLimits::default(),
        )?;
        let (candidates, _, _) = plan_retention(
            &self,
            floors,
            checkpoint.position,
            RetentionScanBudget {
                max_segments: usize::MAX,
                max_read_bytes: usize::MAX,
                max_work: std::time::Duration::MAX,
            },
        )?;
        let unreferenced = candidates
            .iter()
            .map(|candidate| candidate.segment_id)
            .collect::<Vec<_>>();

        if unreferenced.is_empty() {
            return Ok((
                self,
                RetentionResult {
                    unreferenced_segment_ids: unreferenced,
                    removed_segment_ids: Vec::new(),
                    reclaimed_bytes: 0,
                    blocked_by_pin: None,
                },
            ));
        }
        let mut next = self.directory.manifest.clone();
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(DirectoryError::ManifestGeneration)?;
        next.parent_generation = self.directory.manifest.generation;
        let retired = next
            .segments
            .drain(..unreferenced.len())
            .collect::<Vec<_>>();
        self.directory.install_manifest(next, &mut NoopObserver)?;

        let mut removed = Vec::new();
        let mut reclaimed_bytes = 0_u64;
        let mut blocked_by_pin = None;
        for (candidate, reference) in candidates.into_iter().zip(retired) {
            if self.pins.is_pinned(candidate.segment_id)? {
                blocked_by_pin.get_or_insert(candidate.segment_id);
                continue;
            }
            remove_unreferenced_segment(
                &self.directory.root,
                candidate.segment_id,
                &self.directory.root.join(segment_reference_name(reference)),
            )?;
            reclaimed_bytes = reclaimed_bytes
                .checked_add(candidate.bytes)
                .ok_or(RetentionError::LengthOverflow)?;
            removed.push(candidate.segment_id);
        }
        Ok((
            self,
            RetentionResult {
                unreferenced_segment_ids: unreferenced,
                removed_segment_ids: removed,
                reclaimed_bytes,
                blocked_by_pin,
            },
        ))
    }

    /// Drop a bounded eligible prefix from the manifest, leaving physical deletion
    /// to `cleanup_orphan_step`. Floors must describe the selected checkpoint's
    /// committed state. The segment scan has byte/count and cooperative time bounds;
    /// checkpoint verification remains a full pass bounded by `checkpoint_limits`.
    pub fn retire_sealed_prefix(
        mut self,
        floors: &RetentionFloors,
        budget: RetentionScanBudget,
        checkpoint_limits: CheckpointLimits,
    ) -> Result<(Self, RetiredPrefix), DirectoryError> {
        self.require_roll_published()?;
        self.validate_authority_files()?;
        let checkpoint = self
            .directory
            .manifest
            .checkpoint
            .ok_or(DirectoryError::RetentionRequiresCheckpoint)?;
        validate_selected_checkpoint(
            &self.directory.root,
            &self.directory.manifest,
            checkpoint_limits,
        )?;
        let (candidates, scanned_segments, scanned_bytes) =
            plan_retention(&self, floors, checkpoint.position, budget)?;
        let unreferenced_segment_ids = candidates
            .iter()
            .map(|candidate| candidate.segment_id)
            .collect::<Vec<_>>();
        if !candidates.is_empty() {
            let mut next = self.directory.manifest.clone();
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(DirectoryError::ManifestGeneration)?;
            next.parent_generation = self.directory.manifest.generation;
            next.segments.drain(..candidates.len());
            self.directory.install_manifest(next, &mut NoopObserver)?;
        }
        Ok((
            self,
            RetiredPrefix {
                unreferenced_segment_ids,
                scanned_segments,
                scanned_bytes,
            },
        ))
    }

    /// Build and publish one disposable index for a manifest-sealed segment.
    ///
    /// This performs blocking scan/sort/file I/O and belongs on a storage thread.
    pub fn build_sealed_index(
        &self,
        segment_id: u64,
        limits: IndexBuildLimits,
    ) -> Result<SegmentIndex, DirectoryError> {
        let _guard = self
            .directory
            .index_publication_lock
            .lock()
            .map_err(|_| io::Error::other("index publication poisoned"))?;
        let mut publication = self.index_publication();
        let index = self.build_sealed_index_in(segment_id, limits, &mut publication)?;
        publication.finish(|path| Ok(File::open(path)?.sync_all()?))?;
        Ok(index)
    }

    fn index_publication(&self) -> IndexPublication {
        IndexPublication::new(
            self.directory.root.join("indexes"),
            self.directory.root.join("staging"),
        )
    }

    fn build_sealed_index_in(
        &self,
        segment_id: u64,
        limits: IndexBuildLimits,
        publication: &mut IndexPublication,
    ) -> Result<SegmentIndex, DirectoryError> {
        let (reference, source) = self.sealed_index_source(segment_id)?;
        let capacity = usize::try_from(reference.capacity)
            .map_err(|_| DirectoryError::SegmentMismatch(segment_id))?;
        let image = read_limited_file(
            &self.directory.root.join(segment_reference_name(reference)),
            capacity,
            "sealed segment",
        )?;
        let scan = scan_segment(
            &image,
            reference.first_group_number,
            reference.first_chain,
            self.decode_limits,
        )
        .map_err(DirectoryError::from)?;
        validate_operation_bodies(
            &scan,
            self.operation_limits,
            self.directory.manifest.configuration_epoch,
            self.directory.manifest.promised_view,
        )?;
        validate_replay_scan(reference, &scan, &self.writer)?;
        Ok(publication.build(&scan, source, self.operation_limits, limits)?)
    }

    /// Open one previously published sealed-segment index.
    pub fn open_sealed_index(
        &self,
        segment_id: u64,
        limits: crate::IndexLimits,
    ) -> Result<SegmentIndex, DirectoryError> {
        let (_, source) = self.sealed_index_source(segment_id)?;
        Ok(open_segment_index(
            self.directory
                .root
                .join("indexes")
                .join(segment_index_name(source)),
            source,
            limits,
        )?)
    }

    /// Validate/reuse an index, or remove and rebuild only a corrupt derived file.
    ///
    /// Source segment remains authoritative. Serialize with detached readers
    /// through the directory's shared index-publication lock.
    pub fn repair_sealed_index(
        &self,
        segment_id: u64,
        limits: IndexBuildLimits,
    ) -> Result<SegmentIndex, DirectoryError> {
        let _guard = self
            .directory
            .index_publication_lock
            .lock()
            .map_err(|_| io::Error::other("index publication poisoned"))?;
        let mut publication = self.index_publication();
        let index = self.repair_sealed_index_in(segment_id, limits, &mut publication)?;
        publication.finish(|path| Ok(File::open(path)?.sync_all()?))?;
        Ok(index)
    }

    fn repair_sealed_index_in(
        &self,
        segment_id: u64,
        limits: IndexBuildLimits,
        publication: &mut IndexPublication,
    ) -> Result<SegmentIndex, DirectoryError> {
        let (_, source) = self.sealed_index_source(segment_id)?;
        if let Some(index) = publication.reuse(source, limits.file)? {
            return Ok(index);
        }
        self.build_sealed_index_in(segment_id, limits, publication)
    }

    /// Validate all published sealed indexes without retaining record-sized state.
    pub fn open_sealed_index_catalog(
        &self,
        limits: crate::IndexLimits,
    ) -> Result<SegmentIndexCatalog, DirectoryError> {
        Ok(SegmentIndexCatalog::open(
            self.directory.root.join("indexes"),
            self.sealed_index_sources()?,
            limits,
        )?)
    }

    /// Build/reuse every manifest-sealed index, then open their exact catalog.
    ///
    /// Each new file is synchronized before installation. Directory barriers
    /// are shared across the catalog and must succeed before it is returned.
    pub fn build_sealed_indexes(
        &self,
        limits: IndexBuildLimits,
    ) -> Result<SegmentIndexCatalog, DirectoryError> {
        self.build_sealed_indexes_syncing(limits, |path| Ok(File::open(path)?.sync_all()?))
    }

    fn build_sealed_indexes_syncing(
        &self,
        limits: IndexBuildLimits,
        synchronize: impl FnMut(&Path) -> Result<(), IndexBuildError>,
    ) -> Result<SegmentIndexCatalog, DirectoryError> {
        let _guard = self
            .directory
            .index_publication_lock
            .lock()
            .map_err(|_| io::Error::other("index publication poisoned"))?;
        let sources = self.sealed_index_sources()?;
        let mut hot = None;
        let mut publication = self.index_publication();
        for source in &sources {
            let index = self.repair_sealed_index_in(source.segment_id, limits, &mut publication)?;
            if hot.is_none() {
                hot = Some(index);
            }
        }
        publication.finish(synchronize)?;
        Ok(SegmentIndexCatalog::from_validated(
            self.directory.root.join("indexes"),
            sources,
            limits.file,
            hot,
        )?)
    }

    /// Open a pinned exact read snapshot using already-published sealed indexes.
    pub fn open_index_snapshot(
        &self,
        boundary: JournalIndexBoundary,
        limits: crate::IndexLimits,
    ) -> Result<JournalIndexSnapshot, JournalIndexError> {
        let sealed = self.open_sealed_index_catalog(limits)?;
        self.finish_index_snapshot(boundary, sealed, limits)
    }

    /// Build missing sealed indexes, then open one pinned exact read snapshot.
    ///
    /// Blocking maintenance belongs on a storage thread. Active entries remain
    /// in bounded memory and never add a producer-commit persistence barrier.
    pub fn build_index_snapshot(
        &self,
        boundary: JournalIndexBoundary,
        limits: IndexBuildLimits,
    ) -> Result<JournalIndexSnapshot, JournalIndexError> {
        let sealed = self.build_sealed_indexes(limits)?;
        self.finish_index_snapshot(boundary, sealed, limits.file)
    }

    /// Build committed and accepted recovery snapshots from one active scan.
    pub(crate) fn build_recovery_index_snapshots(
        &self,
        limits: IndexBuildLimits,
    ) -> Result<(JournalIndexSnapshot, JournalIndexSnapshot), JournalIndexError> {
        let sealed = self.build_sealed_indexes(limits)?;
        let committed = self.committed_position()?;
        let accepted = self.accepted_position()?;
        let (committed_active, accepted_active) = self.with_validated_active_scan(|scan| {
            let committed_active = ActiveSegmentIndex::build(
                scan,
                committed.op_number,
                self.operation_limits,
                limits.file,
            )?;
            let accepted_active = if accepted == committed {
                committed_active.clone()
            } else {
                ActiveSegmentIndex::build(
                    scan,
                    accepted.op_number,
                    self.operation_limits,
                    limits.file,
                )?
            };
            Ok((committed_active, accepted_active))
        })?;
        let committed_snapshot =
            self.index_snapshot_from_parts(sealed.clone(), committed_active, committed)?;
        let accepted_snapshot =
            self.index_snapshot_from_parts(sealed, accepted_active, accepted)?;
        Ok((committed_snapshot, accepted_snapshot))
    }

    /// Build a snapshot from an already validated sealed-index catalog.
    ///
    /// The caller must bound reads before the active segment's first offset.
    pub(crate) fn finish_index_snapshot(
        &self,
        boundary: JournalIndexBoundary,
        sealed: SegmentIndexCatalog,
        limits: crate::IndexLimits,
    ) -> Result<JournalIndexSnapshot, JournalIndexError> {
        self.validate_sealed_catalog(&sealed)?;
        let through = match boundary {
            JournalIndexBoundary::Written => self.written_position()?,
            JournalIndexBoundary::Accepted => self.accepted_position()?,
            JournalIndexBoundary::Committed => self.committed_position()?,
        };
        let active = self.with_validated_active_scan(|scan| {
            Ok(ActiveSegmentIndex::build(
                scan,
                through.op_number,
                self.operation_limits,
                limits,
            )?)
        })?;
        self.index_snapshot_from_parts(sealed, active, through)
    }

    fn with_validated_active_scan<T>(
        &self,
        inspect: impl FnOnce(&crate::SegmentScan<'_>) -> Result<T, JournalIndexError>,
    ) -> Result<T, JournalIndexError> {
        let reference = self
            .directory
            .manifest
            .segments
            .last()
            .expect("validated manifest has one active segment");
        let capacity = usize::try_from(reference.capacity)
            .map_err(|_| DirectoryError::SegmentMismatch(reference.segment_id))?;
        let written_bytes = usize::try_from(self.writer.written_position().end_offset())
            .map_err(|_| DirectoryError::SegmentMismatch(reference.segment_id))?;
        let (image, file_bytes) = read_prefix_file(
            &self.directory.root.join(segment_reference_name(reference)),
            written_bytes,
            "active segment",
        )?;
        if file_bytes > reference.capacity || written_bytes > capacity {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id).into());
        }
        let scan = scan_segment(
            &image,
            reference.first_group_number,
            reference.first_chain,
            self.decode_limits,
        )
        .map_err(DirectoryError::from)?;
        validate_operation_bodies(
            &scan,
            self.operation_limits,
            self.directory.manifest.configuration_epoch,
            self.directory.manifest.promised_view,
        )?;
        validate_replay_scan(reference, &scan, &self.writer)?;
        inspect(&scan)
    }

    fn index_snapshot_from_parts(
        &self,
        sealed: SegmentIndexCatalog,
        active: Option<ActiveSegmentIndex>,
        through: LogPosition,
    ) -> Result<JournalIndexSnapshot, JournalIndexError> {
        let pin = self.pin_all_segments()?;
        Ok(JournalIndexSnapshot::new(
            pin,
            self.directory.identity,
            sealed,
            active,
            through,
            self.decode_limits,
            self.operation_limits,
        ))
    }

    fn validate_sealed_catalog(
        &self,
        catalog: &SegmentIndexCatalog,
    ) -> Result<(), JournalIndexError> {
        if catalog.sources() != self.sealed_index_sources()? {
            return Err(JournalIndexError::StaleCatalog);
        }
        Ok(())
    }

    /// Install hard-state metadata without changing active segment ownership.
    ///
    /// This consumes the writable handle. If publication has an ambiguous I/O
    /// failure, no caller can continue writing through the old manifest; reopen
    /// resolves `CURRENT` before admission resumes.
    pub fn install_metadata(mut self, next: Manifest) -> Result<Self, DirectoryError> {
        if next.segments != self.directory.manifest.segments {
            return Err(DirectoryError::ActiveSegmentChangeRequiresRoll);
        }
        validate_metadata_successor(
            &self.directory,
            &self.writer,
            &next,
            self.decode_limits,
            self.operation_limits,
        )?;
        self.directory.install_manifest(next, &mut NoopObserver)?;
        Ok(self)
    }

    /// Seal the durable active segment and atomically publish a new empty one.
    ///
    /// Hard state is copied unchanged. The caller must synchronize every written
    /// group first. Consuming the handle fences all uncertain publication errors.
    pub fn roll_active(self, new_capacity: u64) -> Result<Self, DirectoryError> {
        self.roll_active_observing(new_capacity, &mut NoopObserver)
    }

    /// Roll a durable active segment with an explicit occupied-name probe bound.
    /// Exhaustion preserves the selected predecessor and requires reopen/cleanup;
    /// no conflicting file is overwritten. Encoding workspace moves to the successor.
    pub fn roll_active_bounded(
        self,
        new_capacity: u64,
        max_orphan_probes: usize,
    ) -> Result<Self, DirectoryError> {
        self.roll_active_observing_bounded(new_capacity, max_orphan_probes, &mut NoopObserver)
    }

    fn roll_active_observing(
        self,
        new_capacity: u64,
        observer: &mut impl PersistenceObserver,
    ) -> Result<Self, DirectoryError> {
        self.roll_active_observing_bounded(new_capacity, usize::MAX, observer)
    }

    fn roll_active_observing_bounded(
        self,
        new_capacity: u64,
        max_orphan_probes: usize,
        observer: &mut impl PersistenceObserver,
    ) -> Result<Self, DirectoryError> {
        let Self {
            mut directory,
            mut writer,
            evidence,
            decode_limits,
            operation_limits,
            pins,
        } = self;
        let boundary = validate_roll_boundary(&directory, &writer)?;

        let mut next_segment_id = boundary
            .active
            .segment_id
            .checked_add(1)
            .ok_or(DirectoryError::SegmentIdExhausted)?;
        let mut probes = 0;
        let (next_manifest, mut next_writer) = loop {
            if probes == max_orphan_probes {
                return Err(DirectoryError::RollProbeLimit {
                    limit: max_orphan_probes,
                });
            }
            probes += 1;
            let next_header = SegmentHeader::new(
                directory.identity.group_id,
                next_segment_id,
                Some(boundary.active.segment_id),
                boundary.next_chain.previous_digest(),
                new_capacity,
            )?;
            let next_manifest = roll_manifest(&directory, boundary, next_segment_id, new_capacity)?;
            // Reject deterministic metadata/limit failures before creating an orphan.
            encode_manifest_with_limits(&next_manifest, directory.limits)?;

            let result = prepare_roll_segment(
                RollSegmentPreparation {
                    root: &directory.root,
                    header: &next_header,
                    generation: boundary.writer_generation,
                    first_group_number: boundary.next_group_number,
                    first_chain: boundary.next_chain,
                    decode_limits,
                    operation_limits,
                    data_sync: directory.data_sync,
                    direct: directory.direct,
                },
                observer,
            );
            match result {
                Ok(next_writer) => break (next_manifest, next_writer),
                Err(DirectoryError::OrphanSegmentConflict(_)) => {
                    next_segment_id = next_segment_id
                        .checked_add(1)
                        .ok_or(DirectoryError::SegmentIdExhausted)?;
                }
                Err(error) => return Err(error),
            }
        };
        writer.transfer_encode_buffers_to(&mut next_writer);
        directory.install_manifest(next_manifest, observer)?;

        Ok(Self {
            directory,
            writer: next_writer,
            evidence,
            decode_limits,
            operation_limits,
            pins,
        })
    }
}

/// Physical-journal or application-state failure during bounded replay.
#[derive(Debug, Error)]
pub enum ReplayError<E: std::error::Error + 'static> {
    #[error(transparent)]
    Journal(DirectoryError),
    #[error("operation replay was rejected by the state builder")]
    Visitor(#[source] E),
}

/// Group-directory lifecycle, identity, or persistence failure.
#[derive(Debug, Error)]
pub enum DirectoryError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    Operation(#[from] OperationCodecError),
    #[error(transparent)]
    Writer(#[from] WriterError),
    #[error(transparent)]
    Index(#[from] IndexBuildError),
    #[error(transparent)]
    IndexCatalog(#[from] IndexCatalogError),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error(transparent)]
    Retention(#[from] RetentionError),
    #[error("group directory parent is missing")]
    MissingParent,
    #[error("group directory already exists")]
    StoreAlreadyExists,
    #[error("group directory is already owned by another process")]
    Locked,
    #[error("{0} is not a regular file")]
    NotRegularFile(&'static str),
    #[error("{0} is not a directory")]
    NotDirectory(&'static str),
    #[error("configured group identity does not match stored identity")]
    IdentityMismatch,
    /// Expected immutable adapter configuration is missing or differs from disk.
    #[error("configured consensus membership does not match stored configuration")]
    ConfigurationMismatch,
    /// A prior memory-voting session did not publish exact drained history.
    #[error("memory-voting history is unproven; nonvoting group recovery is required")]
    MemoryHistoryUnproven,
    /// Immutable configuration must be nonempty and fit its fixed storage bound.
    #[error("invalid group configuration length")]
    ConfigurationLength,
    #[error("CURRENT does not identify stored group manifest")]
    CurrentMismatch,
    #[error("initial segment does not match group genesis")]
    InvalidInitialSegment,
    #[error("physical segment allocation is unsupported on this platform")]
    AllocationUnsupported,
    #[error("manifest successor generation or identity is invalid")]
    ManifestGeneration,
    #[error("manifest references invalid or missing segment {0}")]
    SegmentMismatch(u64),
    #[error("manifest contains no active segment")]
    MissingActiveSegment,
    #[error("active segment changes require an atomic writer roll")]
    ActiveSegmentChangeRequiresRoll,
    #[error("checkpoint changes require checkpoint installation")]
    CheckpointChangeRequiresInstall,
    #[error("cannot checkpoint the empty journal")]
    CheckpointAtGenesis,
    #[error("local durable progress must be published before checkpoint capture")]
    LocalProgressUnpublished,
    #[error("checkpoint does not match its source manifest or current retained journal")]
    CheckpointMismatch,
    #[error("checkpoint position must advance beyond the selected checkpoint")]
    CheckpointRegression,
    #[error("physical retention requires an installed checkpoint")]
    RetentionRequiresCheckpoint,
    #[error("retention scan budget is zero or cannot hold one eligible segment")]
    RetentionScanBudget,
    #[error("configuration changes require configuration installation")]
    ConfigurationChangeRequiresInstall,
    #[error("journal commit mode is immutable after format")]
    CommitModeChange,
    #[error("ordinary metadata installation cannot regress hard state")]
    HardStateRegression,
    #[error("operation position {0} is beyond the synchronized journal prefix")]
    PositionNotDurable(u64),
    #[error("operation position {0} does not match the retained journal")]
    PositionMismatch(u64),
    #[error("active segment contains writes not covered by a successful sync")]
    ActiveSegmentNotDurable,
    #[error("a buffered segment roll is still awaiting publication")]
    RollPublicationPending,
    #[error("buffered roll completion does not match the live journal")]
    RollPublicationMismatch,
    #[error("prepared successor does not match the live journal generation")]
    PreparedSegmentMismatch,
    #[error("an empty active segment cannot be rolled")]
    EmptyActiveSegment,
    #[error("physical segment ID space exhausted")]
    SegmentIdExhausted,
    #[error("orphan segment {0} conflicts with the requested roll")]
    OrphanSegmentConflict(u64),
    /// No successor name was available within the caller's roll work budget.
    #[error("segment roll exhausted {limit} occupied-name probes")]
    RollProbeLimit { limit: usize },
    #[error("segment {0} is active, absent, or otherwise not sealed")]
    SegmentNotSealed(u64),
    #[error("immutable metadata path already contains different bytes")]
    ImmutableConflict,
    #[error("{object} size {actual} does not equal {expected}")]
    WrongFileSize {
        object: &'static str,
        actual: u64,
        expected: u64,
    },
    #[error("{object} size {actual} exceeds limit {limit}")]
    FileLimit {
        object: &'static str,
        actual: u64,
        limit: usize,
    },
}

impl LogPosition {
    fn following_chain(self) -> Result<ChainPosition, MetadataError> {
        let next = self
            .op_number
            .checked_add(1)
            .ok_or(MetadataError::InvalidLogPosition)?;
        Ok(ChainPosition::new(next, self.digest))
    }
}

fn position_before(chain: ChainPosition) -> Result<LogPosition, DirectoryError> {
    let op_number = chain
        .next_op_number()
        .checked_sub(1)
        .ok_or(DirectoryError::PositionMismatch(0))?;
    let position = LogPosition {
        op_number,
        digest: chain.previous_digest(),
    };
    Ok(position.validate()?)
}

fn validate_initial_segment(
    identity: GroupIdentity,
    segment: &SegmentHeader,
) -> Result<(), DirectoryError> {
    if segment.group_id() != identity.group_id
        || segment.file_generation() != 0
        || segment.predecessor_segment_id().is_some()
        || segment.predecessor_digest() != crate::Digest::ZERO
    {
        return Err(DirectoryError::InvalidInitialSegment);
    }
    Ok(())
}

fn acquire_lock(file: File) -> Result<StoreLock, DirectoryError> {
    match StoreLock::acquire(file) {
        Ok(lock) => Ok(lock),
        Err(TryLockError::WouldBlock) => Err(DirectoryError::Locked),
        Err(TryLockError::Error(error)) => Err(error.into()),
    }
}

fn validate_segment_headers(root: &Path, manifest: &Manifest) -> Result<(), DirectoryError> {
    let mut previous: Option<&SegmentReference> = None;
    for reference in &manifest.segments {
        let path = root.join(segment_reference_name(*reference));
        let (bytes, length) = read_prefix_file(&path, crate::SEGMENT_HEADER_BYTES, "segment")?;
        let header = decode_segment_header(&bytes)?;
        validate_segment_header(manifest, previous, reference, &header, length)?;
        previous = Some(reference);
    }
    Ok(())
}

fn validate_segment_header(
    manifest: &Manifest,
    previous: Option<&SegmentReference>,
    reference: &SegmentReference,
    header: &SegmentHeader,
    length: u64,
) -> Result<(), DirectoryError> {
    if header.group_id() != manifest.identity.group_id
        || header.segment_id() != reference.segment_id
        || header.file_generation() != reference.file_generation
        || header.capacity() != reference.capacity
    {
        return Err(DirectoryError::SegmentMismatch(reference.segment_id));
    }
    if let Some(predecessor) = previous
        && (header.predecessor_segment_id() != Some(predecessor.segment_id)
            || header.predecessor_digest() != reference.first_chain.previous_digest())
    {
        return Err(DirectoryError::SegmentMismatch(reference.segment_id));
    }
    if length < crate::SEGMENT_HEADER_BYTES as u64 || length > reference.capacity {
        return Err(DirectoryError::SegmentMismatch(reference.segment_id));
    }
    if let Some(sealed) = reference.sealed
        && length < sealed.valid_bytes
    {
        return Err(DirectoryError::SegmentMismatch(reference.segment_id));
    }
    Ok(())
}

fn validate_selected_checkpoint(
    root: &Path,
    manifest: &Manifest,
    limits: CheckpointLimits,
) -> Result<(), DirectoryError> {
    let Some(reference) = manifest.checkpoint else {
        return Ok(());
    };
    let checkpoint = open_checkpoint(
        root.join("checkpoints")
            .join(checkpoint_name(reference.checkpoint_id)),
        manifest.identity.group_id,
        manifest.identity.store_id,
        limits,
    )?;
    if checkpoint.manifest().checkpoint_id != reference.checkpoint_id
        || checkpoint.manifest().position != reference.position
        || checkpoint.manifest_digest() != reference.manifest_digest
        || checkpoint.manifest().configuration_epoch != manifest.configuration_epoch
    {
        return Err(DirectoryError::CheckpointMismatch);
    }
    Ok(())
}

fn plan_retention(
    journal: &OpenGroupJournal,
    floors: &RetentionFloors,
    checkpoint: LogPosition,
    budget: RetentionScanBudget,
) -> Result<(Vec<ReclaimCandidate>, usize, usize), DirectoryError> {
    if budget.max_segments == 0 || budget.max_read_bytes == 0 || budget.max_work.is_zero() {
        return Err(DirectoryError::RetentionScanBudget);
    }
    let started = std::time::Instant::now();
    let mut scanned_segments = 0;
    let mut scanned_bytes = 0;
    let mut candidates = Vec::new();
    for pair in journal.directory.manifest.segments.windows(2) {
        if scanned_segments >= budget.max_segments
            || (scanned_segments > 0 && started.elapsed() >= budget.max_work)
        {
            break;
        }
        let reference = pair[0];
        let sealed = reference
            .sealed
            .ok_or(DirectoryError::SegmentMismatch(reference.segment_id))?;
        let last_op_number = pair[1]
            .first_chain
            .next_op_number()
            .checked_sub(1)
            .ok_or(DirectoryError::SegmentMismatch(reference.segment_id))?;
        if last_op_number > checkpoint.op_number {
            break;
        }
        let segment_path = journal
            .directory
            .root
            .join(segment_reference_name(reference));
        let capacity = usize::try_from(reference.capacity)
            .map_err(|_| DirectoryError::SegmentMismatch(reference.segment_id))?;
        if capacity > budget.max_read_bytes - scanned_bytes {
            if scanned_segments == 0 {
                return Err(DirectoryError::RetentionScanBudget);
            }
            break;
        }
        let image = read_limited_file(&segment_path, capacity, "retention segment")?;
        scanned_segments += 1;
        scanned_bytes += image.len();
        let scan = scan_segment(
            &image,
            reference.first_group_number,
            reference.first_chain,
            journal.decode_limits,
        )?;
        validate_operation_bodies(
            &scan,
            journal.operation_limits,
            journal.directory.manifest.configuration_epoch,
            journal.directory.manifest.promised_view,
        )?;
        validate_replay_scan(&reference, &scan, &journal.writer)?;
        if !scan_is_below_floors(
            &scan,
            checkpoint.op_number,
            floors,
            journal.operation_limits,
        )? {
            break;
        }
        let source = IndexSource {
            group_id: journal.directory.identity.group_id,
            segment_id: reference.segment_id,
            valid_bytes: sealed.valid_bytes,
            segment_digest: sealed.digest,
            first_op_number: reference.first_chain.next_op_number(),
            last_op_number,
            last_operation_digest: pair[1].first_chain.previous_digest(),
        };
        let index_path = journal
            .directory
            .root
            .join("indexes")
            .join(segment_index_name(source));
        let index_bytes = optional_regular_file_bytes(&index_path)?.unwrap_or(0);
        let bytes = fs::metadata(segment_path)?
            .len()
            .checked_add(index_bytes)
            .ok_or(RetentionError::LengthOverflow)?;
        candidates.push(ReclaimCandidate {
            segment_id: reference.segment_id,
            bytes,
        });
    }
    Ok((candidates, scanned_segments, scanned_bytes))
}

fn remove_unreferenced_segment(
    root: &Path,
    segment_id: u64,
    segment_path: &Path,
) -> Result<u64, DirectoryError> {
    require_regular_file(segment_path, "unreferenced segment")?;
    let mut reclaimed = fs::metadata(segment_path)?.len();
    let index_directory = root.join("indexes");
    let mut index_paths = Vec::new();
    for entry in fs::read_dir(&index_directory)? {
        let entry = entry?;
        if index_name_segment_id(&entry.file_name()) == Some(segment_id) {
            require_regular_file(&entry.path(), "unreferenced segment index")?;
            reclaimed = reclaimed
                .checked_add(entry.metadata()?.len())
                .ok_or(RetentionError::LengthOverflow)?;
            index_paths.push(entry.path());
        }
    }
    for index_path in &index_paths {
        fs::remove_file(index_path)?;
    }
    if !index_paths.is_empty() {
        sync_directory(&index_directory)?;
    }
    fs::remove_file(segment_path)?;
    sync_directory(&root.join("segments"))?;
    Ok(reclaimed)
}

fn remove_unreferenced_checkpoint(path: &Path) -> Result<u64, DirectoryError> {
    let reclaimed = artifact_bytes(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(reclaimed)
}

fn remove_regular_file(path: &Path, object: &'static str) -> Result<u64, DirectoryError> {
    require_regular_file(path, object)?;
    let bytes = fs::metadata(path)?.len();
    fs::remove_file(path)?;
    Ok(bytes)
}

fn cleanup_abandoned_group_staging(root: &Path) -> Result<(), DirectoryError> {
    let staging = root.join("staging");
    let mut paths = Vec::new();
    for entry in fs::read_dir(&staging)? {
        let entry = entry?;
        if is_index_staging_name(&entry.file_name())
            || is_checkpoint_staging_name(&entry.file_name())
        {
            paths.push(entry.path());
        }
    }
    paths.sort_unstable();
    for path in &paths {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_dir() {
            fs::remove_dir_all(path)?;
        } else {
            fs::remove_file(path)?;
        }
    }
    if !paths.is_empty() {
        sync_directory(&staging)?;
    }
    Ok(())
}

fn is_index_staging_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str().and_then(|name| name.strip_prefix("index-")) else {
        return false;
    };
    let mut fields = name.split('-');
    matches!(
        (fields.next(), fields.next(), fields.next(), fields.next()),
        (Some(segment), Some(process), Some(sequence), None)
            if parse_decimal_name(segment).is_some()
                && parse_decimal_name(process).is_some()
                && parse_decimal_name(sequence).is_some()
    )
}

fn is_checkpoint_staging_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str().and_then(|name| {
        name.strip_prefix(".checkpoint-")
            .and_then(|name| name.strip_suffix(".tmp"))
    }) else {
        return false;
    };
    let mut fields = name.split('-');
    matches!(
        (fields.next(), fields.next(), fields.next(), fields.next()),
        (Some(checkpoint), Some(process), Some(sequence), None)
            if parse_checkpoint_name(OsStr::new(checkpoint)).is_some()
                && parse_decimal_name(process).is_some()
                && parse_decimal_name(sequence).is_some()
    )
}

fn artifact_bytes(path: &Path) -> Result<u64, DirectoryError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Ok(metadata.len());
    }
    fs::read_dir(path)?.try_fold(0_u64, |total, entry| {
        total
            .checked_add(artifact_bytes(&entry?.path())?)
            .ok_or_else(|| RetentionError::LengthOverflow.into())
    })
}

fn parse_segment_name(name: &OsStr) -> Option<u64> {
    let stem = name.to_str()?.strip_suffix(".log")?;
    if let Some((id, generation)) = stem.split_once('.') {
        let generation = parse_decimal_name(generation)?;
        if generation == 0 {
            return None;
        }
        parse_decimal_name(id)
    } else {
        parse_decimal_name(stem)
    }
}

fn parse_manifest_name(name: &OsStr) -> Option<u64> {
    parse_decimal_name(name.to_str()?.strip_prefix("MANIFEST.")?)
}

fn parse_manifest_temporary_name(name: &OsStr) -> Option<u64> {
    parse_decimal_name(
        name.to_str()?
            .strip_prefix(".MANIFEST.")?
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

fn parse_checkpoint_name(name: &OsStr) -> Option<CheckpointId> {
    let name = name.to_str()?;
    if name.len() != 32
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut bytes = [0_u8; 16];
    let (pairs, remainder) = name.as_bytes().as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    for (output, pair) in bytes.iter_mut().zip(pairs) {
        *output = (hex_value(pair[0])? << 4) | hex_value(pair[1])?;
    }
    Some(CheckpointId::from_bytes(bytes))
}

const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn index_name_segment_id(name: &OsStr) -> Option<u64> {
    let name = name.to_str()?.strip_suffix(".idx")?;
    let (segment, digest) = name.split_once('-')?;
    let segment_id: u64 = segment.parse().ok()?;
    (segment == segment_id.to_string()
        && digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then_some(segment_id)
}

fn validate_checkpoint_install(
    directory: &GroupDirectory,
    writer: &SegmentWriter<Arc<File>>,
    checkpoint: &CheckpointImage,
    decode_limits: DecodeLimits,
    operation_limits: OperationLimits,
) -> Result<(), DirectoryError> {
    let manifest = checkpoint.manifest();
    validate_checkpoint_successor(&directory.manifest, manifest)?;
    let source_bytes = read_limited_file(
        &directory
            .root
            .join(manifest_name(manifest.source_manifest_generation)),
        directory.limits.max_manifest_bytes,
        "checkpoint source manifest",
    )?;
    let source = decode_manifest(&source_bytes, directory.limits)?;
    validate_checkpoint_source(
        directory.identity,
        manifest,
        &source,
        manifest_digest(&source_bytes, directory.limits)?,
    )?;
    validate_metadata_positions(
        directory,
        writer,
        [manifest.position, manifest.position],
        decode_limits,
        operation_limits,
    )
}

fn validate_checkpoint_successor(
    current: &Manifest,
    checkpoint: &crate::CheckpointManifest,
) -> Result<(), DirectoryError> {
    if checkpoint.group_id != current.identity.group_id
        || checkpoint.store_id != current.identity.store_id
        || checkpoint.configuration_epoch != current.configuration_epoch
        || checkpoint.source_manifest_generation > current.generation
        || checkpoint.position.op_number > current.committed.op_number
    {
        return Err(DirectoryError::CheckpointMismatch);
    }
    if current
        .checkpoint
        .is_some_and(|previous| checkpoint.position.op_number <= previous.position.op_number)
    {
        return Err(DirectoryError::CheckpointRegression);
    }
    Ok(())
}

fn validate_checkpoint_source(
    identity: GroupIdentity,
    checkpoint: &crate::CheckpointManifest,
    source: &Manifest,
    digest: crate::Digest,
) -> Result<(), DirectoryError> {
    if digest != checkpoint.source_manifest_digest
        || source.identity != identity
        || source.generation != checkpoint.source_manifest_generation
        || source.configuration_epoch != checkpoint.configuration_epoch
        || source.committed != checkpoint.position
    {
        return Err(DirectoryError::CheckpointMismatch);
    }
    Ok(())
}

fn scan_contains_position(
    scan: &crate::SegmentScan<'_>,
    initial: ChainPosition,
    target: ChainPosition,
) -> bool {
    initial == target
        || scan
            .groups
            .iter()
            .flat_map(|group| group.operations.iter())
            .any(|operation| {
                operation.op_number.checked_add(1) == Some(target.next_op_number())
                    && operation.digest == target.previous_digest()
            })
}

fn validate_operation_bodies(
    scan: &crate::SegmentScan<'_>,
    limits: OperationLimits,
    configuration_epoch: u64,
    promised_view: u64,
) -> Result<(), DirectoryError> {
    for operation in scan.groups.iter().flat_map(|group| group.operations.iter()) {
        validate_operation(
            OperationEnvelope {
                kind: operation.kind,
                body: operation.body.as_ref(),
                op_number: operation.op_number,
                configuration_epoch: operation.configuration_epoch,
                original_view: operation.original_view,
            },
            limits,
            configuration_epoch,
            promised_view,
        )?;
    }
    Ok(())
}

fn validate_operation(
    operation: OperationEnvelope<'_>,
    limits: OperationLimits,
    expected_configuration_epoch: u64,
    promised_view: u64,
) -> Result<(), DirectoryError> {
    validate_operation_body(operation.kind, operation.body, limits)?;
    validate_operation_metadata(operation, expected_configuration_epoch, promised_view)
}

fn validate_operation_metadata(
    operation: OperationEnvelope<'_>,
    expected_configuration_epoch: u64,
    promised_view: u64,
) -> Result<(), DirectoryError> {
    validate_operation_metadata_fields(
        operation.op_number,
        operation.configuration_epoch,
        operation.original_view,
        expected_configuration_epoch,
        promised_view,
    )
}

fn validate_operation_metadata_fields(
    op_number: u64,
    configuration_epoch: u64,
    original_view: u64,
    expected_configuration_epoch: u64,
    promised_view: u64,
) -> Result<(), DirectoryError> {
    if configuration_epoch != expected_configuration_epoch {
        return Err(WriterError::ConfigurationMismatch {
            op_number,
            actual: configuration_epoch,
            expected: expected_configuration_epoch,
        }
        .into());
    }
    if original_view > promised_view {
        return Err(WriterError::ViewBeyondPromise {
            op_number,
            actual: original_view,
            promised: promised_view,
        }
        .into());
    }
    Ok(())
}

fn validate_roll_boundary(
    directory: &GroupDirectory,
    writer: &SegmentWriter<Arc<File>>,
) -> Result<RollBoundary, DirectoryError> {
    if directory.current.generation != directory.manifest.generation {
        return Err(DirectoryError::RollPublicationPending);
    }
    let boundary = validate_buffered_roll_boundary(directory, writer)?;
    if writer.written_position() != writer.durable_position() {
        return Err(DirectoryError::ActiveSegmentNotDurable);
    }
    Ok(boundary)
}

fn validate_buffered_roll_boundary(
    directory: &GroupDirectory,
    writer: &SegmentWriter<Arc<File>>,
) -> Result<RollBoundary, DirectoryError> {
    validate_buffered_roll_state(&directory.manifest, writer)
}

fn validate_buffered_roll_state<I>(
    manifest: &Manifest,
    writer: &SegmentWriter<I>,
) -> Result<RollBoundary, DirectoryError> {
    if writer.is_faulted() {
        return Err(WriterError::Faulted.into());
    }
    let written = writer.written_position();
    let active = *manifest
        .segments
        .last()
        .ok_or(DirectoryError::MissingActiveSegment)?;
    if active.sealed.is_some()
        || active.segment_id != writer.header().segment_id()
        || active.capacity != writer.header().capacity()
    {
        return Err(DirectoryError::SegmentMismatch(active.segment_id));
    }

    if written.end_offset() == SEGMENT_HEADER_BYTES as u64 {
        return Err(DirectoryError::EmptyActiveSegment);
    }
    let next_group_number = written
        .group_number()
        .checked_add(1)
        .ok_or(WriterError::GroupNumberExhausted)?;
    next_group_number
        .checked_add(1)
        .ok_or(WriterError::GroupNumberExhausted)?;
    Ok(RollBoundary {
        active,
        valid_bytes: written.end_offset(),
        digest: writer.structural_digest(),
        next_group_number,
        next_chain: written.next_chain(),
        writer_generation: written.generation(),
    })
}

fn roll_manifest(
    directory: &GroupDirectory,
    boundary: RollBoundary,
    next_segment_id: u64,
    new_capacity: u64,
) -> Result<Manifest, DirectoryError> {
    roll_manifest_after(&directory.manifest, boundary, next_segment_id, new_capacity)
}

fn roll_manifest_after(
    manifest: &Manifest,
    boundary: RollBoundary,
    next_segment_id: u64,
    new_capacity: u64,
) -> Result<Manifest, DirectoryError> {
    let mut next = manifest.clone();
    next.generation = next
        .generation
        .checked_add(1)
        .ok_or(DirectoryError::ManifestGeneration)?;
    next.parent_generation = manifest.generation;
    next.segments
        .last_mut()
        .ok_or(DirectoryError::MissingActiveSegment)?
        .sealed = Some(crate::SealedSegment {
        valid_bytes: boundary.valid_bytes,
        digest: boundary.digest,
    });
    next.segments.push(SegmentReference {
        segment_id: next_segment_id,
        file_generation: 0,
        first_group_number: boundary.next_group_number,
        first_chain: boundary.next_chain,
        capacity: new_capacity,
        sealed: None,
    });
    Ok(next)
}

fn validate_metadata_successor(
    directory: &GroupDirectory,
    writer: &SegmentWriter<Arc<File>>,
    next: &Manifest,
    decode_limits: DecodeLimits,
    operation_limits: OperationLimits,
) -> Result<(), DirectoryError> {
    validate_metadata_successor_shape(directory, next)?;
    validate_metadata_positions(
        directory,
        writer,
        [next.accepted, next.committed],
        decode_limits,
        operation_limits,
    )
}

fn validate_progress_successor(
    directory: &GroupDirectory,
    writer: &SegmentWriter<Arc<File>>,
    next: &Manifest,
) -> Result<(), DirectoryError> {
    validate_metadata_successor_shape(directory, next)?;
    if writer.is_faulted() {
        return Err(WriterError::Faulted.into());
    }
    let durable = position_before(writer.durable_position().next_chain())?;
    for (current, published) in [directory.manifest.accepted, directory.manifest.committed]
        .into_iter()
        .zip([next.accepted, next.committed])
    {
        if published != current && published != durable {
            return Err(DirectoryError::PositionMismatch(published.op_number));
        }
    }
    Ok(())
}

fn validate_metadata_successor_shape(
    directory: &GroupDirectory,
    next: &Manifest,
) -> Result<(), DirectoryError> {
    validate_metadata_successor_fields(&directory.manifest, next)
}

fn validate_metadata_successor_fields(
    current: &Manifest,
    next: &Manifest,
) -> Result<(), DirectoryError> {
    if next.configuration_epoch != current.configuration_epoch {
        return Err(DirectoryError::ConfigurationChangeRequiresInstall);
    }
    if next.durable_evidence != current.durable_evidence {
        return Err(DirectoryError::ConfigurationChangeRequiresInstall);
    }
    if next.commit_mode != current.commit_mode {
        return Err(DirectoryError::CommitModeChange);
    }
    if next.checkpoint != current.checkpoint {
        return Err(DirectoryError::CheckpointChangeRequiresInstall);
    }
    if next.promised_view < current.promised_view
        || next.last_normal_view < current.last_normal_view
        || position_regresses(current.accepted, next.accepted)
        || position_regresses(current.committed, next.committed)
    {
        return Err(DirectoryError::HardStateRegression);
    }
    Ok(())
}

fn position_regresses(current: LogPosition, next: LogPosition) -> bool {
    next.op_number < current.op_number
        || (next.op_number == current.op_number && next.digest != current.digest)
}

fn validate_metadata_positions(
    directory: &GroupDirectory,
    writer: &SegmentWriter<Arc<File>>,
    positions: [LogPosition; 2],
    decode_limits: DecodeLimits,
    operation_limits: OperationLimits,
) -> Result<(), DirectoryError> {
    if writer.is_faulted() {
        return Err(WriterError::Faulted.into());
    }
    let written = writer.written_position();
    let durable = writer.durable_position();
    let durable_op = durable
        .next_chain()
        .next_op_number()
        .checked_sub(1)
        .ok_or(DirectoryError::PositionMismatch(0))?;
    for position in positions {
        if position.op_number > durable_op {
            return Err(DirectoryError::PositionNotDurable(position.op_number));
        }
    }

    let mut seen = positions.map(|position| position == LogPosition::GENESIS);
    for reference in &directory.manifest.segments {
        let capacity = usize::try_from(reference.capacity)
            .map_err(|_| DirectoryError::SegmentMismatch(reference.segment_id))?;
        let image = read_limited_file(
            &directory.root.join(segment_reference_name(reference)),
            capacity,
            "segment",
        )?;
        let scan = scan_segment(
            &image,
            reference.first_group_number,
            reference.first_chain,
            decode_limits,
        )?;
        validate_operation_bodies(
            &scan,
            operation_limits,
            directory.manifest.configuration_epoch,
            directory.manifest.promised_view,
        )?;
        if let Some(sealed) = reference.sealed {
            if scan.valid_bytes != sealed.valid_bytes
                || scan.digest != sealed.digest
                || matches!(scan.tail, TailState::Truncated { .. })
            {
                return Err(DirectoryError::SegmentMismatch(reference.segment_id));
            }
        } else if scan.header != *writer.header()
            || scan.valid_bytes != written.end_offset()
            || scan.next_chain != written.next_chain()
            || scan.next_group_number.checked_sub(1) != Some(written.group_number())
            || matches!(scan.tail, TailState::Truncated { .. })
        {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id));
        }
        for (found, position) in seen.iter_mut().zip(positions) {
            *found |=
                scan_contains_position(&scan, reference.first_chain, position.following_chain()?);
        }
    }
    if let Some(position) = positions
        .into_iter()
        .zip(seen)
        .find_map(|(position, seen)| (!seen).then_some(position))
    {
        return Err(DirectoryError::PositionMismatch(position.op_number));
    }
    Ok(())
}

fn validate_replay_scan<I>(
    reference: &SegmentReference,
    scan: &crate::SegmentScan<'_>,
    writer: &SegmentWriter<I>,
) -> Result<(), DirectoryError> {
    if let Some(sealed) = reference.sealed {
        if scan.valid_bytes != sealed.valid_bytes
            || scan.digest != sealed.digest
            || matches!(scan.tail, TailState::Truncated { .. })
        {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id));
        }
    } else {
        let written = writer.written_position();
        if scan.header != *writer.header()
            || scan.valid_bytes != written.end_offset()
            || scan.next_chain != written.next_chain()
            || scan.next_group_number.checked_sub(1) != Some(written.group_number())
            || matches!(scan.tail, TailState::Truncated { .. })
        {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id));
        }
    }
    Ok(())
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), DirectoryError> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn optional_regular_file_bytes(path: &Path) -> Result<Option<u64>, DirectoryError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(Some(metadata.len())),
        Ok(_) => Err(DirectoryError::NotRegularFile("derived index")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn write_new_segment_synced(path: &Path, header: &SegmentHeader) -> Result<(), DirectoryError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    allocate_segment(&file, header.capacity())?;
    file.write_all(&encode_segment_header(header))?;
    file.sync_all()?;
    Ok(())
}

fn prepare_roll_segment(
    preparation: RollSegmentPreparation<'_>,
    observer: &mut impl PersistenceObserver,
) -> Result<SegmentWriter<Arc<File>>, DirectoryError> {
    let RollSegmentPreparation {
        root,
        header,
        generation,
        first_group_number,
        first_chain,
        decode_limits,
        operation_limits,
        data_sync,
        direct,
    } = preparation;
    let path = root.join(segment_name(header.segment_id()));
    let mut writer = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(file) => {
            observer.completed(PersistencePhase::SegmentCreated)?;
            allocate_segment(&file, header.capacity())?;
            observer.completed(PersistencePhase::SegmentAllocated)?;
            let writer = SegmentWriter::initialize_allocated_at(
                std::sync::Arc::new(file),
                header.clone(),
                generation,
                first_group_number,
                first_chain,
                true,
            )?;
            observer.completed(PersistencePhase::SegmentHeaderSynced)?;
            writer
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            require_regular_file(&path, "orphan segment")?;
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            let initialized = validate_unused_roll_segment(&mut file, header)?;
            allocate_segment(&file, header.capacity())?;
            observer.completed(PersistencePhase::SegmentAllocated)?;
            let writer = if initialized {
                SegmentWriter::recover_canonical(
                    std::sync::Arc::new(file),
                    generation,
                    first_group_number,
                    first_chain,
                    decode_limits,
                    header.capacity(),
                    CanonicalRecoveryRequirements {
                        operation_limits,
                        protected: [Some(first_chain), None],
                        configuration_epoch: None,
                        promised_view: None,
                        discard_damaged_tail: false,
                    },
                )?
            } else {
                SegmentWriter::initialize_allocated_at(
                    std::sync::Arc::new(file),
                    header.clone(),
                    generation,
                    first_group_number,
                    first_chain,
                    true,
                )?
            };
            observer.completed(PersistencePhase::SegmentHeaderSynced)?;
            writer
        }
        Err(error) => return Err(error.into()),
    };
    if data_sync {
        writer.set_write_mode(&path, SegmentWriteMode::DataSync)?;
    }
    if direct {
        writer.set_direct(&path, true)?;
    }
    sync_directory(&root.join("segments"))?;
    observer.completed(PersistencePhase::SegmentsDirectorySynced)?;
    Ok(writer)
}

fn validate_unused_roll_segment(
    file: &mut File,
    header: &SegmentHeader,
) -> Result<bool, DirectoryError> {
    let length = file.metadata()?.len();
    if length == 0 {
        return Ok(false);
    }
    if length != header.capacity() {
        return Err(DirectoryError::OrphanSegmentConflict(header.segment_id()));
    }
    let mut bytes = [0; SEGMENT_HEADER_BYTES];
    file.read_exact(&mut bytes)?;
    let initialized = bytes.iter().any(|&byte| byte != 0);
    if (initialized && decode_segment_header(&bytes)? != *header) || !remaining_file_is_zero(file)?
    {
        return Err(DirectoryError::OrphanSegmentConflict(header.segment_id()));
    }
    // Initialization and recovery use positional I/O, independent of this cursor.
    Ok(initialized)
}

pub(crate) fn remaining_file_is_zero(file: &mut File) -> io::Result<bool> {
    let mut buffer = [0; 16 * 1024];
    loop {
        let length = match file.read(&mut buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if length == 0 {
            return Ok(true);
        }
        if buffer[..length].iter().any(|&byte| byte != 0) {
            return Ok(false);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn allocate_segment(file: &File, capacity: u64) -> Result<(), DirectoryError> {
    rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, capacity)
        .map_err(io::Error::from)?;
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn allocate_segment(_file: &File, _capacity: u64) -> Result<(), DirectoryError> {
    Err(DirectoryError::AllocationUnsupported)
}

fn install_immutable(
    root: &Path,
    name: &str,
    bytes: &[u8],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    publication::install_immutable(&mut publication::Filesystem(root), name, bytes, observer)
}

fn replace_current(
    root: &Path,
    generation: u64,
    bytes: &[u8; CURRENT_BYTES],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    publication::replace_current(
        &mut publication::Filesystem(root),
        generation,
        bytes,
        observer,
    )
}

fn require_exact_contents(path: &Path, expected: &[u8]) -> Result<(), DirectoryError> {
    let actual = read_exact_file(path, expected.len(), "immutable metadata")?;
    if actual == expected {
        Ok(())
    } else {
        Err(DirectoryError::ImmutableConflict)
    }
}

fn read_exact_file(
    path: &Path,
    expected: usize,
    object: &'static str,
) -> Result<Vec<u8>, DirectoryError> {
    let mut file = open_regular_file(path, object)?;
    let actual = file.metadata()?.len();
    if actual != expected as u64 {
        return Err(DirectoryError::WrongFileSize {
            object,
            actual,
            expected: expected as u64,
        });
    }
    let mut bytes = vec![0; expected];
    file.read_exact(&mut bytes)?;
    let mut trailing = [0; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(DirectoryError::WrongFileSize {
            object,
            actual: expected as u64 + 1,
            expected: expected as u64,
        });
    }
    Ok(bytes)
}

fn validate_configuration_length(length: usize) -> Result<(), DirectoryError> {
    if length == 0 || length > GROUP_CONFIGURATION_MAX_BYTES {
        return Err(DirectoryError::ConfigurationLength);
    }
    Ok(())
}

pub(crate) fn read_configuration(root: &Path) -> Result<Option<Vec<u8>>, DirectoryError> {
    let path = root.join(CONFIGURATION_FILE);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(metadata) => {
            if !metadata.file_type().is_file() {
                return Err(DirectoryError::NotRegularFile(CONFIGURATION_FILE));
            }
            let length =
                usize::try_from(metadata.len()).map_err(|_| DirectoryError::ConfigurationLength)?;
            validate_configuration_length(length)?;
        }
    }
    let bytes = read_limited_file(&path, GROUP_CONFIGURATION_MAX_BYTES, CONFIGURATION_FILE)?;
    validate_configuration_length(bytes.len())?;
    Ok(Some(bytes))
}

fn read_prefix_file(
    path: &Path,
    expected: usize,
    object: &'static str,
) -> Result<(Vec<u8>, u64), DirectoryError> {
    let mut file = open_regular_file(path, object)?;
    let actual = file.metadata()?.len();
    if actual < expected as u64 {
        return Err(DirectoryError::WrongFileSize {
            object,
            actual,
            expected: expected as u64,
        });
    }
    let mut bytes = vec![0; expected];
    file.read_exact(&mut bytes)?;
    Ok((bytes, actual))
}

fn read_limited_file(
    path: &Path,
    limit: usize,
    object: &'static str,
) -> Result<Vec<u8>, DirectoryError> {
    let file = open_regular_file(path, object)?;
    let actual = file.metadata()?.len();
    if limit == 0 || actual > limit as u64 {
        return Err(DirectoryError::FileLimit {
            object,
            actual,
            limit,
        });
    }
    let length = usize::try_from(actual).map_err(|_| DirectoryError::FileLimit {
        object,
        actual,
        limit,
    })?;
    let read_limit = limit.saturating_add(1) as u64;
    let mut bytes = Vec::with_capacity(length);
    file.take(read_limit).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(DirectoryError::FileLimit {
            object,
            actual: bytes.len() as u64,
            limit,
        });
    }
    Ok(bytes)
}

fn open_regular_file(path: &Path, object: &'static str) -> Result<File, DirectoryError> {
    require_regular_file(path, object)?;
    let file = File::open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(DirectoryError::NotRegularFile(object));
    }
    Ok(file)
}

fn require_regular_file(path: &Path, object: &'static str) -> Result<(), DirectoryError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err(DirectoryError::NotRegularFile(object))
    }
}

fn require_directory(path: &Path, object: &'static str) -> Result<(), DirectoryError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_dir() {
        Ok(())
    } else {
        Err(DirectoryError::NotDirectory(object))
    }
}

fn path_exists(path: &Path) -> Result<bool, DirectoryError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Write `bytes` so that they and the metadata needed to read them are durable
/// on return, waiting only for this range rather than the whole file.
fn write_synchronized(file: &File, offset: u64, bytes: &[u8]) -> Result<(), DirectoryError> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use rustix::io::{Errno, ReadWriteFlags, pwritev2};
        let mut written = 0;
        while written < bytes.len() {
            let slice = [io::IoSlice::new(&bytes[written..])];
            match pwritev2(file, &slice, offset + written as u64, ReadWriteFlags::DSYNC) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
                Ok(count) => written += count,
                Err(Errno::INTR) => {}
                // Kernels without per-write flags: fall back to a full barrier.
                Err(Errno::OPNOTSUPP | Errno::NOSYS) if written == 0 => break,
                Err(error) => return Err(io::Error::from(error).into()),
            }
        }
        if written == bytes.len() {
            return Ok(());
        }
    }
    #[cfg(unix)]
    std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)?;
    #[cfg(not(unix))]
    return Err(io::Error::from(io::ErrorKind::Unsupported).into());
    #[cfg(unix)]
    {
        file.sync_data()?;
        Ok(())
    }
}

fn sync_directory(path: &Path) -> Result<(), DirectoryError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

pub(crate) fn segment_name(segment_id: u64) -> PathBuf {
    PathBuf::from("segments").join(format!("{segment_id}.log"))
}

pub(crate) fn segment_reference_name(
    reference: impl std::borrow::Borrow<SegmentReference>,
) -> PathBuf {
    PathBuf::from("segments").join(reference.borrow().file_name())
}

fn manifest_name(generation: u64) -> String {
    format!("MANIFEST.{generation}")
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};
    use tempfile::TempDir;

    use super::*;
    use crate::{CanonicalOperation, Digest, OperationKind};

    const ROLL_PHASES: [PersistencePhase; 10] = [
        PersistencePhase::SegmentCreated,
        PersistencePhase::SegmentAllocated,
        PersistencePhase::SegmentHeaderSynced,
        PersistencePhase::SegmentsDirectorySynced,
        PersistencePhase::ManifestTemporarySynced,
        PersistencePhase::ManifestLinked,
        PersistencePhase::ManifestDirectorySynced,
        PersistencePhase::CurrentTemporarySynced,
        PersistencePhase::CurrentRenamed,
        PersistencePhase::CurrentDirectorySynced,
    ];

    const BUFFERED_ROLL_PHASES: [PersistencePhase; 9] = [
        PersistencePhase::PredecessorSynced,
        PersistencePhase::SegmentHeaderSynced,
        PersistencePhase::SegmentsDirectorySynced,
        PersistencePhase::ManifestTemporarySynced,
        PersistencePhase::ManifestLinked,
        PersistencePhase::ManifestDirectorySynced,
        PersistencePhase::CurrentTemporarySynced,
        PersistencePhase::CurrentRenamed,
        PersistencePhase::CurrentDirectorySynced,
    ];

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

    #[derive(Debug)]
    struct PauseAfterPredecessor(Arc<Barrier>);

    impl PersistenceObserver for PauseAfterPredecessor {
        fn completed(&mut self, phase: PersistencePhase) -> io::Result<()> {
            if phase == PersistencePhase::PredecessorSynced {
                self.0.wait();
                self.0.wait();
            }
            Ok(())
        }
    }

    fn identity(byte: u8) -> GroupIdentity {
        GroupIdentity {
            group_id: GroupId::from_bytes([byte; 16]),
            replica_node_id: NodeId::from_bytes([byte + 1; 16]),
            volume_id: VolumeId::from_bytes([byte + 2; 16]),
            store_id: StoreId::from_bytes([byte + 3; 16]),
            store_generation: 1,
        }
    }

    fn barrier(
        identity: GroupIdentity,
        op_number: u64,
        previous: Digest,
    ) -> CanonicalOperation<'static> {
        CanonicalOperation {
            group_id: identity.group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number,
            previous_digest: previous,
            kind: OperationKind::Barrier,
            body: &[0xaa; 16],
        }
    }

    pub(super) fn journal_with_three_sealed_indexes(root: &Path) -> OpenGroupJournal {
        let identity = identity(0x10);
        let header = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8192).unwrap();
        let mut journal = GroupDirectory::format_new(root, identity, 1, &header)
            .unwrap()
            .recover(
                JournalGeneration(1),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        let mut previous = Digest::ZERO;
        for number in 1_u8..=3 {
            let body = [number; 16];
            let operation = CanonicalOperation {
                body: &body,
                ..barrier(identity, u64::from(number), previous)
            };
            let written = journal.append(&[operation]).unwrap();
            journal.sync_through(written).unwrap();
            previous = journal.accepted_position().unwrap().digest;
            journal = journal.roll_active(8192).unwrap();
        }
        journal
    }

    fn verify_three_index_operations(catalog: &SegmentIndexCatalog) {
        for number in 1_u8..=3 {
            let found = catalog
                .find_operation(ozzy_proto::OperationId::from_bytes([number; 16]), 3)
                .unwrap()
                .unwrap();
            assert_eq!(found.entry.location.op_number, u64::from(number));
        }
    }

    #[test]
    fn index_catalog_shares_directory_barriers_and_refences_reused_names() {
        let volume = TempDir::new().unwrap();
        let root = volume.path().join("group");
        let journal = journal_with_three_sealed_indexes(&root);
        let accepted = journal.accepted_position().unwrap();
        let mut synchronized = Vec::new();
        let catalog = journal
            .build_sealed_indexes_syncing(IndexBuildLimits::default(), |path| {
                // The catalog fence covers every final name. Per-segment
                // staging runs have already been released to bound disk use.
                assert_eq!(fs::read_dir(root.join("indexes"))?.count(), 3);
                assert_eq!(fs::read_dir(root.join("staging"))?.count(), 0);
                File::open(path)?.sync_all()?;
                synchronized.push(path.file_name().unwrap().to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(synchronized, ["indexes", "staging"]);
        verify_three_index_operations(&catalog);
        assert_eq!(journal.accepted_position().unwrap(), accepted);

        synchronized.clear();
        let reused = journal
            .build_sealed_indexes_syncing(IndexBuildLimits::default(), |path| {
                File::open(path)?.sync_all()?;
                synchronized.push(path.file_name().unwrap().to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(synchronized, ["indexes"]);
        verify_three_index_operations(&reused);
    }

    #[test]
    fn index_catalog_directory_sync_failure_preserves_rebuildable_history() {
        for failed_directory in ["indexes", "staging"] {
            let volume = TempDir::new().unwrap();
            let root = volume.path().join("group");
            let journal = journal_with_three_sealed_indexes(&root);
            let accepted = journal.accepted_position().unwrap();
            let current = fs::read(root.join(CURRENT_FILE)).unwrap();
            let source = fs::read(root.join(segment_name(1))).unwrap();
            let result =
                journal.build_sealed_indexes_syncing(IndexBuildLimits::default(), |path| {
                    if path.file_name() == Some(OsStr::new(failed_directory)) {
                        return Err(io::Error::other("injected directory sync failure").into());
                    }
                    Ok(File::open(path)?.sync_all()?)
                });
            assert!(matches!(
                result,
                Err(DirectoryError::Index(IndexBuildError::Io(_)))
            ));
            assert_eq!(journal.accepted_position().unwrap(), accepted);
            assert_eq!(fs::read(root.join(CURRENT_FILE)).unwrap(), current);
            assert_eq!(fs::read(root.join(segment_name(1))).unwrap(), source);

            // Model loss of an unfenced derived name and corruption of another.
            // The third exact file must be reused, never treated as quorum proof.
            let sources = journal.sealed_index_sources().unwrap();
            fs::remove_file(root.join("indexes").join(segment_index_name(sources[0]))).unwrap();
            fs::write(
                root.join("indexes").join(segment_index_name(sources[1])),
                b"bad index",
            )
            .unwrap();
            let identity = journal.directory().identity();
            drop(journal);
            let reopened = GroupDirectory::open(&root, identity, MetadataLimits::default())
                .unwrap()
                .recover(
                    JournalGeneration(2),
                    DecodeLimits::default(),
                    OperationLimits::default(),
                )
                .unwrap();
            assert_eq!(reopened.accepted_position().unwrap(), accepted);
            let catalog = reopened
                .build_sealed_indexes(IndexBuildLimits::default())
                .unwrap();
            verify_three_index_operations(&catalog);
        }
    }

    #[test]
    fn bounded_roll_counts_occupied_names_without_overwriting_them() {
        for budget in [0, 1, 2] {
            let volume = TempDir::new().unwrap();
            let root = volume.path().join("group");
            let identity = identity(0x20);
            let header =
                SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8192).unwrap();
            let mut journal = GroupDirectory::format_new(&root, identity, 1, &header)
                .unwrap()
                .recover(
                    JournalGeneration(1),
                    DecodeLimits::default(),
                    OperationLimits::default(),
                )
                .unwrap();
            let written = journal
                .append(&[barrier(identity, 1, Digest::ZERO)])
                .unwrap();
            journal.sync_through(written).unwrap();
            fs::write(root.join(segment_name(2)), b"occupied").unwrap();
            let rolled = journal.roll_active_bounded(8192, budget);
            if budget < 2 {
                assert!(
                    matches!(rolled, Err(DirectoryError::RollProbeLimit { limit }) if limit == budget)
                );
            } else {
                assert_eq!(rolled.unwrap().writer().header().segment_id(), 3);
            }
            assert_eq!(fs::read(root.join(segment_name(2))).unwrap(), b"occupied");
            assert_eq!(root.join(segment_name(3)).exists(), budget == 2);
            let journal = GroupDirectory::open(&root, identity, MetadataLimits::default())
                .unwrap()
                .recover(
                    JournalGeneration(2),
                    DecodeLimits::default(),
                    OperationLimits::default(),
                )
                .unwrap();
            assert_eq!(journal.accepted_position().unwrap().op_number, 1);
        }
    }

    #[test]
    fn every_roll_persistence_cut_reopens_old_or_new_generation() {
        for (index, phase) in ROLL_PHASES.into_iter().enumerate() {
            let volume = TempDir::new().unwrap();
            let root = volume.path().join("group");
            let identity = identity(0x20 + index as u8);
            let first =
                SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap();
            let directory = GroupDirectory::format_new(&root, identity, 1, &first).unwrap();
            let mut journal = directory
                .recover(
                    JournalGeneration(1),
                    DecodeLimits::default(),
                    OperationLimits::default(),
                )
                .unwrap();
            let first = journal
                .append(&[barrier(identity, 1, Digest::ZERO)])
                .unwrap();
            journal.sync_through(first).unwrap();

            assert!(matches!(
                journal.roll_active_observing(8 * 1024, &mut FailAfter(phase)),
                Err(DirectoryError::Io(_))
            ));

            let directory =
                GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
            let current_is_new = matches!(
                phase,
                PersistencePhase::CurrentRenamed | PersistencePhase::CurrentDirectorySynced
            );
            assert_eq!(
                directory.current().generation,
                if current_is_new { 2 } else { 1 },
                "unexpected CURRENT after {phase:?}"
            );
            let journal = directory
                .recover(
                    JournalGeneration(2),
                    DecodeLimits::default(),
                    OperationLimits::default(),
                )
                .unwrap_or_else(|error| panic!("recovery failed after {phase:?}: {error:?}"));
            let mut journal = if current_is_new {
                journal
            } else {
                journal.roll_active(8 * 1024).unwrap()
            };
            assert_eq!(journal.writer().header().segment_id(), 2);

            let chain = journal.writer().written_position().next_chain();
            let second = journal
                .append(&[barrier(identity, 2, chain.previous_digest())])
                .unwrap();
            journal.sync_through(second).unwrap();
        }
    }

    #[test]
    fn buffered_roll_writes_successor_while_publication_is_detached() {
        let volume = TempDir::new().unwrap();
        let root = volume.path().join("group");
        let identity = identity(0x50);
        let first = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap();
        let directory = GroupDirectory::format_new(&root, identity, 1, &first).unwrap();
        let mut journal = directory
            .recover(
                JournalGeneration(1),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        let first = journal
            .append(&[barrier(identity, 1, Digest::ZERO)])
            .unwrap();
        let (mut journal, publication) = journal.begin_buffered_roll(8 * 1024).unwrap();
        assert!(journal.buffered_roll_pending());
        assert_eq!(journal.directory().current().generation, 1);
        assert_eq!(journal.directory().manifest().generation, 2);
        assert_eq!(journal.writer().header().segment_id(), 2);

        let pause = Arc::new(Barrier::new(2));
        let publisher_pause = Arc::clone(&pause);
        let publication = std::thread::spawn(move || {
            publication.publish_observing(&mut PauseAfterPredecessor(publisher_pause))
        });
        pause.wait();
        let second = journal
            .append(&[barrier(identity, 2, first.next_chain().previous_digest())])
            .unwrap();
        pause.wait();
        let published = publication.join().unwrap().unwrap();
        let journal = journal.complete_buffered_roll(published).unwrap();
        assert!(!journal.buffered_roll_pending());
        assert_eq!(journal.directory().current().generation, 2);
        drop(journal);

        let directory = GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
        let recovered = directory
            .recover(
                JournalGeneration(2),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        assert_eq!(
            recovered.writer().written_position().next_chain(),
            second.next_chain()
        );
    }

    #[test]
    fn synchronized_write_replaces_only_its_range() {
        use std::os::unix::fs::FileExt;
        let file = tempfile::tempfile().unwrap();
        file.write_all_at(&[7; 8192], 0).unwrap();
        write_synchronized(&file, 4096, &[9; 100]).unwrap();
        let mut read = vec![0; 8192];
        file.read_exact_at(&mut read, 0).unwrap();
        assert!(read[..4096].iter().all(|b| *b == 7));
        assert!(read[4096..4196].iter().all(|b| *b == 9));
        assert!(read[4196..].iter().all(|b| *b == 7));
    }

    #[test]
    fn unpublished_successor_reopens_predecessor_and_rolls_again() {
        let volume = TempDir::new().unwrap();
        let root = volume.path().join("group");
        let identity = identity(0x70);
        let first = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap();
        let directory = GroupDirectory::format_new(&root, identity, 1, &first).unwrap();
        let mut journal = directory
            .recover(
                JournalGeneration(1),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        let first = journal
            .append(&[barrier(identity, 1, Digest::ZERO)])
            .unwrap();
        let (mut journal, publication) = journal.begin_buffered_roll(8 * 1024).unwrap();
        journal
            .append(&[barrier(identity, 2, first.next_chain().previous_digest())])
            .unwrap();
        // Crash before publication: the successor holds unselected appends
        // and CURRENT still names the predecessor.
        drop(publication);
        drop(journal);

        let directory = GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
        assert_eq!(directory.current().generation, 1);
        let journal = directory
            .recover(
                JournalGeneration(2),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        assert_eq!(journal.writer().header().segment_id(), 1);
        assert_eq!(
            journal.writer().written_position().next_chain(),
            first.next_chain()
        );
        let (journal, publication) = journal.begin_buffered_roll(8 * 1024).unwrap();
        let journal = journal
            .complete_buffered_roll(publication.publish().unwrap())
            .unwrap();
        assert!(journal.writer().header().segment_id() > 2);
    }

    #[test]
    fn every_buffered_roll_publication_cut_reopens_old_or_new_generation() {
        for (index, phase) in BUFFERED_ROLL_PHASES.into_iter().enumerate() {
            let volume = TempDir::new().unwrap();
            let root = volume.path().join("group");
            let identity = identity(0x60 + index as u8);
            let first =
                SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap();
            let directory = GroupDirectory::format_new(&root, identity, 1, &first).unwrap();
            let mut journal = directory
                .recover(
                    JournalGeneration(1),
                    DecodeLimits::default(),
                    OperationLimits::default(),
                )
                .unwrap();
            journal
                .append(&[barrier(identity, 1, Digest::ZERO)])
                .unwrap();
            let (journal, publication) = journal.begin_buffered_roll(8 * 1024).unwrap();

            assert!(matches!(
                publication.publish_observing(&mut FailAfter(phase)),
                Err(DirectoryError::Io(_))
            ));
            drop(journal);

            let directory =
                GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
            let current_is_new = matches!(
                phase,
                PersistencePhase::CurrentRenamed | PersistencePhase::CurrentDirectorySynced
            );
            assert_eq!(
                directory.current().generation,
                if current_is_new { 2 } else { 1 },
                "unexpected CURRENT after {phase:?}"
            );
            directory
                .recover(
                    JournalGeneration(2),
                    DecodeLimits::default(),
                    OperationLimits::default(),
                )
                .unwrap_or_else(|error| panic!("recovery failed after {phase:?}: {error:?}"));
        }
    }
}
