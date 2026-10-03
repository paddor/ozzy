//! Explicit persistent volume identity and group-root placement.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::store_lock::StoreLock;
use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_journal::operation::OperationLimits;
use ozzy_journal::progress::JournalGeneration;
use ozzy_proto::{GroupId, VolumeId};
use thiserror::Error;

use crate::{
    CommitMode, DecodeLimits, DirectoryError, GroupDirectory, GroupIdentity, MetadataLimits,
    OpenGroupJournal, SegmentHeader,
};

pub const VOLUME_IDENTITY_BYTES: usize = 4096;

const IDENTITY_FILE: &str = "identity";
const LOCK_FILE: &str = "volume.lock";
const VOLUME_MAGIC: &[u8; 8] = b"OZYVOL\0\0";
const VOLUME_VERSION: u16 = 2;
const VOLUME_HASH_CONTEXT: &str = "ozzy journal volume identity v1";
const VOLUME_DIGEST_START: usize = 32;
const VOLUME_DIGEST_END: usize = 64;

/// Stable identity stored inside one operator-provisioned volume root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeIdentity {
    pub volume_id: VolumeId,
}

impl VolumeIdentity {
    pub fn validate(self) -> Result<Self, VolumeError> {
        if self.volume_id.as_bytes().iter().all(|byte| *byte == 0) {
            Err(VolumeError::ZeroIdentity)
        } else {
            Ok(self)
        }
    }
}

/// Optional platform binding stronger than persistent identity alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountPolicy {
    /// Volume root may be any operator-provisioned directory.
    PortableIdentity,
    /// Volume root itself must be a mount point.
    RequireMountPoint,
}

/// One operator-supplied volume binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeConfig {
    pub root: PathBuf,
    pub identity: VolumeIdentity,
    pub mount_policy: MountPolicy,
}

/// Validated independently locked volume roots for one process.
#[derive(Debug)]
pub struct VolumeSet {
    volumes: Vec<VolumeDirectory>,
}

impl VolumeSet {
    pub fn open(configs: &[VolumeConfig]) -> Result<Self, VolumeError> {
        let mut identities = HashSet::with_capacity(configs.len());
        let mut roots = Vec::with_capacity(configs.len());
        let mut root_file_identities = HashSet::with_capacity(configs.len());
        for config in configs {
            if !identities.insert(config.identity.volume_id) {
                return Err(VolumeError::DuplicateIdentity);
            }
            require_directory(&config.root, "volume root")?;
            let root = fs::canonicalize(&config.root)?;
            if roots
                .iter()
                .any(|existing: &PathBuf| root.starts_with(existing) || existing.starts_with(&root))
            {
                return Err(VolumeError::OverlappingRoots);
            }
            if let Some(identity) = root_file_identity(&root)?
                && !root_file_identities.insert(identity)
            {
                return Err(VolumeError::AliasedRoots);
            }
            roots.push(root);
        }
        let volumes = configs
            .iter()
            .map(|config| VolumeDirectory::open(&config.root, config.identity, config.mount_policy))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { volumes })
    }

    pub fn get(&self, volume_id: VolumeId) -> Option<&VolumeDirectory> {
        self.volumes
            .iter()
            .find(|volume| volume.identity.volume_id == volume_id)
    }

    pub fn len(&self) -> usize {
        self.volumes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.volumes.is_empty()
    }
}

/// Exclusively managed, validated volume root. Open never creates content.
#[derive(Debug)]
pub struct VolumeDirectory {
    root: PathBuf,
    _root: File,
    _lock: StoreLock,
    identity: VolumeIdentity,
}

impl VolumeDirectory {
    /// Format one existing empty operator-provisioned directory.
    ///
    /// Caller creates/mounts `root`. Ozzy never creates a missing mount path.
    pub fn format_new(
        root: impl AsRef<Path>,
        identity: VolumeIdentity,
        mount_policy: MountPolicy,
    ) -> Result<Self, VolumeError> {
        let identity = identity.validate()?;
        let root = root.as_ref();
        require_directory(root, "volume root")?;
        validate_mount_policy(root, mount_policy)?;
        if fs::read_dir(root)?.next().transpose()?.is_some() {
            return Err(VolumeError::NotEmpty);
        }
        let root_handle = File::open(root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(root.join(LOCK_FILE))?;
        let lock = acquire_lock(lock)?;
        write_new_synced(
            &root.join(IDENTITY_FILE),
            &encode_volume_identity(identity)?,
        )?;
        fs::create_dir(root.join("groups"))?;
        fs::create_dir(root.join("staging"))?;
        sync_directory(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            _root: root_handle,
            _lock: lock,
            identity,
        })
    }

    /// Open one exact volume. Missing paths and identities remain errors.
    pub fn open(
        root: impl AsRef<Path>,
        expected: VolumeIdentity,
        mount_policy: MountPolicy,
    ) -> Result<Self, VolumeError> {
        let expected = expected.validate()?;
        let root = root.as_ref();
        require_directory(root, "volume root")?;
        validate_mount_policy(root, mount_policy)?;
        require_directory(&root.join("groups"), "volume groups")?;
        require_directory(&root.join("staging"), "volume staging")?;
        let root_handle = File::open(root)?;
        require_regular_file(&root.join(LOCK_FILE), "volume lock")?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join(LOCK_FILE))?;
        let lock = acquire_lock(lock)?;
        let identity = decode_volume_identity(&read_exact_identity(root)?)?;
        if identity != expected {
            return Err(VolumeError::IdentityMismatch);
        }
        Ok(Self {
            root: root.to_path_buf(),
            _root: root_handle,
            _lock: lock,
            identity,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub const fn identity(&self) -> VolumeIdentity {
        self.identity
    }

    pub fn group_root(&self, group_id: GroupId) -> PathBuf {
        self.root.join("groups").join(id_name(group_id.as_bytes()))
    }

    /// Explicitly format one absent group assigned to this volume.
    pub fn format_group(
        &self,
        identity: GroupIdentity,
        configuration_epoch: u64,
        first_segment: &SegmentHeader,
    ) -> Result<GroupDirectory, VolumeError> {
        self.format_group_with_commit_mode(
            identity,
            configuration_epoch,
            CommitMode::External,
            first_segment,
        )
    }

    pub fn format_group_with_commit_mode(
        &self,
        identity: GroupIdentity,
        configuration_epoch: u64,
        commit_mode: CommitMode,
        first_segment: &SegmentHeader,
    ) -> Result<GroupDirectory, VolumeError> {
        if identity.volume_id != self.identity.volume_id {
            return Err(VolumeError::GroupVolumeMismatch);
        }
        Ok(GroupDirectory::format_new_with_commit_mode(
            self.group_root(identity.group_id),
            identity,
            configuration_epoch,
            commit_mode,
            first_segment,
        )?)
    }

    /// Open and recover one existing group assigned to this volume.
    pub fn open_group(
        &self,
        identity: GroupIdentity,
        metadata_limits: MetadataLimits,
        writer_generation: JournalGeneration,
        decode_limits: DecodeLimits,
        operation_limits: OperationLimits,
    ) -> Result<OpenGroupJournal, VolumeError> {
        if identity.volume_id != self.identity.volume_id {
            return Err(VolumeError::GroupVolumeMismatch);
        }
        Ok(GroupDirectory::open(
            self.group_root(identity.group_id),
            identity,
            metadata_limits,
        )?
        .recover(writer_generation, decode_limits, operation_limits)?)
    }
}

pub fn encode_volume_identity(
    identity: VolumeIdentity,
) -> Result<[u8; VOLUME_IDENTITY_BYTES], VolumeError> {
    let identity = identity.validate()?;
    let mut output = [0_u8; VOLUME_IDENTITY_BYTES];
    output[..8].copy_from_slice(VOLUME_MAGIC);
    output[8..10].copy_from_slice(&VOLUME_VERSION.to_be_bytes());
    output[10..12].copy_from_slice(&(VOLUME_IDENTITY_BYTES as u16).to_be_bytes());
    output[16..32].copy_from_slice(identity.volume_id.as_bytes());
    let digest = volume_digest(&output);
    output[VOLUME_DIGEST_START..VOLUME_DIGEST_END].copy_from_slice(digest.as_bytes());
    Ok(output)
}

pub fn decode_volume_identity(input: &[u8]) -> Result<VolumeIdentity, VolumeError> {
    if input.len() != VOLUME_IDENTITY_BYTES {
        return Err(VolumeError::WrongIdentityBytes(input.len()));
    }
    if &input[..8] != VOLUME_MAGIC {
        return Err(VolumeError::WrongMagic);
    }
    let version = u16::from_be_bytes(input[8..10].try_into().expect("fixed identity field"));
    if version != VOLUME_VERSION {
        return Err(VolumeError::UnsupportedVersion(version));
    }
    if u16::from_be_bytes(input[10..12].try_into().expect("fixed identity field"))
        != VOLUME_IDENTITY_BYTES as u16
        || input[12..16].iter().any(|byte| *byte != 0)
        || input[64..].iter().any(|byte| *byte != 0)
    {
        return Err(VolumeError::UnsupportedFields);
    }
    let expected = &input[VOLUME_DIGEST_START..VOLUME_DIGEST_END];
    if volume_digest(input).as_bytes() != expected {
        return Err(VolumeError::DigestMismatch);
    }
    let mut id = [0_u8; 16];
    id.copy_from_slice(&input[16..32]);
    VolumeIdentity {
        volume_id: VolumeId::from_bytes(id),
    }
    .validate()
}

fn volume_digest(input: &[u8]) -> crate::Digest {
    let mut hasher = Hasher::new(VOLUME_HASH_CONTEXT);
    hasher.update(&input[..VOLUME_DIGEST_START]);
    hasher.update(&[0; VOLUME_DIGEST_END - VOLUME_DIGEST_START]);
    hasher.update(&input[VOLUME_DIGEST_END..]);
    hasher.finish()
}

fn read_exact_identity(root: &Path) -> Result<Vec<u8>, VolumeError> {
    let path = root.join(IDENTITY_FILE);
    require_regular_file(&path, "volume identity")?;
    let mut file = File::open(path)?;
    if file.metadata()?.len() != VOLUME_IDENTITY_BYTES as u64 {
        return Err(VolumeError::WrongIdentityBytes(
            usize::try_from(file.metadata()?.len()).unwrap_or(usize::MAX),
        ));
    }
    let mut bytes = vec![0_u8; VOLUME_IDENTITY_BYTES];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), VolumeError> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn acquire_lock(file: File) -> Result<StoreLock, VolumeError> {
    match StoreLock::acquire(file) {
        Ok(lock) => Ok(lock),
        Err(TryLockError::WouldBlock) => Err(VolumeError::Locked),
        Err(TryLockError::Error(error)) => Err(error.into()),
    }
}

fn require_regular_file(path: &Path, object: &'static str) -> Result<(), VolumeError> {
    if fs::symlink_metadata(path)?.file_type().is_file() {
        Ok(())
    } else {
        Err(VolumeError::NotRegularFile(object))
    }
}

fn require_directory(path: &Path, object: &'static str) -> Result<(), VolumeError> {
    if fs::symlink_metadata(path)?.file_type().is_dir() {
        Ok(())
    } else {
        Err(VolumeError::NotDirectory(object))
    }
}

fn sync_directory(path: &Path) -> Result<(), VolumeError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn id_name(bytes: &[u8; 16]) -> String {
    let mut output = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing into String is infallible");
    }
    output
}

#[cfg(unix)]
fn root_file_identity(path: &Path) -> Result<Option<(u64, u64)>, VolumeError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::metadata(path)?;
    Ok(Some((metadata.dev(), metadata.ino())))
}

#[cfg(not(unix))]
fn root_file_identity(_path: &Path) -> Result<Option<(u64, u64)>, VolumeError> {
    Ok(None)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn validate_mount_policy(path: &Path, policy: MountPolicy) -> Result<(), VolumeError> {
    use rustix::fs::{AtFlags, StatxAttributes, StatxFlags};

    if policy == MountPolicy::PortableIdentity {
        return Ok(());
    }
    let stat = rustix::fs::statx(
        rustix::fs::CWD,
        path,
        AtFlags::NO_AUTOMOUNT | AtFlags::SYMLINK_NOFOLLOW,
        StatxFlags::empty(),
    )
    .map_err(io::Error::from)?;
    if !stat
        .stx_attributes_mask
        .contains(StatxAttributes::MOUNT_ROOT)
    {
        return Err(VolumeError::MountCheckUnsupported);
    }
    if !stat.stx_attributes.contains(StatxAttributes::MOUNT_ROOT) {
        return Err(VolumeError::NotMountPoint);
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn validate_mount_policy(_path: &Path, policy: MountPolicy) -> Result<(), VolumeError> {
    if policy == MountPolicy::RequireMountPoint {
        Err(VolumeError::MountCheckUnsupported)
    } else {
        Ok(())
    }
}

/// Volume identity, provisioning, or group-placement failure.
#[derive(Debug, Error)]
pub enum VolumeError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Group(#[from] DirectoryError),
    #[error("volume identity is zero")]
    ZeroIdentity,
    #[error("volume root is not empty")]
    NotEmpty,
    #[error("volume is already owned by another process")]
    Locked,
    #[error("{0} is not a regular file")]
    NotRegularFile(&'static str),
    #[error("{0} is not a directory")]
    NotDirectory(&'static str),
    #[error("wrong volume identity magic")]
    WrongMagic,
    #[error("unsupported volume identity version {0}")]
    UnsupportedVersion(u16),
    #[error("unsupported volume identity fields")]
    UnsupportedFields,
    #[error("volume identity digest mismatch")]
    DigestMismatch,
    #[error("volume identity has wrong byte length {0}")]
    WrongIdentityBytes(usize),
    #[error("configured volume identity does not match stored identity")]
    IdentityMismatch,
    #[error("volume configuration contains a duplicate identity")]
    DuplicateIdentity,
    #[error("volume roots overlap or alias by canonical path")]
    OverlappingRoots,
    #[error("distinct volume paths identify the same directory inode")]
    AliasedRoots,
    #[error("group identity names another volume")]
    GroupVolumeMismatch,
    #[error("strict mount-point detection is unsupported")]
    MountCheckUnsupported,
    #[error("configured volume root is not a mount point")]
    NotMountPoint,
}
