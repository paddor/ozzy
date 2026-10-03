//! Fixed-size accepted-history evidence, independent of active payload damage.
//!
//! `DURABLE` holds two copies of one checksummed record at fixed offsets, each
//! inside one sector. A publication overwrites both in place under one data
//! barrier. Sector writes are atomic, so a crash leaves each copy with either
//! the new record or the last completed publication. File size and directory
//! entry never change after format.

use super::{
    DirectoryError, GroupDirectory, GroupIdentity, LogPosition, Manifest, MetadataError,
    NoopObserver, OpenGroupJournal, Path, PersistenceObserver, SegmentHeader, WriterError,
    position_regresses, publication,
};
use std::fs::File;

pub(crate) const NAME: &str = "DURABLE";
pub(crate) const RECORD_BYTES: usize = 192;
/// Copies never share a page or filesystem block, so writing one copy cannot
/// rewrite the other's sectors.
pub(crate) const COPY_STRIDE: usize = 64 * 1024;
pub(crate) const FILE_BYTES: usize = 2 * COPY_STRIDE;
const MAGIC: &[u8; 8] = b"OZYDUR\0\0";
const CONTEXT: &str = "ozzy durable accepted evidence v1";

/// Scope and position fields shared with `MEMORY_VOTING`; sequence zero.
pub(crate) fn encode(
    manifest: &Manifest,
    accepted: LogPosition,
) -> Result<[u8; RECORD_BYTES], DirectoryError> {
    encode_record(manifest, accepted, 0)
}

fn encode_record(
    manifest: &Manifest,
    accepted: LogPosition,
    sequence: u64,
) -> Result<[u8; RECORD_BYTES], DirectoryError> {
    accepted.validate()?;
    let mut bytes = [0; RECORD_BYTES];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..24].copy_from_slice(manifest.identity.group_id.as_bytes());
    bytes[24..40].copy_from_slice(manifest.identity.store_id.as_bytes());
    for (offset, value) in [
        (40, manifest.identity.store_generation),
        (48, manifest.configuration_epoch),
        (56, manifest.generation),
        (
            64,
            manifest
                .segments
                .last()
                .ok_or(DirectoryError::MissingActiveSegment)?
                .segment_id,
        ),
        (72, accepted.op_number),
        (144, sequence),
    ] {
        bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
    }
    bytes[80..112].copy_from_slice(accepted.digest.as_bytes());
    bytes[112..128].copy_from_slice(manifest.identity.replica_node_id.as_bytes());
    bytes[128..144].copy_from_slice(manifest.identity.volume_id.as_bytes());
    let digest = ozzy_journal::integrity::hash(CONTEXT, &bytes[..160]);
    bytes[160..].copy_from_slice(digest.as_bytes());
    Ok(bytes)
}

/// Fresh file image: both copies hold the same record.
pub(crate) fn image(manifest: &Manifest, accepted: LogPosition) -> Result<Vec<u8>, DirectoryError> {
    let record = encode(manifest, accepted)?;
    let mut bytes = vec![0; FILE_BYTES];
    for copy in 0..2 {
        bytes[copy * COPY_STRIDE..][..RECORD_BYTES].copy_from_slice(&record);
    }
    Ok(bytes)
}

/// Checksum and scope of one copy. Returns its publication sequence.
fn verify(bytes: &[u8], manifest: &Manifest) -> Result<u64, DirectoryError> {
    let expected = encode(manifest, LogPosition::GENESIS)?;
    if bytes.len() != RECORD_BYTES
        || bytes[..56] != expected[..56]
        || bytes[112..144] != expected[112..144]
        || bytes[152..160] != expected[152..160]
        || bytes[160..] != *ozzy_journal::integrity::hash(CONTEXT, &bytes[..160]).as_bytes()
    {
        return Err(MetadataError::DigestMismatch("durable accepted evidence").into());
    }
    Ok(u64::from_be_bytes(bytes[144..152].try_into().unwrap()))
}

/// Decode one copy's protected position against the selected manifest.
pub(crate) fn decode(bytes: &[u8], manifest: &Manifest) -> Result<LogPosition, DirectoryError> {
    verify(bytes, manifest)?;
    let number = |offset| u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
    let generation = number(56);
    if generation == 0 || generation > manifest.generation {
        return Err(DirectoryError::CurrentMismatch);
    }
    let accepted = LogPosition {
        op_number: number(72),
        digest: crate::Digest::from_bytes(bytes[80..112].try_into().unwrap()),
    }
    .validate()?;
    let expected = encode(manifest, LogPosition::GENESIS)?;
    if bytes[64..72] != expected[64..72] {
        // A selected roll or authorized suffix rewrite uses a fresh active ID.
        // The newer manifest protects sealed bytes or the replacement history.
        if generation == manifest.generation {
            return Err(DirectoryError::CurrentMismatch);
        }
        return Ok(manifest.accepted);
    }
    if accepted.op_number == manifest.accepted.op_number && accepted != manifest.accepted {
        return Err(DirectoryError::CurrentMismatch);
    }
    Ok(if accepted.op_number > manifest.accepted.op_number {
        accepted
    } else {
        manifest.accepted
    })
}

/// The two copies of a `DURABLE` file.
#[derive(Debug)]
pub(crate) struct Copies {
    records: [[u8; RECORD_BYTES]; 2],
    sequences: [Option<u64>; 2],
    newest: usize,
}

impl Copies {
    /// Select the newest intact copy. One damaged or torn copy is tolerated:
    /// a publication completes only after both copies hold its record.
    #[cfg(any(test, feature = "simulation"))]
    pub(crate) fn decode_file(bytes: &[u8], manifest: &Manifest) -> Result<Self, DirectoryError> {
        if bytes.len() != FILE_BYTES {
            return Err(DirectoryError::WrongFileSize {
                object: NAME,
                actual: bytes.len() as u64,
                expected: FILE_BYTES as u64,
            });
        }
        let records = [0, 1].map(|copy| {
            bytes[copy * COPY_STRIDE..][..RECORD_BYTES]
                .try_into()
                .unwrap()
        });
        Self::new(&records, manifest)
    }

    pub(crate) fn new(
        records: &[[u8; RECORD_BYTES]; 2],
        manifest: &Manifest,
    ) -> Result<Self, DirectoryError> {
        let sequences = records.map(|record| verify(&record, manifest).ok());
        let newest = match sequences {
            [None, None] => {
                return Err(MetadataError::DigestMismatch("durable accepted evidence").into());
            }
            [Some(first), Some(second)] if first == second && records[0] != records[1] => {
                return Err(MetadataError::DigestMismatch("durable accepted evidence").into());
            }
            [Some(first), Some(second)] => usize::from(second > first),
            [Some(_), None] => 0,
            [None, Some(_)] => 1,
        };
        Ok(Self {
            records: *records,
            sequences,
            newest,
        })
    }

    fn read(root: &Path, manifest: &Manifest) -> Result<Self, DirectoryError> {
        let file = super::open_regular_file(&root.join(NAME), NAME)?;
        Self::new(&read_records(&file)?, manifest)
    }

    pub(crate) fn protected(&self, manifest: &Manifest) -> Result<LogPosition, DirectoryError> {
        decode(&self.records[self.newest], manifest)
    }

    /// Both copies hold the same intact record.
    pub(crate) fn mirrored(&self) -> bool {
        self.sequences.iter().all(Option::is_some) && self.records[0] == self.records[1]
    }

    /// The next record and the copy it overwrites first.
    pub(crate) fn next(
        &self,
        manifest: &Manifest,
        accepted: LogPosition,
    ) -> Result<([u8; RECORD_BYTES], usize), DirectoryError> {
        let sequence = self.sequences[self.newest]
            .and_then(|sequence| sequence.checked_add(1))
            .ok_or(MetadataError::DigestMismatch("durable accepted evidence"))?;
        Ok((
            encode_record(manifest, accepted, sequence)?,
            1 - self.newest,
        ))
    }

    /// Copy the newest record over the other copy.
    pub(crate) fn repair(&self) -> ([u8; RECORD_BYTES], usize) {
        (self.records[self.newest], 1 - self.newest)
    }
}

fn read_records(file: &File) -> Result<[[u8; RECORD_BYTES]; 2], DirectoryError> {
    use std::os::unix::fs::FileExt;
    let actual = file.metadata()?.len();
    if actual != FILE_BYTES as u64 {
        return Err(DirectoryError::WrongFileSize {
            object: NAME,
            actual,
            expected: FILE_BYTES as u64,
        });
    }
    let mut records = [[0; RECORD_BYTES]; 2];
    for (copy, record) in records.iter_mut().enumerate() {
        file.read_exact_at(record, (copy * COPY_STRIDE) as u64)?;
    }
    Ok(records)
}

/// `DURABLE` as the open journal last wrote it, with the file kept open.
/// Recovery reads it once; each installed publication records what it wrote.
/// The store lock makes this journal the file's only writer.
#[derive(Debug)]
pub(crate) struct Evidence {
    records: [[u8; RECORD_BYTES]; 2],
    file: File,
}

impl Evidence {
    /// Open and check `DURABLE` after `restore`. `None` without evidence.
    pub(super) fn open(
        root: &Path,
        manifest: &Manifest,
    ) -> Result<Option<Box<Self>>, DirectoryError> {
        if !manifest.durable_evidence {
            return Ok(None);
        }
        let path = root.join(NAME);
        super::require_regular_file(&path, NAME)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(DirectoryError::NotRegularFile(NAME));
        }
        let records = read_records(&file)?;
        Copies::new(&records, manifest)?.protected(manifest)?;
        Ok(Some(Box::new(Self { records, file })))
    }
}

pub(super) fn protected(root: &Path, manifest: &Manifest) -> Result<LogPosition, DirectoryError> {
    if !manifest.durable_evidence {
        return Ok(manifest.accepted);
    }
    Copies::read(root, manifest)?.protected(manifest)
}

/// Rewrite a damaged, torn, or older copy before the journal opens for writes.
pub(super) fn restore(root: &Path, manifest: &Manifest) -> Result<(), DirectoryError> {
    if !manifest.durable_evidence {
        return Ok(());
    }
    let copies = Copies::read(root, manifest)?;
    copies.protected(manifest)?;
    if copies.mirrored() {
        return Ok(());
    }
    let (record, copy) = copies.repair();
    publication::SyncedOverwrite::write_synced(
        &mut publication::Filesystem(root),
        NAME,
        &[copy * COPY_STRIDE],
        &record,
    )
}

impl GroupDirectory {
    /// Format an absent configured store with mandatory bounded durability evidence.
    ///
    /// This is a new-store operation, never an implicit upgrade of an old voter.
    pub fn format_new_with_durable_evidence(
        root: impl AsRef<Path>,
        identity: GroupIdentity,
        configuration_epoch: u64,
        first_segment: &SegmentHeader,
        configuration: &[u8],
    ) -> Result<Self, DirectoryError> {
        let mut directory = Self::format_new_with_configuration(
            root,
            identity,
            configuration_epoch,
            first_segment,
            configuration,
        )?;
        let bytes = image(&directory.manifest, LogPosition::GENESIS)?;
        publication::replace_evidence(
            &mut publication::Filesystem(&directory.root),
            &bytes,
            &mut NoopObserver,
        )?;
        let mut next = directory.manifest.clone();
        next.generation += 1;
        next.parent_generation = directory.manifest.generation;
        next.durable_evidence = true;
        directory.install_manifest(next, &mut NoopObserver)?;
        Ok(directory)
    }
}

impl OpenGroupJournal {
    pub(super) fn publish_fixed_evidence(
        &mut self,
        observer: &mut impl PersistenceObserver,
    ) -> Result<(), DirectoryError> {
        if self.writer.is_faulted() {
            return Err(WriterError::Faulted.into());
        }
        if !self.directory.manifest.durable_evidence {
            return Err(DirectoryError::CurrentMismatch);
        }
        self.require_roll_published()?;
        let accepted = self.accepted_position()?;
        let manifest = &self.directory.manifest;
        let evidence = self
            .evidence
            .as_mut()
            .ok_or(DirectoryError::CurrentMismatch)?;
        let copies = Copies::new(&evidence.records, manifest)?;
        let protected = copies.protected(manifest)?;
        if position_regresses(protected, accepted) {
            return Err(DirectoryError::HardStateRegression);
        }
        if protected == accepted {
            return Ok(());
        }
        let (bytes, first) = copies.next(manifest, accepted)?;
        publication::overwrite_evidence(
            &mut publication::OpenEvidence(&evidence.file),
            first,
            &bytes,
            observer,
        )?;
        evidence.records = [bytes; 2];
        Ok(())
    }
}

#[cfg(test)]
mod tests;
