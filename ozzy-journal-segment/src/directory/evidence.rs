//! Fixed-size accepted-history evidence, independent of active payload damage.
//!
//! `DURABLE` holds two copies of one checksummed record at fixed offsets, each
//! inside one sector. A publication overwrites both in place under one data
//! barrier. Sector writes are atomic, so a crash leaves each copy with either
//! the new record or the last completed publication. File size and directory
//! entry never change after format.

use super::DirectoryError;
use crate::{LogPosition, Manifest, MetadataError};
use ozzy_proto::layout::Fixed;

pub(crate) const NAME: &str = "DURABLE";
pub(crate) const RECORD_BYTES: usize = 192;
/// Copies never share a page or filesystem block, so writing one copy cannot
/// rewrite the other's sectors.
pub(crate) const COPY_STRIDE: usize = 64 * 1024;
pub(crate) const FILE_BYTES: usize = 2 * COPY_STRIDE;
const MAGIC: &[u8; 8] = b"OZYDUR\0\0";
const CONTEXT: &str = "ozzy durable accepted evidence v1";

/// Scope and position fields; sequence zero.
fn encode(
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
fn verify<'a>(
    bytes: &'a [u8],
    manifest: &Manifest,
) -> Result<&'a [u8; RECORD_BYTES], DirectoryError> {
    let mismatch = || MetadataError::DigestMismatch("durable accepted evidence").into();
    let expected = encode(manifest, LogPosition::GENESIS)?;
    let bytes: &[u8; RECORD_BYTES] = bytes.try_into().map_err(|_| mismatch())?;
    if bytes[..56] != expected[..56]
        || bytes[112..144] != expected[112..144]
        || bytes[152..160] != expected[152..160]
        || bytes[160..] != *ozzy_journal::integrity::hash(CONTEXT, &bytes[..160]).as_bytes()
    {
        return Err(mismatch());
    }
    Ok(bytes)
}

/// Decode one copy's protected position against the selected manifest.
pub(crate) fn decode(bytes: &[u8], manifest: &Manifest) -> Result<LogPosition, DirectoryError> {
    let bytes = verify(bytes, manifest)?;
    let generation = bytes.u64_at::<56>();
    if generation == 0 || generation > manifest.generation {
        return Err(DirectoryError::CurrentMismatch);
    }
    let accepted = LogPosition {
        op_number: bytes.u64_at::<72>(),
        digest: crate::Digest::from_bytes(bytes.array::<80, 32>()),
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
    #[cfg(test)]
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
        let sequences = records.map(|record| {
            let record = verify(&record, manifest).ok()?;
            Some(record.u64_at::<144>())
        });
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
