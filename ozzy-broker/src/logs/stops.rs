//! Evidence of clean stops, kept across a broker restart. A shard that
//! stops writes which of its replicated logs it led with everything
//! durable. The next start takes the evidence of a whole device at once and
//! removes it before any log opens, so no later start finds it again.

use super::registry::{list, open, remove_file, run, sync_directory};
use crate::{StartupError, io_error};
use ozzy_io::{FileKind, Local, OpenMode, Operation, Outcome, SyncMode, WriteBuffer};
use ozzy_proto::GroupId;
use ozzy_replication::{Digest, OpNumber, Prefix, driver::CleanStop};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
};

/// The directory below a device's log directory that holds the evidence.
const DIRECTORY: &str = "stops";
const STAGED: &str = "staged";
const MAGIC: &[u8; 8] = b"OZZYSTOP";
/// Group, view, operation number, and digest.
const ENTRY_BYTES: usize = 16 + 8 + 8 + 32;

/// The clean stops of the logs of one device, by log.
pub(crate) type Stops = BTreeMap<GroupId, CleanStop>;

fn encode(stops: &[(GroupId, CleanStop)]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(MAGIC.len() + stops.len() * ENTRY_BYTES);
    bytes.extend_from_slice(MAGIC);
    for (group, stop) in stops {
        bytes.extend_from_slice(group.as_bytes());
        bytes.extend_from_slice(&stop.view.to_le_bytes());
        bytes.extend_from_slice(&stop.accepted.op.0.to_le_bytes());
        bytes.extend_from_slice(stop.accepted.digest.as_bytes());
    }
    bytes
}

/// The stops in `bytes`. A file that is not whole holds none: the logs it
/// names take the ordinary election.
fn decode(bytes: &[u8], stops: &mut Stops) {
    let Some(entries) = bytes.strip_prefix(MAGIC) else {
        return;
    };
    let (entries, cut) = entries.as_chunks::<ENTRY_BYTES>();
    if !cut.is_empty() {
        return;
    }
    for entry in entries {
        let (group, rest) = entry.split_at(16);
        let (view, rest) = rest.split_at(8);
        let (op, digest) = rest.split_at(8);
        let (Ok(group), Ok(view), Ok(op), Ok(digest)) = (
            group.try_into(),
            view.try_into(),
            op.try_into(),
            digest.try_into(),
        ) else {
            return;
        };
        stops.insert(
            GroupId::from_bytes(group),
            CleanStop {
                view: u64::from_le_bytes(view),
                accepted: Prefix {
                    op: OpNumber(u64::from_le_bytes(op)),
                    digest: Digest::from_bytes(digest),
                },
            },
        );
    }
}

/// Publish the clean stops of `shard` below `directory`: staged, synced,
/// renamed, and the directory synced.
pub(crate) async fn publish(
    io: &Local,
    directory: &Path,
    shard: u32,
    stops: &[(GroupId, CleanStop)],
) -> Result<(), StartupError> {
    let directory = directory.join(DIRECTORY);
    crate::format::initialize_directory(io, &directory).await?;
    let path = directory.join(shard.to_string());
    let staged = directory.join(format!("{shard}.{STAGED}"));
    remove_file(io, &staged).await?;
    let create = Operation::Open {
        path: staged.clone(),
        mode: OpenMode::CreateNew,
        direct: false,
        data_sync: false,
    };
    let handle = open(io, &staged, create).await?;
    let encoded = encode(stops);
    let length = encoded.len();
    let write = Operation::Write {
        handle: handle.clone(),
        offset: 0,
        data: WriteBuffer::from_vec(encoded),
    };
    let written = match run(io, &staged, write).await {
        Ok(completed) if matches!(*completed, Outcome::Written(bytes) if bytes == length) => {
            let sync = Operation::Sync {
                handle: handle.clone(),
                mode: SyncMode::All,
            };
            run(io, &staged, sync).await.map(|_| ())
        }
        Ok(_) => Err(io_error(&staged, io::ErrorKind::WriteZero.into())),
        Err(error) => Err(error),
    };
    run(io, &staged, Operation::Close { handle }).await?;
    written?;
    let rename = Operation::Rename {
        source: staged,
        destination: path.clone(),
    };
    run(io, &path, rename).await?;
    sync_directory(io, &directory).await
}

/// Read one evidence file of at most `limit` bytes. A longer one is cut,
/// and then holds nothing.
async fn read(io: &Local, path: &Path, limit: usize) -> Result<Vec<u8>, StartupError> {
    let operation = Operation::Open {
        path: path.into(),
        mode: OpenMode::Read,
        direct: false,
        data_sync: false,
    };
    let handle = open(io, path, operation).await?;
    let read = Operation::Read {
        handle: handle.clone(),
        offset: 0,
        length: limit + 1,
    };
    // Copy the bytes out and release the read before the close.
    let bytes = run(io, path, read).await.and_then(|read| match &*read {
        Outcome::Read(buffer) => Ok(buffer.as_slice().to_vec()),
        _ => Err(io_error(path, io::ErrorKind::InvalidData.into())),
    });
    run(io, path, Operation::Close { handle }).await?;
    bytes
}

/// Take every clean stop recorded below `directory`, from at most
/// `max_logs` logs a file. The evidence is removed, durably, before this
/// returns: a log that opens with it may send again, and a crash after
/// that must not find it.
pub(crate) async fn take(
    io: &Local,
    directory: &Path,
    max_logs: usize,
) -> Result<Stops, StartupError> {
    let directory = directory.join(DIRECTORY);
    let mut stops = Stops::new();
    let Some(entries) = list(io, &directory, 4096).await? else {
        return Ok(stops);
    };
    let files: Vec<PathBuf> = entries
        .iter()
        .filter(|(_, kind)| *kind == FileKind::File)
        .map(|(name, _)| directory.join(name))
        .collect();
    if files.is_empty() {
        return Ok(stops);
    }
    let limit = MAGIC.len() + max_logs.saturating_mul(ENTRY_BYTES);
    for path in &files {
        if path.extension().is_none() {
            decode(&read(io, path, limit).await?, &mut stops);
        }
    }
    for path in &files {
        remove_file(io, path).await?;
    }
    sync_directory(io, &directory).await?;
    Ok(stops)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stop(view: u64, op: u64) -> CleanStop {
        CleanStop {
            view,
            accepted: Prefix {
                op: OpNumber(op),
                digest: Digest::from_bytes([op.to_le_bytes()[0]; 32]),
            },
        }
    }

    #[test]
    fn whole_evidence_decodes_and_cut_evidence_holds_nothing() {
        let written = [
            (GroupId::from_bytes([1; 16]), stop(0, 1)),
            (GroupId::from_bytes([2; 16]), stop(7, 900)),
        ];
        let bytes = encode(&written);
        let mut stops = Stops::new();
        decode(&bytes, &mut stops);
        assert_eq!(stops.into_iter().collect::<Vec<_>>(), written);
        for cut in [&bytes[..bytes.len() - 1], &bytes[1..], &[][..]] {
            let mut stops = Stops::new();
            decode(cut, &mut stops);
            assert!(stops.is_empty());
        }
    }
}
