//! Evidence of clean stops, kept across a broker restart. A shard that
//! stops writes which of its replicated logs it led with everything
//! durable. The next start takes the evidence of a whole device at once and
//! removes it before any log opens, so no later start finds it again.

use super::registry::{list, read_whole, remove_file, sync_directory, write_atomic};
use crate::StartupError;
use ozzy_io::{FileKind, Local};
use ozzy_proto::GroupId;
use ozzy_replication::{Digest, OpNumber, Prefix, driver::CleanStop};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// The directory below a device's log directory that holds the evidence.
const DIRECTORY: &str = "stops";
const STAGED: &str = "staged";
const MAGIC: &[u8; 8] = b"OZZYSTP2";
/// Group, view, and the operation number and digest of the accepted and
/// of the committed prefix.
const ENTRY_BYTES: usize = 16 + 8 + 2 * (8 + 32);

/// The clean stops of the logs of one device, by log.
pub(crate) type Stops = BTreeMap<GroupId, CleanStop>;

fn encode(stops: &[(GroupId, CleanStop)]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(MAGIC.len() + stops.len() * ENTRY_BYTES);
    bytes.extend_from_slice(MAGIC);
    for (group, stop) in stops {
        bytes.extend_from_slice(group.as_bytes());
        bytes.extend_from_slice(&stop.view.to_le_bytes());
        for prefix in [stop.accepted, stop.committed] {
            bytes.extend_from_slice(&prefix.op.0.to_le_bytes());
            bytes.extend_from_slice(prefix.digest.as_bytes());
        }
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
        let (accepted, committed) = rest.split_at(8 + 32);
        let (Ok(group), Ok(view), Some(accepted), Some(committed)) = (
            group.try_into(),
            view.try_into(),
            prefix(accepted),
            prefix(committed),
        ) else {
            return;
        };
        stops.insert(
            GroupId::from_bytes(group),
            CleanStop {
                view: u64::from_le_bytes(view),
                accepted,
                committed,
            },
        );
    }
}

/// An operation number and the digest of the log through it.
fn prefix(bytes: &[u8]) -> Option<Prefix> {
    let (op, digest) = bytes.split_at_checked(8)?;
    Some(Prefix {
        op: OpNumber(u64::from_le_bytes(op.try_into().ok()?)),
        digest: Digest::from_bytes(digest.try_into().ok()?),
    })
}

/// Publish the clean stops of `shard` below `directory`, whole.
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
    write_atomic(io, &directory, staged, &path, encode(stops)).await
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
            // A longer file comes back cut, and then holds nothing.
            decode(&read_whole(io, path, limit).await?, &mut stops);
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
        let prefix = |op: u64| Prefix {
            op: OpNumber(op),
            digest: Digest::from_bytes([op.to_le_bytes()[0]; 32]),
        };
        CleanStop {
            view,
            accepted: prefix(op),
            committed: prefix(op / 2),
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
