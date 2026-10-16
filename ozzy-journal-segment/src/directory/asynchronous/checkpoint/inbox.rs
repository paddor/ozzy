//! Private checkpoint input backed by one storage-owned file, never a full image vector.

use crate::checkpoint::{CHECKPOINT_STATE_HASH_CONTEXT, CheckpointError, enforce_u64};
use crate::{Digest, GroupIdentity, async_files::Access, checkpoint_name};
use ozzy_io::{Handle, OpenMode, Operation};
use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_proto::CheckpointId;
use std::{
    io,
    path::{Path, PathBuf},
};
mod read;
pub use read::Stream;

/// One unselected checkpoint receive attempt. Bytes use bounded backend jobs;
/// successful receipt supplies integrity, never recovery or voting authority.
#[derive(Debug)]
pub struct Inbox {
    access: Access,
    root: PathBuf,
    path: PathBuf,
    identity: GroupIdentity,
    file: Handle,
    bytes: u64,
    through: u64,
    expected: Digest,
    hash: Hasher,
    fragment_bytes: usize,
    interrupted: bool,
}

impl Inbox {
    pub(crate) async fn open(
        journal: &crate::GroupJournal,
        id: CheckpointId,
        bytes: u64,
        expected: Digest,
    ) -> Result<Self, CheckpointError> {
        if bytes == 0 || *id.as_bytes() == [0; 16] {
            return Err(CheckpointError::InvalidSpec);
        }
        enforce_u64(
            "checkpoint receive bytes",
            bytes,
            journal.limits.checkpoint.max_state_bytes,
        )?;
        let root = journal.root().to_owned();
        for sequence in 0..1024 {
            let path = root.join("staging").join(format!(
                ".checkpoint-{}-{}-{sequence}.tmp",
                checkpoint_name(id),
                journal.manifest.generation,
            ));
            match journal
                .access
                .open(path.clone(), OpenMode::CreateNew, false, false)
                .await
            {
                Ok(file) => {
                    return Ok(Self {
                        access: journal.access.clone(),
                        root,
                        path,
                        identity: journal.manifest.identity,
                        file,
                        bytes,
                        through: 0,
                        expected,
                        hash: Hasher::new(CHECKPOINT_STATE_HASH_CONTEXT),
                        fragment_bytes: journal.limits.io.chunk_bytes,
                        interrupted: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(CheckpointError::StagingConflict)
    }

    /// Maximum input/read fragment; independent of complete image size.
    pub const fn fragment_bytes(&self) -> usize {
        self.fragment_bytes
    }

    /// Exact declared checkpoint bytes.
    pub const fn state_bytes(&self) -> u64 {
        self.bytes
    }

    /// Successfully written prefix, never advanced by canceled physical work.
    pub const fn through(&self) -> u64 {
        self.through
    }

    /// True only after the complete declared input matches its original digest.
    pub fn is_complete(&self) -> bool {
        !self.interrupted && self.through == self.bytes && self.hash.finish() == self.expected
    }

    /// Resident metadata only; input buffers and physical jobs have separate charges.
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.path.capacity() + self.root.capacity()
    }

    /// Append the exact next bounded range. A failed/canceled physical write
    /// poisons this attempt; an uncertain prefix cannot be retried in place.
    pub async fn push(&mut self, offset: u64, bytes: &[u8]) -> Result<(), CheckpointError> {
        self.healthy()?;
        if offset != self.through
            || bytes.is_empty()
            || bytes.len() > self.fragment_bytes
            || bytes.len() as u64 > self.bytes - self.through
        {
            return Err(CheckpointError::LengthMismatch);
        }
        self.interrupted = true;
        self.access.write_all(&self.file, offset, bytes).await?;
        self.hash.update(bytes);
        self.through += bytes.len() as u64;
        if self.through == self.bytes && self.hash.finish() != self.expected {
            return Err(CheckpointError::DigestMismatch("checkpoint receive"));
        }
        self.interrupted = false;
        Ok(())
    }

    /// Scan complete private input through bounded reads and verify physical
    /// bytes again. Prefixes supply no complete-image integrity or authority.
    pub fn stream(&self) -> Result<Stream<'_>, CheckpointError> {
        self.healthy()?;
        if !self.is_complete() {
            return Err(CheckpointError::LengthMismatch);
        }
        Ok(Stream::new(self))
    }

    pub(crate) fn require_owner(
        &self,
        root: &Path,
        identity: GroupIdentity,
    ) -> Result<(), CheckpointError> {
        if self.root != root || self.identity != identity || !self.is_complete() {
            return Err(CheckpointError::IdentityMismatch);
        }
        Ok(())
    }

    pub(crate) const fn digest(&self) -> Digest {
        self.expected
    }

    /// Remove healthy private staging, including an intentionally abandoned
    /// prefix. Interrupted attempts are cleaned by the journal's next open.
    pub async fn discard(self) -> Result<(), CheckpointError> {
        self.healthy()?;
        self.access
            .done(Operation::Close {
                handle: self.file.clone(),
            })
            .await?;
        self.access
            .done(Operation::RemoveFile {
                path: self.path.clone(),
            })
            .await?;
        self.access
            .sync_directory(self.root.join("staging"))
            .await?;
        Ok(())
    }

    fn healthy(&self) -> Result<(), CheckpointError> {
        if self.interrupted {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "interrupted checkpoint receive").into())
        } else {
            Ok(())
        }
    }
}
