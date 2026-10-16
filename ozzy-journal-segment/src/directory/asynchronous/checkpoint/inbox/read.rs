use super::{CHECKPOINT_STATE_HASH_CONTEXT, CheckpointError, Hasher, Inbox};
use std::io;

/// One sequential bounded scan of a complete, unselected receive file. Dropping
/// a scan releases no physical job's buffers or handles; a canceled scan is poisoned.
#[derive(Debug)]
pub struct Stream<'a> {
    inbox: &'a Inbox,
    through: u64,
    hash: Hasher,
    interrupted: bool,
    verified: bool,
}

impl<'a> Stream<'a> {
    pub(super) fn new(inbox: &'a Inbox) -> Self {
        Self {
            inbox,
            through: 0,
            hash: Hasher::new(CHECKPOINT_STATE_HASH_CONTEXT),
            interrupted: false,
            verified: false,
        }
    }

    /// True only after exact file length and its complete original digest match.
    pub const fn is_verified(&self) -> bool {
        self.verified
    }

    /// Read at most the caller's budget and the backend transfer limit.
    /// Failed/canceled reads cannot subsequently complete this same scan.
    pub async fn next(&mut self, maximum: usize) -> Result<Option<Vec<u8>>, CheckpointError> {
        if self.interrupted {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "interrupted checkpoint receive scan",
            )
            .into());
        }
        if maximum == 0 {
            return Err(CheckpointError::LengthMismatch);
        }
        if self.verified {
            return Ok(None);
        }
        self.interrupted = true;
        let bytes = self.read_next(maximum).await?;
        self.interrupted = false;
        Ok(Some(bytes))
    }

    async fn read_next(&mut self, maximum: usize) -> Result<Vec<u8>, CheckpointError> {
        let source = self.inbox;
        if self.through == 0 && source.access.length(&source.file).await? != source.bytes {
            return Err(CheckpointError::LengthMismatch);
        }
        let count = maximum
            .min(source.fragment_bytes)
            .min((source.bytes - self.through).min(usize::MAX as u64) as usize);
        let bytes = source
            .access
            .read_range(&source.file, self.through, count, source.fragment_bytes)
            .await?;
        self.hash.update(&bytes);
        self.through += bytes.len() as u64;
        if self.through == source.bytes {
            if source.access.length(&source.file).await? != source.bytes {
                return Err(CheckpointError::LengthMismatch);
            }
            if self.hash.finish() != source.expected {
                return Err(CheckpointError::DigestMismatch("checkpoint receive file"));
            }
            self.verified = true;
        }
        Ok(bytes)
    }
}
