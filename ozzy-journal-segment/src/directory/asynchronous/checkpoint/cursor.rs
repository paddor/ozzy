use super::{Checkpoint, CheckpointFiles};
use crate::{CheckpointError, checkpoint::asynchronous::Reader};
use std::{io, ops::Deref, rc::Rc};

#[derive(Debug)]
struct Captured(Rc<CheckpointFiles>);

impl Deref for Captured {
    type Target = Checkpoint;

    fn deref(&self) -> &Self::Target {
        &self.0.checkpoint
    }
}

/// Detached, bounded checkpoint transfer input with captured deletion protection.
/// Sequential requests read and hash each physical chunk once. Other offsets use
/// independently verified ranges without changing sequential progress. A fresh
/// cursor verifies the prefix before continuing at a nonzero starting offset.
#[derive(Debug)]
pub struct CheckpointCursor {
    reader: Reader<Captured>,
    files: Rc<CheckpointFiles>,
    through: u64,
    interrupted: bool,
}

impl CheckpointCursor {
    pub(super) fn new(files: Rc<CheckpointFiles>) -> Self {
        Self {
            reader: Reader::new(Captured(files.clone()), files.limits),
            files,
            through: 0,
            interrupted: false,
        }
    }

    /// Cursor metadata only. Captured file metadata and returned fragments have
    /// their own charges; the cursor retains no response payloads.
    pub const fn retained_bytes() -> usize {
        std::mem::size_of::<Self>()
    }

    /// True after sequential transfer verifies every chunk and the full image.
    pub const fn is_verified(&self) -> bool {
        self.reader.verified()
    }

    /// Return one bounded fragment. Prefixes have no complete-image authority.
    /// A failed or canceled call poisons this cursor; start a fresh capture cursor
    /// to retry. Invalid bounds leave an otherwise healthy cursor usable.
    pub async fn read(&mut self, offset: u64, maximum: usize) -> Result<Vec<u8>, CheckpointError> {
        if self.interrupted {
            return Err(
                io::Error::new(io::ErrorKind::BrokenPipe, "interrupted checkpoint cursor").into(),
            );
        }
        if maximum == 0 || offset >= self.files.manifest().state_bytes {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        let maximum = maximum.min(self.files.fragment_bytes());
        self.interrupted = true;
        if self.through == 0 {
            while self.through < offset {
                let count =
                    (offset - self.through).min(self.files.fragment_bytes() as u64) as usize;
                let bytes = self.reader.next(count).await?.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "checkpoint cursor exhausted")
                })?;
                self.through += bytes.len() as u64;
            }
        }
        let bytes = if offset == self.through {
            let bytes = self.reader.next(maximum).await?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "checkpoint cursor exhausted")
            })?;
            self.through += bytes.len() as u64;
            bytes
        } else {
            self.files.read_range(offset, maximum).await?
        };
        self.interrupted = false;
        Ok(bytes)
    }
}
