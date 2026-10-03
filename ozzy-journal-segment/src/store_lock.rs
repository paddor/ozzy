//! OS ownership released with the last local lease, not an unrelated child FD.

use std::fs::{File, TryLockError};

/// Share this guard, not its file, when detached work must retain ownership.
#[derive(Debug)]
pub(crate) struct StoreLock {
    file: File,
    process: u32,
}

impl StoreLock {
    pub(crate) fn acquire(file: File) -> Result<Self, TryLockError> {
        file.try_lock()?;
        Ok(Self {
            file,
            process: std::process::id(),
        })
    }
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        // flock survives close while a forked child retains the same open file
        // description, even with CLOEXEC. Release explicitly on the last local
        // lease. A copied post-fork guard must never unlock its parent's store.
        if self.process == std::process::id() {
            // Drop cannot report errors. Closing the owned file remains the
            // fallback; failure cannot authorize another writer prematurely.
            let _ = self.file.unlock();
        }
    }
}
