//! Descriptors on demand. A handle keeps its descriptor while jobs use it.
//! Once a shard's descriptor budget is spent, the handles of that shard
//! used longest ago give theirs up, and each opens its path again at its
//! next use. The pool follows renames and removals it performs, so a parked
//! handle still names its file; a file whose name is gone keeps its
//! descriptor. A path that holds another file at the next use is an error.
//! Locked files are never parked: closing one would drop the lock.

use super::handles::{OwnedFile, Reservation};
use super::{Files, Target, open_file};
use crate::Shared;
use ozzy_io::{HandleToken, Outcome, lock_unpoisoned};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// The file a descriptor names, to tell it from another at the same path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Identity {
    device: u64,
    inode: u64,
}

impl Identity {
    #[cfg(unix)]
    pub(super) fn of(metadata: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }

    #[cfg(not(unix))]
    pub(super) fn of(_: &std::fs::Metadata) -> Self {
        Self {
            device: 0,
            inode: 0,
        }
    }
}

/// How to open a parked handle's file again.
#[derive(Debug)]
pub(super) struct Origin {
    pub(super) path: PathBuf,
    pub(super) target: Target,
    pub(super) identity: Identity,
}

#[derive(Debug)]
struct State {
    /// None while parked.
    file: Option<Arc<OwnedFile>>,
    /// None once the file lost its name: it cannot be opened again.
    origin: Option<Origin>,
}

/// One open handle. Lock order: the file table, then `state`. Nothing takes
/// the table while it holds a `state`.
#[derive(Debug)]
pub(super) struct Opened {
    pub(super) token: HandleToken,
    pub(super) direct: bool,
    shard: usize,
    /// The pool's number of the last use.
    used: AtomicU64,
    state: Mutex<State>,
}

impl Opened {
    pub(super) fn new(
        file: OwnedFile,
        origin: Origin,
        token: HandleToken,
        (shard, now): (usize, u64),
    ) -> Self {
        Self {
            token,
            direct: origin.target.direct,
            shard,
            used: AtomicU64::new(now),
            state: Mutex::new(State {
                file: Some(Arc::new(file)),
                origin: Some(origin),
            }),
        }
    }

    /// Give up the descriptor for good, as the handle closes.
    pub(super) fn retire(&self) -> Option<Arc<OwnedFile>> {
        lock_unpoisoned(&self.state).file.take()
    }

    /// The path the handle would open again.
    pub(super) fn path(&self) -> Option<PathBuf> {
        let state = lock_unpoisoned(&self.state);
        state.origin.as_ref().map(|origin| origin.path.clone())
    }

    /// When it was last used, if nothing uses it now and it can be opened
    /// again. A busy handle is not idle.
    fn idle(&self) -> Option<u64> {
        let state = self.state.try_lock().ok()?;
        state.parkable().then(|| self.used.load(Ordering::Relaxed))
    }

    /// Give up the descriptor until the next use.
    fn park(&self) -> Option<Arc<OwnedFile>> {
        let mut state = self.state.try_lock().ok()?;
        state.parkable().then(|| state.file.take()).flatten()
    }

    /// The file's name moved from `source` to `destination`, or a directory
    /// above it did.
    fn moved(&self, source: &Path, destination: &Path) {
        let mut state = lock_unpoisoned(&self.state);
        if let Some(origin) = &mut state.origin
            && let Ok(rest) = origin.path.strip_prefix(source)
        {
            origin.path = if rest.as_os_str().is_empty() {
                destination.to_path_buf()
            } else {
                destination.join(rest)
            };
        }
    }

    /// The file is about to lose its name. Hold its descriptor from now on:
    /// open a parked one a last time, with `reservation`. False when it is
    /// parked and the caller brought no reservation yet.
    fn pin(&self, reservation: Option<io::Result<Reservation>>) -> bool {
        let mut state = lock_unpoisoned(&self.state);
        if state.file.is_none() {
            let Some(reservation) = reservation else {
                return false;
            };
            // A handle that cannot open here fails at its next use.
            let _ = reservation.and_then(|reservation| state.reopen(reservation));
        }
        state.origin = None;
        true
    }
}

impl State {
    fn parkable(&self) -> bool {
        // Jobs clone the descriptor under this lock, so one reference
        // means no job uses it.
        self.origin.is_some()
            && self
                .file
                .as_ref()
                .is_some_and(|file| Arc::strong_count(file) == 1 && !file.locked())
    }

    fn reopen(&mut self, reservation: Reservation) -> io::Result<Arc<OwnedFile>> {
        let origin = self
            .origin
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "closed file handle"))?;
        let (file, identity) = open_file(&origin.path, origin.target.again())?;
        if identity != origin.identity {
            return Err(io::Error::other("another file took a parked handle's path"));
        }
        let file = Arc::new(OwnedFile::new(file, reservation));
        self.file = Some(file.clone());
        Ok(file)
    }
}

/// The handle's descriptor, opened again if it was parked.
pub(super) fn file(shared: &Shared, opened: &Opened) -> io::Result<Arc<OwnedFile>> {
    opened.used.store(shared.now(), Ordering::Relaxed);
    if let Some(file) = &lock_unpoisoned(&opened.state).file {
        return Ok(file.clone());
    }
    let reservation = reserve(shared, opened.shard)?;
    // A rename or removal in progress holds the other side.
    let _paths = shared.paths.read().unwrap_or_else(PoisonError::into_inner);
    let mut state = lock_unpoisoned(&opened.state);
    match &state.file {
        // Another job opened it meanwhile.
        Some(file) => Ok(file.clone()),
        None => state.reopen(reservation),
    }
}

/// Room for one more descriptor on the shard `lane`, parking idle handles
/// for it.
pub(super) fn reserve(shared: &Shared, lane: usize) -> io::Result<Reservation> {
    let budget = shared.files().budget.clone();
    loop {
        let full = match budget.reserve(lane) {
            Ok(reservation) => return Ok(reservation),
            Err(full) => full,
        };
        let parked = shared.files().park(lane);
        if parked.is_empty() {
            return Err(full);
        }
        // Closing may block. The table lock is released by now.
        drop(parked);
    }
}

/// Rename through the pool, so parked handles follow their file.
pub(super) fn rename(shared: &Shared, source: &Path, destination: &Path) -> io::Result<()> {
    let _paths = shared.paths.write().unwrap_or_else(PoisonError::into_inner);
    let directory = std::fs::symlink_metadata(source)?.is_dir();
    // A file at the destination loses its name, once the source exists.
    pin(shared, destination);
    std::fs::rename(source, destination)?;
    shared.files().moved(source, destination, directory);
    Ok(())
}

/// Remove a file or an empty directory through the pool.
pub(super) fn remove(shared: &Shared, path: &Path, directory: bool) -> io::Result<Outcome> {
    let _paths = shared.paths.write().unwrap_or_else(PoisonError::into_inner);
    pin(shared, path);
    if directory {
        std::fs::remove_dir(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(Outcome::Done)
}

fn pin(shared: &Shared, path: &Path) {
    let handles = shared.files().forget(path);
    for opened in handles {
        if !opened.pin(None) {
            opened.pin(Some(reserve(shared, opened.shard)));
        }
    }
}

impl Files {
    /// Park the handles of `shard` used longest ago, an eighth of its
    /// budget at once, and return their descriptors for the caller to close.
    fn park(&mut self, shard: usize) -> Vec<Arc<OwnedFile>> {
        let batch = (self.budget.share(shard) / 8).max(1);
        let mut idle: Vec<_> = self
            .entries
            .values()
            .filter_map(|slot| slot.open.as_ref())
            .filter(|opened| opened.shard == shard)
            .filter_map(|opened| Some((opened.idle()?, opened)))
            .collect();
        if idle.len() > batch {
            idle.select_nth_unstable_by_key(batch - 1, |&(used, _)| used);
            idle.truncate(batch);
        }
        idle.into_iter()
            .filter_map(|(_, opened)| opened.park())
            .collect()
    }

    /// Descriptors `shard` holds now.
    #[cfg(test)]
    pub(crate) fn held(&self, shard: usize) -> usize {
        self.budget.used(shard)
    }

    /// Handles in the table.
    #[cfg(test)]
    pub(crate) fn open(&self) -> usize {
        self.entries.len()
    }

    /// Remember that the handle `key` names the file at `path`.
    pub(super) fn name(&mut self, key: u64, path: &Path) {
        self.paths.entry(path.to_path_buf()).or_default().push(key);
    }

    /// Forget the path of the closing handle `key`.
    pub(super) fn unname(&mut self, key: u64, opened: &Opened) {
        let Some(path) = opened.path() else {
            return;
        };
        if let Some(keys) = self.paths.get_mut(&path) {
            keys.retain(|named| *named != key);
            if keys.is_empty() {
                self.paths.remove(&path);
            }
        }
    }

    /// Take the handles that name `path` out of the path index.
    fn forget(&mut self, path: &Path) -> Vec<Arc<Opened>> {
        let keys = self.paths.remove(path).unwrap_or_default();
        keys.iter()
            .filter_map(|key| self.entries.get(key)?.open.clone())
            .collect()
    }

    /// Follow a rename: the handles at `source`, and below it if it is a
    /// directory, now name their files at `destination`.
    fn moved(&mut self, source: &Path, destination: &Path, directory: bool) {
        let sources: Vec<PathBuf> = if directory {
            let below = |path: &&PathBuf| path.starts_with(source);
            self.paths.keys().filter(below).cloned().collect()
        } else {
            vec![source.to_path_buf()]
        };
        for path in sources {
            let Some(keys) = self.paths.remove(&path) else {
                continue;
            };
            for key in &keys {
                if let Some(opened) = self.entries.get(key).and_then(|slot| slot.open.as_ref()) {
                    opened.moved(source, destination);
                }
            }
            let renamed = match path.strip_prefix(source) {
                Ok(rest) if !rest.as_os_str().is_empty() => destination.join(rest),
                _ => destination.to_path_buf(),
            };
            self.paths.entry(renamed).or_default().extend(keys);
        }
    }
}
