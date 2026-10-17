use super::{Effect, Image, ImageLimits};
use crate::{
    FileKind, Handle, HandleOwner, HandleToken, Metadata, OpenMode, Operation, Outcome, ReadBuffer,
    WriteBuffer,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::Path,
};

#[derive(Debug)]
struct Opened {
    inode: u64,
    origin: usize,
    token: HandleToken,
    writable: bool,
    direct: bool,
    data_sync: bool,
}

/// How a file handle opens; directory handles have no options.
#[derive(Clone, Copy, Debug)]
struct FileOptions {
    mode: OpenMode,
    direct: bool,
    data_sync: bool,
}

impl FileOptions {
    const DIRECTORY: Self = Self {
        mode: OpenMode::Read,
        direct: false,
        data_sync: false,
    };
}

#[derive(Debug)]
pub(super) struct Files {
    owner: HandleOwner,
    next: u64,
    handles: BTreeMap<u64, Opened>,
    locks: BTreeMap<u64, u64>,
    limit: usize,
    used: Vec<usize>,
    closed: bool,
    orphans: BTreeSet<u64>,
}

impl Files {
    pub(super) fn new(owner: HandleOwner, limit: usize, shards: usize) -> Self {
        Self {
            owner,
            next: 0,
            handles: BTreeMap::new(),
            locks: BTreeMap::new(),
            limit,
            used: vec![0; shards],
            closed: false,
            orphans: BTreeSet::new(),
        }
    }

    pub(super) fn reclaim(&mut self) {
        let dead: Vec<_> = self
            .handles
            .iter()
            .filter_map(|(key, open)| (!open.token.is_alive()).then_some(*key))
            .collect();
        for key in dead {
            self.remove(key);
        }
    }

    pub(super) fn clear(&mut self) {
        self.closed |= !self.orphans.is_empty();
        self.handles.clear();
        self.locks.clear();
        self.used.fill(0);
    }

    pub(super) fn inodes(&self) -> impl Iterator<Item = u64> + '_ {
        self.handles.values().map(|open| open.inode)
    }

    pub(super) fn directory(&self, image: &Image, handle: &Handle) -> bool {
        self.get(handle)
            .is_ok_and(|open| image.directory_changed(open.inode))
    }

    pub(super) fn retain_orphans(&mut self, inodes: BTreeSet<u64>) {
        self.orphans = inodes;
    }

    pub(super) fn take_closed(&mut self) -> bool {
        std::mem::take(&mut self.closed)
    }

    fn remove(&mut self, key: u64) -> Option<Opened> {
        let open = self.handles.remove(&key)?;
        self.closed |= self.orphans.contains(&open.inode);
        self.used[open.origin] -= 1;
        if self.locks.get(&open.inode) == Some(&key) {
            self.locks.remove(&open.inode);
        }
        Some(open)
    }

    fn key(&self, handle: &Handle) -> io::Result<u64> {
        self.owner
            .key(handle)
            .ok_or_else(|| io::ErrorKind::InvalidInput.into())
    }

    fn get(&self, handle: &Handle) -> io::Result<&Opened> {
        self.handles
            .get(&self.key(handle)?)
            .ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    fn writable(&self, handle: &Handle) -> io::Result<&Opened> {
        let open = self.get(handle)?;
        if !open.writable {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        Ok(open)
    }

    /// Whether `origin` is below its open-handle share: the limit divides
    /// evenly across shards, and the lowest shards take the remainder.
    fn has_room(&self, origin: usize) -> bool {
        let shards = self.used.len();
        let maximum = self.limit / shards + usize::from(origin < self.limit % shards);
        self.used[origin] < maximum
    }
}

/// One physical job against the device: the handle table, the image and its
/// bounds, the submitting shard, and the effect the schedule chose.
pub(super) struct Execution<'a> {
    pub(super) files: &'a mut Files,
    pub(super) image: &'a mut Image,
    pub(super) limits: ImageLimits,
    pub(super) origin: usize,
    pub(super) effect: Effect,
}

impl Execution<'_> {
    pub(super) fn execute(&mut self, operation: Operation) -> io::Result<Outcome> {
        if let Effect::FailBefore(error) = self.effect {
            return Err(error.into());
        }
        if let Operation::Protected { operation, handles } = operation {
            for handle in &handles {
                self.files.get(handle)?;
            }
            let result = self.execute(*operation);
            drop(handles);
            return result;
        }
        let result = self.apply(operation)?;
        match self.effect {
            Effect::FailAfter(error) | Effect::WriteThenError { error, .. } => Err(error.into()),
            _ => Ok(result),
        }
    }

    /// Handle operations go through the handle table; namespace operations
    /// act on the image by path.
    fn apply(&mut self, operation: Operation) -> io::Result<Outcome> {
        match operation {
            Operation::Protected { .. } => unreachable!("protection handled before execution"),
            Operation::Open {
                path,
                mode,
                direct,
                data_sync,
            } => self.open(
                &path,
                Some(FileOptions {
                    mode,
                    direct,
                    data_sync,
                }),
            ),
            Operation::OpenDirectory { path } => self.open(&path, None),
            Operation::Read {
                handle,
                offset,
                length,
            } => self.read(&handle, offset, length),
            Operation::Write {
                handle,
                offset,
                data,
            } => self.write(&handle, offset, &data),
            Operation::Metadata { handle } => self.metadata(&handle),
            Operation::SetLength { handle, length } => self.set_length(&handle, length),
            Operation::Allocate {
                handle,
                offset,
                length,
            } => self.allocate(&handle, offset, length),
            Operation::Sync { handle, .. } => self.sync(&handle),
            Operation::LockExclusive { handle } => self.lock(&handle),
            Operation::Close { handle } => {
                let key = self.files.key(&handle)?;
                self.files.remove(key).ok_or(io::ErrorKind::NotFound)?;
                Ok(Outcome::Done)
            }
            namespace @ (Operation::ReadDirectory { .. }
            | Operation::CreateDirectory { .. }
            | Operation::Rename { .. }
            | Operation::HardLink { .. }
            | Operation::RemoveFile { .. }
            | Operation::RemoveDirectory { .. }) => self.namespace(namespace),
        }
    }

    fn namespace(&mut self, operation: Operation) -> io::Result<Outcome> {
        match operation {
            Operation::ReadDirectory {
                path,
                max_entries,
                max_name_bytes,
            } => {
                let entries = self.image.list(&path, max_entries, max_name_bytes)?;
                return Ok(Outcome::Directory(entries));
            }
            Operation::CreateDirectory { path } => {
                self.image.create(&path, true, self.limits)?;
            }
            Operation::Rename {
                source,
                destination,
            } => self.image.rename(&source, &destination)?,
            Operation::HardLink {
                source,
                destination,
            } => self.image.link(&source, &destination, self.limits)?,
            Operation::RemoveFile { path } => self.image.remove(&path, false)?,
            Operation::RemoveDirectory { path } => self.image.remove(&path, true)?,
            _ => unreachable!("apply routes only namespace operations here"),
        }
        Ok(Outcome::Done)
    }

    fn metadata(&self, handle: &Handle) -> io::Result<Outcome> {
        let inode = self.files.get(handle)?.inode;
        let kind = self.image.kind(inode)?;
        let length = if kind == FileKind::File {
            self.image.file(inode, false)?.len() as u64
        } else {
            0
        };
        Ok(Outcome::Metadata(Metadata { kind, length }))
    }

    fn set_length(&mut self, handle: &Handle, length: u64) -> io::Result<Outcome> {
        let inode = self.files.writable(handle)?.inode;
        let length = usize::try_from(length).map_err(|_| io::ErrorKind::InvalidInput)?;
        self.image.set_length(inode, length, false, self.limits)?;
        Ok(Outcome::Done)
    }

    fn sync(&mut self, handle: &Handle) -> io::Result<Outcome> {
        let inode = self.files.get(handle)?.inode;
        if !matches!(self.effect, Effect::SkipBarrier) {
            self.image.sync(inode, self.limits)?;
        }
        Ok(Outcome::Done)
    }

    fn allocate(&mut self, handle: &Handle, offset: u64, length: u64) -> io::Result<Outcome> {
        let inode = self.files.writable(handle)?.inode;
        self.image.file(inode, false)?;
        let file_bytes = self.limits.file_bytes;
        let Some(end) = offset
            .checked_add(length)
            .and_then(|end| usize::try_from(end).ok())
            .filter(|&end| end <= file_bytes)
        else {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                format!(
                    "simulation allocation {offset} + {length} exceeds {file_bytes} file bytes"
                ),
            ));
        };
        // KEEP_SIZE changes no readable bytes. Allocation failure is also an
        // explicit fault event; this model does not emulate extent accounting.
        // Reserving memory keeps appends into the range from copying the file.
        self.image.reserve(inode, end)?;
        Ok(Outcome::Done)
    }

    fn lock(&mut self, handle: &Handle) -> io::Result<Outcome> {
        let inode = self.files.get(handle)?.inode;
        let key = self.files.key(handle)?;
        if self
            .files
            .locks
            .get(&inode)
            .is_some_and(|owner| *owner != key)
        {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.files.locks.insert(inode, key);
        Ok(Outcome::Done)
    }

    fn open(&mut self, path: &Path, file: Option<FileOptions>) -> io::Result<Outcome> {
        let origin = self.origin;
        self.files.reclaim();
        if !self.files.has_room(origin) {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let key = self.files.next;
        let next = key.checked_add(1).ok_or(io::ErrorKind::StorageFull)?;
        let inode = if file.is_some_and(|file| file.mode == OpenMode::CreateNew) {
            self.image.create(path, false, self.limits)?
        } else {
            self.image.resolve(path, false)?
        };
        let expected = if file.is_some() {
            FileKind::File
        } else {
            FileKind::Directory
        };
        if self.image.kind(inode)? != expected {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let options = file.unwrap_or(FileOptions::DIRECTORY);
        // Reclamation is explicit at the controller's next scheduling point.
        let (handle, token) = self.files.owner.create(key, futures::task::noop_waker());
        self.files.handles.insert(
            key,
            Opened {
                inode,
                origin,
                token,
                writable: options.mode != OpenMode::Read,
                direct: options.direct,
                data_sync: options.data_sync,
            },
        );
        self.files.next = next;
        self.files.used[origin] += 1;
        Ok(Outcome::Opened(handle))
    }

    fn read(&self, handle: &Handle, offset: u64, length: usize) -> io::Result<Outcome> {
        let open = self.files.get(handle)?;
        if open.direct {
            return Err(io::ErrorKind::Unsupported.into());
        }
        let file = self.image.file(open.inode, false)?;
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(file.len());
        let mut count = length.min(file.len() - start);
        if let Effect::Short(limit) = self.effect {
            count = count.min(limit);
        }
        let mut bytes = vec![0; length].into_boxed_slice();
        bytes[..count].copy_from_slice(&file[start..start + count]);
        Ok(Outcome::Read(ReadBuffer::new(bytes, count)?))
    }

    fn write(&mut self, handle: &Handle, offset: u64, data: &WriteBuffer) -> io::Result<Outcome> {
        let open = self.files.writable(handle)?;
        if open.direct && (!offset.is_multiple_of(4096) || !data.len().is_multiple_of(4096)) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let (inode, data_sync) = (open.inode, open.data_sync);
        let count = match self.effect {
            Effect::Short(n) | Effect::WriteThenError { bytes: n, .. } => n.min(data.len()),
            _ => data.len(),
        };
        let offset = usize::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?;
        let mut bytes = Vec::with_capacity(count);
        for part in data.parts() {
            let take = part.len().min(count - bytes.len());
            bytes.extend_from_slice(&part[..take]);
            if bytes.len() == count {
                break;
            }
        }
        self.image.write(inode, offset, &bytes, self.limits)?;
        if data_sync
            && !matches!(
                self.effect,
                Effect::WriteThenError { .. } | Effect::SkipBarrier
            )
        {
            // O_DSYNC preserves this write, not unrelated dirty ranges from
            // another buffered descriptor. Directory publication is separate.
            self.image
                .persist_inode_range(inode, offset..offset + count, self.limits)?;
        }
        Ok(Outcome::Written(count))
    }
}
