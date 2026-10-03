use super::{Effect, Image, ImageLimits};
use crate::{
    FileKind, Handle, HandleOwner, HandleToken, Metadata, OpenMode, Operation, Outcome, ReadBuffer,
    WriteBuffer,
};
use std::{collections::BTreeMap, io, path::Path};

#[derive(Debug)]
struct Opened {
    inode: u64,
    origin: usize,
    token: HandleToken,
    writable: bool,
    direct: bool,
    data_sync: bool,
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
        self.closed |= !self.handles.is_empty();
        self.handles.clear();
        self.locks.clear();
        self.used.fill(0);
    }

    pub(super) fn inodes(&self) -> impl Iterator<Item = u64> + '_ {
        self.handles.values().map(|open| open.inode)
    }

    pub(super) fn take_closed(&mut self) -> bool {
        std::mem::take(&mut self.closed)
    }

    fn remove(&mut self, key: u64) -> Option<Opened> {
        let open = self.handles.remove(&key)?;
        self.closed = true;
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

    pub(super) fn execute(
        &mut self,
        image: &mut Image,
        limits: ImageLimits,
        origin: usize,
        operation: Operation,
        effect: Effect,
    ) -> io::Result<Outcome> {
        if let Effect::FailBefore(error) = effect {
            return Err(error.into());
        }
        if let Operation::Protected { operation, handles } = operation {
            for handle in &handles {
                self.get(handle)?;
            }
            let result = self.execute(image, limits, origin, *operation, effect);
            drop(handles);
            return result;
        }
        let result = self.apply(image, limits, origin, operation, effect)?;
        match effect {
            Effect::FailAfter(error) | Effect::WriteThenError { error, .. } => Err(error.into()),
            _ => Ok(result),
        }
    }

    fn apply(
        &mut self,
        image: &mut Image,
        limits: ImageLimits,
        origin: usize,
        operation: Operation,
        effect: Effect,
    ) -> io::Result<Outcome> {
        match operation {
            Operation::Protected { .. } => unreachable!("protection handled before execution"),
            Operation::Open {
                path,
                mode,
                direct,
                data_sync,
            } => self.open(
                image,
                limits,
                origin,
                &path,
                Some((mode, direct, data_sync)),
            ),
            Operation::OpenDirectory { path } => self.open(image, limits, origin, &path, None),
            Operation::Read {
                handle,
                offset,
                length,
            } => self.read(image, &handle, offset, length, effect),
            Operation::Write {
                handle,
                offset,
                data,
            } => self.write(image, limits, &handle, offset, &data, effect),
            Operation::Metadata { handle } => {
                let inode = self.get(&handle)?.inode;
                let kind = image.kind(inode)?;
                let length = if kind == FileKind::File {
                    image.file(inode, false)?.len() as u64
                } else {
                    0
                };
                Ok(Outcome::Metadata(Metadata { kind, length }))
            }
            Operation::SetLength { handle, length } => {
                let open = self.writable(&handle)?;
                image.set_length(
                    open.inode,
                    usize::try_from(length).map_err(|_| io::ErrorKind::InvalidInput)?,
                    false,
                    limits,
                )?;
                Ok(Outcome::Done)
            }
            Operation::Allocate {
                handle,
                offset,
                length,
            } => self.allocate(image, limits, &handle, offset, length),
            Operation::Sync { handle, .. } => {
                image.sync(self.get(&handle)?.inode, limits)?;
                Ok(Outcome::Done)
            }
            Operation::LockExclusive { handle } => self.lock(&handle),
            Operation::Close { handle } => {
                let key = self.key(&handle)?;
                self.remove(key).ok_or(io::ErrorKind::NotFound)?;
                Ok(Outcome::Done)
            }
            Operation::ReadDirectory {
                path,
                max_entries,
                max_name_bytes,
            } => Ok(Outcome::Directory(image.list(
                &path,
                max_entries,
                max_name_bytes,
            )?)),
            Operation::CreateDirectory { path } => {
                image.create(&path, true, limits)?;
                Ok(Outcome::Done)
            }
            Operation::Rename {
                source,
                destination,
            } => {
                image.rename(&source, &destination)?;
                Ok(Outcome::Done)
            }
            Operation::HardLink {
                source,
                destination,
            } => {
                image.link(&source, &destination, limits)?;
                Ok(Outcome::Done)
            }
            Operation::RemoveFile { path } => {
                image.remove(&path, false)?;
                Ok(Outcome::Done)
            }
            Operation::RemoveDirectory { path } => {
                image.remove(&path, true)?;
                Ok(Outcome::Done)
            }
        }
    }

    fn writable(&self, handle: &Handle) -> io::Result<&Opened> {
        let open = self.get(handle)?;
        if !open.writable {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        Ok(open)
    }

    fn allocate(
        &self,
        image: &Image,
        limits: ImageLimits,
        handle: &Handle,
        offset: u64,
        length: u64,
    ) -> io::Result<Outcome> {
        image.file(self.writable(handle)?.inode, false)?;
        if offset
            .checked_add(length)
            .is_none_or(|end| end > limits.file_bytes as u64)
        {
            return Err(io::ErrorKind::StorageFull.into());
        }
        // KEEP_SIZE changes no readable bytes. Allocation failure is also an
        // explicit fault event; this model does not emulate extent accounting.
        Ok(Outcome::Done)
    }

    fn lock(&mut self, handle: &Handle) -> io::Result<Outcome> {
        let inode = self.get(handle)?.inode;
        let key = self.key(handle)?;
        if self.locks.get(&inode).is_some_and(|owner| *owner != key) {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.locks.insert(inode, key);
        Ok(Outcome::Done)
    }

    fn open(
        &mut self,
        image: &mut Image,
        limits: ImageLimits,
        origin: usize,
        path: &Path,
        options: Option<(OpenMode, bool, bool)>,
    ) -> io::Result<Outcome> {
        self.reclaim();
        let maximum =
            self.limit / self.used.len() + usize::from(origin < self.limit % self.used.len());
        if self.used[origin] == maximum {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let key = self.next;
        let next = key.checked_add(1).ok_or(io::ErrorKind::StorageFull)?;
        let inode = if matches!(options, Some((OpenMode::CreateNew, _, _))) {
            image.create(path, false, limits)?
        } else {
            image.resolve(path, false)?
        };
        let kind = image.kind(inode)?;
        if (options.is_none() && kind != FileKind::Directory)
            || (options.is_some() && kind != FileKind::File)
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let (mode, direct, data_sync) = options.unwrap_or((OpenMode::Read, false, false));
        // Reclamation is explicit at the controller's next scheduling point.
        let (handle, token) = self.owner.create(key, futures::task::noop_waker());
        self.handles.insert(
            key,
            Opened {
                inode,
                origin,
                token,
                writable: mode != OpenMode::Read,
                direct,
                data_sync,
            },
        );
        self.next = next;
        self.used[origin] += 1;
        Ok(Outcome::Opened(handle))
    }

    fn read(
        &self,
        image: &Image,
        handle: &Handle,
        offset: u64,
        length: usize,
        effect: Effect,
    ) -> io::Result<Outcome> {
        let open = self.get(handle)?;
        if open.direct {
            return Err(io::ErrorKind::Unsupported.into());
        }
        let file = image.file(open.inode, false)?;
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(file.len());
        let mut count = length.min(file.len() - start);
        if let Effect::Short(limit) = effect {
            count = count.min(limit);
        }
        let mut bytes = vec![0; length].into_boxed_slice();
        bytes[..count].copy_from_slice(&file[start..start + count]);
        Ok(Outcome::Read(ReadBuffer::new(bytes, count)?))
    }

    fn write(
        &self,
        image: &mut Image,
        limits: ImageLimits,
        handle: &Handle,
        offset: u64,
        data: &WriteBuffer,
        effect: Effect,
    ) -> io::Result<Outcome> {
        let open = self.writable(handle)?;
        if open.direct && (!offset.is_multiple_of(4096) || !data.len().is_multiple_of(4096)) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let count = match effect {
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
        image.write(open.inode, offset, &bytes, limits)?;
        if open.data_sync && !matches!(effect, Effect::WriteThenError { .. }) {
            // O_DSYNC preserves this write, not unrelated dirty ranges from
            // another buffered descriptor. Directory publication is separate.
            image.persist_inode_range(open.inode, offset..offset + count, limits)?;
        }
        Ok(Outcome::Written(count))
    }
}
