use ozzy_io::{
    Entry, FileKind, Handle, HandleOwner, HandleToken, Metadata, OpenMode, Operation, Outcome,
    ReadBuffer, SyncMode, WriteBuffer,
};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::{Shared, handle_waker};

pub(crate) mod handles;
use handles::{Budget, OwnedFile, Reservation};

#[derive(Debug)]
pub(super) struct Files {
    owner: HandleOwner,
    next: u64,
    budget: Arc<Budget>,
    entries: HashMap<u64, Slot>,
}

#[derive(Debug)]
struct Slot {
    open: Option<Arc<Opened>>,
}

#[derive(Debug)]
struct Opened {
    file: Arc<OwnedFile>,
    token: HandleToken,
    direct: bool,
}

impl Files {
    pub(super) fn new(limit: usize, shards: usize, owner: HandleOwner) -> Self {
        Self {
            owner,
            next: 0,
            budget: Budget::new(limit, shards),
            entries: HashMap::new(),
        }
    }

    fn reserve(&mut self, shard: usize) -> io::Result<(u64, Reservation)> {
        let reservation = self.budget.reserve(shard)?;
        let key = self.next;
        self.next = key
            .checked_add(1)
            .ok_or_else(|| io::Error::other("file handle identity exhausted"))?;
        self.entries.insert(key, Slot { open: None });
        Ok((key, reservation))
    }

    fn remove(&mut self, key: u64) -> Option<Slot> {
        self.entries.remove(&key)
    }

    pub(super) fn reclaim(&mut self) -> Vec<Arc<OwnedFile>> {
        let mut retired = Vec::new();
        self.entries.retain(|_, slot| {
            if slot
                .open
                .as_ref()
                .is_some_and(|open| !open.token.is_alive())
            {
                retired.push(
                    slot.open
                        .take()
                        .expect("abandoned open handle")
                        .file
                        .clone(),
                );
                false
            } else {
                true
            }
        });
        retired
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
    }

    fn key(&self, handle: &Handle) -> io::Result<u64> {
        self.owner.key(handle).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "handle belongs to another backend",
            )
        })
    }
}

fn get(owner: &HandleOwner, handle: &Handle) -> io::Result<(Arc<OwnedFile>, bool)> {
    if owner.key(handle).is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "handle belongs to another backend",
        ));
    }
    let opened = owner
        .resource::<Opened>(handle)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "closed file handle"))?;
    Ok((opened.file.clone(), opened.direct))
}

pub(super) fn direct_file(
    operation: &Operation,
    shared: &Shared,
) -> io::Result<Option<Arc<OwnedFile>>> {
    if let Operation::Protected { handles, .. } = operation {
        for handle in handles {
            get(&shared.handles, handle)?;
        }
    }
    if let Operation::Write { handle, data, .. } = operation.unprotected() {
        let (file, direct) = get(&shared.handles, handle)?;
        // Zero-length writes have no kernel asynchronous work. Keep the same
        // alignment validation and zero result as the ordinary pool backend.
        Ok((direct && !data.is_empty()).then_some(file))
    } else {
        Ok(None)
    }
}

pub(super) fn execute(
    operation: Operation,
    shared: &Arc<Shared>,
    origin: usize,
) -> io::Result<Outcome> {
    let get = |handle: &Handle| get(&shared.handles, handle);
    match operation {
        Operation::Protected { operation, handles } => {
            let protection: Vec<_> = handles.iter().map(get).collect::<io::Result<_>>()?;
            let result = execute(*operation, shared, origin);
            drop(protection);
            drop(handles);
            result
        }
        Operation::Open {
            path,
            mode,
            direct,
            data_sync,
        } => open(shared, origin, &path, Some(mode), direct, data_sync),
        Operation::OpenDirectory { path } => open(shared, origin, &path, None, false, false),
        Operation::Read {
            handle,
            offset,
            length,
        } => read_buffer(get(&handle)?, offset, length),
        Operation::Write {
            handle,
            offset,
            data,
        } => {
            let (file, direct) = get(&handle)?;
            Ok(Outcome::Written(write(&file, offset, &data, direct)?))
        }
        Operation::Metadata { handle } => {
            let metadata = get(&handle)?.0.metadata()?;
            Ok(Outcome::Metadata(Metadata {
                kind: kind(metadata.file_type()),
                length: metadata.len(),
            }))
        }
        Operation::SetLength { handle, length } => {
            get(&handle)?.0.set_len(length)?;
            Ok(Outcome::Done)
        }
        Operation::Allocate {
            handle,
            offset,
            length,
        } => {
            allocate(&get(&handle)?.0, offset, length)?;
            Ok(Outcome::Done)
        }
        Operation::Sync { handle, mode } => {
            let file = get(&handle)?.0;
            match mode {
                SyncMode::Data => file.sync_data()?,
                SyncMode::All => file.sync_all()?,
            }
            Ok(Outcome::Done)
        }
        Operation::LockExclusive { handle } => {
            get(&handle)?.0.try_lock().map_err(|error| match error {
                std::fs::TryLockError::WouldBlock => io::ErrorKind::WouldBlock.into(),
                std::fs::TryLockError::Error(error) => error,
            })?;
            Ok(Outcome::Done)
        }
        Operation::Close { handle } => close(shared, &handle),
        Operation::ReadDirectory {
            path,
            max_entries,
            max_name_bytes,
        } => list_directory(&path, max_entries, max_name_bytes),
        Operation::CreateDirectory { path } => {
            fs::create_dir(path)?;
            Ok(Outcome::Done)
        }
        Operation::Rename {
            source,
            destination,
        } => {
            fs::rename(source, destination)?;
            Ok(Outcome::Done)
        }
        Operation::HardLink {
            source,
            destination,
        } => {
            fs::hard_link(source, destination)?;
            Ok(Outcome::Done)
        }
        Operation::RemoveFile { path } => {
            fs::remove_file(path)?;
            Ok(Outcome::Done)
        }
        Operation::RemoveDirectory { path } => {
            fs::remove_dir(path)?;
            Ok(Outcome::Done)
        }
    }
}

fn close(shared: &Shared, handle: &Handle) -> io::Result<Outcome> {
    let mut files = shared.files();
    let key = files.key(handle)?;
    if !files.entries.contains_key(&key) {
        return Err(io::ErrorKind::NotFound.into());
    }
    files.owner.invalidate(handle);
    let closed = files.remove(key).ok_or(io::ErrorKind::NotFound)?;
    drop(files);
    drop(closed);
    Ok(Outcome::Done)
}

fn open(
    shared: &Arc<Shared>,
    origin: usize,
    path: &Path,
    mode: Option<OpenMode>,
    direct: bool,
    data_sync: bool,
) -> io::Result<Outcome> {
    let (key, reservation) = shared.with_reclaimed_files(|files| files.reserve(origin))?;
    // The reserved slot bounds even concurrent opens. No table lock spans I/O.
    let result = open_file(path, mode, direct, data_sync);
    let mut files = shared.files();
    match result {
        Ok(file) => {
            let (handle, token) =
                files
                    .owner
                    .create_with_direct_io(key, handle_waker(shared), direct);
            let opened = Arc::new(Opened {
                file: Arc::new(OwnedFile::new(file, reservation)),
                token,
                direct,
            });
            files.owner.bind(&handle, &opened);
            files.entries.get_mut(&key).expect("reserved slot").open = Some(opened);
            Ok(Outcome::Opened(handle))
        }
        Err(error) => {
            files.remove(key);
            Err(error)
        }
    }
}

fn read_buffer(
    (file, direct): (Arc<OwnedFile>, bool),
    offset: u64,
    length: usize,
) -> io::Result<Outcome> {
    if direct {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "use a buffered handle for reads",
        ));
    }
    let mut bytes = vec![0_u8; length].into_boxed_slice();
    let count = read(&file, &mut bytes, offset)?;
    Ok(Outcome::Read(ReadBuffer::new(bytes, count)?))
}

fn list_directory(path: &Path, max_entries: usize, max_name_bytes: usize) -> io::Result<Outcome> {
    let mut entries = Vec::with_capacity(max_entries);
    let mut bytes = 0_usize;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        bytes = bytes
            .checked_add(name.capacity())
            .ok_or(io::ErrorKind::InvalidData)?;
        if entries.len() == max_entries || bytes > max_name_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "directory listing exceeds admitted bound",
            ));
        }
        entries.push(Entry {
            name,
            kind: kind(entry.file_type()?),
        });
    }
    entries.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    Ok(Outcome::Directory(entries))
}

fn open_file(
    path: &Path,
    mode: Option<OpenMode>,
    direct: bool,
    data_sync: bool,
) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    if matches!(mode, Some(OpenMode::ReadWrite | OpenMode::CreateNew)) {
        options.write(true);
    }
    if mode == Some(OpenMode::CreateNew) {
        options.create_new(true);
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut flags = libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
        if mode.is_none() {
            flags |= libc::O_DIRECTORY;
        }
        if direct {
            flags |= libc::O_DIRECT;
        }
        if data_sync {
            flags |= libc::O_DSYNC;
        }
        options.custom_flags(flags);
    }
    #[cfg(not(target_os = "linux"))]
    if direct || data_sync {
        return Err(io::ErrorKind::Unsupported.into());
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if if mode.is_some() {
        !metadata.is_file()
    } else {
        !metadata.is_dir()
    } {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unexpected filesystem object type",
        ));
    }
    #[cfg(target_os = "linux")]
    if direct {
        use rustix::fs::{AtFlags, StatxFlags, statx};
        let stat = statx(&file, "", AtFlags::EMPTY_PATH, StatxFlags::DIOALIGN)?;
        if stat.stx_mask & StatxFlags::DIOALIGN.bits() != 0
            && (stat.stx_dio_mem_align == 0
                || stat.stx_dio_offset_align == 0
                || !4096_u32.is_multiple_of(stat.stx_dio_mem_align)
                || !4096_u32.is_multiple_of(stat.stx_dio_offset_align))
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "unsupported direct-I/O alignment",
            ));
        }
    }
    Ok(file)
}

fn kind(kind: fs::FileType) -> FileKind {
    if kind.is_file() {
        FileKind::File
    } else if kind.is_dir() {
        FileKind::Directory
    } else {
        FileKind::Other
    }
}

#[cfg(unix)]
fn read(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    loop {
        match file.read_at(bytes, offset) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

#[cfg(unix)]
fn write(file: &File, mut offset: u64, data: &WriteBuffer, direct: bool) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    if direct {
        if !offset.is_multiple_of(4096) || !data.len().is_multiple_of(4096) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unaligned direct write",
            ));
        }
        let mut staging = vec![0_u8; data.len() + 4096];
        let start = staging.as_ptr().align_offset(4096);
        let mut next = start;
        for part in data.parts() {
            staging[next..next + part.len()].copy_from_slice(part);
            next += part.len();
        }
        return file.write_at(&staging[start..next], offset);
    }
    let mut written = 0;
    for part in data.parts() {
        if part.is_empty() {
            continue;
        }
        let count = loop {
            match file.write_at(part, offset) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => break result?,
            }
        };
        written += count;
        offset += count as u64;
        if count != part.len() {
            break;
        }
    }
    Ok(written)
}

#[cfg(not(unix))]
fn read(_: &File, _: &mut [u8], _: u64) -> io::Result<usize> {
    Err(io::ErrorKind::Unsupported.into())
}
#[cfg(not(unix))]
fn write(_: &File, _: u64, _: &WriteBuffer, _: bool) -> io::Result<usize> {
    Err(io::ErrorKind::Unsupported.into())
}

fn allocate(file: &File, offset: u64, length: u64) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        Ok(rustix::fs::fallocate(
            file,
            rustix::fs::FallocateFlags::KEEP_SIZE,
            offset,
            length,
        )?)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (file, offset, length);
        Err(io::ErrorKind::Unsupported.into())
    }
}
