use ozzy_io::{
    Entry, FileKind, Handle, HandleOwner, Metadata, OpenMode, Operation, Outcome, ReadBuffer,
    SyncMode, WriteBuffer,
};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{Shared, handle_waker};

pub(crate) mod handles;
mod parked;
use handles::{Budget, OwnedFile};
use parked::{Identity, Opened, Origin};

#[derive(Debug)]
pub(super) struct Files {
    owner: HandleOwner,
    next: u64,
    budget: Arc<Budget>,
    entries: HashMap<u64, Slot>,
    /// The handles that would open each path again.
    paths: HashMap<PathBuf, Vec<u64>>,
}

#[derive(Debug)]
struct Slot {
    open: Option<Arc<Opened>>,
}

impl Files {
    pub(super) fn new(limit: usize, shards: usize, owner: HandleOwner) -> Self {
        Self {
            owner,
            next: 0,
            budget: Budget::new(limit, shards),
            entries: HashMap::new(),
            paths: HashMap::new(),
        }
    }

    /// A slot for a handle that is about to open.
    fn slot(&mut self) -> io::Result<u64> {
        let key = self.next;
        self.next = key
            .checked_add(1)
            .ok_or_else(|| io::Error::other("file handle identity exhausted"))?;
        self.entries.insert(key, Slot { open: None });
        Ok(key)
    }

    /// Drop the slot of `key` and return its descriptor, if it holds one,
    /// for the caller to close outside the table lock.
    fn remove(&mut self, key: u64) -> Option<Arc<OwnedFile>> {
        let opened = self.entries.remove(&key)?.open?;
        self.unname(key, &opened);
        opened.retire()
    }

    /// Drop the slots of `lapsed` handles nobody holds any more, and return
    /// their descriptors for the caller to close.
    pub(super) fn reclaim(&mut self, lapsed: &[u64]) -> Vec<Arc<OwnedFile>> {
        let abandoned = |slot: &Slot| {
            let open = slot.open.as_ref();
            open.is_some_and(|open| !open.token.is_alive())
        };
        let mut retired = Vec::new();
        for &key in lapsed {
            if self.entries.get(&key).is_some_and(abandoned) {
                retired.extend(self.remove(key));
            }
        }
        retired
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.paths.clear();
    }

    fn key(&self, handle: &Handle) -> io::Result<u64> {
        self.owner.key(handle).ok_or_else(foreign_handle)
    }
}

/// The error for a handle another backend issued.
fn foreign_handle() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "handle belongs to another backend",
    )
}

/// The descriptor of an open handle, and whether it does direct I/O. A
/// parked handle opens its file again here.
fn get(shared: &Shared, handle: &Handle) -> io::Result<(Arc<OwnedFile>, bool)> {
    let owner = &shared.handles;
    if owner.key(handle).is_none() {
        return Err(foreign_handle());
    }
    let opened = owner
        .resource::<Opened>(handle)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "closed file handle"))?;
    Ok((parked::file(shared, &opened)?, opened.direct))
}

pub(super) fn direct_file(
    operation: &Operation,
    shared: &Shared,
) -> io::Result<Option<Arc<OwnedFile>>> {
    if let Operation::Protected { handles, .. } = operation {
        for handle in handles {
            get(shared, handle)?;
        }
    }
    if let Operation::Write { handle, data, .. } = operation.unprotected() {
        let (file, direct) = get(shared, handle)?;
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
    let get = |handle: &Handle| get(shared, handle);
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
        } => open(shared, origin, &path, Target::file(mode, direct, data_sync)),
        Operation::OpenDirectory { path } => open(shared, origin, &path, Target::DIRECTORY),
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
        Operation::Metadata { handle } => metadata(&get(&handle)?.0),
        Operation::SetLength { handle, length } => {
            get(&handle)?.0.set_len(length).map(|()| Outcome::Done)
        }
        Operation::Allocate {
            handle,
            offset,
            length,
        } => allocate(&get(&handle)?.0, offset, length).map(|()| Outcome::Done),
        Operation::Sync { handle, mode } => sync(&get(&handle)?.0, mode).map(|()| Outcome::Done),
        Operation::LockExclusive { handle } => lock(&get(&handle)?.0).map(|()| Outcome::Done),
        Operation::Close { handle } => close(shared, &handle),
        listing @ (Operation::ReadDirectory { .. } | Operation::ReadDirectoryPage { .. }) => {
            list(listing)
        }
        Operation::CreateDirectory { path } => fs::create_dir(path).map(|()| Outcome::Done),
        Operation::Rename {
            source,
            destination,
        } => parked::rename(shared, &source, &destination).map(|()| Outcome::Done),
        Operation::HardLink {
            source,
            destination,
        } => fs::hard_link(source, destination).map(|()| Outcome::Done),
        Operation::RemoveFile { path } => parked::remove(shared, &path, false),
        Operation::RemoveDirectory { path } => parked::remove(shared, &path, true),
    }
}

fn metadata(file: &File) -> io::Result<Outcome> {
    let metadata = file.metadata()?;
    Ok(Outcome::Metadata(Metadata {
        kind: kind(metadata.file_type()),
        length: metadata.len(),
    }))
}

fn sync(file: &File, mode: SyncMode) -> io::Result<()> {
    match mode {
        SyncMode::Data => file.sync_data(),
        SyncMode::All => file.sync_all(),
    }
}

fn lock(file: &OwnedFile) -> io::Result<()> {
    file.lock().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => io::ErrorKind::WouldBlock.into(),
        std::fs::TryLockError::Error(error) => error,
    })
}

fn close(shared: &Shared, handle: &Handle) -> io::Result<Outcome> {
    let mut files = shared.files();
    let key = files.key(handle)?;
    if !files.entries.contains_key(&key) {
        return Err(io::ErrorKind::NotFound.into());
    }
    files.owner.invalidate(handle)?;
    let closed = files.remove(key);
    drop(files);
    drop(closed);
    Ok(Outcome::Done)
}

/// What an open expects at its path: a file with its access mode and flags, or
/// a directory.
#[derive(Debug, Clone, Copy)]
struct Target {
    mode: Option<OpenMode>,
    direct: bool,
    data_sync: bool,
}

impl Target {
    const DIRECTORY: Self = Self {
        mode: None,
        direct: false,
        data_sync: false,
    };

    const fn file(mode: OpenMode, direct: bool, data_sync: bool) -> Self {
        Self {
            mode: Some(mode),
            direct,
            data_sync,
        }
    }

    /// The same file, opened again: it exists by now.
    const fn again(self) -> Self {
        let mode = match self.mode {
            Some(OpenMode::CreateNew) => Some(OpenMode::ReadWrite),
            mode => mode,
        };
        Self { mode, ..self }
    }

    fn options(self) -> OpenOptions {
        let mut options = OpenOptions::new();
        options.read(true);
        if matches!(self.mode, Some(OpenMode::ReadWrite | OpenMode::CreateNew)) {
            options.write(true);
        }
        if self.mode == Some(OpenMode::CreateNew) {
            options.create_new(true);
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut flags = libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
            if self.mode.is_none() {
                flags |= libc::O_DIRECTORY;
            }
            if self.direct {
                flags |= libc::O_DIRECT;
            }
            if self.data_sync {
                flags |= libc::O_DSYNC;
            }
            options.custom_flags(flags);
        }
        options
    }
}

fn open(shared: &Arc<Shared>, origin: usize, path: &Path, target: Target) -> io::Result<Outcome> {
    shared.reclaim_files();
    // The reservation bounds even concurrent opens. No table lock spans I/O.
    let reservation = parked::reserve(shared, origin)?;
    let (file, identity) = open_file(path, target)?;
    let file = OwnedFile::new(file, reservation);
    let mut files = shared.files();
    let key = files.slot()?;
    let (handle, token) =
        files
            .owner
            .create_with_direct_io(key, handle_waker(shared, key), target.direct);
    let named = Origin {
        path: path.to_path_buf(),
        target,
        identity,
    };
    let opened = Arc::new(Opened::new(file, named, token, (origin, shared.now())));
    if let Err(error) = files.owner.bind(&handle, &opened) {
        files.entries.remove(&key);
        // Close the new file only after releasing the table lock.
        drop(files);
        return Err(error);
    }
    files.name(key, path);
    let slot = files
        .entries
        .get_mut(&key)
        .ok_or_else(|| io::Error::other("new file slot vanished"))?;
    slot.open = Some(opened);
    Ok(Outcome::Opened(handle))
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

/// Run a listing, whole or one page of it.
fn list(listing: Operation) -> io::Result<Outcome> {
    match listing {
        Operation::ReadDirectory {
            path,
            max_entries,
            max_name_bytes,
        } => list_directory(&path, max_entries, max_name_bytes),
        Operation::ReadDirectoryPage {
            path,
            after,
            max_entries,
            max_name_bytes,
        } => list_page(&path, after.as_deref(), (max_entries, max_name_bytes)),
        _ => Err(io::ErrorKind::InvalidInput.into()),
    }
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

/// The entries right after `after` by name that fit `(entries, bytes)`.
/// The directory is read whole, and only the page is kept.
fn list_page(
    path: &Path,
    after: Option<&std::ffi::OsStr>,
    (max_entries, max_name_bytes): (usize, usize),
) -> io::Result<Outcome> {
    if max_entries == 0 {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    // The smallest names seen so far.
    let mut kept = std::collections::BTreeMap::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let full = kept.len() == max_entries;
        if after.is_some_and(|after| name.as_os_str() <= after)
            || full && kept.last_key_value().is_some_and(|(last, _)| name >= *last)
        {
            continue;
        }
        if full {
            kept.pop_last();
        }
        kept.insert(name, kind(entry.file_type()?));
    }
    let mut page = Vec::with_capacity(kept.len());
    let mut bytes = 0_usize;
    for (name, kind) in kept {
        bytes = bytes.saturating_add(name.len());
        if bytes > max_name_bytes {
            break;
        }
        page.push(Entry { name, kind });
    }
    if page.is_empty() && bytes > max_name_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "directory entry name exceeds admitted bound",
        ));
    }
    Ok(Outcome::Directory(page))
}

fn open_file(path: &Path, target: Target) -> io::Result<(File, Identity)> {
    #[cfg(not(target_os = "linux"))]
    if target.direct || target.data_sync {
        return Err(io::ErrorKind::Unsupported.into());
    }
    let file = target.options().open(path)?;
    let metadata = file.metadata()?;
    let expected_kind = if target.mode.is_some() {
        metadata.is_file()
    } else {
        metadata.is_dir()
    };
    if !expected_kind {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unexpected filesystem object type",
        ));
    }
    #[cfg(target_os = "linux")]
    if target.direct {
        check_direct_alignment(&file)?;
    }
    Ok((file, Identity::of(&metadata)))
}

/// Direct I/O buffers and offsets are 4 KiB aligned; the device must accept that.
#[cfg(target_os = "linux")]
fn check_direct_alignment(file: &File) -> io::Result<()> {
    use rustix::fs::{AtFlags, StatxFlags, statx};
    let stat = statx(file, "", AtFlags::EMPTY_PATH, StatxFlags::DIOALIGN)?;
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
    Ok(())
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
