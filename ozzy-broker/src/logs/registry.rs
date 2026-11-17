//! Durable records of runtime logs, one small file per log beside the log
//! directories of a device. Every step runs on a shard's device lane.

use crate::{StartupError, io_error};
use ozzy_config::LogRecord;
use ozzy_io::{Class, FileKind, Local, OpenMode, Operation, Outcome, SyncMode, WriteBuffer};
use std::{
    io,
    path::{Path, PathBuf},
};

/// Encoded bytes one record may take.
const RECORD_BYTES: usize = 4096;
const RECORD: &str = "record";
const STAGED: &str = "staged";

/// The record file of log `label` in `directory`.
fn record_path(directory: &Path, label: &str) -> PathBuf {
    directory.join(format!("{label}.{RECORD}"))
}

pub(super) async fn run(
    io: &Local,
    path: &Path,
    operation: Operation,
) -> Result<ozzy_io::Completed, StartupError> {
    io.execute(Class::Progress, operation)
        .await
        .map_err(|error| io_error(path, error))
}

pub(super) async fn open(
    io: &Local,
    path: &Path,
    operation: Operation,
) -> Result<ozzy_io::Handle, StartupError> {
    match &*run(io, path, operation).await? {
        Outcome::Opened(handle) => Ok(handle.clone()),
        _ => Err(io_error(path, io::ErrorKind::InvalidData.into())),
    }
}

/// Open the lock file at `path`, creating it if needed, and lock it. The
/// lock lasts while the handle does.
pub(crate) async fn lock(io: &Local, path: PathBuf) -> Result<ozzy_io::Handle, StartupError> {
    let file = |mode| Operation::Open {
        path: path.clone(),
        mode,
        direct: false,
        data_sync: false,
    };
    let created = io.execute(Class::Progress, file(OpenMode::CreateNew)).await;
    let handle = match created {
        Ok(completed) => match &*completed {
            Outcome::Opened(handle) => handle.clone(),
            _ => return Err(io_error(&path, io::ErrorKind::InvalidData.into())),
        },
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            open(io, &path, file(OpenMode::ReadWrite)).await?
        }
        Err(error) => return Err(io_error(&path, error)),
    };
    let locked = Operation::LockExclusive {
        handle: handle.clone(),
    };
    run(io, &path, locked).await?;
    Ok(handle)
}

/// Persist the directory entry changes made below `directory`.
pub(super) async fn sync_directory(io: &Local, directory: &Path) -> Result<(), StartupError> {
    let path = directory.to_path_buf();
    let handle = open(io, directory, Operation::OpenDirectory { path }).await?;
    let mode = SyncMode::All;
    let synced = run(
        io,
        directory,
        Operation::Sync {
            handle: handle.clone(),
            mode,
        },
    )
    .await;
    run(io, directory, Operation::Close { handle }).await?;
    synced.map(|_| ())
}

/// Whether a directory is at `path`.
pub(crate) async fn holds_directory(io: &Local, path: &Path) -> Result<bool, StartupError> {
    let operation = Operation::OpenDirectory { path: path.into() };
    let opened = match io.execute(Class::Progress, operation).await {
        Ok(opened) => opened,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(io_error(path, error)),
    };
    let Outcome::Opened(handle) = &*opened else {
        return Err(io_error(path, io::ErrorKind::InvalidData.into()));
    };
    let handle = handle.clone();
    // A lane may run as few as two jobs at once.
    drop(opened);
    run(io, path, Operation::Close { handle }).await?;
    Ok(true)
}

/// Remove `path` if it exists.
pub(super) async fn remove_file(io: &Local, path: &Path) -> Result<(), StartupError> {
    let operation = Operation::RemoveFile { path: path.into() };
    match io.execute(Class::Progress, operation).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(path, error)),
    }
}

/// Publish `record` in place of the log's previous record: staged, synced,
/// renamed, and the directory synced. A crash leaves the old or the new
/// record whole.
pub(crate) async fn publish(
    io: &Local,
    directory: &Path,
    record: &LogRecord,
) -> Result<(), StartupError> {
    let label = record.log.simple().to_string();
    let path = record_path(directory, &label);
    let staged = directory.join(format!("{label}.{STAGED}"));
    let encoded = record.encode()?.into_bytes();
    if encoded.len() > RECORD_BYTES {
        return Err(io_error(path, io::ErrorKind::InvalidInput.into()));
    }
    remove_file(io, &staged).await?;
    let create = Operation::Open {
        path: staged.clone(),
        mode: OpenMode::CreateNew,
        direct: false,
        data_sync: false,
    };
    let handle = open(io, &staged, create).await?;
    let length = encoded.len();
    let write = Operation::Write {
        handle: handle.clone(),
        offset: 0,
        data: WriteBuffer::from_vec(encoded),
    };
    let written = match run(io, &staged, write).await {
        Ok(completed) if matches!(*completed, Outcome::Written(bytes) if bytes == length) => {
            let mode = SyncMode::All;
            run(
                io,
                &staged,
                Operation::Sync {
                    handle: handle.clone(),
                    mode,
                },
            )
            .await
            .map(|_| ())
        }
        Ok(_) => Err(io_error(&staged, io::ErrorKind::WriteZero.into())),
        Err(error) => Err(error),
    };
    run(io, &staged, Operation::Close { handle }).await?;
    written?;
    let rename = Operation::Rename {
        source: staged,
        destination: path.clone(),
    };
    run(io, &path, rename).await?;
    sync_directory(io, directory).await
}

/// Read one record file whole.
async fn read(io: &Local, path: &Path) -> Result<LogRecord, StartupError> {
    let operation = Operation::Open {
        path: path.into(),
        mode: OpenMode::Read,
        direct: false,
        data_sync: false,
    };
    let handle = open(io, path, operation).await?;
    let read = Operation::Read {
        handle: handle.clone(),
        offset: 0,
        length: RECORD_BYTES + 1,
    };
    // A lane may run as few as two jobs at once: copy the bytes out and
    // release the read before the close.
    let bytes = run(io, path, read).await.and_then(|read| match &*read {
        Outcome::Read(buffer) => Ok(buffer.as_slice().to_vec()),
        _ => Err(io_error(path, io::ErrorKind::InvalidData.into())),
    });
    run(io, path, Operation::Close { handle }).await?;
    let bytes = bytes?;
    let text = std::str::from_utf8(&bytes)
        .ok()
        .filter(|text| text.len() <= RECORD_BYTES)
        .ok_or_else(|| io_error(path, io::ErrorKind::InvalidData.into()))?;
    Ok(LogRecord::decode(text)?)
}

/// The entries of `directory`, owned, with the listing job released. `None`
/// for a missing directory.
pub(super) async fn list(
    io: &Local,
    directory: &Path,
    max_entries: usize,
) -> Result<Option<Vec<(std::ffi::OsString, FileKind)>>, StartupError> {
    let listing = Operation::ReadDirectory {
        path: directory.into(),
        max_entries,
        max_name_bytes: max_entries.saturating_mul(64),
    };
    let entries = match io.execute(Class::Progress, listing).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(directory, error)),
    };
    let Outcome::Directory(entries) = &*entries else {
        return Err(io_error(directory, io::ErrorKind::InvalidData.into()));
    };
    Ok(Some(
        entries
            .iter()
            .map(|entry| (entry.name.clone(), entry.kind))
            .collect(),
    ))
}

/// Entries one listing job of [`load`] holds.
const PAGE: usize = 1024;

/// The entries of `directory` right after `after` by name, at most
/// [`PAGE`], with the listing job released. Empty at the end and for a
/// missing directory.
async fn page(
    io: &Local,
    directory: &Path,
    after: Option<std::ffi::OsString>,
) -> Result<Vec<(std::ffi::OsString, FileKind)>, StartupError> {
    let listing = Operation::ReadDirectoryPage {
        path: directory.into(),
        after,
        max_entries: PAGE,
        max_name_bytes: PAGE * 64,
    };
    let entries = match io.execute(Class::Progress, listing).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error(directory, error)),
    };
    let Outcome::Directory(entries) = &*entries else {
        return Err(io_error(directory, io::ErrorKind::InvalidData.into()));
    };
    Ok(entries
        .iter()
        .map(|entry| (entry.name.clone(), entry.kind))
        .collect())
}

/// Every record in `directory`, however many it holds, a page at a time. A
/// missing directory holds none. Leftover staged files are removed.
pub(crate) async fn load(io: &Local, directory: &Path) -> Result<Vec<LogRecord>, StartupError> {
    let mut records = Vec::new();
    let mut after = None;
    loop {
        let entries = page(io, directory, after).await?;
        let Some((last, _)) = entries.last() else {
            return Ok(records);
        };
        after = Some(last.clone());
        for (name, _) in entries.iter().filter(|(_, kind)| *kind == FileKind::File) {
            let path = directory.join(name);
            match path.extension().and_then(|extension| extension.to_str()) {
                Some(RECORD) => records.push(read(io, &path).await?),
                Some(STAGED) => remove_file(io, &path).await?,
                _ => {}
            }
        }
    }
}

/// Remove the directory tree at `path`, if any, a bounded listing at a
/// time, then persist the removal in `parent`.
pub(crate) async fn remove_tree(
    io: &Local,
    parent: &Path,
    path: &Path,
) -> Result<(), StartupError> {
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Some(entries) = list(io, &directory, 4096).await? else {
            continue;
        };
        let mut children = Vec::new();
        for (name, kind) in entries {
            let child = directory.join(&name);
            if kind == FileKind::Directory {
                children.push(child);
            } else {
                remove_file(io, &child).await?;
            }
        }
        if children.is_empty() {
            let remove = Operation::RemoveDirectory {
                path: directory.clone(),
            };
            run(io, &directory, remove).await?;
        } else {
            // Come back to this directory once its children are gone.
            pending.push(directory);
            pending.extend(children);
        }
    }
    sync_directory(io, parent).await
}

/// Remove the record of a log that never served, and persist the removal.
pub(crate) async fn discard(
    io: &Local,
    directory: &Path,
    record: &LogRecord,
) -> Result<(), StartupError> {
    remove_file(
        io,
        &record_path(directory, &record.log.simple().to_string()),
    )
    .await?;
    sync_directory(io, directory).await
}
