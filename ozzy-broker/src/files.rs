//! Small synchronous filesystem steps of provisioning and startup.

use std::{
    fs,
    io::{self, Write},
    path::Path,
};

use crate::{StartupError, io_error};

/// Whether anything is at `path`. A final symlink counts as itself.
pub(crate) fn exists(path: &Path) -> Result<bool, StartupError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error(path, error)),
    }
}

/// Remove the file at `path`, if any.
pub(crate) fn remove_if_present(path: &Path) -> Result<(), StartupError> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(io_error(path, error)),
        _ => Ok(()),
    }
}

/// Persist the entry changes made in `directory`.
pub(crate) fn sync_directory(directory: &Path) -> Result<(), StartupError> {
    fs::File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error(directory, error))
}

/// The directory that holds `path`: the current one for a bare name.
pub(crate) fn parent_or_dot(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// Publish `text` at `path` whole: staged beside it, synced, renamed, and
/// the directory synced. With `clobber` it takes the place of what is
/// there; without, an existing file fails the call and stays.
pub(crate) fn publish(path: &Path, text: &str, clobber: bool) -> Result<(), StartupError> {
    let parent = parent_or_dot(path);
    let directory = fs::File::open(parent).map_err(|error| io_error(parent, error))?;
    let mut staging =
        tempfile::NamedTempFile::new_in(parent).map_err(|error| io_error(parent, error))?;
    staging
        .write_all(text.as_bytes())
        .and_then(|()| staging.as_file().sync_all())
        .map_err(|error| io_error(staging.path(), error))?;
    // persist_noclobber refuses an existing destination atomically. A check
    // before it would race another publisher and could replace authority.
    let published = if clobber {
        staging.persist(path)
    } else {
        staging.persist_noclobber(path)
    };
    published
        .map_err(|error| io_error(path, error.error))?
        .sync_all()
        .map_err(|error| io_error(path, error))?;
    directory
        .sync_all()
        .map_err(|error| io_error(parent, error))
}
