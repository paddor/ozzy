//! Offline volume checks. Run before starting application or device workers.

use ozzy_config::{BrokerIdentity, VolumeIdentity};
use std::{collections::BTreeSet, fs, io, path::PathBuf};

use crate::{CheckedConfig, StartupError, files, io_error, provisioning};

const MARKER: &str = "OZZY_VOLUME";

/// Explicitly bind existing device roots to provisioned volume identities.
/// Roots must already exist. Existing markers, including damaged ones, are never
/// replaced. A partial failure leaves published markers for operator inspection.
pub fn initialize_volumes(
    checked: &CheckedConfig,
    identity: &BrokerIdentity,
) -> Result<(), StartupError> {
    let roots = roots(checked, identity)?;
    // Check the whole set before writing. Publication still uses noclobber to
    // reject a concurrent initializer after this preflight.
    for (root, _) in &roots {
        let marker = root.join(MARKER);
        if files::exists(&marker)? {
            return Err(io_error(marker, io::ErrorKind::AlreadyExists.into()));
        }
    }
    for (root, expected) in roots {
        files::publish(&root.join(MARKER), &expected.encode()?, false)?;
    }
    Ok(())
}

/// Bind the device roots of a broker that has never served, creating the
/// missing ones. A root this identity already marked keeps its marker and
/// loses the partition stores an interrupted formatting left on it. A root
/// with another marker is refused.
pub(crate) fn prepare_volumes(
    checked: &CheckedConfig,
    identity: &BrokerIdentity,
) -> Result<(), StartupError> {
    for device in checked.local_broker()?.devices.values() {
        create_durable(&device.root)?;
    }
    let broker = checked.local_broker()?;
    for (root, expected) in roots(checked, identity)? {
        let marker = root.join(MARKER);
        if !files::exists(&marker)? {
            files::publish(&marker, &expected.encode()?, false)?;
            continue;
        }
        let actual = VolumeIdentity::decode(&provisioning::read_bounded(&marker, 4096)?)?;
        if actual != expected {
            return Err(io_error(
                marker,
                io::Error::new(io::ErrorKind::InvalidData, "volume identity mismatch"),
            ));
        }
        for placement in &checked.plan.partitions {
            if broker
                .devices
                .get(&placement.device)
                .map(|device| &device.root)
                != Some(&root)
            {
                continue;
            }
            match fs::remove_dir_all(&placement.directory) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => {
                    return Err(io_error(&placement.directory, error));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Create `directory` and its missing ancestors, each synced into its
/// parent, so a power cut cannot take back a directory that holds stores.
pub(crate) fn create_durable(directory: &std::path::Path) -> Result<(), StartupError> {
    let mut missing = Vec::new();
    let mut current = directory;
    while !current.as_os_str().is_empty()
        && !current.try_exists().map_err(|e| io_error(current, e))?
    {
        missing.push(current);
        match current.parent() {
            Some(parent) => current = parent,
            None => break,
        }
    }
    for path in missing.into_iter().rev() {
        match fs::create_dir(path) {
            Err(error) if error.kind() != io::ErrorKind::AlreadyExists => {
                return Err(io_error(path, error));
            }
            _ => {}
        }
        files::sync_directory(files::parent_or_dot(path))?;
    }
    Ok(())
}

/// Verify every configured volume before opening any partition journals.
/// This performs no writes and never treats absence as initialization.
pub fn check_volumes(
    checked: &CheckedConfig,
    identity: &BrokerIdentity,
) -> Result<(), StartupError> {
    for (root, expected) in roots(checked, identity)? {
        let path = root.join(MARKER);
        let actual = VolumeIdentity::decode(&provisioning::read_bounded(&path, 4096)?)?;
        if actual != expected {
            return Err(io_error(
                path,
                io::Error::new(io::ErrorKind::InvalidData, "volume identity mismatch"),
            ));
        }
    }
    Ok(())
}

fn roots(
    checked: &CheckedConfig,
    identity: &BrokerIdentity,
) -> Result<Vec<(PathBuf, VolumeIdentity)>, StartupError> {
    checked
        .deployment
        .check_broker_identity(&checked.identity, &checked.plan.name, identity)?;
    let broker = checked.local_broker()?;
    let mut selected = BTreeSet::new();
    #[cfg(unix)]
    let mut objects = BTreeSet::new();
    let mut roots = Vec::with_capacity(broker.devices.len());
    for (name, device) in &broker.devices {
        let metadata =
            fs::symlink_metadata(&device.root).map_err(|error| io_error(&device.root, error))?;
        if !metadata.is_dir() || metadata.is_symlink() {
            return Err(io_error(
                &device.root,
                io::Error::new(io::ErrorKind::InvalidInput, "expected device directory"),
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if !objects.insert((metadata.dev(), metadata.ino())) {
                return Err(io_error(
                    &device.root,
                    io::Error::new(io::ErrorKind::InvalidInput, "aliased device roots"),
                ));
            }
        }
        let canonical =
            fs::canonicalize(&device.root).map_err(|error| io_error(&device.root, error))?;
        if selected
            .iter()
            .any(|other: &PathBuf| other.starts_with(&canonical) || canonical.starts_with(other))
        {
            return Err(io_error(
                &device.root,
                io::Error::new(io::ErrorKind::InvalidInput, "overlapping device roots"),
            ));
        }
        selected.insert(canonical);
        roots.push((
            device.root.clone(),
            identity
                .volume(name)
                .ok_or_else(|| io_error(&device.root, io::ErrorKind::NotFound.into()))?,
        ));
    }
    Ok(roots)
}
