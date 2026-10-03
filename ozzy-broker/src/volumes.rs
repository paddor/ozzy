//! Offline volume checks. Run before starting application or device workers.

use ozzy_config::{BrokerIdentity, VolumeIdentity};
use std::{collections::BTreeSet, fs, io, path::PathBuf};

use crate::{CheckedConfig, StartupError, io_error, provisioning};

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
        match fs::symlink_metadata(&marker) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(marker, error)),
            Ok(_) => {
                return Err(io_error(marker, io::ErrorKind::AlreadyExists.into()));
            }
        }
    }
    for (root, expected) in roots {
        provisioning::publish_identity(&root.join(MARKER), &expected.encode()?)?;
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
    let broker = &checked.deployment.deployment().brokers[&checked.plan.name];
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
            identity.volume(name).expect("validated device identity"),
        ));
    }
    Ok(roots)
}
