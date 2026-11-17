//! First-start provisioning in one call, for a broker embedded in a program.
//!
//! The shared identity is derived, so no broker waits for a file from
//! another. The local identity is published under a staging name, used to
//! bind the volumes and format the stores, and renamed last. Its final name
//! therefore means provisioning finished, and only a broker that never
//! served is provisioned again.

use std::{
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
};

use ozzy_config::{BrokerIdentity, HostResources, ValidatedDeployment};

use crate::{
    CheckedConfig, StartupError, check_volumes, format_partition_journals,
    initialize_broker_identity, io_error, load_broker_identity, provisioning, volumes,
};

/// Check a deployment built in code and make `broker` ready to serve it.
///
/// The first call derives the shared identity from `cluster.id`, creates the
/// device roots, binds them, formats the configured topics and publishes the
/// local identity at `local_identity`. A call that was interrupted is
/// finished by the next one. Once the local identity exists, a call only
/// checks: it writes nothing, and a missing or foreign volume fails as it
/// does at any start.
///
/// Every broker of the deployment calls this with the same configuration and
/// its own name and paths.
///
/// A broker of three whose local identity and stores were emptied looks like
/// one that never served, and this call provisions it again. Its peers then
/// see another store origin than the one they recorded and give it no link,
/// so it cannot vote with its empty logs. It returns only with its old local
/// identity, as a nonvoting copy.
///
/// # Errors
///
/// Fails on a deployment without `cluster.id`, on a device root bound to
/// another identity, and on any filesystem or formatting failure.
pub async fn provision(
    deployment: ValidatedDeployment,
    broker: &str,
    local_identity: &Path,
    host: &HostResources,
) -> Result<(CheckedConfig, BrokerIdentity), StartupError> {
    let identity = deployment.derive_identity()?;
    let checked = provisioning::checked(deployment, identity, broker, host)?;
    let staged = staging(local_identity);
    if exists(local_identity)? {
        // A crash between the last two steps of an earlier call left it.
        remove(&staged)?;
        let local = load_broker_identity(&checked, local_identity)?;
        check_volumes(&checked, &local)?;
        return Ok((checked, local));
    }
    let local = if exists(&staged)? {
        load_broker_identity(&checked, &staged)?
    } else {
        volumes::create_durable(parent(local_identity))?;
        initialize_broker_identity(&checked, &staged, uuid::Uuid::now_v7)?
    };
    volumes::prepare_volumes(&checked, &local)?;
    format_partition_journals(&checked, &local).await?;
    publish(&staged, local_identity)?;
    Ok((checked, local))
}

/// The name the local identity has while its stores are being made.
fn staging(local_identity: &Path) -> PathBuf {
    let mut name = OsString::from(local_identity.as_os_str());
    name.push(".provisioning");
    name.into()
}

fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn exists(path: &Path) -> Result<bool, StartupError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error(path, error)),
    }
}

fn remove(path: &Path) -> Result<(), StartupError> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(io_error(path, error)),
        _ => Ok(()),
    }
}

/// Give the staged identity its final name. A link never replaces a file
/// another call published first.
fn publish(staged: &Path, local_identity: &Path) -> Result<(), StartupError> {
    fs::hard_link(staged, local_identity).map_err(|error| io_error(local_identity, error))?;
    let directory = parent(local_identity);
    let sync = || {
        fs::File::open(directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error(directory, error))
    };
    sync()?;
    remove(staged)?;
    sync()
}
