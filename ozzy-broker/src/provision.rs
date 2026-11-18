//! First-start provisioning in one call, for a broker embedded in a program.
//!
//! The shared identity is derived, so no broker waits for a file from
//! another. The local identity is published under a staging name, used to
//! bind the volumes and format the stores, and renamed last. Its final name
//! therefore means provisioning finished, and only a broker that never
//! served is provisioned again.

use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use ozzy_config::{BrokerIdentity, HostResources, ValidatedDeployment};

use crate::{
    CheckedConfig, StartupError, check_volumes, files, format::format_journals,
    initialize_broker_identity, io_error, load_broker_identity, provisioning,
    replacement::Replacement, volumes,
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
/// so it cannot vote with its empty logs. It returns with its old local
/// identity, or emptied once more and through [`provision_replacement`].
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
    run(deployment, broker, local_identity, host, false).await
}

/// Make `broker` ready to serve in place of one that lost its local
/// identity and stores, as [`provision`] does a new one.
///
/// The broker it replaces may have voted in any replicated log, so this
/// one formats none. It starts each as a nonvoting copy, and so every
/// replicated log it is told to create, and a copy votes once it holds the
/// log of the other two. Its peers take its new store origin because it
/// says that it is a replacement. This lasts across restarts until
/// [`crate::Broker::end_replacement`]. Partitions no other broker holds
/// are formatted empty: their records are lost.
///
/// A call that was interrupted is finished by the next one. Once the
/// replacement ended, a call only checks, as [`provision`] does.
///
/// # Errors
///
/// Fails as [`provision`] does, and on a device that already holds the
/// store of a replicated partition.
pub async fn provision_replacement(
    deployment: ValidatedDeployment,
    broker: &str,
    local_identity: &Path,
    host: &HostResources,
) -> Result<(CheckedConfig, BrokerIdentity), StartupError> {
    run(deployment, broker, local_identity, host, true).await
}

async fn run(
    deployment: ValidatedDeployment,
    broker: &str,
    local_identity: &Path,
    host: &HostResources,
    replacement: bool,
) -> Result<(CheckedConfig, BrokerIdentity), StartupError> {
    let identity = deployment.derive_identity()?;
    let checked = provisioning::checked(deployment, identity, broker, host)?;
    let staged = staging(local_identity);
    if files::exists(local_identity)? {
        // A crash between the last two steps of an earlier call left it.
        files::remove_if_present(&staged)?;
        let local = load_broker_identity(&checked, local_identity)?;
        check_volumes(&checked, &local)?;
        return Ok((checked, local));
    }
    let local = if files::exists(&staged)? {
        load_broker_identity(&checked, &staged)?
    } else {
        volumes::create_durable(files::parent_or_dot(local_identity))?;
        initialize_broker_identity(&checked, &staged, uuid::Uuid::now_v7)?
    };
    volumes::prepare_volumes(&checked, &local)?;
    // An interrupted replacement stays one, whoever finishes it.
    let replacement = replacement || Replacement::load(&checked)?.on();
    if replacement {
        // Before any store: a start without it would format voting copies.
        Replacement::begin(&checked)?;
    }
    format_journals(&checked, &local, replacement).await?;
    publish(&staged, local_identity)?;
    Ok((checked, local))
}

/// The name the local identity has while its stores are being made.
fn staging(local_identity: &Path) -> PathBuf {
    let mut name = OsString::from(local_identity.as_os_str());
    name.push(".provisioning");
    name.into()
}

/// Give the staged identity its final name. A link never replaces a file
/// another call published first.
fn publish(staged: &Path, local_identity: &Path) -> Result<(), StartupError> {
    fs::hard_link(staged, local_identity).map_err(|error| io_error(local_identity, error))?;
    let directory = files::parent_or_dot(local_identity);
    files::sync_directory(directory)?;
    files::remove_if_present(staged)?;
    files::sync_directory(directory)
}
