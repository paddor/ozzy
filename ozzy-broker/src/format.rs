//! Explicit segment initialization using the configured shared execution pools.

use ozzy_config::BrokerIdentity;
use ozzy_replication::JournalGeneration;
use std::{collections::BTreeSet, fs, io, sync::Arc};

use crate::{
    ApplicationShards, CheckedConfig, DevicePools, JournalPlan, StartupError, check_volumes,
    io_error,
};

/// Format every broker-local partition once. Run offline, before starting a
/// frontend. Volume bindings must already exist. Established or partial stores
/// are refused. File execution and destruction run on shared device workers;
/// each configured application shard initializes its own assigned partitions.
pub async fn format_partition_journals(
    checked: &CheckedConfig,
    local: &BrokerIdentity,
) -> Result<(), StartupError> {
    check_volumes(checked, local)?;
    let journals = JournalPlan::from_trusted_deployment(checked, local)?;
    preflight(&journals)?;
    let journals = Arc::new(journals.partitions);
    let roots: Arc<std::collections::BTreeMap<_, _>> = Arc::new(
        checked.deployment.deployment().brokers[&checked.plan.name]
            .devices
            .iter()
            .map(|(name, device)| (name.clone(), device.root.clone()))
            .collect(),
    );
    let (devices, lanes) = DevicePools::start(&checked.plan)?;
    let application = ApplicationShards::start(&checked.plan, lanes, move |mut context| {
        let journals = journals.clone();
        let root = roots[&context.plan.device].clone();
        async move {
            let mut directories = BTreeSet::new();
            for partition in journals.iter().filter(|partition| partition.placement.shard == context.plan.id) {
                if context.shutdown.is_requested() { return Ok(()); }
                let path = partition.placement.directory.clone();
                let parent = path.parent().expect("checked partition path").to_path_buf();
                let relative = parent.strip_prefix(&root).expect("checked device root");
                let mut directory = root.clone();
                for component in relative.components() {
                    directory.push(component);
                    if directories.insert(directory.clone()) {
                        initialize_directory(&context.io, &directory).await?;
                    }
                }
                let opened = tokio::select! {
                    () = context.shutdown.requested() => return Ok(()),
                    result = partition.clone().format(context.io.clone(), JournalGeneration(uuid::Uuid::now_v7().as_u128())) => result?,
                };
                opened.journal.shutdown().await.map_err(|source| StartupError::Journal { path, source })?;
            }
            context.ready()?;
            context.shutdown.requested().await;
            Ok(())
        }
    }).await;
    let result = match application {
        Ok(application) => application.shutdown().await,
        Err(error) => Err(error),
    };
    devices.shutdown().await;
    result
}

// Offline preflight rejects a mixed established/unformatted set before any
// worker starts. Backend exclusive creation still protects the race afterward.
fn preflight(journals: &JournalPlan) -> Result<(), StartupError> {
    for partition in &journals.partitions {
        let path = &partition.placement.directory;
        match fs::symlink_metadata(path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(path, error)),
            Ok(_) => return Err(io_error(path, io::ErrorKind::AlreadyExists.into())),
        }
        let parent = path.parent().expect("checked partition path");
        match fs::symlink_metadata(parent) {
            Ok(metadata) if metadata.is_dir() && !metadata.is_symlink() => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(parent, error)),
            Ok(_) => return Err(io_error(parent, io::ErrorKind::InvalidInput.into())),
        }
    }
    Ok(())
}

async fn initialize_directory(
    io: &ozzy_io::Local,
    path: &std::path::Path,
) -> Result<(), StartupError> {
    use ozzy_io::{Class, Operation, Outcome, SyncMode};
    match io
        .execute(
            Class::Progress,
            Operation::CreateDirectory { path: path.into() },
        )
        .await
    {
        Ok(completed) if matches!(*completed, Outcome::Done) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(io_error(path, error)),
        Ok(_) => return Err(io_error(path, io::ErrorKind::InvalidData.into())),
    }
    // Open verifies directory type without following the final path component.
    // Sync both this directory and its parent before publishing partition files.
    for directory in [path, path.parent().expect("checked directory path")] {
        let opened = io
            .execute(
                Class::Progress,
                Operation::OpenDirectory {
                    path: directory.into(),
                },
            )
            .await
            .map_err(|error| io_error(directory, error))?;
        let handle = match &*opened {
            Outcome::Opened(handle) => handle.clone(),
            _ => return Err(io_error(directory, io::ErrorKind::InvalidData.into())),
        };
        drop(opened);
        io.execute(
            Class::Progress,
            Operation::Sync {
                handle: handle.clone(),
                mode: SyncMode::All,
            },
        )
        .await
        .map_err(|error| io_error(directory, error))?;
        io.execute(Class::Progress, Operation::Close { handle })
            .await
            .map_err(|error| io_error(directory, error))?;
    }
    Ok(())
}
