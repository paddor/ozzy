//! Explicit segment initialization using the configured shared execution pools.

use ozzy_config::BrokerIdentity;
use std::{collections::BTreeSet, fs, io, sync::Arc};

use crate::{
    ApplicationShards, CheckedConfig, DevicePools, JournalPlan, StartupError, check_volumes,
    io_error,
};

/// Zeros written where each partition's first appends land. More would only
/// lengthen format: later segments start on unwritten extents anyway, until
/// retention recycles them.
const FIRST_APPEND_ZEROS: u64 = 128 << 20;

/// Format every broker-local partition once. Run offline, before starting a
/// frontend. Volume bindings must already exist. Established or partial stores
/// are refused. File execution and destruction run on shared device workers;
/// each configured application shard initializes its own assigned partitions.
pub async fn format_partition_journals(
    checked: &CheckedConfig,
    local: &BrokerIdentity,
) -> Result<(), StartupError> {
    format_journals(checked, local, false).await
}

/// Format this broker's partitions, or with `unreplicated` only those no
/// other broker holds: a broker that replaces a lost one makes the others
/// as nonvoting copies when it starts, below the directories made here.
pub(crate) async fn format_journals(
    checked: &CheckedConfig,
    local: &BrokerIdentity,
    unreplicated: bool,
) -> Result<(), StartupError> {
    check_volumes(checked, local)?;
    let mut journals = JournalPlan::from_trusted_deployment(checked, local)?;
    // Every partition, also those not formatted here: a store that is
    // there was not lost.
    preflight(&journals)?;
    if unreplicated {
        keep_unreplicated(&mut journals)?;
    }
    let journals = Arc::new(journals.partitions);
    let roots: Arc<std::collections::BTreeMap<_, _>> = Arc::new(
        checked
            .local_broker()?
            .devices
            .iter()
            .map(|(name, device)| (name.clone(), device.root.clone()))
            .collect(),
    );
    let (devices, lanes) = DevicePools::start(&checked.plan)?;
    let application = ApplicationShards::start(&checked.plan, lanes, move |mut context| {
        let journals = journals.clone();
        let root = roots.get(&context.plan.device).cloned();
        async move {
            let root = root.ok_or_else(|| {
                StartupError::Runtime(format!("unknown device {}", context.plan.device))
            })?;
            let mut directories = BTreeSet::new();
            for partition in journals.iter().filter(|partition| partition.placement.shard == context.plan.id) {
                if context.shutdown.is_closed() { return Ok(()); }
                let path = partition.placement.directory.clone();
                let parent = crate::parent_directory(&path)?;
                let relative = parent
                    .strip_prefix(&root)
                    .map_err(|_| io_error(parent, io::ErrorKind::InvalidInput.into()))?;
                let mut directory = root.clone();
                for component in relative.components() {
                    directory.push(component);
                    if directories.insert(directory.clone()) {
                        initialize_directory(&context.io, &directory).await?;
                    }
                }
                let mut opened = tokio::select! {
                    () = context.shutdown.closed() => return Ok(()),
                    result = partition.clone().format(context.io.clone(), crate::journals::generation()) => result?,
                };
                let journal_error = |source| StartupError::Journal { path: path.clone(), source };
                // A new partition's first appends skip the slow writes into
                // unwritten extents.
                opened.journal.zero_first_segment(FIRST_APPEND_ZEROS).await.map_err(journal_error)?;
                opened.journal.shutdown().await.map_err(journal_error)?;
            }
            context.ready()?;
            context.shutdown.closed().await;
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

/// Drop the replicated partitions from `journals`, and make the directory
/// each of their stores goes below.
fn keep_unreplicated(journals: &mut JournalPlan) -> Result<(), StartupError> {
    let mut parents = BTreeSet::new();
    journals.partitions.retain(|partition| {
        let kept = matches!(partition.config, crate::JournalConfig::Local(_));
        if !kept && let Some(parent) = partition.placement.directory.parent() {
            parents.insert(parent.to_path_buf());
        }
        kept
    });
    for parent in &parents {
        crate::volumes::create_durable(parent)?;
    }
    Ok(())
}

// Offline preflight rejects a mixed established/unformatted set before any
// worker starts. Backend exclusive creation still protects the race afterward.
fn preflight(journals: &JournalPlan) -> Result<(), StartupError> {
    for partition in &journals.partitions {
        let path = &partition.placement.directory;
        if crate::files::exists(path)? {
            return Err(io_error(path, io::ErrorKind::AlreadyExists.into()));
        }
        let parent = crate::parent_directory(path)?;
        match fs::symlink_metadata(parent) {
            Ok(metadata) if metadata.is_dir() && !metadata.is_symlink() => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(parent, error)),
            Ok(_) => return Err(io_error(parent, io::ErrorKind::InvalidInput.into())),
        }
    }
    Ok(())
}

pub(crate) async fn initialize_directory(
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
    for directory in [path, crate::parent_directory(path)?] {
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
