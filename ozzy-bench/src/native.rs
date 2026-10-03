//! Production broker assembly for native qualification workers.
//!
//! Explicit initialization publishes one deployment identity. Worker startup
//! reads established files and runs the same broker as the executable.

use crate::{BenchResult, bench_error};
use ozzy_broker::{Broker, CheckedConfig};
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

mod prepare;
pub use prepare::PreparedBroker;

/// Shared TOML and persistent identity for one benchmark run.
#[derive(Debug, Clone)]
pub struct DeploymentArtifact {
    pub configuration: PathBuf,
    pub identity: PathBuf,
}

impl DeploymentArtifact {
    /// Initialize fresh run files in an existing directory. Validate input and
    /// preflight both destinations. Exclusive creation never replaces a file.
    pub fn initialize(directory: &Path, source: &str) -> BenchResult<Self> {
        let deployment = ozzy_config::Deployment::parse(source)?.validate()?;
        check_storage_parent(directory)?;
        let artifact = Self {
            configuration: directory.join("deployment.toml"),
            identity: directory.join("deployment.identity"),
        };
        require_absent(&artifact.configuration)?;
        require_absent(&artifact.identity)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&artifact.configuration)?;
        file.write_all(source.as_bytes())?;
        file.sync_all()?;
        ozzy_broker::initialize_identity(&deployment, &artifact.identity, uuid::Uuid::now_v7)?;
        Ok(artifact)
    }

    /// Install exact shared authority on another worker. Check both documents
    /// before creating files. Existing destinations are never replaced.
    pub fn install(directory: &Path, source: &str, identity: &str) -> BenchResult<Self> {
        let deployment = ozzy_config::Deployment::parse(source)?.validate()?;
        deployment.check_identity(&ozzy_config::DeploymentIdentity::decode(identity)?)?;
        check_storage_parent(directory)?;
        let artifact = Self {
            configuration: directory.join("deployment.toml"),
            identity: directory.join("deployment.identity"),
        };
        require_absent(&artifact.configuration)?;
        require_absent(&artifact.identity)?;
        for (path, contents) in [
            (&artifact.configuration, source),
            (&artifact.identity, identity),
        ] {
            let mut staging = tempfile::NamedTempFile::new_in(directory)?;
            staging.write_all(contents.as_bytes())?;
            staging.as_file().sync_all()?;
            staging.persist_noclobber(path)?;
        }
        fs::File::open(directory)?.sync_all()?;
        Ok(artifact)
    }

    /// Reload and validate exact persisted authority and current local placement.
    pub fn checked(&self, broker: &str) -> BenchResult<CheckedConfig> {
        Ok(ozzy_broker::check_config(
            ozzy_broker::load_deployment(&self.configuration)?,
            &self.identity,
            broker,
            &ozzy_broker::host_resources()?,
        )?)
    }
}

/// One named broker's established deployment and local store bindings.
#[derive(Debug, Clone)]
pub struct BrokerWorker {
    pub deployment: DeploymentArtifact,
    pub broker: String,
    pub identity: PathBuf,
}

impl BrokerWorker {
    /// Create fresh benchmark volumes and journals through production provisioners.
    /// Roots must be absent, with existing disk-backed parents. This is explicit
    /// initialization, separate from ordinary startup or recovery.
    pub async fn initialize(&self) -> BenchResult<()> {
        let checked = self.deployment.checked(&self.broker)?;
        let roots = checked.deployment.deployment().brokers[&self.broker]
            .devices
            .values()
            .map(|device| &device.root)
            .collect::<Vec<_>>();
        require_absent(&self.identity)?;
        for root in &roots {
            require_absent(root)?;
            check_storage_parent(
                root.parent()
                    .ok_or_else(|| bench_error("storage root has no parent"))?,
            )?;
        }
        let local =
            ozzy_broker::initialize_broker_identity(&checked, &self.identity, uuid::Uuid::now_v7)?;
        for root in roots {
            fs::create_dir(root)?;
        }
        ozzy_broker::initialize_volumes(&checked, &local)?;
        ozzy_broker::format_partition_journals(&checked, &local).await?;
        Ok(())
    }

    /// Start the production broker in the benchmark's explicitly trusted domain.
    /// Missing stores or bindings fail; startup never initializes them.
    pub async fn start_trusted(&self) -> BenchResult<Broker> {
        let checked = self.deployment.checked(&self.broker)?;
        let local = ozzy_broker::load_broker_identity(&checked, &self.identity)?;
        Ok(Broker::start_trusted(checked, local).await?)
    }

    /// Share the worker's owned OMQ context with benchmark process control.
    /// The context's I/O count must match the validated broker topology.
    pub async fn start_trusted_with_context(
        &self,
        context: omq_tokio::Context,
    ) -> BenchResult<Broker> {
        let checked = self.deployment.checked(&self.broker)?;
        let local = ozzy_broker::load_broker_identity(&checked, &self.identity)?;
        Ok(Broker::start_trusted_with_context(checked, local, context).await?)
    }
}

fn require_absent(path: &Path) -> BenchResult<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
        Ok(_) => Err(bench_error(format!(
            "benchmark destination exists: {}",
            path.display()
        ))),
    }
}

/// Require an existing disk-backed directory before allocating run storage.
pub fn check_storage_parent(path: &Path) -> BenchResult<()> {
    if !fs::metadata(path)?.is_dir() {
        return Err(bench_error("benchmark storage parent must be a directory"));
    }
    let filesystem = rustix::fs::statfs(path)?;
    if filesystem.f_type == 0x0102_1994 || filesystem.f_type == 0x8584_58f6 {
        return Err(bench_error(
            "native benchmark requires disk-backed storage, not tmpfs/ramfs",
        ));
    }
    Ok(())
}
