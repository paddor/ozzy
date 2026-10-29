//! One loopback broker in one directory, provisioned by the explicit steps.
//!
//! The config file is published last, so its presence means provisioning
//! finished. A later run serves the same stores and never formats them again.

use std::{
    fs, io,
    net::TcpListener,
    path::{Path, PathBuf},
};

use ozzy_broker::{
    StartupError, check_config, format_partition_journals, host_resources,
    initialize_broker_identity, initialize_identity, initialize_volumes, load_broker_identity,
    load_deployment,
};
use ozzy_config::Deployment;

const BROKER: &str = "local";
const TOPIC: &str = "orders";
/// A topic whose records may carry a due time.
const DELAYED: &str = "delayed";

/// Files of one quickstart directory.
struct Paths {
    directory: PathBuf,
    config: PathBuf,
    shared: PathBuf,
    local: PathBuf,
    data: PathBuf,
}

impl Paths {
    fn new(directory: PathBuf) -> Self {
        Self {
            config: directory.join("deployment.toml"),
            shared: directory.join("shared.identity"),
            local: directory.join("local.identity"),
            data: directory.join("data"),
            directory,
        }
    }
}

pub(super) fn run(directory: &Path) -> Result<(), StartupError> {
    let paths = prepare(directory)?;
    let checked = check_config(
        load_deployment(&paths.config)?,
        &paths.shared,
        BROKER,
        &host_resources()?,
    )?;
    let local = load_broker_identity(&checked, &paths.local)?;
    let config = paths.config.display();
    let demo = format!(
        "cargo run --release -p ozzy-broker --example ozzy_demo -- --config {config} \
         --broker {BROKER} --identity {}",
        paths.shared.display()
    );
    println!("Broker {BROKER} with topics {TOPIC} and {DELAYED}: {config}");
    println!("Watch it:");
    println!("  cargo run --release -p ozzy-ui -- --config {config} --broker {BROKER}");
    println!("Write and read records:");
    println!("  {demo}");
    println!("Deliver each record 10 s after writing it:");
    println!("  {demo} --topic {DELAYED} --due-secs 10");
    println!("Ctrl-C stops the broker. Run the same command again to restart it.");
    super::runtime()?.block_on(super::serve::run(checked, local, &[]))
}

/// Provision a new or empty directory; keep a finished one as it is.
fn prepare(directory: &Path) -> Result<Paths, StartupError> {
    match fs::read_dir(directory) {
        Ok(mut entries) => {
            let paths = Paths::new(canonical(directory)?);
            if paths
                .config
                .try_exists()
                .map_err(|e| io_error(&paths.config, e))?
            {
                return Ok(paths);
            }
            if entries.next().is_some() {
                return Err(io_error(
                    &paths.directory,
                    io::Error::new(
                        io::ErrorKind::DirectoryNotEmpty,
                        "holds files but no deployment.toml, so an earlier quickstart stopped \
                         early; remove it and run again",
                    ),
                ));
            }
            provision(paths)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(directory).map_err(|e| io_error(directory, e))?;
            provision(Paths::new(canonical(directory)?))
        }
        Err(error) => Err(io_error(directory, error)),
    }
}

fn provision(paths: Paths) -> Result<Paths, StartupError> {
    fs::create_dir(&paths.data).map_err(|e| io_error(&paths.data, e))?;
    let text = deployment_text(&paths.data, free_ports()?)?;
    let deployment = Deployment::parse(&text)?.validate()?;
    initialize_identity(&deployment, &paths.shared, uuid::Uuid::now_v7)?;
    let checked = check_config(deployment, &paths.shared, BROKER, &host_resources()?)?;
    initialize_broker_identity(&checked, &paths.local, uuid::Uuid::now_v7)?;
    let local = load_broker_identity(&checked, &paths.local)?;
    initialize_volumes(&checked, &local)?;
    super::runtime()?.block_on(format_partition_journals(&checked, &local))?;
    let staged = paths.directory.join("deployment.toml.new");
    fs::write(&staged, text).map_err(|e| io_error(&staged, e))?;
    fs::rename(&staged, &paths.config).map_err(|e| io_error(&paths.config, e))?;
    println!(
        "Provisioned broker {BROKER} in {}.",
        paths.directory.display()
    );
    Ok(paths)
}

/// Small segments and retention, so formatting writes 128 MiB and a long
/// demo keeps at most about 1 GiB per topic.
fn deployment_text(
    data: &Path,
    [peer, data_peer, reader]: [u16; 3],
) -> Result<String, StartupError> {
    let root = data.to_str().ok_or_else(|| {
        io_error(
            data,
            io::Error::new(io::ErrorKind::InvalidFilename, "path is not UTF-8"),
        )
    })?;
    let root = root.replace('\\', "\\\\").replace('"', "\\\"");
    Ok(format!(
        r#"# Written by `ozzy quickstart`: one broker on loopback for trying Ozzy.
[cluster]
mode = "single"

[brokers.{BROKER}.endpoints]
peer = "tcp://127.0.0.1:{peer}"
data_peer = "tcp://127.0.0.1:{data_peer}"
reader_pub = "tcp://127.0.0.1:{reader}"
admin = "ipc://@ozzy-quickstart-{peer}-admin"

[brokers.{BROKER}.devices.data]
root = "{root}"
controller = "local"

[brokers.{BROKER}.devices.data.workers]
write_threads = 1

[topics.{TOPIC}]
partitions = 4
segment_bytes = 16777216

[topics.{TOPIC}.retention]
max_bytes = 268435456

[topics.{DELAYED}]
partitions = 4
segment_bytes = 16777216

[topics.{DELAYED}.retention]
max_bytes = 268435456

[topics.{DELAYED}.delay]
max_secs = 3600
max_entries = 1000000
"#
    ))
}

/// Three loopback ports that are free now. The config keeps them for restarts.
fn free_ports() -> Result<[u16; 3], StartupError> {
    let bind = || {
        TcpListener::bind("127.0.0.1:0")
            .and_then(|socket| Ok((socket.local_addr()?.port(), socket)))
            .map_err(|e| io_error("127.0.0.1:0", e))
    };
    // Hold all three until each has a port, so none repeats.
    let sockets = [bind()?, bind()?, bind()?];
    Ok(sockets.map(|(port, _)| port))
}

fn canonical(directory: &Path) -> Result<PathBuf, StartupError> {
    fs::canonicalize(directory).map_err(|e| io_error(directory, e))
}

fn io_error(path: impl Into<PathBuf>, source: io::Error) -> StartupError {
    StartupError::Io {
        path: path.into(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_validates_and_escapes_the_root() {
        let text = deployment_text(Path::new("/tmp/a \"b\" \\c/data"), [7200, 7201, 7202]).unwrap();
        let deployment = Deployment::parse(&text).unwrap().validate().unwrap();
        let root = &deployment.deployment().brokers[BROKER].devices["data"].root;
        assert_eq!(root, Path::new("/tmp/a \"b\" \\c/data"));
        assert_eq!(deployment.deployment().topics[TOPIC].partitions, 4);
        assert!(deployment.deployment().topics[TOPIC].delay.is_none());
        assert_eq!(
            deployment.deployment().topics[DELAYED]
                .delay
                .unwrap()
                .max_secs,
            3600
        );
    }

    #[test]
    fn partial_directory_is_refused_and_left_alone() {
        let directory = tempfile::tempdir().unwrap();
        let stray = directory.path().join("shared.identity");
        fs::write(&stray, "partial").unwrap();
        let error = prepare(directory.path()).err().unwrap();
        assert!(error.to_string().contains("no deployment.toml"), "{error}");
        assert_eq!(fs::read_to_string(&stray).unwrap(), "partial");
        assert!(!directory.path().join("data").exists());
    }

    #[test]
    fn free_ports_differ() {
        let [a, b, c] = free_ports().unwrap();
        assert!(a != b && b != c && a != c);
    }
}
