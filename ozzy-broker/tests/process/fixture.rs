//! Explicit CLI provisioning and process ownership. Serving output stays in files.

use super::super::{SINGLE, command};
use ozzy_broker::{CheckedConfig, check_config, host_resources, load_deployment};
use std::{
    fs,
    future::Future,
    net::TcpListener,
    os::unix::process::ExitStatusExt,
    path::PathBuf,
    pin::pin,
    process::{Child, Command, ExitStatus, Stdio},
    time::Duration,
};

pub(super) struct Fixture {
    directory: tempfile::TempDir,
    config: PathBuf,
    shared: PathBuf,
    local: PathBuf,
    root: PathBuf,
    image: Option<String>,
}

impl Fixture {
    pub(super) fn keep_artifacts(mut self) -> Self {
        self.directory.disable_cleanup(true);
        eprintln!(
            "single-broker soak artifacts: {}",
            self.directory.path().display()
        );
        self
    }
    pub(super) fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let config = directory.path().join("deployment.toml");
        let root = directory.path().join("volume");
        // Allocate once. Every process uses the same explicit persisted endpoints.
        let sockets = [
            TcpListener::bind("127.0.0.1:0").unwrap(),
            TcpListener::bind("127.0.0.1:0").unwrap(),
        ];
        let source = SINGLE
            .replace("/var/lib/ozzy/data", root.to_str().unwrap())
            .replace("7100", &sockets[0].local_addr().unwrap().port().to_string())
            .replace("7101", &sockets[1].local_addr().unwrap().port().to_string());
        fs::write(
            &config,
            format!(
                "{source}\npartitions = 4\nsegment_bytes = 1048576\nmax_append_bytes = 65536\n\
                 [brokers.laptop.devices.ssd.workers]\nbackend = \"pool\"\nwrite_threads = 1\n"
            ),
        )
        .unwrap();
        Self {
            shared: directory.path().join("shared.identity"),
            local: directory.path().join("local.identity"),
            directory,
            config,
            root,
            image: None,
        }
    }

    pub(super) fn container(image: String) -> Self {
        Self {
            image: Some(image),
            ..Self::new()
        }
    }

    fn command(&self, name: Option<&str>) -> Command {
        match &self.image {
            None => command(&self.config),
            Some(image) => {
                let mut command = podman();
                command.args(["run", "--rm", "--network", "host", "--userns", "keep-id"]);
                if let Some(name) = name {
                    command.args(["--name", name]);
                }
                command
                    .arg("--volume")
                    .arg(format!(
                        "{}:{}:rw",
                        self.directory.path().display(),
                        self.directory.path().display()
                    ))
                    .arg(image)
                    .arg("--config")
                    .arg(&self.config);
                command
            }
        }
    }

    fn broker_command(&self, operation: &str, name: Option<&str>) -> Command {
        let mut command = self.command(name);
        command
            .args([operation, "--broker", "laptop", "--identity"])
            .arg(&self.shared)
            .arg("--local-identity")
            .arg(&self.local);
        command
    }

    pub(super) fn serve(&self, trusted: bool) -> Command {
        let mut command = self.broker_command("serve", None);
        if trusted {
            command.arg("--trusted-transport");
        }
        command
    }

    pub(super) fn recover(&self, selections: &[&str], trusted: bool) -> Command {
        let mut command = self.broker_command("recover", None);
        if trusted {
            command.arg("--trusted-transport");
        }
        command.args(selections);
        command
    }

    pub(super) fn provision_volumes(&self) {
        success(
            self.command(None)
                .arg("init")
                .arg("--identity")
                .arg(&self.shared),
        );
        success(&mut self.broker_command("init-broker", None));
        fs::create_dir(&self.root).unwrap();
        success(&mut self.broker_command("init-volumes", None));
    }

    pub(super) fn format(&self) {
        success(&mut self.broker_command("format", None));
    }

    pub(super) fn checked(&self) -> CheckedConfig {
        check_config(
            load_deployment(&self.config).unwrap(),
            &self.shared,
            "laptop",
            &host_resources().unwrap(),
        )
        .unwrap()
    }

    pub(super) fn assert_no_history(&self) {
        assert!(!self.root.join("data").exists());
    }

    pub(super) fn start(&self, round: usize) -> Running {
        self.start_on_cpu(round, None)
    }

    pub(super) fn start_on_cpu(&self, round: usize, cpu: Option<u32>) -> Running {
        let log = self.directory.path().join(format!("serve-{round}.log"));
        let name = self
            .image
            .as_ref()
            .map(|_| format!("ozzy-test-{}-{round}", uuid::Uuid::now_v7()));
        let mut command = self.broker_command("serve", name.as_deref());
        command.arg("--trusted-transport");
        if let Some(cpu) = cpu {
            assert!(self.image.is_none());
            let mut pinned = Command::new("taskset");
            pinned
                .args(["-c", &cpu.to_string()])
                .arg(command.get_program())
                .args(command.get_args());
            command = pinned;
        }
        Running::spawn(command, log, name)
    }
}

pub(super) fn success(command: &mut Command) {
    let output = command.output().unwrap();
    assert!(output.status.success(), "{command:?}: {output:?}");
}

fn podman() -> Command {
    let mut command = Command::new("podman");
    for (variable, flag) in [
        ("OZZY_CONTAINER_ROOT", "--root"),
        ("OZZY_CONTAINER_RUNROOT", "--runroot"),
    ] {
        if let Some(value) = std::env::var_os(variable) {
            command.arg(flag).arg(value);
        }
    }
    command
}

pub(super) struct Running {
    child: Child,
    log: PathBuf,
    container: Option<String>,
}

impl Running {
    pub(super) fn spawn(mut command: Command, log: PathBuf, container: Option<String>) -> Self {
        let output = fs::File::create(&log).unwrap();
        let child = command
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .unwrap();
        Self {
            child,
            log,
            container,
        }
    }

    pub(super) fn signal(&mut self, name: &str) {
        let mut command = if let Some(container) = &self.container {
            let mut command = podman();
            command.args(["kill", "--signal", name, container]);
            command
        } else {
            let mut command = Command::new("kill");
            command
                .args(["-s", name, "--"])
                .arg(self.child.id().to_string());
            command
        };
        let status = command.status().unwrap();
        assert!(status.success());
    }

    pub(super) async fn exited(&mut self, killed: bool) {
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|error| panic!("process did not exit: {error}; {}", self.log()));
        if killed {
            if self.container.is_some() {
                assert_eq!(status.code(), Some(137), "{}", self.log());
            } else {
                assert_eq!(status.signal(), Some(9), "{}", self.log());
            }
        } else {
            assert!(status.success(), "{status}: {}", self.log());
        }
    }

    pub(super) async fn observe<T>(&mut self, stage: &str, future: impl Future<Output = T>) -> T {
        observe(&mut [self], stage, future).await
    }

    pub(super) async fn ready(&mut self) {
        let log = self.log.clone();
        self.observe("broker startup", async move {
            loop {
                if fs::read_to_string(&log).is_ok_and(|text| text.contains("Serving broker ")) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    }

    fn failed(&self, stage: &str, status: ExitStatus) -> ! {
        panic!("{stage}: broker exited with {status}; {}", self.log());
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

pub(super) async fn observe<T>(
    brokers: &mut [&mut Running],
    stage: &str,
    future: impl Future<Output = T>,
) -> T {
    let mut future = pin!(future);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                result = &mut future => return result,
                () = tokio::time::sleep(Duration::from_millis(10)) => {
                    for broker in brokers.iter_mut() {
                        if let Some(status) = broker.child.try_wait().unwrap() {
                            broker.failed(stage, status);
                        }
                    }
                }
            }
        }
    })
    .await
    .unwrap_or_else(|error| {
        let logs = brokers
            .iter()
            .map(|broker| broker.log())
            .collect::<Vec<_>>();
        panic!("{stage}: {error}; {logs:?}")
    })
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(container) = &self.container {
            let _ = podman()
                .args(["rm", "--force", "--ignore", container])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
