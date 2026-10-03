//! Bounded process/SSH lifecycle and diagnostic supervision.

use ozzy_bench::control::Connection;
use ozzy_proto::GroupId;
use serde_json::Value;
use std::io::Read;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use super::super::{Args, Result, error};

const MAX_LINE: u64 = 64 * 1024;
/// Control reply deadline for child processes.
const DEADLINE: Duration = Duration::from_secs(60);

#[cfg(test)]
mod tests;

pub(in super::super) use ozzy_bench::placement::Placement;

pub(in super::super) use ozzy_bench::control::{input, reply};

#[derive(Debug)]
pub(in super::super) struct Process {
    child: Child,
    control: Connection,
    pub errors: PathBuf,
    pub remote: bool,
    pub placement: Option<Placement>,
    external_log: Option<regex::bytes::Regex>,
}

#[derive(Clone)]
pub(in super::super) struct Monitor {
    pid: u32,
    errors: PathBuf,
    control: ozzy_bench::control::Monitor,
    external_log: Option<regex::bytes::Regex>,
}

impl Process {
    fn command(
        args: &Args,
        index: usize,
        group: GroupId,
        placement: &Placement,
        role: &str,
    ) -> Result<Command> {
        let mut arguments = vec![
            role.into(),
            index.to_string(),
            "--bind".into(),
            placement.bind.to_string(),
            "--group".into(),
            group.to_string(),
            "--record-bytes".into(),
            args.record_bytes.to_string(),
            "--history-mib".into(),
            args.history_mib.to_string(),
        ];
        Self::execution_arguments(args, &mut arguments);
        if let (Some(threads), "--worker-index") = (args.broker_io_threads, role) {
            let offset = arguments
                .iter()
                .position(|arg| arg == "--io-threads")
                .expect("I/O thread argument");
            arguments[offset + 1] = threads.to_string();
        }
        if let Some(storage) = &placement.storage_dir {
            let offset = arguments
                .iter()
                .position(|arg| arg == "--storage-dir")
                .expect("storage argument");
            arguments[offset + 1] = storage
                .to_str()
                .ok_or_else(|| error("non-UTF8 storage path"))?
                .into();
        }
        if args.network_ingress {
            arguments.push("--network-ingress".into());
        }
        if args.streaming {
            arguments.extend([
                "--streaming".into(),
                "--request-records".into(),
                args.request_records.to_string(),
            ]);
        }
        if args.payload_compression == super::super::PayloadCompression::Off {
            arguments.extend(["--payload-compression".into(), "off".into()]);
        }
        if args.network_ingress {
            arguments.push("--processes".into());
        }
        for (name, value) in [
            ("--producer-workers", args.producer_workers),
            ("--window", args.window),
        ] {
            if let Some(value) = value {
                arguments.extend([name.into(), value.to_string()]);
            }
        }
        placed_command(placement, arguments)
    }

    fn execution_arguments(args: &Args, arguments: &mut Vec<String>) {
        for (enabled, flag) in [
            (args.balanced_partitions, "--balanced-partitions"),
            (args.random_payload, "--random-payload"),
            (args.json_payload, "--json-payload"),
            (args.live_readers, "--live-readers"),
            (args.binary_payload, "--binary-payload"),
        ] {
            if enabled {
                arguments.push(flag.into());
            }
        }

        #[cfg(feature = "comparisons")]
        Self::external_arguments(args, arguments);
        args.control.append(arguments);
        args.native.append(arguments);
        #[cfg(feature = "comparisons")]
        if args.external_system.is_none() {
            arguments.extend(["--system".into(), args.system.name().into()]);
        }
        #[cfg(not(feature = "comparisons"))]
        arguments.extend(["--system".into(), args.system.name().into()]);
        arguments.extend([
            "--storage-dir".into(),
            args.storage_dir.to_string_lossy().into_owned(),
            "--segment-decoded-mib".into(),
            args.segment_decoded_mib.to_string(),
            "--segment-mib".into(),
            args.segment_mib.to_string(),
            "--direct-io".into(),
            args.direct_io.to_string(),
            "--io-backend".into(),
            args.io_backend.name().into(),
            "--aio-depth".into(),
            args.aio_depth.to_string(),
            "--writer-batch-records".into(),
            args.writer_batch_records.to_string(),
            "--writer-batch-target-kib".into(),
            args.writer_batch_target_kib.to_string(),
            "--writer-inflight-appends".into(),
            args.writer_inflight_appends.to_string(),
            "--reader-records".into(),
            args.reader_records.to_string(),
            "--reader-payload-mib".into(),
            args.reader_payload_mib.to_string(),
            "--io-threads".into(),
            args.io_threads.to_string(),
            "--app-threads".into(),
            args.app_threads.to_string(),
        ]);
        if let Some(duration) = args.duration {
            arguments.extend([
                "--duration".into(),
                duration.to_string(),
                "--warmup".into(),
                args.warmup.to_string(),
                "--reader-workers".into(),
                args.reader_workers().to_string(),
                "--readers-per-partition".into(),
                args.readers_per_partition.to_string(),
                "--history-operations".into(),
                args.history_operations.to_string(),
                "--drain-timeout-secs".into(),
                args.drain_timeout_secs.to_string(),
            ]);
        }
        if let Some(rate) = args.records_per_second {
            arguments.extend(["--records-per-second".into(), rate.to_string()]);
        }
        if let Some(ramp) = &args.ramp {
            arguments.extend(["--ramp".into(), ramp.to_string()]);
        }
    }

    #[cfg(feature = "comparisons")]
    fn external_arguments(args: &Args, arguments: &mut Vec<String>) {
        if let Some(system) = args.external_system {
            arguments.extend([
                "--external-system".into(),
                system.name().into(),
                "--external-policy".into(),
                args.external_policy
                    .expect("validated policy")
                    .name()
                    .into(),
                "--external-endpoint".into(),
                args.external_endpoint.clone().expect("validated endpoint"),
            ]);
        }
    }

    pub(in super::super) async fn spawn(
        args: &Args,
        index: usize,
        group: GroupId,
        placement: &Placement,
        directory: &Path,
    ) -> Result<Self> {
        if placement.remote.is_some() && args.control.control_bind.is_none() {
            return Err(error(
                "remote workers require --control-bind tcp://<reachable-address>:0",
            ));
        }
        let (control, config) = Connection::listen(args.control.control_bind.as_deref()).await?;
        let mut args = args.clone();
        args.control = config;
        let command = Self::command(&args, index, group, placement, "--worker-index")?;
        let errors = directory.join(format!("voter-{index}.stderr"));
        let mut process =
            Self::spawn_command(command, errors, placement.remote.is_some(), control)?;
        process.placement = Some(placement.clone());
        Ok(process)
    }

    pub(in super::super) async fn spawn_timed(
        args: &Args,
        index: usize,
        group: GroupId,
        reader: bool,
        directory: &Path,
    ) -> Result<Self> {
        let placement = Placement {
            bind: IpAddr::from([127, 0, 0, 1]),
            remote: None,
            storage_dir: None,
            cpus: None,
        };
        let role = if reader { "reader" } else { "producer" };
        let flag = if reader {
            "--reader-worker"
        } else {
            "--producer-worker"
        };
        let (control, config) = Connection::listen(None).await?;
        let mut args = args.clone();
        args.control = config;
        let command = Self::command(&args, index, group, &placement, flag)?;
        let process = Self::spawn_command(
            command,
            directory.join(format!("{role}-{index}.stderr")),
            false,
            control,
        )?;
        #[cfg(feature = "comparisons")]
        let process = {
            use super::super::timed::external::System;
            let mut process = process;
            process.external_log = match args.external_system {
                Some(System::Kafka | System::Redpanda) => Some(external_log("librdkafka")?),
                Some(System::Iggy) => Some(external_log("iggy(?:::[A-Za-z0-9_]+)*")?),
                None => None,
            };
            process
        };
        Ok(process)
    }

    fn spawn_command(
        mut command: Command,
        errors: PathBuf,
        remote: bool,
        control: Connection,
    ) -> Result<Self> {
        if let Some(library) = std::env::var_os("OZZY_BENCH_HEAPTRACK") {
            let library = PathBuf::from(library);
            if remote || !library.is_absolute() || !library.is_file() {
                return Err(error(
                    "OZZY_BENCH_HEAPTRACK requires a local absolute preload library path",
                ));
            }
            if std::env::var_os("LD_PRELOAD").is_some() {
                return Err(error(
                    "heaptrack mode cannot replace an existing LD_PRELOAD",
                ));
            }
            // Each worker gets a separate raw trace. Interpretation runs afterward
            // so no helper process competes with the measured workload.
            command.env("LD_PRELOAD", library).env(
                "DUMP_HEAPTRACK_OUTPUT",
                errors.with_extension("heaptrack.raw"),
            );
        }
        let stderr = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&errors)?;
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(stderr)
            .spawn()?;
        Ok(Self {
            child,
            control,
            errors,
            remote,
            placement: None,
            external_log: None,
        })
    }

    pub(in super::super) fn monitor(&self) -> Monitor {
        Monitor {
            pid: self.child.id(),
            errors: self.errors.clone(),
            control: self.control.monitor(),
            external_log: self.external_log.clone(),
        }
    }

    pub(in super::super) fn id(&self) -> u32 {
        self.child.id()
    }
    pub(in super::super) fn send(&mut self, value: &Value) -> Result<()> {
        self.control.send(value)
    }
    pub(in super::super) async fn receive(&mut self, expected: &str) -> Result<Value> {
        self.receive_until(expected, tokio::time::Instant::now() + DEADLINE)
            .await
    }
    pub(in super::super) async fn receive_until(
        &mut self,
        expected: &str,
        deadline: tokio::time::Instant,
    ) -> Result<Value> {
        let reply = tokio::time::timeout_at(deadline, self.control.receive())
            .await
            .map_err(|cause| {
                error(format!(
                    "worker pid={} ({}) waiting for {expected}: {cause}",
                    self.id(),
                    self.errors.display()
                ))
            })?
            .map_err(|cause| {
                error(format!(
                    "worker pid={} ({}) waiting for {expected}: {cause}",
                    self.id(),
                    self.errors.display()
                ))
            })?;
        if reply["event"] != expected {
            return Err(error(format!("expected {expected}, got {reply}")));
        }
        Ok(reply)
    }
    pub(in super::super) async fn stop(&mut self) -> Result<()> {
        self.control.shutdown()?;
        tokio::time::timeout(DEADLINE, async {
            loop {
                if let Some(status) = self.child.try_wait()? {
                    if fatal_stderr(&self.errors, self.external_log.as_ref())? {
                        return Err(error(diagnostics(&self.errors)?));
                    }
                    if !status.success() {
                        return Err(error(format!("worker exited {status}")));
                    }
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.control.shutdown();
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

pub(in super::super) async fn watch_diagnostics(
    monitors: &[Monitor],
) -> Box<dyn std::error::Error + Send + Sync> {
    loop {
        for monitor in monitors {
            if let Some(cause) = monitor.control.failure() {
                return error(format!(
                    "worker pid={} ({}): {cause}",
                    monitor.pid,
                    monitor.errors.display()
                ));
            }
            let path = &monitor.errors;
            if fatal_stderr(path, monitor.external_log.as_ref()).unwrap_or(true) {
                return error(format!(
                    "worker diagnostics: {}",
                    diagnostics(path).unwrap_or_default()
                ));
            }
            let status = std::fs::read_to_string(format!("/proc/{}/stat", monitor.pid));
            if status.as_ref().map_or(true, |line| {
                line.rsplit_once(')')
                    .is_none_or(|(_, rest)| rest.starts_with(" Z "))
            }) {
                return error(format!("worker pid={} exited", monitor.pid));
            }
            if !monitor.control.is_live() {
                return error(format!(
                    "worker exited or closed output: {}",
                    path.display()
                ));
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn placed_command(placement: &Placement, arguments: Vec<String>) -> Result<Command> {
    let command = if let Some((host, executable)) = &placement.remote {
        let quote = |value: &str| format!("'{}'", value.replace('\'', "'\\''"));
        let mut shell = format!(
            "exec timeout --signal=TERM --kill-after=5s 180s {}{}",
            placement
                .cpu_list()
                .map_or_else(String::new, |cpus| format!(
                    "taskset --cpu-list {} ",
                    quote(&cpus)
                )),
            quote(
                executable
                    .to_str()
                    .ok_or_else(|| error("non-UTF8 executable"))?
            )
        );
        for argument in &arguments {
            shell.push(' ');
            shell.push_str(&quote(argument));
        }
        let mut command = Command::new("ssh");
        command.args([
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "ConnectTimeout=8",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=2",
            "--",
        ]);
        command.arg(host).arg(shell);
        command
    } else {
        let executable = std::env::current_exe()?;
        let mut command = if let Some(cpus) = placement.cpu_list() {
            let mut command = Command::new("taskset");
            command.args(["--cpu-list", &cpus]).arg(executable);
            command
        } else {
            Command::new(executable)
        };
        command.args(arguments);
        command
    };
    Ok(command)
}

#[cfg(any(feature = "comparisons", test))]
fn external_log(target: &str) -> Result<regex::bytes::Regex> {
    // External SDK informational and warning logs remain saved. Ozzy, OMQ,
    // benchmark output, external SDK errors, and unstructured stderr stay fatal.
    Ok(regex::bytes::Regex::new(&format!(
        r"^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9:.]+Z\s+(TRACE|DEBUG|INFO|WARN) {target}: "
    ))?)
}

fn fatal_stderr(path: &Path, external_log: Option<&regex::bytes::Regex>) -> std::io::Result<bool> {
    if std::fs::metadata(path)?.len() == 0 {
        return Ok(false);
    }
    let Some(allowed) = external_log else {
        return Ok(true);
    };
    let bytes = std::fs::read(path)?;
    Ok(bytes
        .split_inclusive(|byte| *byte == b'\n')
        .any(|line| !allowed.is_match(line)))
}

fn diagnostics(path: &Path) -> std::io::Result<String> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(MAX_LINE)
        .read_to_end(&mut bytes)?;
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if std::fs::metadata(path)?.len() > MAX_LINE {
        text.push_str("\n[diagnostics truncated; full stderr preserved in artifacts]");
    }
    Ok(text)
}
