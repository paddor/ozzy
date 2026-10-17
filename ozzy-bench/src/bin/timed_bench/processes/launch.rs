//! Bounded process/SSH lifecycle and diagnostic supervision.

use ozzy_bench::control::Connection;
use ozzy_proto::GroupId;
use serde_json::Value;
use std::io::Read;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::Duration;

use super::super::{Args, Result, error};

use ozzy_bench::budget::CONTROL_STEP;

const MAX_LINE: u64 = 64 * 1024;
/// How often a stopping worker is checked for exit.
const EXIT_POLL: Duration = Duration::from_millis(10);
/// How often running workers are checked for diagnostics.
const DIAGNOSTIC_POLL: Duration = Duration::from_millis(50);

mod command;
#[cfg(test)]
mod tests;

use command::placed_command;

pub(in super::super) use ozzy_bench::placement::Placement;

pub(in super::super) use ozzy_bench::control::{input, reply};

#[derive(Debug)]
pub(in super::super) struct Process {
    worker: Worker,
    control: Connection,
    pub errors: PathBuf,
    pub remote: bool,
    pub placement: Option<Placement>,
    external_log: Option<regex::bytes::Regex>,
}

/// A worker process, or an inproc role thread sharing this process.
#[derive(Debug)]
enum Worker {
    Child(Child),
    Thread {
        handle: Option<JoinHandle<Result<()>>>,
        finished: Arc<AtomicBool>,
    },
}

#[derive(Clone)]
pub(in super::super) struct Monitor {
    pid: u32,
    errors: PathBuf,
    control: ozzy_bench::control::Monitor,
    external_log: Option<regex::bytes::Regex>,
    finished: Option<Arc<AtomicBool>>,
}

struct Finished(Arc<AtomicBool>);

impl Drop for Finished {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl Process {
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
        let arguments = Self::arguments(&args, index, group, placement, "--worker-index")?;
        let errors = directory.join(format!("voter-{index}.stderr"));
        let mut process = if args.inproc {
            if placement.remote.is_some() {
                return Err(error("inproc brokers cannot use remote placements"));
            }
            Self::spawn_thread(
                format!("bench/voter-{index}"),
                arguments,
                errors,
                placement.cpus.clone(),
                control,
            )?
        } else {
            let command = placed_command(placement, arguments, args.budget().lifetime())?;
            Self::spawn_command(command, errors, placement.remote.is_some(), control)?
        };
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
        // A timed process keeps its application and SDK threads on one client
        // core, so each record handoff stays in that core's caches. Readers
        // continue the rotation after the writers.
        let slot = if reader {
            args.producer_workers.unwrap_or(1) + index
        } else {
            index
        };
        let cpus = worker_cpu(slot)?;
        let placement = Placement {
            bind: IpAddr::from([127, 0, 0, 1]),
            remote: None,
            storage_dir: None,
            cpus,
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
        let arguments = Self::arguments(&args, index, group, &placement, flag)?;
        let errors = directory.join(format!("{role}-{index}.stderr"));
        let process = if args.inproc {
            Self::spawn_thread(
                format!("bench/{role}-{index}"),
                arguments,
                errors,
                None,
                control,
            )?
        } else {
            Self::spawn_command(
                placed_command(&placement, arguments, args.budget().lifetime())?,
                errors,
                false,
                control,
            )?
        };
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
            worker: Worker::Child(child),
            control,
            errors,
            remote,
            placement: None,
            external_log: None,
        })
    }

    /// Start an inproc role on its own current-thread runtime. Brokers pin
    /// before starting so their shard and dispatcher threads inherit the mask.
    /// Failures return through control; the empty stderr file keeps artifacts uniform.
    fn spawn_thread(
        name: String,
        arguments: Vec<String>,
        errors: PathBuf,
        cpus: Option<Vec<usize>>,
        control: Connection,
    ) -> Result<Self> {
        if std::env::var_os("OZZY_BENCH_HEAPTRACK").is_some() {
            return Err(error("heaptrack mode requires worker processes"));
        }
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&errors)?;
        let context = ozzy_bench::control::context();
        let finished = Arc::new(AtomicBool::new(false));
        let done = Finished(finished.clone());
        let handle = std::thread::Builder::new().name(name).spawn(move || {
            let _done = done;
            if let Some(cpus) = cpus {
                let mut set = rustix::thread::CpuSet::new();
                for cpu in cpus {
                    set.set(cpu);
                }
                rustix::thread::sched_setaffinity(None, &set)?;
            }
            super::super::run_inproc_worker(arguments, context)
        })?;
        Ok(Self {
            worker: Worker::Thread {
                handle: Some(handle),
                finished,
            },
            control,
            errors,
            remote: false,
            placement: None,
            external_log: None,
        })
    }

    pub(in super::super) fn monitor(&self) -> Monitor {
        Monitor {
            pid: self.id(),
            errors: self.errors.clone(),
            control: self.control.monitor(),
            external_log: self.external_log.clone(),
            finished: match &self.worker {
                Worker::Child(_) => None,
                Worker::Thread { finished, .. } => Some(finished.clone()),
            },
        }
    }

    /// Thread roles report the shared process ID.
    pub(in super::super) fn id(&self) -> u32 {
        match &self.worker {
            Worker::Child(child) => child.id(),
            Worker::Thread { .. } => std::process::id(),
        }
    }
    pub(in super::super) fn send(&mut self, value: &Value) -> Result<()> {
        self.control.send(value)
    }
    pub(in super::super) async fn receive(&mut self, expected: &str) -> Result<Value> {
        self.receive_until(expected, tokio::time::Instant::now() + CONTROL_STEP)
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
        tokio::time::timeout(CONTROL_STEP, async {
            loop {
                match &mut self.worker {
                    Worker::Child(child) => {
                        if let Some(status) = child.try_wait()? {
                            if fatal_stderr(&self.errors, self.external_log.as_ref())? {
                                return Err(error(diagnostics(&self.errors)?));
                            }
                            if !status.success() {
                                return Err(error(format!("worker exited {status}")));
                            }
                            return Ok(());
                        }
                    }
                    Worker::Thread { handle, .. } => {
                        if handle.as_ref().is_none_or(JoinHandle::is_finished) {
                            return match handle.take().map(JoinHandle::join) {
                                None | Some(Ok(Ok(()))) => Ok(()),
                                Some(Ok(Err(cause))) => Err(cause),
                                Some(Err(_)) => Err(error("inproc worker panicked")),
                            };
                        }
                    }
                }
                tokio::time::sleep(EXIT_POLL).await;
            }
        })
        .await?
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.control.shutdown();
        // A still-running inproc role exits with the process.
        if let Worker::Child(child) = &mut self.worker {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
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
            if let Some(finished) = &monitor.finished {
                if finished.load(Ordering::Acquire) {
                    return error(format!("inproc worker exited: {}", path.display()));
                }
            } else {
                let status = std::fs::read_to_string(format!("/proc/{}/stat", monitor.pid));
                if status.as_ref().map_or(true, |line| {
                    line.rsplit_once(')')
                        .is_none_or(|(_, rest)| rest.starts_with(" Z "))
                }) {
                    return error(format!("worker pid={} exited", monitor.pid));
                }
            }
            if !monitor.control.is_live() {
                return error(format!(
                    "worker exited or closed output: {}",
                    path.display()
                ));
            }
        }
        tokio::time::sleep(DIAGNOSTIC_POLL).await;
    }
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

/// The client CPU of timed process `slot`, round robin over this process's
/// CPU pool. `None` when no client pool restricts this process.
fn worker_cpu(slot: usize) -> Result<Option<Vec<usize>>> {
    let pool = rustix::thread::sched_getaffinity(None)?;
    let cpus: Vec<usize> = (0..rustix::thread::CpuSet::MAX_CPU)
        .filter(|&cpu| pool.is_set(cpu))
        .collect();
    // Unlike `available_parallelism`, this ignores the affinity mask.
    let online = std::fs::read_to_string("/sys/devices/system/cpu/online")?
        .trim()
        .split(',')
        .filter_map(|range| match range.split_once('-') {
            Some((first, last)) => {
                Some(last.parse::<usize>().ok()? + 1 - first.parse::<usize>().ok()?)
            }
            None => range.parse::<usize>().ok().map(|_| 1),
        })
        .sum::<usize>();
    if cpus.is_empty() || cpus.len() >= online {
        return Ok(None);
    }
    Ok(Some(vec![cpus[slot % cpus.len()]]))
}
