//! Async pipe supervision, process-group cleanup, and CPU provenance.
use super::{Result, capture, check_canceled, isolation::Guard, server};
use regex::bytes::Regex;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    os::unix::process::CommandExt,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use tokio::io::AsyncReadExt;

/// One read from a worker output pipe.
const READ_CHUNK: usize = 64 * 1024;
/// Trailing stdout bytes kept to match a diagnostic split across reads.
const WARNING_TAIL: usize = 64;
/// Maximum stdout a worker report may take.
const MAX_OUTPUT: usize = 64 * 1024 * 1024;
/// Maximum stderr kept for one diagnostic.
const MAX_DIAGNOSTIC: usize = 64 * 1024;
/// A diagnostic written in several pieces completes within this.
const DIAGNOSTIC_GRACE: Duration = Duration::from_secs(1);
/// How often cancellation and the isolation guard are checked.
const TICK: Duration = Duration::from_millis(100);
/// How often an external server is checked.
const SERVER_CHECK: Duration = Duration::from_millis(500);

#[derive(Debug)]
struct ProcessGroup {
    pid: u32,
    finished: bool,
}
impl ProcessGroup {
    fn kill(&self) {
        let _ = Command::new("kill")
            .args(["-KILL", "--", &format!("-{}", self.pid)])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if !self.finished {
            self.kill();
        }
    }
}

/// What a run checks besides its worker: an external server's liveness and
/// the process isolation guard.
#[derive(Debug, Default)]
pub struct Monitors<'a> {
    /// External server checked every half second and after the worker exits.
    pub server: Option<server::Monitor<'a>>,
    /// Isolation guard checked against the worker's process group every tick.
    pub isolation: Option<&'a mut Guard>,
}

/// Run and supervise one controlled worker until completion or its deadline.
pub async fn execute(
    command: &[String],
    directory: &Path,
    timeout: Duration,
    environment: &BTreeMap<String, String>,
    monitors: Monitors<'_>,
) -> Result<Value> {
    let mut worker = Worker::spawn(command, directory, environment)?;
    let mut output = Output::create(directory)?;
    let result = worker.supervise(&mut output, timeout, monitors).await;
    if result.is_err() {
        worker.group.kill();
        let _ = worker.child.wait().await;
    }
    worker.group.finished = true;
    result?;
    Ok(serde_json::from_slice(&output.report)?)
}

/// A spawned worker in its own process group, with both output pipes.
struct Worker {
    child: tokio::process::Child,
    group: ProcessGroup,
    stdout: tokio::process::ChildStdout,
    stderr: tokio::process::ChildStderr,
}

impl Worker {
    fn spawn(
        command: &[String],
        directory: &Path,
        environment: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let mut process = tokio::process::Command::new(command.first().ok_or("empty command")?);
        process
            .args(&command[1..])
            .current_dir(super::root())
            .envs(environment)
            .env("TMPDIR", super::artifact_root())
            .env("XDG_CACHE_HOME", directory.join("cache"))
            .env_remove("OZZY_BENCH_AFFINITY")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(affinity) = environment.get("OZZY_BENCH_AFFINITY") {
            process.env("OZZY_BENCH_AFFINITY", affinity);
        }
        process.as_std_mut().process_group(0);
        let mut child = process.spawn()?;
        let group = ProcessGroup {
            pid: child.id().ok_or("missing child PID")?,
            finished: false,
        };
        let stdout = child.stdout.take().ok_or("missing child stdout")?;
        let stderr = child.stderr.take().ok_or("missing child stderr")?;
        Ok(Self {
            child,
            group,
            stdout,
            stderr,
        })
    }

    /// Pump both pipes to EOF, then require a successful exit, all within
    /// `timeout`. Every tick checks cancellation, isolation, and the server.
    async fn supervise(
        &mut self,
        output: &mut Output,
        timeout: Duration,
        mut monitors: Monitors<'_>,
    ) -> Result<()> {
        let mut out_buffer = vec![0; READ_CHUNK];
        let mut err_buffer = vec![0; READ_CHUNK];
        let mut out_done = false;
        let mut err_done = false;
        let deadline = tokio::time::Instant::now() + timeout;
        let mut tick = tokio::time::interval(TICK);
        let mut log_at = Instant::now();
        while !out_done || !err_done {
            tokio::select! {
                n = self.stdout.read(&mut out_buffer), if !out_done => {
                    let n = n?;
                    if n == 0 { out_done = true; continue; }
                    output.stdout(&out_buffer[..n])?;
                }
                n = self.stderr.read(&mut err_buffer), if !err_done => {
                    let n = n?;
                    if n == 0 { err_done = true; continue; }
                    return output.fail_on_stderr(&mut self.stderr, &mut err_buffer, n).await;
                }
                _ = tick.tick() => {
                    check_canceled()?;
                    if let Some(guard) = monitors.isolation.as_deref_mut() { guard.check(self.group.pid)?; }
                    if log_at.elapsed() >= SERVER_CHECK {
                        if let Some(server) = monitors.server { server.check()?; }
                        log_at = Instant::now();
                    }
                }
                () = tokio::time::sleep_until(deadline) => return Err("benchmark watchdog expired".into()),
            }
        }
        let status = tokio::time::timeout_at(deadline, self.child.wait()).await??;
        if !status.success() {
            return Err(format!("benchmark exited {status}").into());
        }
        if let Some(server) = monitors.server {
            server.check()?;
        }
        check_canceled()
    }
}

/// Worker output: both pipes are logged to files, and stdout is kept as the
/// report and scanned for diagnostics.
struct Output {
    out: fs::File,
    err: fs::File,
    report: Vec<u8>,
    /// Recent stdout bytes, to match a diagnostic split across reads.
    tail: Vec<u8>,
    warning: Regex,
}

impl Output {
    fn create(directory: &Path) -> Result<Self> {
        Ok(Self {
            out: fs::File::create(directory.join("stdout"))?,
            err: fs::File::create(directory.join("stderr"))?,
            report: Vec::new(),
            tail: Vec::new(),
            warning: Regex::new(r"(?i)\b(warn(?:ing)?|error|fatal|panic|timed out|timeout)\b")?,
        })
    }

    /// Log and keep one stdout chunk. A diagnostic or an oversized report
    /// fails the run.
    fn stdout(&mut self, chunk: &[u8]) -> Result<()> {
        self.out.write_all(chunk)?;
        self.out.flush()?;
        self.tail.extend_from_slice(chunk);
        if self.warning.is_match(&self.tail) {
            return Err("benchmark warning or timeout diagnostic".into());
        }
        if self.tail.len() > WARNING_TAIL {
            self.tail.drain(..self.tail.len() - WARNING_TAIL);
        }
        self.report.extend_from_slice(chunk);
        if self.report.len() > MAX_OUTPUT {
            return Err(format!("benchmark output exceeded {MAX_OUTPUT} bytes").into());
        }
        Ok(())
    }

    /// Any stderr output fails the run. The first `n` bytes in `buffer` start
    /// the diagnostic; keep the rest of one written in several pieces, reading
    /// until EOF or a short grace.
    async fn fail_on_stderr(
        &mut self,
        stderr: &mut tokio::process::ChildStderr,
        buffer: &mut [u8],
        n: usize,
    ) -> Result<()> {
        self.err.write_all(&buffer[..n])?;
        self.err.flush()?;
        let mut text = buffer[..n].to_vec();
        let grace = tokio::time::Instant::now() + DIAGNOSTIC_GRACE;
        while text.len() < MAX_DIAGNOSTIC {
            match tokio::time::timeout_at(grace, stderr.read(buffer)).await {
                Ok(Ok(0)) | Err(_) => break,
                Ok(Ok(n)) => {
                    self.err.write_all(&buffer[..n])?;
                    self.err.flush()?;
                    text.extend_from_slice(&buffer[..n]);
                }
                Ok(Err(error)) => return Err(error.into()),
            }
        }
        Err(format!("benchmark diagnostics: {}", String::from_utf8_lossy(&text)).into())
    }
}

#[derive(Debug)]
/// Captured process CPU time used to compute measurement-window utilization.
pub struct CpuSnapshot {
    at: Instant,
    ticks: u64,
    steal: u64,
    processes: BTreeMap<u32, (u64, u64)>,
}

impl CpuSnapshot {
    /// Capture CPU accounting for the selected process or current process.
    pub fn take(pid: Option<u32>) -> Result<Self> {
        Self::take_servers(&pid.into_iter().collect::<Vec<_>>())
    }

    /// Capture combined CPU accounting for the supplied server processes.
    pub fn take_servers(pids: &[u32]) -> Result<Self> {
        let stat = fs::read_to_string("/proc/stat")?;
        let ticks = stat
            .lines()
            .next()
            .ok_or("missing CPU counters")?
            .split_whitespace()
            .skip(1)
            .take(8)
            .map(str::parse::<u64>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if ticks.len() != 8 {
            return Err("incomplete CPU counters".into());
        }
        let processes = pids
            .iter()
            .map(|pid| -> Result<_> {
                let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
                let fields = stat
                    .rsplit_once(')')
                    .ok_or("invalid process stat")?
                    .1
                    .split_whitespace()
                    .collect::<Vec<_>>();
                Ok((
                    *pid,
                    (
                        fields[19].parse::<u64>()?,
                        fields[11].parse::<u64>()? + fields[12].parse::<u64>()?,
                    ),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            at: Instant::now(),
            ticks: ticks.iter().sum(),
            steal: ticks[7],
            processes,
        })
    }
    /// Compute CPU time and utilization between two compatible snapshots.
    pub fn elapsed(&self, after: &Self) -> Result<Value> {
        let seconds = after.at.duration_since(self.at).as_secs_f64();
        let ticks = after
            .ticks
            .checked_sub(self.ticks)
            .ok_or("CPU counters reversed")?;
        let steal = after
            .steal
            .checked_sub(self.steal)
            .ok_or("steal counter reversed")?;
        if seconds <= 0.0 || ticks == 0 || steal > ticks {
            return Err("invalid CPU counter interval".into());
        }
        let hz = capture(Command::new("getconf").arg("CLK_TCK"))?.parse::<f64>()?;
        let mut result = json!({"scope":"whole benchmark child run: provisioning, startup, warmup, measurement, verification drain, cleanup and shutdown; external server prestarted","elapsed_seconds":seconds,"host_steal_fraction":steal as f64/ticks as f64,"host_ticks":ticks,"host_steal_ticks":steal,"host_steal_seconds":steal as f64/hz,"host_scope":"coordinator host only, not remote broker hosts"});
        if self.processes.keys().ne(after.processes.keys()) {
            return Err("external broker set changed".into());
        }
        let mut total_cpu = 0;
        for (pid, (a_start, a_cpu)) in &self.processes {
            let (b_start, b_cpu) = &after.processes[pid];
            if a_start != b_start || b_cpu < a_cpu {
                return Err("external server changed during workload".into());
            }
            total_cpu += b_cpu - a_cpu;
        }
        if !self.processes.is_empty() {
            let cpu = total_cpu as f64 / hz;
            result["external_server_cpu_seconds"] = json!(cpu);
            result["external_server_average_cores"] = json!(cpu / seconds);
        }
        Ok(result)
    }
}
