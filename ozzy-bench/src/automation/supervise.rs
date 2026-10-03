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

pub async fn execute(
    command: &[String],
    directory: &Path,
    timeout: Duration,
    environment: &BTreeMap<String, String>,
    server: Option<server::Monitor<'_>>,
    mut guard: Option<&mut Guard>,
) -> Result<Value> {
    let mut process = tokio::process::Command::new(command.first().ok_or("empty command")?);
    process
        .args(&command[1..])
        .current_dir(super::root())
        .envs(environment)
        .env("TMPDIR", super::SSD)
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
    let mut group = ProcessGroup {
        pid: child.id().ok_or("missing child PID")?,
        finished: false,
    };
    let mut stdout = child.stdout.take().ok_or("missing child stdout")?;
    let mut stderr = child.stderr.take().ok_or("missing child stderr")?;
    let mut out = fs::File::create(directory.join("stdout"))?;
    let mut err = fs::File::create(directory.join("stderr"))?;
    let mut out_buffer = vec![0; 65536];
    let mut err_buffer = vec![0; 65536];
    let mut out_done = false;
    let mut err_done = false;
    let mut bytes = Vec::new();
    let mut suffix = Vec::new();
    let warning = Regex::new(r"(?i)\b(warn(?:ing)?|error|fatal|panic|timed out|timeout)\b")?;
    let deadline = tokio::time::Instant::now() + timeout;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut log_at = Instant::now();
    let result:Result<()> = async {
        while !out_done || !err_done {
            tokio::select! {
                n = stdout.read(&mut out_buffer), if !out_done => {
                    let n = n?;
                    if n==0 {out_done=true;continue;}
                    out.write_all(&out_buffer[..n])?; out.flush()?;
                    suffix.extend_from_slice(&out_buffer[..n]);
                    if warning.is_match(&suffix) { return Err("benchmark warning or timeout diagnostic".into()); }
                    if suffix.len()>64 { suffix.drain(..suffix.len()-64); }
                    bytes.extend_from_slice(&out_buffer[..n]);
                    if bytes.len()>64*1024*1024 { return Err("benchmark output exceeded 64 MiB".into()); }
                }
                n = stderr.read(&mut err_buffer), if !err_done => {
                    let n=n?;
                    if n==0 {err_done=true;continue;}
                    err.write_all(&err_buffer[..n])?;err.flush()?;
                    return Err(format!("benchmark diagnostics: {}",String::from_utf8_lossy(&err_buffer[..n])).into());
                }
                _ = tick.tick() => {
                    check_canceled()?;
                    if let Some(guard)=guard.as_deref_mut() {guard.check(group.pid)?;}
                    if log_at.elapsed()>=Duration::from_millis(500) {
                        if let Some(server)=server {server.check()?;}
                        log_at=Instant::now();
                    }
                }
                () = tokio::time::sleep_until(deadline) => return Err("benchmark watchdog expired".into()),
            }
        }
        let status=tokio::time::timeout_at(deadline,child.wait()).await??;
        if !status.success() {return Err(format!("benchmark exited {status}").into());}
        if let Some(server)=server {server.check()?;}
        check_canceled()?;
        Ok(())
    }.await;
    if result.is_err() {
        group.kill();
        let _ = child.wait().await;
    }
    group.finished = true;
    result?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(Debug)]
pub struct CpuSnapshot {
    at: Instant,
    ticks: u64,
    steal: u64,
    processes: BTreeMap<u32, (u64, u64)>,
}

impl CpuSnapshot {
    pub fn take(pid: Option<u32>) -> Result<Self> {
        Self::take_servers(&pid.into_iter().collect::<Vec<_>>())
    }

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
