//! Include paused/stopped processes in the benchmark isolation audit.
use super::Result;
use rustix::thread::{CpuSet, Pid, sched_getaffinity, sched_setaffinity};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

/// Result-row identifier for the required benchmark isolation contract.
pub const CONTRACT: &str = "fresh-server-per-case; no-other-broker-resident; process-group-audit";
const BENCHMARKS: &[&str] = &[
    "ozzy_timed_bench",
    "ozzy_segment_verify_bench",
    "ozzy_lz4_dict_bench",
];
const COMPETITORS: &[&str] = &[
    "dd",
    "fio",
    "cargo",
    "rustc",
    "rustdoc",
    "rustfmt",
    "clippy-driver",
    "cargo-clippy",
    "perf",
    "heaptrack",
    "strace",
    "ozzy_chart",
    "ozzy_compare",
    "ozzy_workloads",
    "ozzy_profile",
];
const SERVERS: &[&str] = &["iggy-server", "redpanda", "java"];

/// Read the allowed CPU IDs for a process, defaulting to the current process.
pub fn cpus(pid: Option<u32>) -> Result<Vec<usize>> {
    let mask = sched_getaffinity(pid.map(|id| Pid::from_raw(id.try_into().unwrap()).unwrap()))?;
    Ok((0..CpuSet::MAX_CPU)
        .filter(|&cpu| mask.is_set(cpu))
        .collect())
}

/// Restrict a process to the selected CPU IDs.
pub fn pin(pid: Option<u32>, cpus: &[usize]) -> Result<()> {
    if cpus.is_empty() || cpus.iter().any(|&cpu| cpu >= CpuSet::MAX_CPU) {
        return Err("invalid CPU mask".into());
    }
    let mut mask = CpuSet::new();
    for &cpu in cpus {
        mask.set(cpu);
    }
    sched_setaffinity(
        pid.map(|id| Pid::from_raw(id.try_into().unwrap()).unwrap()),
        &mask,
    )?;
    Ok(())
}

fn inspect(path: &Path) -> Result<Option<Value>> {
    let comm = fs::read_to_string(path.join("comm"))?;
    let comm = comm.trim();
    if !comm.starts_with("ozzy_")
        && !comm.starts_with("ozzy-")
        && !comm.starts_with("omq_")
        && !BENCHMARKS
            .iter()
            .chain(COMPETITORS)
            .chain(SERVERS)
            .chain(&["ld.so", "ld-linux-x86-64.so.2"])
            .any(|name| comm == &name[..name.len().min(15)])
    {
        return Ok(None);
    }
    let exe = fs::read_link(path.join("exe"))?;
    let name = exe
        .file_name()
        .ok_or("missing executable name")?
        .to_string_lossy();
    let name = name.strip_suffix(" (deleted)").unwrap_or(&name);
    let stat = fs::read_to_string(path.join("stat"))?;
    let fields = stat
        .rsplit_once(')')
        .ok_or("invalid process stat")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let args = fs::read(path.join("cmdline"))?
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect::<Vec<_>>();
    // The official Redpanda distribution uses a private glibc loader. Keep
    // loader-launched brokers visible, including stopped or unowned processes.
    let name = if matches!(name, "ld.so" | "ld-linux-x86-64.so.2")
        && args.get(1).is_some_and(|arg| arg == "--library-path")
        && args.get(3).is_some_and(|arg| {
            Path::new(arg)
                .file_name()
                .is_some_and(|name| name == "redpanda")
        }) {
        "redpanda"
    } else {
        name
    };
    // Old cached workers still count as competitors. Named Cargo/rustc symlinks
    // resolve to their actual tool; named shell suite launchers retain `comm`.
    let name = if name.starts_with("ozzy-") {
        name.replace('-', "_")
    } else if matches!(name, "bash" | "sh")
        && (comm.starts_with("ozzy_") || comm.starts_with("omq_"))
    {
        comm.to_owned()
    } else {
        name.to_owned()
    };
    if !name.starts_with("ozzy_")
        && !name.starts_with("omq_")
        && !BENCHMARKS.contains(&name.as_str())
        && !COMPETITORS.contains(&name.as_str())
        && !SERVERS.contains(&name.as_str())
    {
        return Ok(None);
    }
    Ok(Some(
        json!({"pid":path.file_name().unwrap().to_string_lossy().parse::<u32>()?,"executable":name,"state":fields[0],"group":fields[2].parse::<u32>()?,"started":fields[19].parse::<u64>()?,"arguments":args}),
    ))
}

/// Enumerate relevant resident broker and benchmark processes.
pub fn processes() -> Result<Vec<Value>> {
    let mut found = vec![];
    for entry in fs::read_dir("/proc")? {
        let path = entry?.path();
        let Ok(pid) = path.file_name().unwrap().to_string_lossy().parse::<u32>() else {
            continue;
        };
        if pid == std::process::id() {
            continue;
        }
        match inspect(&path) {
            Ok(Some(row)) => found.push(row),
            Ok(None) => (),
            Err(error) if disappeared(error.as_ref()) => {}
            Err(error) => {
                return Err(
                    format!("benchmark isolation: inspect {}: {error}", path.display()).into(),
                );
            }
        }
    }
    Ok(found)
}

fn disappeared(error: &(dyn std::error::Error + 'static)) -> bool {
    error.downcast_ref::<std::io::Error>().is_some_and(|error| {
        // procfs can report either missing entry or exited task between reads.
        error.kind() == std::io::ErrorKind::NotFound
            || error.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error())
    })
}

/// Construct initial isolation evidence before observations are collected.
pub fn empty_proof() -> Value {
    json!({"contract":CONTRACT,"resident_processes":[]})
}

/// Reject competing resident servers before launching a fresh case.
pub fn require_idle() -> Result<Value> {
    let found = processes()?;
    if !found.is_empty() {
        return Err(format!("benchmark isolation: processes still resident: {found:?}").into());
    }
    Ok(empty_proof())
}

#[derive(Debug)]
/// Retained process and remote isolation evidence for one benchmark case.
pub struct Guard {
    remote: Option<super::distributed::Audit>,
    implementation: String,
    server_pids: BTreeSet<u32>,
    samples: u64,
    seen: BTreeMap<(u64, u64), Value>,
    profiled: bool,
    profiler_ancestors: BTreeSet<(u64, u64)>,
}

impl Guard {
    /// Track the selected implementation and its primary server process.
    pub fn new(implementation: &str, server_pid: Option<u32>) -> Self {
        Self {
            remote: None,
            implementation: implementation.into(),
            server_pids: server_pid.into_iter().collect(),
            samples: 0,
            seen: BTreeMap::new(),
            profiled: false,
            profiler_ancestors: BTreeSet::new(),
        }
    }

    #[must_use]
    /// Extend the allowed process set with all server processes in this case.
    pub fn with_servers(mut self, pids: &[u32]) -> Self {
        self.server_pids = pids.iter().copied().collect();
        self
    }

    /// Permit diagnostic profilers only inside this workload's process group.
    pub fn with_profiling(mut self, enabled: bool) -> Result<Self> {
        self.profiled = enabled;
        if enabled {
            let mut pid = std::process::id();
            while pid > 1 {
                let path = std::path::PathBuf::from(format!("/proc/{pid}"));
                if let Some(row) = inspect(&path)?
                    && matches!(
                        row["executable"].as_str(),
                        Some("perf" | "heaptrack" | "strace")
                    )
                {
                    self.profiler_ancestors.insert((
                        u64::from(pid),
                        row["started"].as_u64().ok_or("missing profiler start")?,
                    ));
                }
                let stat = fs::read_to_string(path.join("stat"))?;
                pid = stat
                    .rsplit_once(')')
                    .ok_or("invalid ancestor stat")?
                    .1
                    .split_whitespace()
                    .nth(1)
                    .ok_or("missing parent")?
                    .parse()?;
            }
        }
        Ok(self)
    }

    /// Inspect current processes and fail on competing work outside the case process group.
    pub fn check(&mut self, group: u32) -> Result<()> {
        if let Some(remote) = &mut self.remote {
            remote.check()?;
        }
        self.check_processes(group, processes()?)
    }

    /// Attach the distributed isolation monitor for this case.
    pub fn set_remote(&mut self, audit: super::distributed::Audit) {
        self.remote = Some(audit);
    }
    /// Stop an attached remote audit and return its final evidence.
    pub fn finish_remote(&mut self) -> Result<Value> {
        Ok(self
            .remote
            .take()
            .map(super::distributed::Audit::finish)
            .transpose()?
            .unwrap_or(Value::Null))
    }

    /// Validate a captured process inventory against the allowed case processes.
    pub fn check_processes(&mut self, group: u32, found: Vec<Value>) -> Result<()> {
        let mut servers_seen = BTreeSet::new();
        for process in found {
            let name = process["executable"].as_str().unwrap_or("");
            let pid = process["pid"].as_u64().ok_or("missing process PID")?;
            let args = process["arguments"]
                .as_array()
                .ok_or("missing process arguments")?;
            let valid = if self.profiled
                && self
                    .profiler_ancestors
                    .contains(&(pid, process["started"].as_u64().unwrap_or(0)))
            {
                true
            } else if SERVERS.contains(&name) {
                let expected = match self.implementation.as_str() {
                    "iggy" => "iggy-server",
                    "redpanda" => "redpanda",
                    "kafka" => "java",
                    _ => "",
                };
                let valid = name == expected
                    && u32::try_from(pid).is_ok_and(|pid| self.server_pids.contains(&pid));
                if valid {
                    servers_seen.insert(pid as u32);
                }
                valid
            } else {
                (name
                    == if self.implementation == "lz4-dictionary" {
                        "ozzy_lz4_dict_bench"
                    } else {
                        "ozzy_timed_bench"
                    }
                    || self.profiled && matches!(name, "perf" | "heaptrack" | "strace"))
                    && process["group"] == group
                    && !(self.implementation != "ozzy"
                        && args.iter().any(|a| a == "--worker-index"))
                    && !(self.implementation == "ozzy"
                        && args.iter().any(|a| a == "--external-system"))
            };
            if !valid {
                return Err(format!("benchmark isolation violated: {process}").into());
            }
            self.seen.insert(
                (
                    pid,
                    process["started"].as_u64().ok_or("missing process start")?,
                ),
                process,
            );
        }
        if self.server_pids != servers_seen {
            return Err("expected external server disappeared".into());
        }
        self.samples += 1;
        Ok(())
    }

    /// Serialize collected isolation evidence or return the detected interference.
    pub fn report(&self) -> Result<Value> {
        if self.samples == 0 {
            return Err("benchmark isolation was not observed".into());
        }
        Ok(
            json!({"contract":CONTRACT,"observations":self.samples,"processes":self.seen.values().collect::<Vec<_>>(),"poll_seconds":0.1,"controller_cpus":cpus(None)?}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exited_process_errors_are_distinct_from_failed_inspection() {
        for errno in [rustix::io::Errno::NOENT, rustix::io::Errno::SRCH] {
            assert!(disappeared(&std::io::Error::from_raw_os_error(
                errno.raw_os_error()
            )));
        }
        for errno in [
            rustix::io::Errno::ACCESS,
            rustix::io::Errno::IO,
            rustix::io::Errno::PERM,
        ] {
            assert!(!disappeared(&std::io::Error::from_raw_os_error(
                errno.raw_os_error()
            )));
        }
        assert!(!disappeared(&std::io::Error::other("invalid process stat")));
    }

    #[test]
    fn named_tests_tools_and_cached_workers_remain_isolated() {
        for (comm, executable, expected) in [
            (
                "ozzy_automation",
                "ozzy_automation-123abc",
                "ozzy_automation-123abc",
            ),
            ("rustc", "rustc", "rustc"),
            ("ozzy_test_all", "bash", "ozzy_test_all"),
            ("ozzy-timed-bench", "ozzy-timed-bench", "ozzy_timed_bench"),
            ("ozzy_timed_benc", "ozzy_timed_bench", "ozzy_timed_bench"),
        ] {
            let root = tempfile::tempdir().unwrap();
            let process = root.path().join("123");
            fs::create_dir(&process).unwrap();
            fs::write(process.join("comm"), comm).unwrap();
            std::os::unix::fs::symlink(format!("/fixture/{executable}"), process.join("exe"))
                .unwrap();
            fs::write(
                process.join("stat"),
                format!("123 ({comm}) T 1 123 {}", "0 ".repeat(18)),
            )
            .unwrap();
            fs::write(process.join("cmdline"), format!("{comm}\0")).unwrap();
            let row = inspect(&process).unwrap().unwrap();
            assert_eq!(row["executable"], expected);
            assert!(
                Guard::new("ozzy", None)
                    .check_processes(456, vec![row])
                    .is_err(),
                "unowned {comm} must block comparison timings"
            );
        }
    }

    #[test]
    fn private_loader_broker_is_visible_and_requires_owned_pid() {
        let root = tempfile::tempdir().unwrap();
        let process = root.path().join("123");
        fs::create_dir(&process).unwrap();
        fs::write(process.join("comm"), "ld.so\n").unwrap();
        std::os::unix::fs::symlink("/fixture/lib/ld.so", process.join("exe")).unwrap();
        fs::write(
            process.join("stat"),
            format!("123 (ld.so) T 1 123 {}", "0 ".repeat(18)),
        )
        .unwrap();
        fs::write(process.join("cmdline"), b"/fixture/lib/ld.so\0--library-path\0/fixture/lib\0/fixture/libexec/redpanda\0--smp\x002\0").unwrap();
        let row = inspect(&process).unwrap().unwrap();
        assert_eq!(row["executable"], "redpanda");
        assert_eq!(row["state"], "T");
        assert!(
            Guard::new("ozzy", None)
                .check_processes(456, vec![row.clone()])
                .is_err()
        );
        assert!(
            Guard::new("redpanda", Some(999))
                .check_processes(456, vec![row.clone()])
                .is_err()
        );
        Guard::new("redpanda", Some(123))
            .check_processes(456, vec![row])
            .unwrap();
        fs::write(
            process.join("cmdline"),
            b"ld.so\0--library-path\0/fixture/lib\0/bin/true\0",
        )
        .unwrap();
        assert!(inspect(&process).unwrap().is_none());
    }
}
