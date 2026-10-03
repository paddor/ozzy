//! Explicit broker hosts, storage parents, and optional process CPU masks.

use crate::{BenchResult, bench_error};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    io::Read,
    net::IpAddr,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct Placement {
    pub bind: IpAddr,
    pub remote: Option<(String, PathBuf)>,
    pub storage_dir: Option<PathBuf>,
    pub cpus: Option<Vec<usize>>,
}

impl Placement {
    pub fn load(path: Option<&Path>) -> BenchResult<[Self; 3]> {
        let Some(path) = path else {
            return Ok(std::array::from_fn(|_| Self {
                bind: IpAddr::from([127, 0, 0, 1]),
                remote: None,
                storage_dir: None,
                cpus: None,
            }));
        };
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(65537)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 65536 {
            return Err(bench_error("placement file too large"));
        }
        Self::parse(&bytes)
    }

    pub fn parse(bytes: &[u8]) -> BenchResult<[Self; 3]> {
        let value: Value = serde_json::from_slice(bytes)?;
        let entries = value
            .as_array()
            .filter(|entries| entries.len() == 3)
            .ok_or_else(|| bench_error("exactly three broker placements required"))?;
        entries
            .iter()
            .map(Self::entry)
            .collect::<BenchResult<Vec<_>>>()?
            .try_into()
            .map_err(|_| bench_error("wrong broker count"))
    }

    fn entry(entry: &Value) -> BenchResult<Self> {
        let object = entry
            .as_object()
            .ok_or_else(|| bench_error("invalid placement"))?;
        if object.keys().any(|key| {
            !["bind", "ssh", "executable", "storage_dir", "cpus"].contains(&key.as_str())
        }) {
            return Err(bench_error("unknown placement field"));
        }
        let bind: IpAddr = entry["bind"]
            .as_str()
            .ok_or_else(|| bench_error("missing bind"))?
            .parse()?;
        if bind.is_unspecified() || bind.is_multicast() {
            return Err(bench_error("concrete unicast bind required"));
        }
        let remote = if let Some(host) = entry.get("ssh") {
            let host = host
                .as_str()
                .ok_or_else(|| bench_error("invalid SSH host"))?;
            if host.is_empty()
                || host.starts_with('-')
                || !host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-@:[]".contains(&b))
            {
                return Err(bench_error("unsafe SSH host"));
            }
            Some((host.to_owned(), absolute(&entry["executable"])?))
        } else {
            if entry.get("executable").is_some() {
                return Err(bench_error("local executable override is unsupported"));
            }
            None
        };
        let storage_dir = entry.get("storage_dir").map(absolute).transpose()?;
        let cpus = entry
            .get("cpus")
            .map(|value| -> BenchResult<Vec<usize>> {
                let values = value
                    .as_array()
                    .filter(|values| !values.is_empty())
                    .ok_or_else(|| bench_error("empty CPU placement"))?;
                let mut seen = BTreeSet::new();
                for value in values {
                    let cpu = value
                        .as_u64()
                        .and_then(|v| usize::try_from(v).ok())
                        .filter(|&v| v < rustix::thread::CpuSet::MAX_CPU)
                        .ok_or_else(|| bench_error("invalid CPU placement"))?;
                    if !seen.insert(cpu) {
                        return Err(bench_error("duplicate placement CPU"));
                    }
                }
                Ok(seen.into_iter().collect())
            })
            .transpose()?;
        Ok(Self {
            bind,
            remote,
            storage_dir,
            cpus,
        })
    }

    pub fn cpu_list(&self) -> Option<String> {
        self.cpus.as_ref().map(|cpus| {
            cpus.iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(",")
        })
    }

    /// Check the worker's observed masks, not the local SSH process's mask.
    pub fn verify_execution(&self, execution: &Value) -> BenchResult<()> {
        if let Some(cpus) = &self.cpus {
            let threads = execution["threads"]
                .as_array()
                .filter(|rows| !rows.is_empty())
                .ok_or_else(|| bench_error("missing worker CPU observation"))?;
            let expected = serde_json::json!(cpus);
            if threads.iter().any(|thread| thread["cpus"] != expected) {
                return Err(bench_error("worker CPU mask differs from placement"));
            }
        }
        Ok(())
    }
}

/// Capture on the worker itself, before warmup or after the measured cohort.
pub fn execution() -> BenchResult<Value> {
    let at = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    let monotonic_ns = at.tv_sec as u64 * 1_000_000_000 + at.tv_nsec as u64;
    let scheduler_wait_enabled =
        std::fs::read_to_string("/proc/sys/kernel/sched_schedstats")?.trim() == "1";
    let mut threads = Vec::new();
    for task in std::fs::read_dir("/proc/self/task")? {
        let task = task?;
        let tid: u32 = task
            .file_name()
            .to_str()
            .ok_or_else(|| bench_error("invalid task name"))?
            .parse()?;
        if let Some(thread) = execution_thread(&task.path(), tid, scheduler_wait_enabled)? {
            threads.push(thread);
        }
    }
    threads.sort_by_key(|thread| thread["tid"].as_u64());
    Ok(
        serde_json::json!({"threads": threads, "monotonic_ns": monotonic_ns,
        "scheduler_wait_enabled": scheduler_wait_enabled}),
    )
}

fn execution_thread(path: &Path, tid: u32, scheduler_wait: bool) -> BenchResult<Option<Value>> {
    let read = || -> BenchResult<Value> {
        let schedstat = std::fs::read_to_string(path.join("schedstat"))?;
        let counters: Vec<u64> = schedstat
            .split_whitespace()
            .map(str::parse)
            .collect::<Result<_, _>>()?;
        let [runtime_ns, runqueue_wait_ns, timeslices] = counters.as_slice() else {
            return Err(bench_error("invalid thread schedstat"));
        };
        Ok(serde_json::json!({
            "tid": tid,
            "name": std::fs::read_to_string(path.join("comm"))?.trim(),
            "cpus": crate::automation::isolation::cpus(Some(tid))?,
            "runtime_ns": runtime_ns,
            "runqueue_wait_ns": scheduler_wait.then_some(runqueue_wait_ns),
            "timeslices": timeslices,
        }))
    };
    match read() {
        Ok(thread) => Ok(Some(thread)),
        // A task can exit after read_dir, or between proc reads and affinity.
        // Process CPU counters still include that task's completed work.
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                || error.downcast_ref::<rustix::io::Errno>() == Some(&rustix::io::Errno::SRCH) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

pub fn storage(parent: &Path) -> BenchResult<Value> {
    use std::os::unix::fs::MetadataExt;
    let path = std::fs::canonicalize(parent)?;
    let fs = rustix::fs::statfs(&path)?;
    Ok(serde_json::json!({
        "requested_parent": parent, "parent": path,
        "device": std::fs::metadata(&path)?.dev(), "filesystem_type": fs.f_type,
    }))
}

fn absolute(value: &Value) -> BenchResult<PathBuf> {
    let text = value
        .as_str()
        .ok_or_else(|| bench_error("missing placement path"))?;
    let path = PathBuf::from(text);
    if !path.is_absolute() || text.contains('\0') {
        return Err(bench_error("absolute placement path required"));
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn execution_observation_skips_missing_tasks_and_refuses_malformed_counters() {
        let (send, receive) = std::sync::mpsc::channel();
        let (stop, stopped) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            send.send(rustix::thread::gettid().as_raw_nonzero().get() as u32)
                .unwrap();
            stopped.recv().unwrap();
        });
        let tid = receive.recv().unwrap();
        let path = PathBuf::from(format!("/proc/self/task/{tid}"));
        assert_eq!(
            execution_thread(&path, tid, false).unwrap().unwrap()["tid"],
            tid
        );
        stop.send(()).unwrap();
        worker.join().unwrap();
        let missing = tempfile::tempdir().unwrap();
        assert!(
            execution_thread(missing.path(), tid, false)
                .unwrap()
                .is_none()
        );
        let malformed = tempfile::tempdir().unwrap();
        std::fs::write(malformed.path().join("schedstat"), "invalid counters").unwrap();
        assert!(execution_thread(malformed.path(), tid, false).is_err());
    }

    fn rows() -> Value {
        json!([
            {"bind":"192.168.11.155", "storage_dir":"/mnt/ssd/tmp/ozzy-bench", "cpus":[0]},
            {"bind":"192.168.11.155", "storage_dir":"/mnt/bench/tmp/ozzy-bench", "cpus":[1]},
            {"bind":"192.168.11.100", "ssh":"er-dev", "executable":"/mnt/bench/tmp/ozzy-bin/ozy_timed_bench", "storage_dir":"/mnt/bench/tmp/ozzy-bench", "cpus":[0]}
        ])
    }

    #[test]
    fn independent_storage_and_cpu_masks_for_local_and_remote_brokers() {
        let placements = Placement::parse(&serde_json::to_vec(&rows()).unwrap()).unwrap();
        assert_eq!(
            placements[1].storage_dir.as_deref(),
            Some(Path::new("/mnt/bench/tmp/ozzy-bench"))
        );
        assert_eq!(placements[1].cpu_list().as_deref(), Some("1"));
        assert_eq!(placements[2].remote.as_ref().unwrap().0, "er-dev");
        assert_eq!(
            Placement::load(None).unwrap()[0].bind,
            IpAddr::from([127, 0, 0, 1])
        );
    }

    #[test]
    fn worker_masks_must_cover_every_reported_thread() {
        let placement = Placement::parse(&serde_json::to_vec(&rows()).unwrap()).unwrap();
        assert!(
            placement[0]
                .verify_execution(&json!({
                    "threads": [{"cpus": [0]}, {"cpus": [0]}]
                }))
                .is_ok()
        );
        for execution in [
            json!(null),
            json!({"threads": []}),
            json!({"threads": [{"cpus": [0]}, {"cpus": [0, 1]}]}),
            json!({"threads": [{"cpus": [0]}, {}]}),
        ] {
            assert!(placement[0].verify_execution(&execution).is_err());
        }
    }

    #[test]
    fn malformed_placement_fails_before_launch() {
        for (field, invalid) in [
            ("storage_dir", json!("relative")),
            ("storage_dir", json!("/bad\u{0}path")),
            ("cpus", json!([])),
            ("cpus", json!([0, 0])),
            ("cpus", json!([-1])),
            ("cpus", json!([rustix::thread::CpuSet::MAX_CPU])),
            ("cpus", json!("0")),
            ("ssh", json!("-oProxyCommand=bad")),
            ("executable", json!("relative")),
            ("typo", json!(1)),
            ("bind", json!("0.0.0.0")),
        ] {
            let mut rows = rows();
            rows[2][field] = invalid;
            assert!(
                Placement::parse(&serde_json::to_vec(&rows).unwrap()).is_err(),
                "{field}"
            );
        }
    }
}
