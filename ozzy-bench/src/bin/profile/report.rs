//! Profiler reports of a verified run: the segment write syscall summary, or
//! `perf report` CPU profiles per process group.
use crate::Kind;
use ozzy_bench::automation::{self, Result};
use serde_json::{Value, json};
use std::{fs, path::Path, process::Command};

/// Save the report of `kind`; stage profiles need none beyond the result.
pub(crate) fn save(kind: Kind, directory: &Path, row: &Value) -> Result<()> {
    match kind {
        Kind::Syscall => {
            let writes = WriteTrace::read(directory)?.summary()?;
            automation::json_file(&directory.join("writes.json"), &writes)?;
            println!("WRITES {writes}");
        }
        Kind::Cpu | Kind::CpuKernel => cpu_report(directory, row)?,
        Kind::Stages => {}
    }
    Ok(())
}

/// Segment write and kernel AIO syscalls from the strace output files.
#[derive(Default)]
struct WriteTrace {
    sizes: Vec<u64>,
    /// Microseconds per synchronous segment write.
    durations: Vec<f64>,
    aio_submitted: u64,
    aio_completed: u64,
}

impl WriteTrace {
    fn read(directory: &Path) -> Result<Self> {
        let mut trace = Self::default();
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if !entry.file_name().to_string_lossy().starts_with("trace.") {
                continue;
            }
            for line in fs::read_to_string(entry.path())?.lines() {
                trace.add(line);
            }
        }
        Ok(trace)
    }

    /// Count one successful syscall line; failed calls carry no byte count.
    fn add(&mut self, line: &str) {
        let Some((_, result)) = line.rsplit_once(" = ") else {
            return;
        };
        let Some(bytes) = result
            .split_whitespace()
            .next()
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return;
        };
        if line.contains("io_submit(") {
            self.aio_submitted += bytes;
            return;
        }
        if line.contains("io_getevents(") || line.contains("io_pgetevents(") {
            self.aio_completed += bytes;
            return;
        }
        let segment_write = (line.contains("pwritev(") || line.contains("pwrite64("))
            && line.contains("/segments/");
        if !segment_write {
            return;
        }
        let Some(seconds) = result
            .rsplit_once('<')
            .and_then(|(_, s)| s.strip_suffix('>'))
            .and_then(|s| s.parse::<f64>().ok())
        else {
            return;
        };
        self.sizes.push(bytes);
        self.durations.push(seconds * 1e6);
    }

    fn summary(mut self) -> Result<Value> {
        if self.sizes.is_empty() && self.aio_submitted == 0 {
            return Err(
                "syscall profile captured no segment writes or kernel AIO submissions".into(),
            );
        }
        self.sizes.sort_unstable();
        self.durations.sort_by(f64::total_cmp);
        let (sizes, durations) = (&self.sizes, &self.durations);
        let rank = |percent: usize| (sizes.len() * percent).div_ceil(100) - 1;
        let mut summary = json!({"calls":sizes.len(), "bytes_max":sizes.last(),
            "duration_max_us":durations.last(),
            "kernel_aio":{"submitted":self.aio_submitted,"completed":self.aio_completed,
                "scope":"all kernel AIO requests; submission duration is not persistence latency"},
            "scope":"synchronous segment writes, including setup, warmup and drain; profiler overhead included"});
        if !sizes.is_empty() {
            summary["bytes_p50"] = json!(sizes[rank(50)]);
            summary["bytes_p99"] = json!(sizes[rank(99)]);
            summary["duration_p50_us"] = json!(durations[rank(50)]);
            summary["duration_p99_us"] = json!(durations[rank(99)]);
        }
        Ok(summary)
    }
}

fn cpu_report(directory: &Path, row: &Value) -> Result<()> {
    let start = row["measurement_start_ns"]
        .as_u64()
        .ok_or("missing measurement start")?;
    let end = row["measurement_end_ns"]
        .as_u64()
        .ok_or("missing measurement end")?;
    let window = format!("{:.9},{:.9}", start as f64 / 1e9, end as f64 / 1e9);
    for report in cpu_reports(row, &super::local_host()?)? {
        report.write(directory, &window)?;
    }
    Ok(())
}

/// One `perf report` output file, limited to some threads or processes.
struct CpuReport {
    file: String,
    filter: Option<Filter>,
}

/// A `perf report` thread or process ID filter.
struct Filter {
    flag: &'static str,
    ids: String,
}

/// All samples, all local brokers together and each alone, and each client
/// role when clients run in their own processes.
fn cpu_reports(row: &Value, host: &str) -> Result<Vec<CpuReport>> {
    let brokers = broker_pids(row, host)?;
    // Inproc brokers share one process; their own threads identify them.
    let inproc = row["brokers"][0]["usage"]["cpu_scope"]
        .as_str()
        .is_some_and(|scope| scope.starts_with("inproc"));
    let flag = if inproc { "--tid" } else { "--pid" };
    let mut reports = vec![
        CpuReport {
            file: "cpu.txt".to_owned(),
            filter: None,
        },
        CpuReport {
            file: "broker-cpu.txt".into(),
            filter: Some(Filter {
                flag,
                ids: brokers
                    .iter()
                    .map(|(_, ids)| ids.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
            }),
        },
    ];
    reports.extend(brokers.into_iter().map(|(index, ids)| CpuReport {
        file: format!("broker-{index}-cpu.txt"),
        filter: Some(Filter { flag, ids }),
    }));
    for role in ["writers", "readers"].into_iter().filter(|_| !inproc) {
        let pids = row[role]
            .as_array()
            .ok_or("missing client processes")?
            .iter()
            .map(|client| {
                client["pid"]
                    .as_u64()
                    .map(|pid| pid.to_string())
                    .ok_or("missing client PID")
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        reports.push(CpuReport {
            file: format!("{role}-cpu.txt"),
            filter: Some(Filter {
                flag: "--pid",
                ids: pids.join(","),
            }),
        });
    }
    Ok(reports)
}

impl CpuReport {
    /// Run `perf report` over the measurement `window` into this file.
    fn write(&self, directory: &Path, window: &str) -> Result<()> {
        let mut command = Command::new("timeout");
        command
            .args([
                "30s",
                "perf",
                "report",
                "--stdio",
                "--no-children",
                "--no-inline",
                "--call-graph",
                "none",
                "--percent-limit",
                "0.5",
                "--sort",
                // Keep same-named OMQ threads from different processes apart
                // before applying the broker/client PID filters.
                "pid,comm,symbol",
                "-i",
            ])
            .arg(directory.join("perf.data"));
        command.args(["--time", window]);
        if let Some(Filter { flag, ids }) = &self.filter {
            command.args([flag, ids.as_str(), "--percentage", "relative"]);
        }
        let output = command.output()?;
        if !output.status.success() || !output.stderr.is_empty() {
            return Err(format!(
                "CPU report failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        fs::write(directory.join(&self.file), output.stdout)?;
        Ok(())
    }
}

fn broker_pids(row: &Value, host: &str) -> Result<Vec<(usize, String)>> {
    Ok(row["brokers"]
        .as_array()
        .ok_or("missing brokers")?
        .iter()
        .enumerate()
        .filter(|(_, b)| {
            b["identity"]["host"]
                .as_str()
                .is_none_or(|name| name == host)
        })
        .map(|(index, b)| {
            if b["usage"]["cpu_scope"]
                .as_str()
                .is_some_and(|scope| scope.starts_with("inproc"))
            {
                let tids = b["usage"]["execution"]["threads"]
                    .as_array()
                    .ok_or("missing inproc broker threads")?
                    .iter()
                    .map(|thread| thread["tid"].as_u64().map(|tid| tid.to_string()))
                    .collect::<Option<Vec<_>>>()
                    .ok_or("missing inproc broker thread ID")?;
                return Ok((index, tids.join(",")));
            }
            b["identity"]["pid"]
                .as_u64()
                .map(|p| (index, p.to_string()))
                .ok_or("missing broker PID")
        })
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_report_selects_local_broker_identity_pids() {
        let row = json!({"brokers":[
            {"identity":{"host":"local", "pid":101}},
            {"identity":{"host":"remote", "pid":202}},
            {"identity":{"host":"local", "pid":303}}
        ]});
        assert_eq!(
            broker_pids(&row, "local").unwrap(),
            [(0, "101".into()), (2, "303".into())]
        );
    }

    #[test]
    fn syscall_summary_ignores_metadata_and_failed_calls() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("trace.1"), "1 pwritev(9</x/segments/1.log>, [], 1, 64) = 4096 <0.002000>\n2 pwritev(9</x/segments/1.log>, [], 1, 64) = -1 EIO <0.010000>\n3 pwritev(8</x/manifest>, [], 1, 0) = 8192 <0.020000>\n").unwrap();
        let row = WriteTrace::read(directory.path())
            .unwrap()
            .summary()
            .unwrap();
        assert_eq!(row["calls"], 1);
        assert_eq!(row["bytes_p50"], 4096);
        assert_eq!(row["duration_p99_us"], 2000.0);
    }

    #[test]
    fn syscall_summary_distinguishes_aio_requests_from_synchronous_writes() {
        let directory = tempfile::tempdir().unwrap();
        let summary = || {
            WriteTrace::read(directory.path())
                .unwrap()
                .summary()
                .unwrap()
        };
        fs::write(directory.path().join("trace.1"), "1 pwrite64(9</x/segments/1.log>, [], 4096, 64) = 4096 <0.002000>\n2 io_submit(1, 2, []) = 2 <0.000100>\n3 io_getevents(1, 1, 2, [], NULL) = 1 <0.003000>\n4 io_pgetevents(1, 1, 2, [], NULL, NULL) = 1 <0.004000>\n5 io_submit(1, 1, []) = -1 EIO <0.010000>\n").unwrap();
        let row = summary();
        assert_eq!(row["calls"], 1);
        assert_eq!(row["bytes_p50"], 4096);
        assert_eq!(row["kernel_aio"]["submitted"], 2);
        assert_eq!(row["kernel_aio"]["completed"], 2);
        fs::write(directory.path().join("trace.1"), "1 io_submit(1, 1, []) = 1 <0.000100>\n2 io_getevents(1, 1, 1, [], NULL) = 1 <0.003000>\n").unwrap();
        let row = summary();
        assert_eq!(row["calls"], 0);
        assert!(row["bytes_p50"].is_null());
        assert!(row["duration_p50_us"].is_null());
        assert_eq!(row["kernel_aio"]["submitted"], 1);
        assert_eq!(row["kernel_aio"]["completed"], 1);
    }
}
