//! Whole-device diagnostics outside the measured workload window.
//! Counter units: <https://docs.kernel.org/block/stat.html>

use super::Result;
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub(super) struct Snapshot {
    device: PathBuf,
    counters: Counters,
    started: Instant,
}

fn block_device(directory: &Path) -> Result<PathBuf> {
    let device = rustix::fs::stat(directory)?.st_dev;
    let mut device = fs::canonicalize(format!(
        "/sys/dev/block/{}:{}",
        rustix::fs::major(device),
        rustix::fs::minor(device),
    ))?;
    // Flush completions are counted on the whole device, not partitions.
    if device.join("partition").try_exists()? && !device.pop() {
        return Err("missing parent block device".into());
    }
    Ok(device)
}

/// Wait until the device completes no writes, then require consecutive fast
/// synchronized probe writes. A case never starts while the previous case's
/// writeback or SSD recovery is still running.
pub(super) async fn settle(directory: &Path) -> Result<Value> {
    const QUIET: Duration = Duration::from_secs(5);
    const LIMIT: Duration = Duration::from_secs(300);
    const PROBES: usize = 3;
    const PROBE_LIMIT_MS: f64 = 60.0;
    let started = Instant::now();
    let device = block_device(directory)?;
    let mut probes = vec![];
    loop {
        wait_quiet(&device, QUIET, started + LIMIT).await?;
        let passed = (0..PROBES)
            .map(|_| probe(directory))
            .collect::<Result<Vec<_>>>()?;
        let fast = passed.iter().all(|ms| *ms <= PROBE_LIMIT_MS);
        probes.push(passed);
        if fast {
            return Ok(json!({
                "seconds": started.elapsed().as_secs_f64(),
                "probe_ms": probes,
                "rule": "5 s without completed writes, flushes or discards; three 1 MiB O_DSYNC writes at most 60 ms each",
            }));
        }
        if started.elapsed() >= LIMIT {
            return Err(format!("device did not settle: probe_ms={probes:?}").into());
        }
    }
}

async fn wait_quiet(device: &Path, quiet: Duration, deadline: Instant) -> Result<()> {
    let activity = |c: &Counters| [c.0[4], c.0[8], c.0[11], c.0[15]];
    let mut last = activity(&Counters::read(device)?);
    let mut since = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let now = activity(&Counters::read(device)?);
        if now != last || now[1] != 0 {
            (last, since) = (now, Instant::now());
        } else if since.elapsed() >= quiet {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("device writes did not stop".into());
        }
    }
}

/// One synchronized 1 MiB write in `directory`, in milliseconds.
fn probe(directory: &Path) -> Result<f64> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let path = directory.join(".ozzy-settle-probe");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DSYNC)
        .open(&path)?;
    let bytes: Vec<u8> = (0..1 << 20)
        .map(|n: u32| (n.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect();
    let started = Instant::now();
    file.write_all(&bytes)?;
    let elapsed = started.elapsed();
    drop(file);
    fs::remove_file(&path)?;
    Ok(elapsed.as_secs_f64() * 1e3)
}

impl Snapshot {
    pub(super) fn take(directory: &Path) -> Result<Self> {
        let device = block_device(directory)?;
        Ok(Self {
            counters: Counters::read(&device)?,
            device,
            started: Instant::now(),
        })
    }

    pub(super) fn finish(self) -> Result<Value> {
        let after = Counters::read(&self.device)?;
        let mut report = self.counters.delta(&after)?;
        report["device"] = json!(
            self.device
                .file_name()
                .ok_or("unnamed block device")?
                .to_string_lossy()
        );
        report["elapsed_seconds"] = json!(self.started.elapsed().as_secs_f64());
        report["scope"] = json!(
            "whole comparison worker lifetime: setup, warmup, measurement and drain; entire SSD device including filesystem I/O"
        );
        Ok(report)
    }
}

struct Counters([u64; 17]);

impl Counters {
    fn read(device: &Path) -> Result<Self> {
        Self::parse(&fs::read_to_string(device.join("stat"))?)
    }

    fn parse(input: &str) -> Result<Self> {
        let values = input
            .split_whitespace()
            .map(str::parse)
            .collect::<std::result::Result<Vec<u64>, _>>()?;
        Ok(Self(
            values
                .get(..17)
                .ok_or("incomplete block-device statistics")?
                .try_into()?,
        ))
    }

    fn delta(&self, after: &Self) -> Result<Value> {
        let delta = |index: usize| {
            after.0[index]
                .checked_sub(self.0[index])
                .ok_or("block-device counter reset during comparison")
        };
        let bytes = |index| {
            delta(index)?
                .checked_mul(512)
                .ok_or("block-device byte counter overflow")
        };
        let mean = |ticks, requests| -> Result<Option<f64>> {
            let count = delta(requests)?;
            Ok((count != 0)
                .then(|| delta(ticks).map(|ticks| ticks as f64 / count as f64))
                .transpose()?)
        };
        Ok(json!({
            "before": self.0, "after": after.0,
            "read_requests": delta(0)?, "read_bytes": bytes(2)?, "read_wait_ms": delta(3)?,
            "write_requests": delta(4)?, "write_bytes": bytes(6)?, "write_wait_ms": delta(7)?,
            "in_flight_before": self.0[8], "in_flight_after": after.0[8],
            "busy_ms": delta(9)?, "queue_ms": delta(10)?,
            "discard_requests": delta(11)?, "discard_bytes": bytes(13)?,
            "flush_requests": delta(15)?, "flush_wait_ms": delta(16)?,
            "mean_write_ms": mean(7, 4)?, "mean_flush_ms": mean(16, 15)?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_use_sector_bytes_and_completed_requests_not_inflight_gauges() {
        let mut before = Counters([10; 17]);
        before.0[8] = 4;
        let mut after = Counters([10; 17]);
        after.0[4] += 4;
        after.0[6] += 16;
        after.0[7] += 12;
        after.0[8] = 0;
        after.0[15] += 2;
        after.0[16] += 10;
        let report = before.delta(&after).unwrap();
        assert_eq!(report["write_bytes"], 8192);
        assert_eq!(report["mean_write_ms"], 3.0);
        assert_eq!(report["mean_flush_ms"], 5.0);
        assert_eq!(report["in_flight_after"], 0);
        assert!(after.delta(&before).is_err());
        assert!(before.delta(&before).unwrap()["mean_write_ms"].is_null());
    }

    #[test]
    fn probe_writes_synchronously_and_removes_its_file() {
        let directory = tempfile::tempdir().unwrap();
        let ms = probe(directory.path()).unwrap();
        assert!(ms.is_finite() && ms >= 0.0);
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn malformed_or_incomplete_counters_fail_closed() {
        assert!(Counters::parse("1 2 3").is_err());
        assert!(Counters::parse(&"x ".repeat(17)).is_err());
        assert!(Counters::parse(&"0 ".repeat(17)).is_ok());
        assert!(Counters::parse(&"0 ".repeat(19)).is_ok());
    }
}
