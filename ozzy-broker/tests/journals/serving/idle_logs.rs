//! Cost of idle runtime logs on real files: open handles, allocated disk,
//! resident memory and CPU time per log, on three brokers in one process.

use super::*;
use ozzy_broker::LogSpec;
use ozzy_broker::log::LogRole;
use ozzy_proto::{GroupId, PartitionIncarnation};
use std::os::unix::fs::MetadataExt;

/// What the process uses now.
#[derive(Clone, Copy, Debug)]
struct Usage {
    /// CPU time in clock ticks.
    ticks: u64,
    resident: u64,
    handles: u64,
    /// Bytes the file system allocated below the test root.
    disk: u64,
}

fn allocated(path: &Path) -> u64 {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    let below = if metadata.is_dir() {
        std::fs::read_dir(path).map_or(0, |entries| {
            entries
                .flatten()
                .map(|entry| allocated(&entry.path()))
                .sum()
        })
    } else {
        0
    };
    metadata.blocks() * 512 + below
}

/// CPU time of the process in clock ticks. Cheap, unlike `usage`, so the
/// idle slices measure the brokers and not the measurement.
fn ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
}

fn usage(root: &Path) -> Usage {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
    let pages: u64 = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
    Usage {
        ticks: ticks(),
        resident: pages * 4096,
        handles: std::fs::read_dir("/proc/self/fd").unwrap().count() as u64,
        disk: allocated(root),
    }
}

fn spec(index: usize, members: &[NodeId]) -> LogSpec {
    let mut id = [0xA5; 16];
    id[..8].copy_from_slice(&(index as u64).to_le_bytes());
    let mut incarnation = id;
    incarnation[15] = 1;
    LogSpec {
        id: GroupId::from_bytes(id),
        incarnation: PartitionIncarnation::from_bytes(incarnation),
        members: members.to_vec(),
    }
}

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| value.parse().unwrap())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "measurement; OZZY_IDLE_LOGS, OZZY_IDLE_SEGMENT and OZZY_IDLE_SECONDS select the run"]
async fn idle_replicated_logs_report_their_cost() {
    let count = setting("OZZY_IDLE_LOGS", 32);
    let segment = setting("OZZY_IDLE_SEGMENT", 2 * 1024 * 1024) as u64;
    let seconds = setting("OZZY_IDLE_SECONDS", 5) as u64;
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let deployment = deployment_with(root.path(), DeploymentMode::Three, 1, |config| {
        config.logs = Some(ozzy_config::LogClass {
            segment_bytes: segment,
            max_append_bytes: segment / 4,
            max_logs: count,
        });
        for broker in config.brokers.values_mut() {
            for device in broker.devices.values_mut() {
                device.workers.open_handles = 16 * count + 4096;
            }
        }
    });
    let nodes: Vec<NodeId> = deployment
        .iter()
        .map(|(checked, _)| {
            NodeId::from_bytes(*checked.identity.brokers[&checked.plan.name].as_bytes())
        })
        .collect();
    let brokers = start_brokers(&runtime, deployment).await;
    let before = usage(root.path());
    let started = std::time::Instant::now();
    for index in 0..count {
        let spec = spec(index, &nodes);
        for broker in &brokers {
            broker.create_log(spec.clone()).await.unwrap();
        }
    }
    for index in 0..count {
        let id = spec(index, &nodes).id;
        let led = || {
            brokers.iter().any(|broker| {
                broker.runtime_log(id).unwrap().status().borrow().role == LogRole::Leader
            })
        };
        while !led() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let created = usage(root.path());
    let events = ozzy_runtime::profiling::totals();
    let copies = 3 * count as u64;
    println!(
        "logs={count} segment={segment} create_and_elect={:?} create_cpu_ticks={}",
        started.elapsed(),
        created.ticks - before.ticks
    );
    println!(
        "per_copy: disk={} resident={} handles={:.1}",
        (created.disk - before.disk) / copies,
        created.resident.saturating_sub(before.resident) / copies,
        (created.handles - before.handles) as f64 / copies as f64
    );
    // One line per five seconds shows work that starts late or comes in bursts.
    let mut last = ticks();
    let first = last;
    for _ in 0..seconds / 5 {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let now = ticks();
        println!("idle_slice cpu_ticks={}", now - last);
        last = now;
    }
    tokio::time::sleep(Duration::from_secs(seconds % 5)).await;
    let idle_ticks = ticks() - first;
    let idle = usage(root.path());
    println!(
        "idle_seconds={seconds} idle_cpu_ticks={idle_ticks} handles={} resident={}",
        idle.handles, idle.resident
    );
    let totals = ozzy_runtime::profiling::totals();
    for ((name, _, after), (_, _, before)) in totals.rows().zip(events.rows()) {
        if after != before {
            println!("idle_event {name}={}", after.wrapping_sub(before));
        }
    }
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}
