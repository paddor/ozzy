//! Cost of idle runtime logs on real files: open handles, allocated disk,
//! resident memory and CPU time per log, on three brokers in one process.

use super::*;
use ozzy_broker::LogSpec;
use ozzy_broker::log::LogRole;
use ozzy_proto::{GroupId, PartitionIncarnation};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

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

/// Paths below `root` this process holds open.
fn open_below(root: &Path) -> Vec<PathBuf> {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .flatten()
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .filter(|path| path.starts_with(root))
        .collect()
}

/// Wait until each of the first `count` logs has a leader.
async fn all_led(brokers: &[Broker], count: usize, nodes: &[NodeId]) {
    for index in 0..count {
        let id = spec(index, nodes).id;
        let led = || {
            brokers.iter().any(|broker| {
                broker
                    .runtime_log(id)
                    .is_some_and(|log| log.status().borrow().role == LogRole::Leader)
            })
        };
        while !led() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// Twelve replicated logs per broker elect leaders within a budget of 24
/// descriptors per broker, less than two per log: idle handles give their
/// descriptors up, and one lock per device guards all its log stores.
#[tokio::test(flavor = "current_thread")]
async fn idle_logs_share_a_small_descriptor_budget_and_one_lock() {
    let count = 12;
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let deployment = deployment_with(root.path(), DeploymentMode::Three, 1, |config| {
        config.logs = Some(ozzy_config::LogClass {
            segment_bytes: 2 * 1024 * 1024,
            max_append_bytes: 512 * 1024,
            max_logs: count,
        });
        for broker in config.brokers.values_mut() {
            for device in broker.devices.values_mut() {
                device.workers.open_handles = 24;
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
    let elected = async {
        for index in 0..count {
            let spec = spec(index, &nodes);
            for broker in &brokers {
                broker.create_log(spec.clone()).await.unwrap();
            }
            let led = || {
                brokers.iter().any(|broker| {
                    broker.runtime_log(spec.id).unwrap().status().borrow().role == LogRole::Leader
                })
            };
            while !led() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(60), elected)
        .await
        .unwrap();
    let open = open_below(root.path());
    assert!(open.len() <= 3 * 24, "{} descriptors", open.len());
    let named = |name: &str| {
        let below_logs = |path: &&PathBuf| path.components().any(|part| part.as_os_str() == "logs");
        let count = open.iter().filter(below_logs);
        count.filter(|path| path.ends_with(name)).count()
    };
    assert_eq!(named("volume.lock"), 3, "one lock per broker device");
    assert_eq!(named("group.lock"), 0, "a log store locks no file");
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "measurement; OZZY_IDLE_LOGS, OZZY_IDLE_MAX_LOGS, OZZY_IDLE_SEGMENT, OZZY_IDLE_SECONDS, OZZY_IDLE_HANDLES and OZZY_IDLE_RESTART select the run"]
async fn idle_replicated_logs_report_their_cost() {
    let count = setting("OZZY_IDLE_LOGS", 32);
    let segment = setting("OZZY_IDLE_SEGMENT", 2 * 1024 * 1024) as u64;
    let seconds = setting("OZZY_IDLE_SECONDS", 5) as u64;
    // Descriptors per broker device. A small budget parks idle handles.
    let handles = setting("OZZY_IDLE_HANDLES", 16 * count + 4096);
    // The class size the brokers plan for. Budgets are split by it.
    let planned = setting("OZZY_IDLE_MAX_LOGS", count);
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let deployment = deployment_with(root.path(), DeploymentMode::Three, 1, |config| {
        config.logs = Some(ozzy_config::LogClass {
            segment_bytes: segment,
            max_append_bytes: segment / 4,
            max_logs: planned,
        });
        for broker in config.brokers.values_mut() {
            for device in broker.devices.values_mut() {
                device.workers.open_handles = handles;
            }
        }
    });
    let nodes: Vec<NodeId> = deployment
        .iter()
        .map(|(checked, _)| {
            NodeId::from_bytes(*checked.identity.brokers[&checked.plan.name].as_bytes())
        })
        .collect();
    let again = deployment.clone();
    let brokers = start_brokers(&runtime, deployment).await;
    let before = usage(root.path());
    let fresh = ozzy_runtime::profiling::totals();
    let started = std::time::Instant::now();
    for index in 0..count {
        let spec = spec(index, &nodes);
        for broker in &brokers {
            broker.create_log(spec.clone()).await.unwrap();
        }
    }
    all_led(&brokers, count, &nodes).await;
    let created = usage(root.path());
    let events = ozzy_runtime::profiling::totals();
    let views = (0..count).map(|index| {
        let id = spec(index, &nodes).id;
        let view = |broker: &Broker| broker.runtime_log(id).unwrap().status().borrow().view;
        brokers.iter().map(view).max().unwrap()
    });
    let mut by_view = std::collections::BTreeMap::<_, usize>::new();
    for view in views {
        *by_view.entry(view).or_default() += 1;
    }
    println!("logs_by_view={by_view:?}");
    for ((name, _, after), (_, _, before)) in events.rows().zip(fresh.rows()) {
        if after != before {
            println!("create_event {name}={}", after.wrapping_sub(before));
        }
    }
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
    if setting("OZZY_IDLE_RESTART", 0) == 0 {
        return;
    }
    // Every broker starts again on the logs it has.
    let (stopped, started) = (ticks(), std::time::Instant::now());
    let events = ozzy_runtime::profiling::totals();
    let mut brokers = Vec::new();
    for (checked, local) in again {
        let context = runtime.context().clone();
        brokers.push(
            Broker::start_trusted_with_context(checked, local, context)
                .await
                .unwrap(),
        );
    }
    let opened = started.elapsed();
    all_led(&brokers, count, &nodes).await;
    println!(
        "restart: started={opened:?} led={:?} cpu_ticks={}",
        started.elapsed(),
        ticks() - stopped
    );
    let totals = ozzy_runtime::profiling::totals();
    for ((name, _, after), (_, _, before)) in totals.rows().zip(events.rows()) {
        if after != before {
            println!("restart_event {name}={}", after.wrapping_sub(before));
        }
    }
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

/// The `logs` directory below `root` that holds record files.
fn records_directory(root: &Path) -> PathBuf {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        if directory.ends_with("logs") {
            return directory;
        }
        let entries = std::fs::read_dir(&directory).unwrap().flatten();
        pending.extend(
            entries
                .map(|entry| entry.path())
                .filter(|path| path.is_dir()),
        );
    }
    panic!("no logs directory below {}", root.display());
}

/// A broker starts on more records than one listing job holds, whatever
/// the size of its log class, and removes a staged record left behind.
#[tokio::test(flavor = "current_thread")]
async fn a_broker_starts_on_more_log_records_than_one_listing_holds() {
    let tombstones = 2500;
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let deployment = deployment_with(root.path(), DeploymentMode::Single, 1, |config| {
        config.logs = Some(ozzy_config::LogClass {
            segment_bytes: 2 * 1024 * 1024,
            max_append_bytes: 512 * 1024,
            max_logs: 4,
        });
    });
    let again = deployment.clone();
    let nodes: Vec<NodeId> = deployment
        .iter()
        .map(|(checked, _)| {
            NodeId::from_bytes(*checked.identity.brokers[&checked.plan.name].as_bytes())
        })
        .collect();
    let mut brokers = start_brokers(&runtime, deployment).await;
    let broker = brokers.pop().unwrap();
    let (kept, dropped) = (spec(1, &nodes), spec(2, &nodes));
    for spec in [&kept, &dropped] {
        broker.create_log(spec.clone()).await.unwrap();
    }
    broker.delete_log(dropped.id).await.unwrap();
    broker.shutdown().await.unwrap();

    // Copies of the deleted log's record under other IDs, and a staged
    // record a crash left.
    let directory = records_directory(root.path());
    let label = uuid::Uuid::from_bytes(*dropped.id.as_bytes()).simple();
    let text = std::fs::read_to_string(directory.join(format!("{label}.record"))).unwrap();
    let deleted = ozzy_config::LogRecord::decode(&text).unwrap();
    for index in 0..tombstones {
        let record = ozzy_config::LogRecord {
            log: uuid::Uuid::from_bytes(*spec(100 + index, &nodes).id.as_bytes()),
            ..deleted.clone()
        };
        let path = directory.join(format!("{}.record", record.log.simple()));
        std::fs::write(path, record.encode().unwrap()).unwrap();
    }
    let staged = directory.join(format!("{label}.staged"));
    std::fs::write(&staged, text).unwrap();

    let (checked, local) = again.into_iter().next().unwrap();
    let broker = Broker::start_trusted_with_context(checked, local, runtime.context().clone())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), broker.ready())
        .await
        .unwrap()
        .unwrap();
    let logs = broker.logs();
    assert_eq!(logs.len(), tombstones + 2);
    let ready =
        |(_, state): &&(GroupId, ozzy_config::LogState)| *state == ozzy_config::LogState::Ready;
    assert_eq!(logs.iter().filter(ready).count(), 1);
    assert!(broker.runtime_log(kept.id).is_some());
    assert!(!staged.exists());
    broker.shutdown().await.unwrap();
}
