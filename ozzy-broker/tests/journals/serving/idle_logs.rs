//! Cost of idle runtime logs on real files: open handles, allocated disk,
//! resident memory and CPU time per log, on three brokers in one process.

use super::*;
use ozzy_broker::LogSpec;
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
        segment_bytes: None,
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
        poll_until(|| brokers.iter().any(|broker| leads(broker.runtime_log(id)))).await;
    }
}

/// How many of the first `count` logs have a leader.
fn led(brokers: &[Broker], count: usize, nodes: &[NodeId]) -> usize {
    let led = |index: &usize| {
        let id = spec(*index, nodes).id;
        brokers.iter().any(|broker| leads(broker.runtime_log(id)))
    };
    (0..count).filter(led).count()
}

/// How many of the first `count` logs are in each view.
fn logs_by_view(
    brokers: &[Broker],
    count: usize,
    nodes: &[NodeId],
) -> std::collections::BTreeMap<u64, usize> {
    let mut by_view = std::collections::BTreeMap::new();
    for index in 0..count {
        let id = spec(index, nodes).id;
        let view = |broker: &Broker| broker.runtime_log(id).unwrap().status().borrow().view;
        *by_view
            .entry(brokers.iter().map(view).max().unwrap())
            .or_default() += 1;
    }
    by_view
}

/// Print every event counted since `before`, as `label name=count`.
fn print_events(label: &str, before: &ozzy_runtime::profiling::Totals) {
    let now = ozzy_runtime::profiling::totals();
    for ((name, _, after), (_, _, before)) in now.rows().zip(before.rows()) {
        if after != before {
            println!("{label} {name}={}", after.wrapping_sub(before));
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
        config.logs = Some(log_class(2 * 1024 * 1024, 512 * 1024, count));
        for broker in config.brokers.values_mut() {
            for device in broker.devices.values_mut() {
                device.workers.open_handles = 24;
            }
        }
    });
    let nodes = nodes(&deployment);
    let brokers = start_brokers(&runtime, deployment).await;
    let elected = async {
        for index in 0..count {
            let spec = spec(index, &nodes);
            for broker in &brokers {
                broker.create_log(spec.clone()).await.unwrap();
            }
            let log = |broker: &Broker| Some(broker.runtime_log(spec.id).unwrap());
            poll_until(|| brokers.iter().any(|broker| leads(log(broker)))).await;
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

/// A log holds segments of the size its spec names, another log of the
/// same class those of the class, and both keep theirs over a restart.
#[tokio::test(flavor = "current_thread")]
async fn a_log_keeps_the_segment_size_its_spec_names() {
    const MIB: u64 = 1024 * 1024;
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let deployment = deployment_with(root.path(), DeploymentMode::Single, 1, |config| {
        config.logs = Some(log_class(2 * MIB, 256 * 1024, 2));
    });
    let nodes = nodes(&deployment);
    let again = deployment.clone();
    let brokers = start_brokers(&runtime, deployment).await;
    let large = LogSpec {
        segment_bytes: Some(16 * MIB),
        ..spec(1, &nodes)
    };
    let empty = allocated(root.path());
    brokers[0].create_log(spec(0, &nodes)).await.unwrap();
    let small = allocated(root.path());
    brokers[0].create_log(large).await.unwrap();
    let both = allocated(root.path());
    assert!(
        (2 * MIB..4 * MIB).contains(&(small - empty)),
        "{small} {empty}"
    );
    assert!(
        (16 * MIB..18 * MIB).contains(&(both - small)),
        "{both} {small}"
    );
    tokio::time::timeout(Duration::from_secs(30), all_led(&brokers, 2, &nodes))
        .await
        .unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    let (checked, local) = again.into_iter().next().unwrap();
    let brokers = [start_broker(&runtime, checked, local).await];
    tokio::time::timeout(Duration::from_secs(30), all_led(&brokers, 2, &nodes))
        .await
        .unwrap();
    let reopened = allocated(root.path());
    assert!(
        (18 * MIB..22 * MIB).contains(&(reopened - empty)),
        "{reopened} {empty}"
    );
    let [broker] = brokers;
    broker.shutdown().await.unwrap();
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
        config.logs = Some(log_class(segment, segment / 4, planned));
        for broker in config.brokers.values_mut() {
            for device in broker.devices.values_mut() {
                device.workers.open_handles = handles;
            }
        }
    });
    let nodes = nodes(&deployment);
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
    println!("logs_by_view={:?}", logs_by_view(&brokers, count, &nodes));
    print_events("create_event", &fresh);
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
    report_idle(root.path(), seconds, &events).await;
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    if setting("OZZY_IDLE_RESTART", 0) == 0 {
        return;
    }
    let brokers = restarted(&runtime, again, count, &nodes).await;
    // Cleanup of what the view change replaced runs after the election.
    let settle = setting("OZZY_IDLE_SETTLE", 10) as u64;
    tokio::time::sleep(Duration::from_secs(settle)).await;
    println!(
        "restart_per_copy: disk={} settle_seconds={settle}",
        (usage(root.path()).disk - before.disk) / copies
    );
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

/// Let the brokers idle for `seconds` and print what that cost since
/// `events`.
async fn report_idle(root: &Path, seconds: u64, events: &ozzy_runtime::profiling::Totals) {
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
    let idle = usage(root);
    println!(
        "idle_seconds={seconds} idle_cpu_ticks={idle_ticks} handles={} resident={}",
        idle.handles, idle.resident
    );
    print_events("idle_event", events);
}

/// Start every broker again on the logs it has, and print what it costs
/// until each of the `count` logs has a leader.
async fn restarted(
    runtime: &WriterRuntime,
    deployment: Vec<(CheckedConfig, BrokerIdentity)>,
    count: usize,
    nodes: &[NodeId],
) -> Vec<Broker> {
    let (stopped, started) = (ticks(), std::time::Instant::now());
    let events = ozzy_runtime::profiling::totals();
    let mut brokers = Vec::new();
    for (checked, local) in deployment {
        brokers.push(start_broker(runtime, checked, local).await);
    }
    let opened = started.elapsed();
    loop {
        let led = led(&brokers, count, nodes);
        println!(
            "restart_progress t={:.1} led={led} ticks={}",
            started.elapsed().as_secs_f64(),
            ticks() - stopped
        );
        if led == count {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    let by_view = logs_by_view(&brokers, count, nodes);
    println!("restart_logs_by_view={by_view:?}");
    println!(
        "restart: started={opened:?} led={:?} cpu_ticks={}",
        started.elapsed(),
        ticks() - stopped
    );
    print_events("restart_event", &events);
    brokers
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
        config.logs = Some(log_class(2 * 1024 * 1024, 512 * 1024, 4));
    });
    let again = deployment.clone();
    let nodes = nodes(&deployment);
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
    let broker = start_broker(&runtime, checked, local).await;
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
