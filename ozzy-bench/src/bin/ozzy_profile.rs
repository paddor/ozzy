//! Replay one verified native case under a profiler, outside comparison ledgers.
#![forbid(unsafe_code)]

use clap::Parser;
use omq_tokio::{Context, Options, Socket, SocketType};
use ozzy_bench::automation::{self, Result, isolation, source, supervise};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

#[derive(Default)]
struct CounterFeed {
    previous: [u64; 6],
    previous_elapsed_ms: u64,
    sequence: u64,
    samples: u64,
    dropped: u64,
    events: Value,
    stages: Value,
    last_alert: Option<Instant>,
}

/// The collector binds local abstract IPC before any worker starts. Each
/// application thread owns its PUB socket and enqueues snapshots directly.
async fn bind_counters(directory: &Path) -> Result<(Context, Socket, String)> {
    let name = directory
        .file_name()
        .ok_or("profile directory has no name")?;
    let endpoint = format!("ipc://@ozzy-profile-{}", name.to_string_lossy());
    let context = Context::new();
    let socket = context.socket(
        SocketType::Sub,
        Options::default()
            .recv_hwm(8192)
            .max_message_size(256 * 1024)
            .linger(Duration::ZERO),
    );
    socket.subscribe(Vec::<u8>::new()).await?;
    socket.bind(endpoint.parse()?).await?;
    Ok((context, socket, endpoint))
}

/// Surface anomalous repair or dispatch refusal while workload still runs.
async fn watch_counters(
    socket: &Socket,
    feeds: &mut BTreeMap<(u64, String), CounterFeed>,
    trace: Option<&mut Vec<Value>>,
    started: Instant,
) -> Result<()> {
    let mut trace = trace;
    loop {
        let message = socket.recv().await?;
        observe_counters(&message, feeds, trace.as_deref_mut(), started)?;
    }
}

fn observe_counters(
    message: &omq_tokio::Message,
    feeds: &mut BTreeMap<(u64, String), CounterFeed>,
    trace: Option<&mut Vec<Value>>,
    started: Instant,
) -> Result<()> {
    let received_ns = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
    let mut row: Value = serde_json::from_slice(
        message
            .part_slice(0)
            .ok_or("counter message has no payload")?,
    )?;
    let pid = row["pid"].as_u64().ok_or("counter pid missing")?;
    let thread = row["thread"].as_str().ok_or("counter thread missing")?;
    let sequence = row["sequence"].as_u64().ok_or("counter sequence missing")?;
    let feed = feeds.entry((pid, thread.to_owned())).or_default();
    feed.dropped += sequence.saturating_sub(feed.sequence.saturating_add(1));
    feed.sequence = sequence;
    feed.samples += 1;
    feed.events = row["events"].clone();
    feed.stages = row["stages"].clone();
    {
        let elapsed_ms = row["elapsed_ms"]
            .as_u64()
            .ok_or("counter elapsed time missing")?;
        let interval_ms = elapsed_ms.saturating_sub(feed.previous_elapsed_ms);
        feed.previous_elapsed_ms = elapsed_ms;
        let current = [
            row["events"]["replica_send"]
                .as_u64()
                .ok_or("missing fresh sends")?,
            row["events"]["replica_repair"]
                .as_u64()
                .ok_or("missing repairs")?,
            row["events"]["native_proposal_refusal"]
                .as_u64()
                .ok_or("missing proposal refusal count")?,
            row["events"]["native_admission_refusal"]
                .as_u64()
                .ok_or("missing admission refusal count")?,
            row["events"]["shard_reply_refusal"]
                .as_u64()
                .ok_or("missing shard reply refusal count")?,
            row["events"]["shard_port_refusal"]
                .as_u64()
                .ok_or("missing shard port refusal count")?,
        ];
        let delta = std::array::from_fn::<_, 6, _>(|slot| {
            current[slot].saturating_sub(feed.previous[slot])
        });
        feed.previous = current;
        let refused: u64 = delta[2..].iter().sum();
        if ((delta[1] >= 8 && delta[1] >= delta[0] / 2) || refused >= 8)
            && feed
                .last_alert
                .is_none_or(|at| at.elapsed() >= Duration::from_millis(200))
        {
            println!(
                "COUNTER_RATE pid={pid} thread={thread} interval_ms={interval_ms} fresh={} repair={} refused={} (proposal={} admission={} reply={} port={})",
                delta[0], delta[1], refused, delta[2], delta[3], delta[4], delta[5]
            );
            std::io::stdout().flush()?;
            feed.last_alert = Some(Instant::now());
        }
    }
    if let Some(trace) = trace {
        row["collector_elapsed_ns"] = json!(received_ns);
        trace.push(row);
    }
    Ok(())
}

#[derive(Default)]
struct StageTotals {
    bins: Vec<(usize, u64)>,
    total_ns: u64,
    max_ns: u64,
}

fn merge_counters(row: &mut Value, feeds: &BTreeMap<(u64, String), CounterFeed>) -> Result<()> {
    for role in ["brokers", "writers", "readers"] {
        let processes = row[role].as_array_mut().ok_or("missing profile workers")?;
        for process in processes {
            let pid = if role == "brokers" {
                process["identity"]["pid"].as_u64()
            } else {
                process["pid"].as_u64()
            }
            .ok_or("missing profile worker pid")?;
            let mut events = BTreeMap::<String, u64>::new();
            let mut stages = BTreeMap::<String, StageTotals>::new();
            for ((source, _), feed) in feeds {
                if *source != pid {
                    continue;
                }
                for (name, amount) in feed.events.as_object().ok_or("missing thread events")? {
                    let amount = amount.as_u64().ok_or("invalid thread event count")?;
                    let entry = events.entry(name.clone()).or_default();
                    if matches!(
                        name.as_str(),
                        "live_window_peak_operations" | "live_window_peak_bytes"
                    ) {
                        *entry = (*entry).max(amount);
                    } else {
                        *entry += amount;
                    }
                }
                for stage in feed.stages.as_array().ok_or("missing thread stages")? {
                    let name = stage["stage"].as_str().ok_or("missing stage name")?;
                    let total = stages.entry(name.to_owned()).or_default();
                    total.total_ns += stage["total_ns"].as_u64().ok_or("missing stage total")?;
                    total.max_ns = total
                        .max_ns
                        .max(stage["max_ns"].as_u64().ok_or("missing stage maximum")?);
                    for pair in stage["bins"].as_array().ok_or("missing stage bins")? {
                        let bins = pair.as_array().ok_or("invalid stage bin")?;
                        if bins.len() != 2 {
                            return Err("invalid stage bin".into());
                        }
                        total.bins.push((
                            bins[0].as_u64().ok_or("invalid stage bin index")? as usize,
                            bins[1].as_u64().ok_or("invalid stage bin count")?,
                        ));
                    }
                }
            }
            if events.is_empty() {
                continue;
            }
            process["usage"]["replication_events"] = json!(events);
            process["usage"]["stages"] = Value::Array(
                stages
                    .into_iter()
                    .map(|(name, total)| {
                        let stage = ozzy_runtime::profiling::summarize_stage(
                            &name,
                            &total.bins,
                            total.total_ns,
                            total.max_ns,
                        )
                        .ok_or("invalid merged stage bins")?;
                        Ok(json!({"stage":stage.stage,"samples":stage.samples,
                            "total_ns":stage.total_ns,"p50_ns":stage.p50_ns,
                            "p99_ns":stage.p99_ns,"max_ns":stage.max_ns}))
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod counter_tests {
    use super::*;

    #[test]
    fn counter_trace_retains_received_snapshot_and_collector_time() {
        ozzy_runtime::profiling::enable();
        let events: BTreeMap<_, _> = ozzy_runtime::profiling::events()
            .unwrap()
            .into_iter()
            .collect();
        let row = json!({
            "pid":42,"thread":"shard-a","sequence":1,"elapsed_ms":20,
            "events":events,"stages":[]
        });
        let message = omq_tokio::Message::from(serde_json::to_vec(&row).unwrap());
        let mut feeds = BTreeMap::new();
        let mut trace = Vec::new();
        observe_counters(&message, &mut feeds, Some(&mut trace), Instant::now()).unwrap();
        assert_eq!(trace.len(), 1);
        assert_eq!(trace[0]["events"], row["events"]);
        assert_eq!(trace[0]["stages"], row["stages"]);
        assert!(trace[0]["collector_elapsed_ns"].as_u64().is_some());
        assert_eq!(feeds[&(42, "shard-a".to_owned())].samples, 1);
    }

    #[test]
    fn merge_sums_counts_but_keeps_largest_peak() {
        let mut row = json!({
            "brokers":[{"identity":{"pid":42},"usage":{}}],
            "writers":[],"readers":[]
        });
        let feeds = BTreeMap::from([
            (
                (42, "shard-a".to_owned()),
                CounterFeed {
                    events: json!({"replica_repair":7,"live_window_peak_operations":11}),
                    stages: json!([]),
                    ..Default::default()
                },
            ),
            (
                (42, "shard-b".to_owned()),
                CounterFeed {
                    events: json!({"replica_repair":9,"live_window_peak_operations":5}),
                    stages: json!([]),
                    ..Default::default()
                },
            ),
        ]);
        merge_counters(&mut row, &feeds).unwrap();
        assert_eq!(
            row["brokers"][0]["usage"]["replication_events"]["replica_repair"],
            16
        );
        assert_eq!(
            row["brokers"][0]["usage"]["replication_events"]["live_window_peak_operations"],
            11
        );
    }
}

#[derive(Debug, Parser)]
struct Args {
    /// Saved native workload containing command.json.
    #[arg(long)]
    case_dir: PathBuf,
    /// Physical syscalls, CPU samples, or opt-in application stage timing.
    #[arg(long, value_parser = ["syscall", "cpu", "cpu-kernel", "stages"])]
    kind: String,
    #[arg(long, default_value_t = 5.0)]
    duration: f64,
    #[arg(long, default_value_t = 2.0)]
    warmup: f64,
    /// CPU samples per second per active thread. Profiling overhead is excluded from comparisons.
    #[arg(long, default_value_t = 99, value_parser = clap::value_parser!(u32).range(1..=999))]
    frequency: u32,
    /// Replay the workload with the current verified build instead of the case binary.
    #[arg(long)]
    current_build: bool,
    /// Retain received SUB snapshots in an SSD artifact for time correlation.
    #[arg(long)]
    counter_trace: bool,
}

fn replace(command: &mut [String], name: &str, value: String) -> Result<()> {
    let position = command
        .iter()
        .position(|v| v == name)
        .ok_or("missing case argument")?;
    *command
        .get_mut(position + 1)
        .ok_or("missing argument value")? = value;
    Ok(())
}

fn workload(args: &Args, directory: &Path) -> Result<Vec<String>> {
    let mut command: Vec<String> =
        serde_json::from_value(automation::read_json(&args.case_dir.join("command.json"))?)?;
    let worker = automation::worker_binary();
    if !command.iter().any(|arg| Path::new(arg) == worker) {
        return Err("case uses another worker build; rerun comparison for this checkout".into());
    }
    if !command.windows(2).any(|v| {
        v[0] == "--system"
            && matches!(
                v[1].as_str(),
                "single-durable" | "disk-quorum" | "replicated-persisting"
            )
    }) || command.iter().any(|v| v == "--external-system")
    {
        return Err("profiling requires a native case".into());
    }
    replace(&mut command, "--duration", args.duration.to_string())?;
    replace(&mut command, "--warmup", args.warmup.to_string())?;
    replace(
        &mut command,
        "--storage-dir",
        format!(
            "{}/ozzy-profile/{}",
            automation::artifact_root().display(),
            directory.file_name().unwrap().to_string_lossy()
        ),
    )?;
    let mut prefix = match args.kind.as_str() {
        "stages" => Vec::new(),
        "syscall" => vec![
            "strace".into(),
            "-ff".into(),
            "--seccomp-bpf".into(),
            "-qq".into(),
            "-ttt".into(),
            "-T".into(),
            "-y".into(),
            "-s".into(),
            "0".into(),
            "-e".into(),
            "trace=openat,fdatasync,fsync,sync_file_range,pwrite64,pwritev,io_submit,io_getevents,io_pgetevents".into(),
            "-o".into(),
            directory.join("trace").display().to_string(),
        ],
        "cpu" | "cpu-kernel" => vec![
            "perf".into(),
            "record".into(),
            "-q".into(),
            "-e".into(),
            if args.kind == "cpu-kernel" {
                "cpu-clock"
            } else {
                "cpu-clock:u"
            }
            .into(),
            "-F".into(),
            args.frequency.to_string(),
            "--clockid".into(),
            "mono".into(),
            "-g".into(),
            "--call-graph".into(),
            "dwarf,65528".into(),
            // Full Rust async stacks can exceed the default perf ring during
            // short scheduling stalls. Lost samples invalidate the capture.
            "--mmap-pages".into(),
            "8M".into(),
            "-o".into(),
            directory.join("perf.data").display().to_string(),
            "--".into(),
            // perf 6.12 can retain the controller's mapping after a child exec
            // and use its lower ASLR base to unwind the broker's same binary.
            // Fixed addresses avoid that ambiguity only in profiling runs.
            "setarch".into(),
            std::env::consts::ARCH.into(),
            "-R".into(),
        ],
        _ => unreachable!("validated kind"),
    };
    prefix.append(&mut command);
    Ok(prefix)
}

fn physical_writes(directory: &Path) -> Result<Value> {
    let mut sizes = Vec::new();
    let mut durations = Vec::new();
    let mut aio_submitted = 0_u64;
    let mut aio_completed = 0_u64;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_name().to_string_lossy().starts_with("trace.") {
            continue;
        }
        for line in fs::read_to_string(entry.path())?.lines() {
            let Some((_, result)) = line.rsplit_once(" = ") else {
                continue;
            };
            let Some(bytes) = result
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            if line.contains("io_submit(") {
                aio_submitted += bytes;
                continue;
            }
            if line.contains("io_getevents(") || line.contains("io_pgetevents(") {
                aio_completed += bytes;
                continue;
            }
            if !(line.contains("pwritev(") || line.contains("pwrite64("))
                || !line.contains("/segments/")
            {
                continue;
            }
            let Some(seconds) = result
                .rsplit_once('<')
                .and_then(|(_, s)| s.strip_suffix('>'))
                .and_then(|s| s.parse::<f64>().ok())
            else {
                continue;
            };
            sizes.push(bytes);
            durations.push(seconds * 1e6);
        }
    }
    if sizes.is_empty() && aio_submitted == 0 {
        return Err("syscall profile captured no segment writes or kernel AIO submissions".into());
    }
    sizes.sort_unstable();
    durations.sort_by(f64::total_cmp);
    let rank = |percent: usize| (sizes.len() * percent).div_ceil(100) - 1;
    let mut summary = json!({"calls":sizes.len(), "bytes_max":sizes.last(),
        "duration_max_us":durations.last(),
        "kernel_aio":{"submitted":aio_submitted,"completed":aio_completed,
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

fn cpu_report(directory: &Path, row: &Value) -> Result<()> {
    let start = row["measurement_start_ns"]
        .as_u64()
        .ok_or("missing measurement start")?;
    let end = row["measurement_end_ns"]
        .as_u64()
        .ok_or("missing measurement end")?;
    let window = format!("{:.9},{:.9}", start as f64 / 1e9, end as f64 / 1e9);
    let host = fs::read_to_string("/proc/sys/kernel/hostname")?;
    let brokers = broker_pids(row, host.trim())?;
    let mut reports = vec![
        ("cpu.txt".to_owned(), None),
        (
            "broker-cpu.txt".into(),
            Some(
                brokers
                    .iter()
                    .map(|(_, pid)| pid.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ),
    ];
    reports.extend(
        brokers
            .into_iter()
            .map(|(index, pid)| (format!("broker-{index}-cpu.txt"), Some(pid))),
    );
    for role in ["writers", "readers"] {
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
        reports.push((format!("{role}-cpu.txt"), Some(pids.join(","))));
    }
    for (file, pid) in reports {
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
        command.args(["--time", &window]);
        if let Some(pid) = pid {
            command.args(["--pid", &pid, "--percentage", "relative"]);
        }
        let output = command.output()?;
        if !output.status.success() || !output.stderr.is_empty() {
            return Err(format!(
                "CPU report failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        fs::write(directory.join(file), output.stdout)?;
    }
    Ok(())
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
            b["identity"]["pid"]
                .as_u64()
                .map(|p| (index, p.to_string()))
                .ok_or("missing broker PID")
        })
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn verify_build(
    saved: &Value,
    identity: &Value,
    worker_inputs: &Value,
    executable: &str,
) -> Result<()> {
    let inputs_match = if saved["worker_inputs"].is_null() {
        saved["source"] == *identity
    } else {
        saved["worker_inputs"] == *worker_inputs
    };
    if !inputs_match || saved["executable_sha256"] != executable {
        return Err("profile build stamp differs; run ozzy_compare --check-only first".into());
    }
    Ok(())
}

fn verify_live_readers(row: &Value) -> Result<()> {
    let live_records: u64 = row["readers"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|reader| reader["topic_readers"].as_array().into_iter().flatten())
        .filter_map(|reader| reader["live_records"].as_u64())
        .sum();
    if row["broker_runtime"] != "production-deployment"
        || row["native_reader_api"] != "decoded-records"
        || row["live_readers"] != true
        || live_records == 0
        || row["reader_transport"]
            != "TCP PUB/SUB live records; PEER subscriptions, replay, gap repair"
    {
        return Err("profile did not use production live readers".into());
    }
    Ok(())
}

fn stage_samples(process: &Value, stage: &str) -> u64 {
    process["usage"]["stages"]
        .as_array()
        .and_then(|stages| stages.iter().find(|entry| entry["stage"] == stage))
        .and_then(|entry| entry["samples"].as_u64())
        .unwrap_or(0)
}

fn verify_stage_profiles(row: &Value, host: &str) -> Result<()> {
    for (role, stage) in [
        ("writers", "request_roundtrip"),
        ("readers", "reader_materialize"),
    ] {
        let processes = row[role].as_array().ok_or("missing profiled clients")?;
        if processes.is_empty()
            || processes
                .iter()
                .any(|process| stage_samples(process, stage) == 0)
        {
            return Err(format!("{role} have no {stage} stage samples").into());
        }
    }
    let brokers = row["brokers"]
        .as_array()
        .ok_or("missing profiled brokers")?;
    if brokers
        .iter()
        .filter(|broker| {
            broker["identity"]["host"]
                .as_str()
                .is_none_or(|name| name == host)
        })
        .any(|broker| stage_samples(broker, "replica_confirmation") > 0)
    {
        Ok(())
    } else {
        Err("local brokers have no replica_confirmation stage samples".into())
    }
}

fn monitor_remote(
    command: &[String],
    directory: &Path,
    guard: &mut isolation::Guard,
) -> Result<()> {
    if let Some([_, path]) = command.windows(2).find(|pair| pair[0] == "--placements") {
        let placements = ozzy_bench::placement::Placement::load(Some(Path::new(path)))?;
        automation::distributed::idle(&placements)?;
        automation::distributed::deploy(&placements, &automation::worker_binary(), false)?;
        if let Some(remote) = placements.iter().find(|p| p.remote.is_some()) {
            guard.set_remote(automation::distributed::Audit::start(
                remote, "ozzy", directory,
            )?);
        }
    }
    Ok(())
}

async fn execute_profile(
    args: &Args,
    command: &[String],
    directory: &Path,
    guard: &mut isolation::Guard,
) -> Result<Value> {
    let mut environment = BTreeMap::from([("OZZY_BENCH_PROFILE".into(), args.kind.clone())]);
    let counters = if args.kind == "stages" {
        let bound = bind_counters(directory).await?;
        environment.insert("OZZY_BENCH_COUNTERS_ENDPOINT".into(), bound.2.clone());
        Some(bound)
    } else {
        None
    };
    let run = supervise::execute(
        command,
        directory,
        Duration::from_secs(90),
        &environment,
        None,
        Some(guard),
    );
    if let Some((_context, socket, endpoint)) = counters {
        let mut feeds = BTreeMap::new();
        let mut trace = Vec::new();
        let started = Instant::now();
        let result = tokio::select! {
            result = run => result,
            result = watch_counters(
                &socket,
                &mut feeds,
                args.counter_trace.then_some(&mut trace),
                started,
            ) => {
                result?;
                return Err("counter monitor stopped".into());
            }
        };
        for _ in 0..4096 {
            let Ok(Ok(message)) =
                tokio::time::timeout(Duration::from_millis(20), socket.recv()).await
            else {
                break;
            };
            observe_counters(
                &message,
                &mut feeds,
                args.counter_trace.then_some(&mut trace),
                started,
            )?;
        }
        let samples: u64 = feeds.values().map(|feed: &CounterFeed| feed.samples).sum();
        let dropped: u64 = feeds.values().map(|feed| feed.dropped).sum();
        if feeds.is_empty() {
            return Err("local counter SUB received no worker samples".into());
        }
        if args.counter_trace {
            if dropped != 0 || trace.len() as u64 != samples {
                return Err("counter trace lost PUB snapshots".into());
            }
            let path = directory.join("counter-samples.jsonl");
            let mut file = std::io::BufWriter::new(fs::File::create(&path)?);
            for snapshot in &trace {
                serde_json::to_writer(&mut file, snapshot)?;
                file.write_all(b"\n")?;
            }
            file.flush()?;
            println!("COUNTER_TRACE {} samples={samples}", path.display());
        }
        let mut row = result?;
        merge_counters(&mut row, &feeds)?;
        println!(
            "COUNTER_FEED {endpoint} threads={} samples={samples} missed={dropped}",
            feeds.len()
        );
        Ok(row)
    } else {
        run.await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    if !(0.1..=30.0).contains(&args.duration) || !(0.0..=10.0).contains(&args.warmup) {
        return Err("invalid bounded profile duration".into());
    }
    if args.counter_trace && args.kind != "stages" {
        return Err("counter traces require --kind stages".into());
    }
    automation::install_signals()?;
    isolation::require_idle()?;
    automation::require_artifact_disk()?;
    let case_manifest = automation::read_json(
        &args
            .case_dir
            .parent()
            .ok_or("missing case parent")?
            .join("manifest.json"),
    )?;
    let client_cpus: Vec<usize> =
        serde_json::from_value(case_manifest["arguments"]["client_cpus"].clone())?;
    isolation::pin(None, &client_cpus)?;
    let id = format!("{}-{}", automation::run_id()?, args.kind);
    let directory = automation::artifact_root().join("ozzy-profiles").join(id);
    fs::create_dir_all(directory.parent().unwrap())?;
    fs::create_dir(&directory)?;
    let identity = source::identity(&automation::root())?;
    let executable = automation::worker_binary();
    let executable_sha256 = source::sha256(&executable)?;
    verify_build(
        &automation::read_json(&automation::artifacts().join("comparison-build.json"))?,
        &identity,
        &source::worker_identity(&automation::root())?,
        &executable_sha256,
    )?;
    verify_case_build(&case_manifest, &executable_sha256, args.current_build)?;
    let command = workload(&args, &directory)?;
    automation::json_file(&directory.join("command.json"), &json!(command))?;
    automation::json_file(
        &directory.join("manifest.json"),
        &json!({"source":identity,"executable_sha256":executable_sha256,
        "case_executable_sha256":case_manifest["executable_sha256"], "current_build":args.current_build,
        "case_dir":args.case_dir, "kind":args.kind, "frequency":args.frequency, "comparison_timing":false,
        "aslr_disabled":matches!(args.kind.as_str(), "cpu" | "cpu-kernel"),
        "capture_scope":"coordinator host only; remote brokers participate but are not sampled",
        "controller_cpus":isolation::cpus(None)?, "case_configuration":case_manifest["arguments"]}),
    )?;
    // Drain earlier filesystem writes before starting the profiled workload.
    automation::capture(
        Command::new("sync")
            .arg("-f")
            .arg(automation::artifact_root()),
    )?;
    let mut guard = isolation::Guard::new("ozzy", None).with_profiling(true)?;
    monitor_remote(&command, &directory, &mut guard)?;
    println!("PROFILE {}", directory.display());
    let row = execute_profile(&args, &command, &directory, &mut guard).await?;
    let remote_audit = guard.finish_remote()?;
    automation::json_file(&directory.join("remote-audit-proof.json"), &remote_audit)?;
    if row["profiled"] != true
        || row["native_client_protocol"] != true
        || row["total_verified_records"]
            .as_u64()
            .is_none_or(|n| n == 0)
        || row["total_confirmed_records"] != row["total_verified_records"]
    {
        return Err("profile failed reader verification or instrumentation label".into());
    }
    verify_live_readers(&row)?;
    if args.kind == "stages" {
        let host = fs::read_to_string("/proc/sys/kernel/hostname")?;
        verify_stage_profiles(&row, host.trim())?;
    }
    automation::validation::production_cpu_placement(&row, &case_manifest["arguments"])?;
    source::require_unchanged(&automation::root(), &identity)?;
    automation::json_file(&directory.join("result.json"), &row)?;
    if args.kind == "syscall" {
        let writes = physical_writes(&directory)?;
        automation::json_file(&directory.join("writes.json"), &writes)?;
        println!("WRITES {writes}");
    } else if matches!(args.kind.as_str(), "cpu" | "cpu-kernel") {
        cpu_report(&directory, &row)?;
    }
    println!("VERIFIED {} records", row["total_verified_records"]);
    Ok(())
}

fn verify_case_build(manifest: &Value, executable: &str, current_build: bool) -> Result<()> {
    if !current_build && manifest["executable_sha256"] != executable {
        return Err(
            "profile case used a different executable; use --current-build to replay its workload"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replay_requires_the_worker_from_this_checkout() {
        let directory = tempfile::tempdir().unwrap();
        let args = Args {
            case_dir: directory.path().into(),
            kind: "cpu".into(),
            duration: 3.0,
            warmup: 1.0,
            frequency: 99,
            current_build: false,
            counter_trace: false,
        };
        let mut command = vec![
            "taskset".into(),
            "-c".into(),
            "0,1".into(),
            "/another/checkout/release/ozzy_timed_bench".into(),
            "--system".into(),
            "single-durable".into(),
            "--duration".into(),
            "10".into(),
            "--warmup".into(),
            "5".into(),
            "--storage-dir".into(),
            "/old/storage".into(),
        ];
        let path = directory.path().join("command.json");
        automation::json_file(&path, &json!(command)).unwrap();
        assert!(
            workload(&args, directory.path())
                .unwrap_err()
                .to_string()
                .contains("another worker build")
        );
        command[3] = automation::worker_binary().display().to_string();
        automation::json_file(&path, &json!(command)).unwrap();
        let replay = workload(&args, directory.path()).unwrap();
        assert!(replay.iter().any(|part| part == &command[3]));
        assert!(replay.windows(2).any(|v| v == ["--duration", "3"]));
        assert!(replay.windows(2).any(|v| v == ["--warmup", "1"]));
        assert_ne!(replay.first(), command.first());
        command[5] = "replicated-persisting".into();
        automation::json_file(&path, &json!(command)).unwrap();
        assert!(workload(&args, directory.path()).is_ok());
        command[5] = "single-buffered".into();
        automation::json_file(&path, &json!(command)).unwrap();
        assert!(workload(&args, directory.path()).is_err());
        command[5] = "ram-quorum".into();
        automation::json_file(&path, &json!(command)).unwrap();
        assert!(workload(&args, directory.path()).is_err());
    }

    #[test]
    fn current_build_stamps_check_worker_inputs_and_binary_independently() {
        let source = json!({"revision": "original"});
        let inputs = json!({"code": "unchanged"});
        let saved = json!({"source":source,"worker_inputs":inputs,"executable_sha256":"binary","workload_sha256":"fixture"});
        assert!(verify_build(&saved, &source, &inputs, "binary").is_ok());
        assert!(verify_build(&saved, &json!({"docs":"changed"}), &inputs, "binary").is_ok());
        assert!(verify_build(&saved, &source, &json!({"code":"changed"}), "binary").is_err());
        assert!(verify_build(&saved, &source, &inputs, "changed-binary").is_err());
        let legacy = json!({"source":source,"executable_sha256":"binary"});
        assert!(verify_build(&legacy, &source, &inputs, "binary").is_ok());
        assert!(verify_build(&legacy, &json!({}), &inputs, "binary").is_err());
    }

    #[test]
    fn changing_profile_binary_requires_explicit_opt_in() {
        let manifest = json!({"executable_sha256":"original"});
        assert!(verify_case_build(&manifest, "original", false).is_ok());
        assert!(verify_case_build(&manifest, "modified", false).is_err());
        assert!(verify_case_build(&manifest, "modified", true).is_ok());
    }

    #[test]
    fn profiles_require_production_readers_on_the_live_publication_path() {
        let row = json!({"broker_runtime":"production-deployment", "native_reader_api":"decoded-records", "live_readers":true,
            "reader_transport":"TCP PUB/SUB live records; PEER subscriptions, replay, gap repair",
            "readers":[{"topic_readers":[{"live_records":2,"replayed_records":0}]}]});
        verify_live_readers(&row).unwrap();
        let mut replay_only = row.clone();
        replay_only["readers"][0]["topic_readers"][0]["live_records"] = json!(0);
        assert!(verify_live_readers(&replay_only).is_err());
        for key in [
            "broker_runtime",
            "native_reader_api",
            "live_readers",
            "reader_transport",
        ] {
            let mut bad = row.clone();
            bad[key] = Value::Null;
            assert!(verify_live_readers(&bad).is_err());
        }
    }

    #[test]
    fn stage_profile_requires_samples_from_each_local_role() {
        let mut row = json!({
            "writers":[{"usage":{"stages":[{"stage":"request_roundtrip","samples":2}]}}],
            "readers":[{"usage":{"stages":[{"stage":"reader_materialize","samples":3}]}}],
            "brokers":[
                {"identity":{"host":"local"},"usage":{"stages":[{"stage":"replica_confirmation","samples":4}]}},
                {"identity":{"host":"remote"},"usage":{}}
            ]
        });
        verify_stage_profiles(&row, "local").unwrap();
        row["readers"][0]["usage"]["stages"][0]["samples"] = json!(0);
        assert!(verify_stage_profiles(&row, "local").is_err());
        row["readers"][0]["usage"]["stages"][0]["samples"] = json!(3);
        row["writers"][0]["usage"]["stages"] = json!([]);
        assert!(verify_stage_profiles(&row, "local").is_err());
        row["writers"][0]["usage"]["stages"] = json!([{"stage":"request_roundtrip","samples":2}]);
        row["brokers"][0]["usage"]["stages"] = json!([]);
        assert!(verify_stage_profiles(&row, "local").is_err());
    }

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
        let row = physical_writes(directory.path()).unwrap();
        assert_eq!(row["calls"], 1);
        assert_eq!(row["bytes_p50"], 4096);
        assert_eq!(row["duration_p99_us"], 2000.0);
    }

    #[test]
    fn syscall_summary_distinguishes_aio_requests_from_synchronous_writes() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("trace.1"), "1 pwrite64(9</x/segments/1.log>, [], 4096, 64) = 4096 <0.002000>\n2 io_submit(1, 2, []) = 2 <0.000100>\n3 io_getevents(1, 1, 2, [], NULL) = 1 <0.003000>\n4 io_pgetevents(1, 1, 2, [], NULL, NULL) = 1 <0.004000>\n5 io_submit(1, 1, []) = -1 EIO <0.010000>\n").unwrap();
        let row = physical_writes(directory.path()).unwrap();
        assert_eq!(row["calls"], 1);
        assert_eq!(row["bytes_p50"], 4096);
        assert_eq!(row["kernel_aio"]["submitted"], 2);
        assert_eq!(row["kernel_aio"]["completed"], 2);
        fs::write(directory.path().join("trace.1"), "1 io_submit(1, 1, []) = 1 <0.000100>\n2 io_getevents(1, 1, 1, [], NULL) = 1 <0.003000>\n").unwrap();
        let row = physical_writes(directory.path()).unwrap();
        assert_eq!(row["calls"], 0);
        assert!(row["bytes_p50"].is_null());
        assert!(row["duration_p50_us"].is_null());
        assert_eq!(row["kernel_aio"]["submitted"], 1);
        assert_eq!(row["kernel_aio"]["completed"], 1);
    }
}
