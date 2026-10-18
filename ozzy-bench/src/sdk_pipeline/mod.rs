//! Concurrent SDK producers, real broker, and verified consumer over TCP.
#![allow(clippy::unwrap_used, clippy::expect_used)]
mod args;
mod clients;
mod process;
use args::{Args, cpus, validate};
use clap::Parser;
use clients::clients;
use ozzy_bench::automation::supervise::CpuSnapshot;
use ozzy_sim::broker::network as support;
use process::{Worker, barrier, broadcast, memory};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    process::Command,
    sync::mpsc::{self, Sender},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
type Error = Box<dyn std::error::Error + Send + Sync>;
fn broker(args: &Args) -> Result<(), Error> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let mode = if args.mode == "single" {
        ozzy_config::DeploymentMode::Single
    } else {
        ozzy_config::DeploymentMode::Three
    };
    let cluster = runtime.block_on(support::cluster(
        mode,
        &args.transport,
        args.memory_mib * 1024 * 1024,
        Some((
            args.segment_mib as u64 * 1024 * 1024,
            args.retention_mib as u64 * 1024 * 1024,
        )),
    ));
    println!(
        "{}",
        json!({"event":"ready", "brokers": support::addresses(&cluster)})
    );
    std::io::stdout().flush()?;
    let (send, commands) = mpsc::channel();
    thread::spawn(move || {
        let mut line = String::new();
        let result = std::io::stdin().read_line(&mut line);
        let _ = send.send(result);
    });
    while matches!(
        commands.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ) {
        if !diagnostics().is_empty() {
            return Err(format!("broker diagnostics: {:?}", diagnostics()).into());
        }
    }
    let mut failures = Vec::new();
    for broker in cluster.brokers {
        if let Err(error) = runtime.block_on(broker.shutdown()) {
            failures.push(error.to_string());
        }
    }
    for image in cluster.images {
        runtime.block_on(image)?;
    }
    if !failures.is_empty() || !diagnostics().is_empty() {
        return Err(format!("broker failures: {failures:?}, {:?}", diagnostics()).into());
    }
    Ok(())
}

fn diagnostics() -> Vec<ozzy_telemetry::LogEntry> {
    ozzy_telemetry::recent_logs()
        .into_iter()
        .filter(|entry| entry.level != ozzy_telemetry::Level::Info)
        .collect()
}

fn start(args: &Args, events: &Sender<(String, Value)>) -> Result<BTreeMap<String, Worker>, Error> {
    let mut command = Command::new("taskset");
    command
        .args(["-c", &args.broker_cpus])
        .arg(std::env::current_exe()?)
        .args(std::env::args_os().skip(1))
        .arg("--broker-child");
    let mut workers = BTreeMap::new();
    workers.insert(
        "broker".into(),
        Worker::spawn("broker", &mut command, events)?,
    );
    Ok(workers)
}

fn run(args: &Args) -> Result<Value, Error> {
    let (send, events) = mpsc::channel();
    let mut workers = start(args, &send)?;
    let ready = barrier(&events, &mut workers, &["broker".into()], "ready")?;
    let roles = clients(args, &ready["broker"]["brokers"], &mut workers, &send)?;
    barrier(&events, &mut workers, &roles, "ready")?;
    let mut result = json!({});
    for (phase, seconds) in [args.warmup, args.duration].into_iter().enumerate() {
        let cpu = CpuSnapshot::take(Some(workers["broker"].child.id()))?;
        let start = Instant::now();
        let deadline = (SystemTime::now().duration_since(UNIX_EPOCH)?
            + Duration::from_secs_f64(seconds))
        .as_nanos();
        for role in &roles {
            workers
                .get_mut(role)
                .unwrap()
                .send(&json!({"command":"run", "deadline":u64::try_from(deadline)?}))?;
        }
        let counts = barrier(&events, &mut workers, &roles, "done")?;
        let elapsed = start.elapsed().as_secs_f64();
        let mut cpu = cpu.elapsed(&CpuSnapshot::take(Some(workers["broker"].child.id()))?)?;
        cpu["scope"] = json!("broker process during SDK phase, including verification drain");
        if counts.values().any(|v| v["phase"] != phase) {
            return Err("phase mismatch".into());
        }
        let produced = verified(&counts)?;
        result = json!({"records":produced, "seconds":elapsed, "records_per_second":produced as f64 / elapsed, "payload_bytes_per_second":produced as f64 * args.size as f64 / elapsed, "clients":counts, "window":args.window});
        result["broker_cpu"] = cpu;
    }
    result["process_memory"] = json!(memory(&workers)?);
    broadcast(&mut workers, &roles, &json!({"command":"finish"}))?;
    let metadata = barrier(&events, &mut workers, &roles, "finished")?;
    let reference = &metadata["consumer"];
    if metadata.values().any(|v| v != reference) {
        return Err("client versions or workload differ".into());
    }
    result
        .as_object_mut()
        .unwrap()
        .extend(reference.as_object().unwrap().clone());
    result.as_object_mut().unwrap().remove("event");
    for role in &roles {
        workers.get_mut(role).unwrap().finish()?;
    }
    workers
        .get_mut("broker")
        .unwrap()
        .send(&json!({"command":"stop"}))?;
    workers.get_mut("broker").unwrap().finish()?;
    if events.try_iter().next().is_some() {
        return Err("unexpected final diagnostics".into());
    }
    Ok(result)
}

fn verified(counts: &BTreeMap<String, Value>) -> Result<u64, Error> {
    let produced: u64 = counts
        .iter()
        .filter(|(r, _)| r.starts_with("producer"))
        .map(|(_, v)| v["records"].as_u64().unwrap())
        .sum();
    let consumed = counts["consumer"]["records"].as_u64().unwrap();
    if produced == 0 || produced != consumed {
        return Err(format!("record count mismatch: {produced} vs {consumed}").into());
    }
    Ok(produced)
}

fn snapshot() -> Result<Vec<u8>, Error> {
    let mut bytes = Command::new("git").args(["diff", "HEAD"]).output()?.stdout;
    let untracked = Command::new("git")
        .args(["ls-files", "--others", "--exclude-standard", "-z"])
        .output()?;
    for path in untracked
        .stdout
        .split(|b| *b == 0)
        .filter(|p| !p.is_empty())
    {
        bytes.extend_from_slice(path);
        bytes.extend(std::fs::read(std::str::from_utf8(path)?)?);
    }
    Ok(bytes)
}

fn run_metadata(args: &Args, source: &[u8]) -> Result<Value, Error> {
    let revision = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .to_string();
    Ok(json!({
            "kind": if args.java.is_some() { "java-pipeline" } else { "python-pipeline" },
            "run_id": run_id,
            "label": args.label,
            "mode": args.mode,
            "codec": args.codec,
            "api": args.api,
            "transport": args.transport,
            "size": args.size,
            "warmup": args.warmup,
            "duration": args.duration,
            "broker_cpus": args.broker_cpus,
            "producer_cpus": args.producer_cpus,
            "consumer_cpu": args.consumer_cpu,
            "reader_queue_messages": args.reader_queue_messages,
            "memory_mib": args.memory_mib,
            "segment_mib": args.segment_mib,
            "retention_mib": args.retention_mib,
            "confirmation": args.confirmation,
            "revision": String::from_utf8_lossy(&revision.stdout).trim(),
            "runner_sha256": format!("{:x}", Sha256::digest(std::fs::read(std::env::current_exe()?)?)),
            "source_sha256": format!("{:x}", Sha256::digest(source)),
            "source_diff": String::from_utf8_lossy(source),
            "boundary": "Concurrent SDK producers, real broker process, SDK consumer; TCP, memory storage, four partitions; durable confirmation and full-byte/order verification",
    }))
}

pub(super) fn main() -> Result<(), Error> {
    let args = Args::parse();
    validate(&args)?;
    if args.broker_child {
        return broker(&args);
    }
    let source = snapshot()?;
    let metadata = run_metadata(&args, &source)?;
    let mut rows = Vec::new();
    for repetition in 0..args.repetitions {
        let mut row = run(&args)?;
        row.as_object_mut()
            .unwrap()
            .extend(metadata.as_object().unwrap().clone());
        row["repetition"] = json!(repetition);
        rows.push(row);
    }
    if source != snapshot()? {
        return Err("source changed during benchmark".into());
    }
    if args.profile.is_none() && !args.no_ledger {
        let cache = PathBuf::from(std::env::var("HOME")?).join(".cache/ozzy");
        std::fs::create_dir_all(&cache)?;
        let mut ledger = OpenOptions::new()
            .create(true)
            .append(true)
            .open(cache.join(if args.java.is_some() {
                "java-sdk.jsonl"
            } else {
                "python-sdk.jsonl"
            }))?;
        for row in &rows {
            writeln!(ledger, "{row}")?;
        }
    }
    for mut row in rows {
        row.as_object_mut().unwrap().remove("source_diff");
        println!("{row}");
    }
    Ok(())
}
