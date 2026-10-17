//! Verified Python SDK benchmark. Brokers use the shared memory-storage harness.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use clap::Parser;
use ozzy_config::DeploymentMode;
use ozzy_sim::broker::Cluster;
use serde_json::{Value, json};
use std::{
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
};
#[path = "../tests/support/mod.rs"]
mod support;

#[derive(Debug, Parser)]
#[command(about = "Verified Python send/confirm/read throughput over TCP and memory storage")]
struct Args {
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value = "single", value_parser = ["single", "quorum"])]
    mode: String,
    #[arg(long, default_value = "lz4", value_parser = ["raw", "lz4"])]
    codec: String,
    #[arg(long, default_value = "blocking", value_parser = ["blocking", "asyncio"])]
    api: String,
    #[arg(long, default_value = "tcp", value_parser = ["tcp", "lz4+tcp"])]
    transport: String,
    #[arg(long, default_value_t = 1024)]
    size: usize,
    #[arg(long, default_value_t = 2.0)]
    warmup: f64,
    #[arg(long, default_value_t = 8.0)]
    duration: f64,
    #[arg(long, default_value_t = 2)]
    repetitions: usize,
    #[arg(long)]
    profile: Option<PathBuf>,
    #[arg(long, default_value = "candidate")]
    label: String,
    #[arg(long, default_value = "0,1")]
    broker_cpus: String,
    #[arg(long, default_value_t = 2)]
    client_cpu: usize,
    #[arg(long, default_value_t = 32)]
    reader_queue_messages: usize,
    #[arg(long, default_value_t = 256)]
    memory_mib: usize,
}

fn run(args: &Args) -> Result<Value, Box<dyn std::error::Error>> {
    let mode = if args.mode == "single" {
        DeploymentMode::Single
    } else {
        DeploymentMode::Three
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let bytes = args
        .memory_mib
        .checked_mul(1024 * 1024)
        .ok_or("memory limit overflow")?;
    let cluster = runtime.block_on(support::cluster(mode, &args.transport, bytes, None));
    let brokers = support::addresses(&cluster);
    let output = Command::new("taskset")
        .args(["-c", &args.client_cpu.to_string()]).arg(&args.python)
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/examples/workload.py"))
        .env("OZZY_TEST_BROKERS", serde_json::to_string(&brokers)?)
        .env("OZZY_BENCH_OPTIONS", json!({"api": args.api, "codec": args.codec, "size": args.size, "warmup": args.warmup, "duration": args.duration, "profile": args.profile, "reader_queue_messages": args.reader_queue_messages}).to_string())
        .env_remove("PYTHONPATH").stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
    let mut child = output;
    while child.try_wait()?.is_none() {
        if ozzy_telemetry::recent_logs()
            .iter()
            .any(|entry| entry.level != ozzy_telemetry::Level::Info)
        {
            let _ = child.kill();
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let output = child.wait_with_output();
    drain(cluster, &runtime)?;
    let output = output?;
    if !output.status.success() || !output.stderr.is_empty() {
        return Err(format!(
            "invalid benchmark: {:?}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn drain(
    cluster: Cluster,
    runtime: &tokio::runtime::Runtime,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut failures = Vec::new();
    for broker in cluster.brokers {
        if let Err(error) = runtime.block_on(broker.shutdown()) {
            failures.push(error.to_string());
        }
    }
    let mut bytes = Vec::new();
    for image in cluster.images {
        bytes.push(runtime.block_on(image)?.stored_bytes());
    }
    let logs: Vec<_> = ozzy_telemetry::recent_logs()
        .into_iter()
        .filter(|entry| entry.level != ozzy_telemetry::Level::Info)
        .collect();
    if !failures.is_empty() || !logs.is_empty() {
        return Err(format!(
            "invalid broker run: {failures:?}; image bytes {bytes:?}; diagnostics {logs:?}"
        )
        .into());
    }
    Ok(())
}

fn pin(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.size < 16
        || args.size > 65536
        || !args.warmup.is_finite()
        || args.warmup < 0.0
        || !args.duration.is_finite()
        || args.duration <= 0.0
        || args.repetitions == 0
        || args.memory_mib < 32
        || !(1..=64).contains(&args.reader_queue_messages)
    {
        return Err("invalid record size, timing, or repetition count".into());
    }
    let broker_cpus: Vec<usize> = args
        .broker_cpus
        .split(',')
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    if broker_cpus.is_empty() || broker_cpus.contains(&args.client_cpu) {
        return Err("broker and client CPUs must be disjoint".into());
    }
    let pin = Command::new("taskset")
        .args(["-pc", &args.broker_cpus, &std::process::id().to_string()])
        .output()?;
    if !pin.status.success() {
        return Err(String::from_utf8_lossy(&pin.stderr).into_owned().into());
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    pin(&args)?;
    let revision = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    let diff = Command::new("git").args(["diff", "HEAD"]).output()?;
    let source = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/examples/workload.py"))?;
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos()
        .to_string();
    let mut rows = Vec::new();
    for repetition in 0..args.repetitions {
        let mut row = run(&args)?;
        row["kind"] = json!("python-sdk");
        row["memory_mib"] = json!(args.memory_mib);
        row["run_id"] = json!(run_id);
        row["label"] = json!(args.label);
        row["mode"] = json!(args.mode);
        row["transport"] = json!(args.transport);
        row["broker_cpus"] = json!(args.broker_cpus);
        row["client_cpu"] = json!(args.client_cpu);
        row["repetition"] = json!(repetition);
        row["boundary"] = json!(
            "Python send, durable confirmation, read and full-byte verification; TCP, memory storage; one client, four partitions, window 256"
        );
        row["revision"] = json!(String::from_utf8_lossy(&revision.stdout).trim());
        row["source_diff"] = json!(String::from_utf8_lossy(&diff.stdout));
        rows.push(row);
    }
    let final_diff = Command::new("git").args(["diff", "HEAD"]).output()?;
    if final_diff.stdout != diff.stdout
        || std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/examples/workload.py"))? != source
    {
        return Err("source changed during benchmark; discarding measurements".into());
    }
    if args.profile.is_none() {
        let cache = PathBuf::from(std::env::var("HOME")?).join(".cache/ozzy");
        std::fs::create_dir_all(&cache)?;
        let mut ledger = OpenOptions::new()
            .create(true)
            .append(true)
            .open(cache.join("python-sdk.jsonl"))?;
        for row in &rows {
            writeln!(ledger, "{row}")?;
        }
    }
    for row in &mut rows {
        row.as_object_mut().unwrap().remove("source_diff");
        println!("{row}");
    }
    Ok(())
}
