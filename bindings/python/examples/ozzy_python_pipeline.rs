//! Concurrent producer, broker, and verified consumer processes over TCP.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use clap::Parser;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs::OpenOptions,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[path = "../tests/support/mod.rs"]
mod support;
type Error = Box<dyn std::error::Error>;

#[derive(Debug, Parser)]
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
    #[arg(long, default_value = "0,1")]
    broker_cpus: String,
    #[arg(long, default_value = "2,3")]
    producer_cpus: String,
    #[arg(long, default_value_t = 4)]
    consumer_cpu: usize,
    #[arg(long, default_value_t = 32)]
    reader_queue_messages: usize,
    #[arg(long, default_value_t = 1024)]
    memory_mib: usize,
    #[arg(long, default_value_t = 16)]
    segment_mib: usize,
    #[arg(long, default_value_t = 64)]
    retention_mib: usize,
    #[arg(long, default_value_t = 8192)]
    window: usize,
    #[arg(long, default_value = "flush", value_parser = ["each", "flush"])]
    confirmation: String,
    #[arg(long, default_value = "candidate")]
    label: String,
    #[arg(long)]
    profile: Option<PathBuf>,
    #[arg(long)]
    no_ledger: bool,
    #[arg(long, hide = true)]
    broker_child: bool,
}

struct Worker {
    child: Child,
    input: ChildStdin,
    readers: Vec<thread::JoinHandle<()>>,
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}
impl Worker {
    fn spawn(
        role: &str,
        command: &mut Command,
        events: &Sender<(String, Value)>,
    ) -> Result<Self, Error> {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let input = child.stdin.take().ok_or("missing stdin")?;
        let mut readers = Vec::new();
        for (stderr, pipe) in [
            (
                false,
                Box::new(child.stdout.take().unwrap()) as Box<dyn std::io::Read + Send>,
            ),
            (true, Box::new(child.stderr.take().unwrap())),
        ] {
            let (role, events) = (role.to_owned(), events.clone());
            readers.push(thread::spawn(move || {
                for line in BufReader::new(pipe).lines() {
                    let value = match line {
                        Ok(line) if !stderr => serde_json::from_str(&line)
                            .unwrap_or_else(|_| json!({"event":"error", "text":line})),
                        line => json!({"event":"error", "text":format!("{line:?}")}),
                    };
                    if events.send((role.clone(), value)).is_err() {
                        break;
                    }
                }
            }));
        }
        Ok(Self {
            child,
            input,
            readers,
        })
    }
    fn send(&mut self, command: &Value) -> Result<(), Error> {
        writeln!(self.input, "{command}")?;
        self.input.flush()?;
        Ok(())
    }
    fn finish(&mut self) -> Result<(), Error> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait()? {
                if !status.success() {
                    return Err(format!("child failed: {status}").into());
                }
                for reader in self.readers.drain(..) {
                    reader.join().map_err(|_| "output reader panicked")?;
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("child shutdown timed out".into());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn receive(
    events: &Receiver<(String, Value)>,
    workers: &mut BTreeMap<String, Worker>,
) -> Result<(String, Value), Error> {
    let mut deadline = Instant::now() + Duration::from_secs(45);
    let mut progress = Instant::now();
    let mut credits = 0_u64;
    loop {
        match events.recv_timeout(Duration::from_millis(100)) {
            Ok((role, value)) if value["event"] == "error" => {
                let mut details = format!("{role}: {value}\n");
                let collect_until = Instant::now() + Duration::from_millis(100);
                while let Ok((role, value)) = events.recv_timeout(Duration::from_millis(20)) {
                    writeln!(details, "{role}: {value}")?;
                    if Instant::now() >= collect_until {
                        break;
                    }
                }
                return Err(details.into());
            }
            Ok((role, value)) if role == "consumer" && value["event"] == "credit" => {
                let producer = format!("producer{}", value["producer"]);
                workers
                    .get_mut(&producer)
                    .ok_or("unknown producer credit")?
                    .send(&json!({"command":"credit"}))?;
                deadline = Instant::now() + Duration::from_secs(45);
                credits += 1;
                if progress.elapsed() >= Duration::from_secs(30) {
                    eprintln!(
                        "{}",
                        json!({"event":"progress", "credits":credits, "memory":memory(workers)?})
                    );
                    progress = Instant::now();
                }
            }
            Ok(value) => return Ok(value),
            Err(mpsc::RecvTimeoutError::Disconnected) => return Err("event stream closed".into()),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        for (role, worker) in workers.iter_mut() {
            if let Some(status) = worker.child.try_wait()?
                && !status.success()
            {
                return Err(format!("{role} exited: {status}").into());
            }
        }
        if Instant::now() >= deadline {
            return Err("no protocol progress for 45 seconds".into());
        }
    }
}

fn memory(workers: &BTreeMap<String, Worker>) -> Result<BTreeMap<String, Value>, Error> {
    workers
        .iter()
        .map(|(role, worker)| {
            let status = std::fs::read_to_string(format!("/proc/{}/status", worker.child.id()))?;
            let mut fields = BTreeMap::new();
            for line in status.lines() {
                if let Some((key, value)) = line.split_once(':')
                    && matches!(key, "VmRSS" | "VmHWM" | "Threads")
                {
                    fields.insert(key, value.trim());
                }
            }
            Ok((role.clone(), json!(fields)))
        })
        .collect()
}

fn barrier(
    events: &Receiver<(String, Value)>,
    workers: &mut BTreeMap<String, Worker>,
    roles: &[String],
    event: &str,
) -> Result<BTreeMap<String, Value>, Error> {
    let mut pending: BTreeSet<_> = roles.iter().cloned().collect();
    let mut values = BTreeMap::new();
    while !pending.is_empty() {
        let (role, value) = receive(events, workers)?;
        if value["event"] != event || !pending.remove(&role) {
            return Err(format!("unexpected {role}: {value}").into());
        }
        values.insert(role, value);
    }
    Ok(values)
}

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

fn clients(
    args: &Args,
    addresses: &Value,
    workers: &mut BTreeMap<String, Worker>,
    events: &Sender<(String, Value)>,
) -> Result<Vec<String>, Error> {
    let cpus = cpus(&args.producer_cpus)?;
    let mut roles = Vec::new();
    for (i, cpu) in cpus.iter().copied().chain([args.consumer_cpu]).enumerate() {
        let role = if i == cpus.len() {
            "consumer".to_owned()
        } else {
            format!("producer{i}")
        };
        let mut command = Command::new("taskset");
        command
            .args(["-c", &cpu.to_string()])
            .arg(&args.python)
            .arg("-I")
            .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/examples/pipeline.py"))
            .env_remove("PYTHONPATH")
            .env(
                "OZZY_BENCH_OPTIONS",
                json!({
                    "role": if i == cpus.len() { "consumer" } else { "producer" },
                    "producer": i,
                    "producers": cpus.len(),
                    "brokers": addresses,
                    "api": args.api,
                    "codec": args.codec,
                    "size": args.size,
                    "profile": args.profile,
                    "confirmation": args.confirmation,
                    "reader_queue_messages": args.reader_queue_messages,
                    "window": args.window,
                    "warmup": args.warmup,
                    "duration": args.duration,
                })
                .to_string(),
            );
        workers.insert(role.clone(), Worker::spawn(&role, &mut command, events)?);
        roles.push(role);
    }
    Ok(roles)
}

fn run(args: &Args) -> Result<Value, Error> {
    let (send, events) = mpsc::channel();
    let mut workers = start(args, &send)?;
    let ready = barrier(&events, &mut workers, &["broker".into()], "ready")?;
    let roles = clients(args, &ready["broker"]["brokers"], &mut workers, &send)?;
    barrier(&events, &mut workers, &roles, "ready")?;
    let mut result = json!({});
    for (phase, seconds) in [args.warmup, args.duration].into_iter().enumerate() {
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
        if counts.values().any(|v| v["phase"] != phase) {
            return Err("phase mismatch".into());
        }
        let produced: u64 = counts
            .iter()
            .filter(|(r, _)| r.starts_with("producer"))
            .map(|(_, v)| v["records"].as_u64().unwrap())
            .sum();
        let consumed = counts["consumer"]["records"].as_u64().unwrap();
        if produced == 0 || produced != consumed {
            return Err(format!("record count mismatch: {produced} vs {consumed}").into());
        }
        result = json!({"records":produced, "seconds":elapsed, "records_per_second":produced as f64 / elapsed, "payload_bytes_per_second":produced as f64 * args.size as f64 / elapsed, "clients":counts, "window":args.window});
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

fn cpus(value: &str) -> Result<Vec<usize>, Error> {
    Ok(value.split(',').map(str::parse).collect::<Result<_, _>>()?)
}

fn broadcast(
    workers: &mut BTreeMap<String, Worker>,
    roles: &[String],
    value: &Value,
) -> Result<(), Error> {
    for role in roles {
        workers.get_mut(role).unwrap().send(value)?;
    }
    Ok(())
}

fn validate(args: &Args) -> Result<(), Error> {
    let producers = cpus(&args.producer_cpus)?;
    let brokers = cpus(&args.broker_cpus)?;
    let all: Vec<_> = producers
        .iter()
        .chain(&brokers)
        .copied()
        .chain([args.consumer_cpu])
        .collect();
    if all.iter().copied().collect::<BTreeSet<_>>().len() != all.len() {
        return Err("CPU sets overlap".into());
    }
    if !(16..=65536).contains(&args.size)
        || !args.warmup.is_finite()
        || args.warmup <= 0.0
        || args.warmup > 3600.0
        || !args.duration.is_finite()
        || args.duration <= 0.0
        || args.duration > 3600.0
        || args.repetitions == 0
        || !(32..=4096).contains(&args.memory_mib)
        || args.segment_mib == 0
        || args.retention_mib < args.segment_mib
        || args
            .segment_mib
            .saturating_add(args.retention_mib)
            .saturating_mul(8)
            > args.memory_mib
        || !(1..=64).contains(&args.reader_queue_messages)
        || args.window == 0
        || !args.window.is_multiple_of(256)
        || args
            .window
            .saturating_mul(producers.len())
            .saturating_mul(args.size)
            > args.memory_mib * 1024 * 1024 / 8
    {
        return Err("invalid workload limits".into());
    }
    Ok(())
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
            "kind": "python-pipeline",
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
            "source_diff": String::from_utf8_lossy(source),
            "boundary": "Concurrent Python producers, real broker process, Python consumer; TCP, memory storage, four partitions; durable confirmation and full-byte/order verification",
    }))
}

fn main() -> Result<(), Error> {
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
            .open(cache.join("python-sdk.jsonl"))?;
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
