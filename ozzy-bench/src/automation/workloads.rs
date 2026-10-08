//! General native/externally managed workload sweeps, sharing strict supervision.
use super::{
    Result, artifact_root, cache, capture, check_canceled, install_signals, isolation, json_file,
    records, root, run_id, source, supervise, validation,
};
use clap::Parser;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

const NATIVE: &[&str] = &["single-durable", "disk-quorum", "replicated-persisting"];
const EXTERNAL: &[&str] = &[
    "kafka-buffered",
    "redpanda-buffered",
    "redpanda-durable",
    "iggy-buffered",
    "iggy-durable",
];
fn disk(profile: &str) -> bool {
    matches!(
        profile,
        "single-durable" | "disk-quorum" | "replicated-persisting"
    )
}

#[derive(Debug, Parser)]
#[command(
    about = "Serial verified workload sweeps; external profiles require one prestarted test server"
)]
/// General native and externally managed workload sweep options.
pub struct Args {
    #[arg(long)]
    /// Explicit workload worker executable; none uses the checkout-specific release worker.
    pub executable: Option<PathBuf>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "single-durable,replicated-persisting"
    )]
    /// Native or external persistence profiles included in the sweep.
    pub profiles: Vec<String>,
    #[arg(long, value_delimiter = ',', default_value = "128")]
    /// Payload byte sizes included in the sweep.
    pub sizes: Vec<u64>,
    #[arg(long, value_delimiter = ',', default_value = "1,4")]
    /// Application worker counts included in the sweep.
    pub workers: Vec<u64>,
    #[arg(long, default_value_t = 4)]
    /// Total producer connections across application workers.
    pub connections: u64,
    #[arg(long, default_value_t = 1024)]
    /// Maximum records retained or submitted per producer request.
    pub request_records: u64,
    #[arg(long, default_value_t = 0)]
    /// Explicit record batch size; zero selects natural batching.
    pub writer_batch_records: u64,
    #[arg(long, default_value_t = 3)]
    /// Maximum outstanding confirmed APPEND requests per connection.
    pub writer_inflight_appends: u64,
    #[arg(long, default_value_t = 3.0)]
    /// Measured submission window in seconds.
    pub duration: f64,
    #[arg(long, default_value_t = 0.25)]
    /// Unmeasured warmup window in seconds.
    pub warmup: f64,
    #[arg(long, default_value_t = 3)]
    /// Independent repetitions per expanded case.
    pub repetitions: u64,
    #[arg(long)]
    /// Consumer process count; none derives a bounded default from connections.
    pub reader_workers: Option<u64>,
    #[arg(long)]
    /// Native application owner count; none derives a bounded default from connections.
    pub app_threads: Option<u64>,
    #[arg(long, value_delimiter = ',', default_value = "64")]
    /// Physical segment capacities included in the sweep, in MiB.
    pub segment_mib: Vec<u64>,
    #[arg(long)]
    /// Optional document describing the three broker hosts and storage roots.
    pub placements: Option<PathBuf>,
    #[arg(long)]
    /// Reachable TCP benchmark-control bind for remote workers.
    pub control_bind: Option<String>,
    #[arg(long, default_value = "127.0.0.1:19092")]
    /// Client endpoint of the externally managed Kafka server.
    pub kafka_endpoint: String,
    #[arg(long, default_value = "127.0.0.1:19094")]
    /// Client endpoint of the externally managed Redpanda server.
    pub redpanda_endpoint: String,
    #[arg(long, default_value = "127.0.0.1:18090")]
    /// Client endpoint of the externally managed Iggy server.
    pub iggy_endpoint: String,
    #[arg(long)]
    /// Optional Kafka container name used for liveness supervision.
    pub kafka_container: Option<String>,
    #[arg(long)]
    /// Optional Redpanda container name used for liveness supervision.
    pub redpanda_container: Option<String>,
    #[arg(long)]
    /// Optional Iggy container name used for liveness supervision.
    pub iggy_container: Option<String>,
    #[arg(long)]
    /// Emit the case matrix without launching measured workers.
    pub dry_run: bool,
}

impl Args {
    fn owners(&self) -> u64 {
        self.app_threads.unwrap_or(self.connections.min(4))
    }
    fn readers(&self) -> u64 {
        self.reader_workers.unwrap_or(self.connections.min(4))
    }
    fn binary(&self) -> PathBuf {
        self.executable.clone().unwrap_or_else(super::worker_binary)
    }
    fn external(&self, profile: &str) -> (&str, Option<&str>) {
        match profile.split('-').next().unwrap_or("") {
            "iggy" => (&self.iggy_endpoint, self.iggy_container.as_deref()),
            "redpanda" => (&self.redpanda_endpoint, self.redpanda_container.as_deref()),
            _ => (&self.kafka_endpoint, self.kafka_container.as_deref()),
        }
    }
    /// Reject unsupported profiles, invalid capacities, and conflicting worker placement.
    pub fn validate(&self) -> Result<()> {
        if std::env::var_os("OZZY_BENCH_AFFINITY").is_some() {
            return Err("workload sweeps do not support legacy affinity placement".into());
        }
        if self.executable.as_ref().is_some_and(|path| !path.is_file()) {
            return Err("explicit workload executable does not exist".into());
        }
        if self.profiles.is_empty()
            || self
                .profiles
                .iter()
                .any(|p| !NATIVE.contains(&p.as_str()) && !EXTERNAL.contains(&p.as_str()))
            || self.sizes.is_empty()
            || self.sizes.iter().any(|s| !matches!(s, 128 | 1024 | 8192))
            || self.workers.is_empty()
            || self
                .workers
                .iter()
                .any(|w| !(1..=self.connections).contains(w))
            || self.segment_mib.is_empty()
        {
            return Err("invalid workload selection".into());
        }
        if !(1..=16).contains(&self.connections)
            || !(1..=65536).contains(&self.request_records)
            || self.writer_batch_records > 65536
            || !(1..=65536).contains(&self.writer_inflight_appends)
            || !(1..=20).contains(&self.repetitions)
            || !(1.0..=30.0).contains(&self.duration)
            || !(0.0..=10.0).contains(&self.warmup)
            || self.segment_mib.iter().any(|s| !(4..=4096).contains(s))
        {
            return Err("invalid bounded workload".into());
        }
        if !(1..=self.connections).contains(&self.owners())
            || !(1..=self.connections).contains(&self.readers())
        {
            return Err("invalid worker or storage bounds".into());
        }
        if self.placements.is_some()
            && (self.profiles.iter().any(|p| EXTERNAL.contains(&p.as_str()))
                || !self
                    .control_bind
                    .as_ref()
                    .is_some_and(|s| s.starts_with("tcp://")))
            || self.control_bind.is_some() && self.placements.is_none()
        {
            return Err("placements need native profiles and reachable TCP control bind".into());
        }
        if !self.dry_run
            && self.profiles.iter().any(|p| EXTERNAL.contains(&p.as_str()))
            && self.profiles.len() != 1
        {
            return Err(
                "prestarted external servers require a single profile; never mix resident brokers"
                    .into(),
            );
        }
        Ok(())
    }
    /// Expand profiles, sizes, worker counts, and segment capacities into serial cases.
    pub fn cases(&self) -> Vec<Value> {
        let mut cases = vec![];
        for profile in &self.profiles {
            for size in &self.sizes {
                for workers in &self.workers {
                    for segment in if disk(profile) {
                        self.segment_mib.clone()
                    } else {
                        vec![0]
                    } {
                        let mut case = json!({"profile":profile,"size":size,"workers":workers,"partitions":self.connections});
                        if !EXTERNAL.contains(&profile.as_str()) {
                            case["app_threads"] = json!(if profile.starts_with("single-") {
                                self.owners()
                            } else {
                                1
                            });
                            case["writer_batch_records"] =
                                json!(if self.writer_batch_records == 0 {
                                    self.request_records
                                } else {
                                    self.writer_batch_records.min(self.request_records)
                                });
                            case["writer_inflight_appends"] = json!(self.writer_inflight_appends);
                        }
                        if segment > 0 {
                            case["segment_mib"] = json!(segment);
                        }
                        cases.push(case);
                    }
                }
            }
        }
        cases
    }
    /// Build workload worker arguments for one expanded case and storage directory.
    pub fn command(&self, case: &Value, storage: &Path) -> Vec<String> {
        let profile = case["profile"].as_str().unwrap();
        let external = EXTERNAL.contains(&profile);
        let mut cmd = vec![
            self.binary().display().to_string(),
            "--processes".into(),
            "--network-ingress".into(),
            "--streaming".into(),
        ];
        let mut add = |flag: &str, value: String| cmd.extend([flag.into(), value]);
        if external {
            add(
                "--external-policy",
                if profile.ends_with("-durable") {
                    "durable"
                } else {
                    "buffered"
                }
                .into(),
            );
        } else {
            add("--system", profile.into());
        }
        for (flag, value) in [
            ("--record-bytes", case["size"].to_string()),
            ("--duration", self.duration.to_string()),
            ("--warmup", self.warmup.to_string()),
            ("--window", self.connections.to_string()),
            ("--producer-workers", case["workers"].to_string()),
            ("--reader-workers", self.readers().to_string()),
            ("--request-records", self.request_records.to_string()),
            ("--storage-dir", storage.display().to_string()),
        ] {
            add(flag, value);
        }
        add("--partitions", case["partitions"].to_string());
        if external {
            add(
                "--external-system",
                profile.split('-').next().unwrap().into(),
            );
            add("--external-endpoint", self.external(profile).0.into());
        } else {
            for key in [
                "app_threads",
                "writer_batch_records",
                "writer_inflight_appends",
                "segment_mib",
            ] {
                if let Some(value) = case.get(key) {
                    add(&format!("--{}", key.replace('_', "-")), value.to_string());
                }
            }
        }
        if let Some(path) = &self.placements {
            add("--placements", path.display().to_string());
            add("--control-bind", self.control_bind.clone().unwrap());
        }
        cmd
    }
}

fn server_identity(args: &Args, profile: &str) -> Result<Option<Value>> {
    if !EXTERNAL.contains(&profile) {
        return Ok(None);
    }
    let name = args
        .external(profile)
        .1
        .ok_or("external profile needs its --*-container")?;
    let info =
        serde_json::from_str::<Value>(&capture(Command::new("podman").args(["inspect", name]))?)?
            [0]
        .clone();
    if info["State"]["Running"] != true || info["State"]["Paused"] == true {
        return Err("external server must be running and unpaused".into());
    }
    let image = info["Config"]["Image"].as_str().unwrap_or("");
    if (profile.starts_with("redpanda-") && !image.contains("redpanda"))
        || (profile.starts_with("kafka-") && image.contains("redpanda"))
    {
        return Err("external server image does not match profile".into());
    }
    if profile.starts_with("iggy-") {
        let version =
            capture(Command::new("podman").args(["exec", name, "iggy-server", "--version"]))?;
        if !version.ends_with(" 0.9.0") {
            return Err("Iggy workload adapter requires server 0.9.0".into());
        }
    }
    Ok(Some(info))
}

async fn execute_matrix(
    args: &Args,
    id: &str,
    directory: &Path,
    identity: &Value,
    hash: &str,
    server: Option<&Value>,
) -> Result<Vec<Value>> {
    let cases = args.cases();
    let mut rows = vec![];
    for repetition in 1..=args.repetitions {
        let mut order = (0..cases.len()).collect::<Vec<_>>();
        if repetition % 2 == 0 {
            order.reverse();
        }
        for index in order {
            check_canceled()?;
            source::require_unchanged(&root(), identity)?;
            if source::sha256(&args.binary())? != hash {
                return Err("workload binary changed".into());
            }
            let case = &cases[index];
            let profile = case["profile"].as_str().unwrap();
            let implementation = if EXTERNAL.contains(&profile) {
                profile.split('-').next().unwrap()
            } else {
                "ozzy"
            };
            let pid = server
                .map(|s| s["State"]["Pid"].as_u64().ok_or("missing server PID"))
                .transpose()?
                .map(u32::try_from)
                .transpose()?;
            let profiled = ["OZZY_BENCH_PROFILE", "OZZY_BENCH_HEAPTRACK"]
                .iter()
                .any(|key| std::env::var_os(key).is_some());
            let mut guard = isolation::Guard::new(implementation, pid).with_profiling(profiled)?;
            if pid.is_some() || profiled {
                guard.check(0)?;
            } else {
                isolation::require_idle()?;
            }
            let case_dir = directory.join(format!("r{repetition}-{index}"));
            fs::create_dir(&case_dir)?;
            let cmd = args.command(case, &artifact_root().join("ozzy-bench"));
            json_file(&case_dir.join("command.json"), &json!(cmd))?;
            capture(Command::new("sync").args(["-f"]).arg(artifact_root()))?;
            let before = supervise::CpuSnapshot::take(pid)?;
            let name = if pid.is_some() {
                args.external(profile).1.map(|s| (s, case_dir.as_path()))
            } else {
                None
            };
            let raw = supervise::execute(
                &cmd,
                &case_dir,
                Duration::from_secs_f64(args.warmup + args.duration + 90.0),
                &std::collections::BTreeMap::new(),
                name.map(|(name, root)| super::server::Monitor::Container(name, root)),
                Some(&mut guard),
            )
            .await?;
            let cpu = before.elapsed(&supervise::CpuSnapshot::take(pid)?)?;
            source::require_unchanged(&root(), identity)?;
            let row = json!({"kind":"measurement","run_id":id,"repetition":repetition,"case":case,"raw":raw,"measurements":validation::measurements(case,&raw)?,"execution_cpu":cpu,"source":identity,"executable_sha256":hash,"artifact_dir":case_dir,"process_isolation":guard.report()?});
            if pid.is_some() || profiled {
                guard.check(0)?;
            } else {
                isolation::require_idle()?;
            }
            records::append(&cache(), implementation, &row)?;
            println!("VALID {}", row["measurements"]);
            rows.push(row);
        }
    }
    Ok(rows)
}

/// Execute supervised cases serially and append verified measurement rows.
pub async fn run(mut args: Args) -> Result<()> {
    install_signals()?;
    args.validate()?;
    if args.dry_run {
        for case in args.cases() {
            println!(
                "{}",
                json!(args.command(&case, &artifact_root().join("ozzy-bench")))
            );
        }
        return Ok(());
    }
    fs::create_dir_all(cache())?;
    fs::create_dir_all(super::artifacts())?;
    let lock = OpenOptions::new()
        .create(true)
        .append(true)
        .open(super::artifacts().join("runner.lock"))?;
    lock.try_lock()?;
    super::require_artifact_disk()?;
    let id = run_id()?;
    let directory = super::artifacts().join("runs").join(&id);
    fs::create_dir_all(&directory)?;
    if let Some(path) = args.placements.as_mut() {
        let frozen = directory.join("placements.json");
        fs::copy(&*path, &frozen)?;
        *path = frozen;
    }
    let server = server_identity(&args, &args.profiles[0])?;
    let identity = source::identity(&root())?;
    let hash = source::sha256(&args.binary())?;
    let manifest = json!({"run_id":id,"source":identity,"executable":args.binary(),"executable_sha256":hash,"external_server":server,"affinity":isolation::cpus(None)?,"cpuinfo":fs::read_to_string("/proc/cpuinfo")?,"memory":fs::read_to_string("/proc/meminfo")?,"command":std::env::args().collect::<Vec<_>>(),"limitations":"placements may put brokers remotely; process audit covers this coordinator only; finite RAM history; confirmation-paced saturation"});
    json_file(&directory.join("manifest.json"), &manifest)?;
    let implementations = args
        .profiles
        .iter()
        .map(|p| {
            if EXTERNAL.contains(&p.as_str()) {
                p.split('-').next().unwrap()
            } else {
                "ozzy"
            }
        })
        .collect::<BTreeSet<_>>();
    let rows = match execute_matrix(&args, &id, &directory, &identity, &hash, server.as_ref()).await
    {
        Ok(rows) => rows,
        Err(error) => {
            for implementation in &implementations {
                records::append(
                    &cache(),
                    implementation,
                    &json!({"kind":"run-failed","run_id":id,"reason":error.to_string()}),
                )?;
            }
            return Err(error);
        }
    };
    for implementation in implementations {
        records::append(
            &cache(),
            implementation,
            &json!({"kind":"run-complete","run_id":id}),
        )?;
    }
    json_file(&directory.join("summary.json"), &records::summarize(&rows)?)?;
    println!("COMPLETE {id}");
    Ok(())
}
