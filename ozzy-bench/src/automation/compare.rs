//! Repeatable comparisons with a fresh, isolated server for every case.
use super::cpus::BrokerCpus;
use super::{
    Result, SSD, cache, capture, check_canceled, install_signals, isolation, json_file, read_json,
    records, root, run_id, server::External, source, supervise, validation,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
/// Build and correctness checks required before comparison runs.
pub enum Checks {
    /// Run the complete qualification checks before measuring.
    Full,
    /// Run formatting and worker build checks for a focused development loop.
    Focused,
}

const PARTITIONS_PER_WRITER_PROCESS: u64 = 2;

#[derive(Debug, Parser)]
#[command(about = "Run serial verified comparisons; append results under ~/.cache/ozzy/")]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent command-line switches"
)]
/// Serial comparison cases, placement, resource limits, and build policy.
pub struct Args {
    /// Three placements: all-local, or two local and one remote broker.
    #[arg(long)]
    pub placements: Option<PathBuf>,
    #[arg(long)]
    /// Reachable TCP control bind for distributed worker orchestration.
    pub control_bind: Option<String>,
    #[arg(long="impl",default_value="all",value_parser=["all","ozzy","iggy","redpanda"])]
    /// Selected implementation, or all implementations in the comparison set.
    pub implementation: String,
    #[arg(long, value_delimiter = ',', default_value = "128,1024,8192")]
    /// Payload byte sizes included in the case matrix.
    pub sizes: Vec<u64>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "durable,replicated-persisting"
    )]
    /// Confirmation and persistence modes included in the case matrix.
    pub modes: Vec<String>,
    /// Events use binary fields at 16 B and OMQ's JSON event stream at larger sizes.
    #[arg(long, value_delimiter = ',', default_value = "events")]
    pub patterns: Vec<String>,
    #[arg(long, default_value_t = 1)]
    /// Independent fresh-server repetitions per case.
    pub repetitions: u64,
    #[arg(long, default_value_t = 10.0)]
    /// Measured submission window in seconds.
    pub duration: f64,
    #[arg(long, default_value_t = 5.0)]
    /// Unmeasured warmup window in seconds.
    pub warmup: f64,
    #[arg(long, default_value_t = 32768)]
    /// Retained disk history capacity per run in MiB.
    pub disk_history_mib: u64,
    #[arg(long, default_value_t = 8)]
    /// Total shared partitions distributed across broker leaders.
    pub partitions: u64,
    /// Native Ozzy readers subscribed to each partition; each verifies every record.
    #[arg(long, default_value_t = 1)]
    pub readers_per_partition: u64,
    /// Ozzy disk groups: readers take live records from one shared publication
    /// per partition instead of their own subscription.
    #[arg(long)]
    pub live_readers: bool,
    /// Owned OMQ I/O threads in each Ozzy broker process; clients keep one.
    #[arg(long)]
    pub broker_io_threads: Option<u64>,
    /// Ozzy writer LZ4 and leader packing; `off` keeps plain payload bytes on
    /// the wire, in replication, and on disk.
    #[arg(long, default_value = "adaptive", value_parser = ["adaptive", "off"])]
    pub payload_compression: String,
    /// Native local shards, independent of broker CPU count; default min(partitions, CPUs).
    #[arg(long)]
    pub shards: Option<u64>,
    /// Records in flight per writer: the native queue capacity and Iggy's
    /// records per request.
    #[arg(long, default_value_t = 1024)]
    pub request_records: u64,
    /// Maximum records per native delivery or Iggy poll.
    #[arg(long, default_value_t = 16384)]
    pub reader_records: u64,
    /// Maximum payload MiB per native delivery or Iggy poll.
    #[arg(long, default_value_t = 128)]
    pub reader_payload_mib: u64,
    /// Native APPEND requests awaiting full confirmation per writer connection.
    #[arg(long, default_value_t = 1)]
    pub writer_inflight_appends: u64,
    /// Native SDK APPEND collection ceiling in KiB; record limits still apply.
    #[arg(long, default_value_t = (crate::native::DEFAULT_SDK_BATCH_TARGET_BYTES / 1024) as u32,
        value_parser = clap::value_parser!(u32).range(1..=16384))]
    pub writer_batch_target_kib: u32,
    /// Native retained APPEND byte budget per application shard, in MiB.
    #[arg(long)]
    pub shard_resident_mib: Option<u64>,
    /// One CPU list shared by all brokers, or one list per broker separated
    /// by `/`. The default gives each broker one physical core.
    #[arg(long, default_value = "0/1/2", value_parser = BrokerCpus::parse)]
    pub broker_cpus: BrokerCpus,
    #[arg(long, value_delimiter = ',', default_value = "3,4,5")]
    /// CPU IDs reserved for benchmark client processes.
    pub client_cpus: Vec<usize>,
    /// Ozzy disk groups write segment groups with `O_DIRECT`.
    #[arg(long)]
    pub direct_io: Option<bool>,
    /// Ozzy disk groups: who writes segment data. `aio` needs `--direct-io true`.
    #[arg(long, value_parser = ["pool", "aio"])]
    pub io_backend: Option<String>,
    /// Ozzy disk groups: kernel AIO data writes in flight per broker device controller.
    #[arg(long)]
    pub aio_depth: Option<usize>,
    /// Physical segment limit; default depends on record size, see `default_segment_mib`.
    #[arg(long)]
    pub segment_mib: Option<u64>,
    #[arg(long)]
    /// Total offered record rate; none selects completion-paced saturation.
    pub records_per_second: Option<u64>,
    /// Continuous offered-load ramp, `RATE:SECONDS,...`; `--duration` must
    /// equal its total. Each stage becomes one result row.
    #[arg(long, conflicts_with = "records_per_second")]
    pub ramp: Option<crate::schedule::Ramp>,
    #[arg(long)]
    /// Reuse existing checked worker and external-server binaries.
    pub no_build: bool,
    /// Development loop: fmt and worker build; run affected tests separately.
    #[arg(long, value_enum, default_value = "full")]
    pub checks: Checks,
    #[arg(long)]
    /// Run qualification checks without launching measured cases.
    pub check_only: bool,
    #[arg(long)]
    /// Emit case configuration without starting servers or workloads.
    pub dry_run: bool,
}

/// A roll pauses background writes. With 1 GiB segments the write backlog drains
/// between rolls, so writers never wait on it. Tiny records keep 256 MiB: a
/// segment's resident record tables grow with its record count, not its bytes.
pub const fn default_segment_mib(record_bytes: u64) -> u64 {
    if record_bytes >= 1024 { 1024 } else { 256 }
}

impl Args {
    fn local_shards(&self) -> u64 {
        self.shards
            .unwrap_or(self.partitions.min(self.broker_cpus.pool().len() as u64))
    }

    /// Reader connections, each in its own process.
    const fn readers(&self) -> u64 {
        self.partitions.saturating_mul(self.readers_per_partition)
    }

    const fn writer_processes(&self) -> u64 {
        self.partitions.div_ceil(PARTITIONS_PER_WRITER_PROCESS)
    }

    /// Text patterns need room after the 8 B submission clock.
    fn patterns_valid(&self) -> bool {
        let known = ["events", "json", "random", "structured"];
        !self.patterns.is_empty()
            && self.patterns.iter().all(|p| known.contains(&p.as_str()))
            && !(self.sizes.contains(&16) && self.patterns.iter().any(|p| p == "json"))
    }

    fn validate_readers(&self) -> Result<()> {
        // One broker group admits at most 32 preprovisioned readers.
        if !(1..=32).contains(&self.readers())
            || (self.readers_per_partition != 1 && self.implementation != "ozzy")
        {
            return Err("several readers per partition require Ozzy and at most 32 readers".into());
        }
        if self
            .broker_io_threads
            .is_some_and(|threads| !(1..=32).contains(&threads) || self.implementation != "ozzy")
        {
            return Err("broker I/O threads require Ozzy and 1..=32".into());
        }
        if self.payload_compression != "adaptive" && self.implementation != "ozzy" {
            return Err("payload compression control requires Ozzy".into());
        }
        if self.live_readers
            && (self.implementation != "ozzy"
                || self
                    .modes
                    .iter()
                    .any(|m| !matches!(m.as_str(), "replicated-persisting" | "disk-quorum")))
        {
            return Err("live readers require Ozzy replicated-persisting or disk-quorum".into());
        }
        Ok(())
    }

    fn validate_shards(&self) -> Result<()> {
        if self.shards.is_some_and(|shards| {
            shards == 0
                || shards > self.partitions
                || self.implementation != "ozzy"
                || self.modes.iter().any(|mode| mode != "durable")
        }) {
            return Err("shard override requires native durable mode and 1..=partitions".into());
        }
        Ok(())
    }

    fn validate_placements(&self) -> Result<()> {
        if let Some(path) = &self.placements {
            let placements = crate::placement::Placement::load(Some(path))?;
            let pool = self.broker_cpus.pool();
            let all_local = placements.iter().all(|p| p.remote.is_none());
            let shared_pool =
                all_local && placements.iter().all(|p| p.cpus.as_ref() == Some(&pool));
            let local = if all_local {
                &placements[..]
            } else {
                &placements[..2]
            };
            if placements[..2].iter().any(|p| p.remote.is_some())
                || (!all_local && placements[2].remote.is_none())
                || placements.iter().any(|p| {
                    // Cross-host brokers keep one CPU. A local broker may own
                    // several, such as both threads of one physical core.
                    p.storage_dir.is_none()
                        || p.cpus.as_ref().is_none_or(|c| {
                            !shared_pool
                                && (c.is_empty()
                                    || (!all_local && c.len() != 1)
                                    || (all_local && c.iter().any(|cpu| !pool.contains(cpu))))
                        })
                })
                || (!all_local && placements[2].cpus.as_ref().unwrap().contains(&5))
                || (!shared_pool
                    && local.iter().enumerate().any(|(i, p)| {
                        local[..i].iter().any(|q| {
                            let taken = q.cpus.as_ref().unwrap();
                            p.cpus.as_ref().unwrap().iter().any(|c| taken.contains(c))
                        })
                    }))
                || local.iter().any(|p| {
                    p.cpus
                        .as_ref()
                        .unwrap()
                        .iter()
                        .any(|c| self.client_cpus.contains(c))
                })
                || self
                    .modes
                    .iter()
                    .any(|m| !matches!(m.as_str(), "replicated-persisting" | "disk-quorum"))
                || self.implementation == "redpanda"
                || self
                    .control_bind
                    .as_ref()
                    .is_none_or(|s| !s.starts_with("tcp://"))
            {
                return Err("placements require all-local or two local/one remote brokers, disjoint local broker CPU sets (one CPU each across hosts) or one all-local broker CPU pool, explicit storage, replicated-persisting or disk-quorum, and TCP control".into());
            }
        } else if self.control_bind.is_some() {
            return Err("control bind needs placements".into());
        }
        Ok(())
    }

    /// Reject unsupported cases, conflicting options, and invalid placement or capacity.
    pub fn validate(&self) -> Result<()> {
        self.validate_shards()?;
        self.validate_placements()?;
        let allowed = isolation::cpus(None)?;
        if self.sizes.is_empty()
            || self
                .sizes
                .iter()
                .any(|s| *s != 16 && !(32..=16384).contains(s))
            || self.modes.is_empty()
            || self.modes.iter().any(|m| {
                !matches!(
                    m.as_str(),
                    "buffered" | "durable" | "disk-quorum" | "replicated-persisting"
                )
            })
            || !self.patterns_valid()
        {
            return Err("invalid case selection".into());
        }
        if !(1..=30).contains(&self.repetitions)
            || !(1..=32).contains(&self.partitions)
            || !(1..=65536).contains(&self.request_records)
            || !(1..=65536).contains(&self.reader_records)
            || !(1..=128).contains(&self.reader_payload_mib)
            || !(1..=65536).contains(&self.writer_inflight_appends)
            || self.shard_resident_mib.is_some_and(|mib| {
                !(1..=16384).contains(&mib)
                    || matches!(self.implementation.as_str(), "iggy" | "redpanda")
            })
            || !(0.05..=120.0).contains(&self.duration)
            || !(0.0..=10.0).contains(&self.warmup)
            || !(4..=65536).contains(&self.disk_history_mib)
            || !(4..=1024).contains(&self.segment_mib.unwrap_or(4))
        {
            return Err("invalid bounded workload".into());
        }
        self.validate_readers()?;
        let pool = self.broker_cpus.pool();
        // A shared pool is one CPU range; per-broker lists may pair
        // hyperthread siblings such as 0 and 6.
        if self.client_cpus.is_empty()
            || (!self.broker_cpus.per_broker()
                && pool.windows(2).any(|w| w[1] != w[0] + 1)
                && !(self.placements.is_some() && self.implementation == "ozzy"))
            || pool.iter().any(|c| self.client_cpus.contains(c))
            || pool
                .iter()
                .chain(&self.client_cpus)
                .any(|c| !allowed.contains(c))
        {
            return Err("invalid CPU placement".into());
        }
        if self.records_per_second == Some(0) {
            return Err("offered load must be positive".into());
        }
        if let Some(ramp) = &self.ramp
            && ((self.duration * 1e9).round() as u64 != ramp.duration_ns() || self.check_only)
        {
            return Err("--duration must equal the ramp total".into());
        }
        if self.cases().is_empty() {
            return Err("selected implementation has no cases in these modes".into());
        }
        if ["OZZY_BENCH_PROFILE", "OZZY_BENCH_HEAPTRACK"]
            .iter()
            .any(|k| std::env::var_os(k).is_some())
        {
            return Err("profiling must run separately from comparisons".into());
        }
        Ok(())
    }
    /// Serialize the complete comparison configuration for result provenance.
    pub fn configuration(&self) -> Value {
        let mut config = json!({"impl":self.implementation,"sizes":self.sizes,"modes":self.modes,"codecs":["raw"],"patterns":self.patterns,"repetitions":self.repetitions,"duration":self.duration,"warmup":self.warmup,"disk_history_mib":self.disk_history_mib,"partitions":self.partitions,"request_records":self.request_records,"reader_records":self.reader_records,"reader_payload_mib":self.reader_payload_mib,"writer_inflight_appends":self.writer_inflight_appends,"compression_workers":0,"broker_cpus":self.broker_cpus.pool(),"broker_cpu_sets":self.broker_cpus.sets(),"client_cpus":self.client_cpus,"durable_segment_io":"odsync","segment_mib":self.segment_mib,"records_per_second":self.records_per_second,"ramp":self.ramp.as_ref().map(ToString::to_string),"no_build":self.no_build,"check_only":self.check_only});
        if self.payload_compression == "off" {
            config["payload_compression"] = json!("none");
            config["payload_compression_threshold"] = Value::Null;
        } else {
            config["payload_compression"] = json!("adaptive-lz4");
            config["payload_compression_threshold"] =
                json!(ozzy_runtime::replicated::PAYLOAD_COMPRESSION_THRESHOLD);
        }
        config["native_reader_api"] = json!("decoded-records");
        if let Some(path) = &self.placements {
            config["deployment"] = read_json(path).expect("validated placement file");
            config["deployment_kind"] = json!(if config["deployment"]
                .as_array()
                .unwrap()
                .iter()
                .all(|p| p.get("ssh").is_none())
            {
                "all-local"
            } else {
                "two-local-one-remote"
            });
        }
        if let Some(shards) = self.shards {
            config["native_shards"] = json!(shards);
        }
        config["native_batch_target_bytes"] = json!(u64::from(self.writer_batch_target_kib) * 1024);
        config["writer_batching"] = json!(true);
        config["writer_protocol"] = json!("peer-appends");
        if self.readers_per_partition != 1 {
            config["readers_per_partition"] = json!(self.readers_per_partition);
        }
        if let Some(threads) = self.broker_io_threads {
            config["broker_io_threads"] = json!(threads);
        }
        if self.live_readers {
            config["live_readers"] = json!(true);
        }
        config["native_max_record_bytes"] = json!(1024 * 1024);
        if let Some(direct) = self.direct_io {
            config["disk_direct_io"] = json!(direct);
        }
        if let Some(backend) = &self.io_backend {
            config["disk_io_backend"] = json!(backend);
        }
        if let Some(depth) = self.aio_depth {
            config["disk_aio_depth"] = json!(depth);
        }
        if let Some(mib) = self.shard_resident_mib {
            config["shard_resident_mib"] = json!(mib);
        }
        config
    }

    fn request_records_for(&self, _case: &Value) -> u64 {
        self.request_records
    }

    /// Actual per-implementation controls, including the explicit Iggy ceiling.
    pub fn case_configuration(&self, case: &Value) -> Value {
        let mut config = self.configuration();
        config["request_records"] = json!(self.request_records_for(case));
        if case["impl"] != "ozzy" {
            config["payload_compression"] = json!("none");
            config["payload_compression_threshold"] = Value::Null;
        }
        config
    }
    fn case(&self, implementation: &str, mode: &str, size: u64, pattern: &str) -> Value {
        let mut case = json!({"impl":implementation,"mode":mode,"size":size,"codec":"raw","pattern":pattern,"rate":self.records_per_second});
        if let Some(ramp) = &self.ramp {
            case["ramp"] = json!(ramp.to_string());
        }
        case
    }
    /// Expand modes, sizes, payload patterns, and repetitions into serial cases.
    pub fn cases(&self) -> Vec<Value> {
        let mut cases = vec![];
        for mode in &self.modes {
            for size in &self.sizes {
                for pattern in &self.patterns {
                    if matches!(self.implementation.as_str(), "all" | "iggy") {
                        cases.push(self.case("iggy", mode, *size, pattern));
                    }
                    if matches!(self.implementation.as_str(), "all" | "redpanda") {
                        cases.push(self.case("redpanda", mode, *size, pattern));
                    }
                    if mode != "buffered" && matches!(self.implementation.as_str(), "all" | "ozzy")
                    {
                        cases.push(self.case("ozzy", mode, *size, pattern));
                    }
                }
            }
        }
        cases
    }
    #[expect(
        clippy::too_many_lines,
        reason = "One auditable table of benchmark arguments"
    )]
    /// Build the worker command for one case with its storage and endpoint bindings.
    pub fn command(
        &self,
        binary: &Path,
        case: &Value,
        endpoint: Option<&str>,
    ) -> Result<Vec<String>> {
        let mode = case["mode"].as_str().ok_or("missing mode")?;
        let external = case["impl"] != "ozzy";
        if !external && mode == "buffered" {
            return Err("Ozzy buffered mode is no longer a live comparison".into());
        }
        let cluster = super::cluster_mode(mode);
        let size = case["size"].as_u64().ok_or("missing size")?;
        let batch = self.request_records_for(case);
        let cpus = self.client_cpus.clone();
        let mut command = vec![
            "taskset".into(),
            "-c".into(),
            cpus.iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(","),
            binary.display().to_string(),
            "--processes".into(),
            "--network-ingress".into(),
            "--streaming".into(),
        ];
        let mut add = |flag: &str, value: String| command.extend([flag.into(), value]);
        add(
            if external {
                "--external-policy"
            } else {
                "--system"
            },
            if external || cluster {
                mode.to_owned()
            } else {
                format!("single-{mode}")
            },
        );
        for (flag, value) in [
            ("--record-bytes", size.to_string()),
            ("--window", self.partitions.to_string()),
            ("--request-records", batch.to_string()),
            ("--reader-records", self.reader_records.to_string()),
            ("--reader-payload-mib", self.reader_payload_mib.to_string()),
            (
                "--writer-batch-records",
                batch
                    .min(ozzy_runtime::replicated::MAX_APPEND_RECORDS as u64)
                    .to_string(),
            ),
            // Each writer process owns two partition-targeted SDK writers.
            ("--producer-workers", self.writer_processes().to_string()),
            ("--reader-workers", self.readers().to_string()),
            (
                "--readers-per-partition",
                self.readers_per_partition.to_string(),
            ),
            ("--duration", self.duration.to_string()),
            ("--warmup", self.warmup.to_string()),
            (
                "--segment-mib",
                self.segment_mib
                    .unwrap_or_else(|| default_segment_mib(size))
                    .to_string(),
            ),
            ("--storage-dir", format!("{SSD}/ozzy-bench")),
        ] {
            add(flag, value);
        }
        if external {
            add("--history-mib", self.disk_history_mib.to_string());
            add("--history-operations", 262_144.to_string());
            add(
                "--segment-decoded-mib",
                default_segment_mib(size).to_string(),
            );
        }
        add("--partitions", self.partitions.to_string());
        if external {
            add(
                "--external-system",
                case["impl"]
                    .as_str()
                    .ok_or("missing implementation")?
                    .into(),
            );
            add("--external-storage-dir", format!("{SSD}/ozzy-bench"));
            add(
                "--external-endpoint",
                endpoint.ok_or("missing external endpoint")?.into(),
            );
        } else {
            if let Some(threads) = self.broker_io_threads {
                add("--broker-io-threads", threads.to_string());
            }
            if let Some(path) = &self.placements {
                add("--placements", path.display().to_string());
                add(
                    "--control-bind",
                    self.control_bind.clone().expect("validated control bind"),
                );
            }
            add(
                "--writer-inflight-appends",
                self.writer_inflight_appends.to_string(),
            );
            add(
                "--writer-batch-target-kib",
                self.writer_batch_target_kib.to_string(),
            );
            if let Some(mib) = self.shard_resident_mib {
                add("--shard-resident-mib", mib.to_string());
            }
            if let Some(direct) = self.direct_io {
                add("--direct-io", direct.to_string());
            }
            if let Some(backend) = &self.io_backend {
                add("--io-backend", backend.clone());
            }
            if let Some(depth) = self.aio_depth {
                add("--aio-depth", depth.to_string());
            }
            if !cluster {
                let owners = self.local_shards();
                add("--app-threads", owners.to_string());
            }
        }
        if let Some(ramp) = &self.ramp {
            add("--ramp", ramp.to_string());
        }
        if let Some(rate) = self.records_per_second {
            add("--records-per-second", rate.to_string());
        }
        if !external && !cluster {
            command.push("--balanced-partitions".into());
        }
        if !external && self.live_readers {
            command.push("--live-readers".into());
        }
        if !external && self.payload_compression == "off" {
            command.extend(["--payload-compression".into(), "off".into()]);
        }
        match case["pattern"].as_str() {
            Some("events") if size == 16 => command.push("--binary-payload".into()),
            Some("events" | "json") => command.push("--json-payload".into()),
            Some("random") => command.push("--random-payload".into()),
            _ => (),
        }
        Ok(command)
    }
}

fn build(args: &Args, directory: &Path) -> Result<PathBuf> {
    let binary = super::worker_binary();
    let stamp = super::artifacts().join("comparison-build.json");
    let identity = source::identity(&root())?;
    if args.no_build {
        let saved = read_json(&stamp)?;
        let inputs_match = if saved["worker_inputs"].is_null() {
            saved["source"] == identity
        } else {
            saved["worker_inputs"] == source::worker_identity(&root())?
        };
        if !inputs_match || saved["executable_sha256"] != source::sha256(&binary)? {
            return Err("build stamp differs; rerun without --no-build".into());
        }
        if saved["worker_inputs"].is_null() {
            let mut saved = saved;
            saved["worker_inputs"] = source::worker_identity(&root())?;
            saved["workload_sha256"] = json!(source::workload_fingerprint(&root())?);
            json_file(&stamp, &saved)?;
        }
        return Ok(binary);
    }
    let mut commands = vec![
        vec!["fmt", "--all", "--", "--check"],
        vec![
            "clippy",
            "--locked",
            "-p",
            "ozzy-bench",
            "--features",
            "comparisons",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
        vec![
            "test",
            "--locked",
            "-p",
            "ozzy-bench",
            "--features",
            "comparisons",
            "--lib",
            "--bins",
            "--test",
            "ozzy_automation",
        ],
        vec![
            "build",
            "--locked",
            "--release",
            "-p",
            "ozzy-bench",
            "--features",
            "comparisons",
            "--bin",
            "ozy_timed_bench",
            "--bin",
            "ozy_host",
        ],
    ];
    if args.checks == Checks::Focused {
        commands.drain(1..3);
    }
    for (index, args) in commands.iter().enumerate() {
        check_canceled()?;
        println!("CHECK cargo {}", args.join(" "));
        let output = Command::new(root().join("scripts/ozzy_cargo"))
            .args(args)
            .current_dir(root())
            .env("CARGO_TARGET_DIR", super::build_target(&root()))
            .env("TMPDIR", SSD)
            .output()?;
        let log = [output.stdout, output.stderr].concat();
        fs::write(directory.join(format!("build-{index}.log")), &log)?;
        print!("{}", String::from_utf8_lossy(&log));
        if !output.status.success() || String::from_utf8_lossy(&log).contains("warning:") {
            return Err("build/format/lint gate failed".into());
        }
    }
    source::require_unchanged(&root(), &identity)?;
    json_file(
        &stamp,
        &json!({"source":identity,"focused_checks":args.checks == Checks::Focused,"worker_inputs":source::worker_identity(&root())?,
            "workload_sha256":source::workload_fingerprint(&root())?,
            "executable_sha256":source::sha256(&binary)?}),
    )?;
    Ok(binary)
}

fn host() -> Result<Value> {
    let mut host = serde_json::Map::new();
    for (name, flag) in [
        ("system", "-s"),
        ("node", "-n"),
        ("release", "-r"),
        ("version", "-v"),
        ("machine", "-m"),
        ("processor", "-p"),
    ] {
        host.insert(
            name.into(),
            json!(capture(Command::new("uname").arg(flag))?),
        );
    }
    Ok(json!(host))
}

fn manifest(args: &Args, id: &str, binary: &Path) -> Result<Value> {
    let fixture_source = source::identity(&root())?;
    let build = read_json(&super::artifacts().join("comparison-build.json"))?;
    let source = &build["source"];
    let host = host()?;
    let cpu = fs::read_to_string("/proc/cpuinfo")?;
    let memory = fs::read_to_string("/proc/meminfo")?;
    let dependencies = source["checkouts"]
        .as_array()
        .ok_or("missing checkouts")?
        .iter()
        .filter(|c| c["root"] != root().to_string_lossy().as_ref())
        .cloned()
        .collect::<Vec<_>>();
    let cpu_lines = cpu
        .lines()
        .filter(|line| {
            line.split_once(':').is_some_and(|(name, _)| {
                matches!(
                    name.trim(),
                    "processor" | "model name" | "cpu cores" | "siblings" | "flags"
                )
            })
        })
        .collect::<Vec<_>>();
    Ok(
        json!({"run_id":id,"source":source,"focused_checks":build.get("focused_checks").and_then(Value::as_bool).unwrap_or(false),"fixture_source":fixture_source,"executable":binary,"executable_sha256":source::sha256(binary)?,"host":host,"cpuinfo":cpu,"memory":memory,"filesystem":capture(Command::new("findmnt").args(["-T",SSD]))?,"arguments":args.configuration(),"workload_sha256":build["workload_sha256"],"environment":{"host":host,"cpu":cpu_lines,"memory":memory.lines().find(|l|l.starts_with("MemTotal:")).ok_or("missing MemTotal")?,"filesystem":capture(Command::new("findmnt").args(["-n","-o","SOURCE,FSTYPE,OPTIONS","-T",SSD]))?,"dependencies":dependencies}}),
    )
}

impl Args {
    /// Native brokers get explicit process masks; clients inherit their own pool.
    pub fn case_command(
        &self,
        binary: &Path,
        case: &Value,
        directory: &Path,
        endpoint: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut command = self.command(binary, case, endpoint)?;
        if case["impl"] == "ozzy" && self.placements.is_none() {
            let brokers = if super::cluster_mode(case["mode"].as_str().ok_or("missing mode")?) {
                3
            } else {
                1
            };
            let cpus = self.broker_cpus.brokers(brokers);
            let placements: Vec<_> = (0..3)
                .map(|index| {
                    json!({"bind":"127.0.0.1", "storage_dir":format!("{SSD}/ozzy-bench"),
                    "cpus":cpus[index.min(brokers - 1)]})
                })
                .collect();
            let path = directory.join("broker-placement.json");
            json_file(&path, &json!(placements))?;
            command.extend(["--placements".into(), path.display().to_string()]);
        }
        Ok(command)
    }
}

async fn case(
    args: &Args,
    binary: &Path,
    case: &Value,
    directory: &Path,
    server: Option<&External>,
) -> Result<Value> {
    let external = case["impl"] != "ozzy";
    if external {
        json_file(
            &directory.join("server-affinity-before.json"),
            &server.unwrap().affinity(false)?,
        )?;
    }
    let cmd = args.case_command(binary, case, directory, server.map(External::endpoint))?;
    json_file(&directory.join("command.json"), &json!(cmd))?;
    let pids = server.map_or_else(Vec::new, External::pids);
    let storage = super::device::Snapshot::take(Path::new(SSD))?;
    let before = supervise::CpuSnapshot::take_servers(&pids)?;
    let mut guard = isolation::Guard::new(case["impl"].as_str().unwrap(), None).with_servers(&pids);
    if let Some(path) = &args.placements {
        let placements = crate::placement::Placement::load(Some(path))?;
        if let Some(remote) = placements.iter().find(|p| p.remote.is_some()) {
            guard.set_remote(super::distributed::Audit::start(
                remote,
                case["impl"].as_str().unwrap(),
                directory,
            )?);
        }
    }
    let raw = supervise::execute(
        &cmd,
        directory,
        Duration::from_secs(180),
        &std::collections::BTreeMap::new(),
        server.map(External::monitor),
        Some(&mut guard),
    )
    .await?;
    let remote_audit = guard.finish_remote()?;
    let storage = storage.finish()?;
    let cpu = before.elapsed(&supervise::CpuSnapshot::take_servers(&pids)?)?;
    if let Some(server) = server {
        json_file(
            &directory.join("server-affinity-after.json"),
            &server.affinity(false)?,
        )?;
    }
    Ok(
        json!({"case":case,"measurements":validation::comparison(case,&raw,&args.case_configuration(case))?,"execution_cpu":cpu,"device_io":storage,"raw":raw,"process_isolation":guard.report()?,"remote_process_isolation":remote_audit}),
    )
}

/// A ramp case becomes one ledger row per stage, keyed by its offered rate.
/// Stages the ramp could not sustain keep only their failure.
fn stage_rows(row: Value) -> Result<Vec<Value>> {
    let Some(stages) = row["measurements"].get("stages").cloned() else {
        return Ok(vec![row]);
    };
    stages
        .as_array()
        .ok_or("invalid ramp stages")?
        .iter()
        .map(|stage| {
            let mut row = row.clone();
            row["case"]["rate"] = stage["rate"].clone();
            if let Some(reason) = stage.get("failure") {
                row["failure"] = reason.clone();
                row["measurements"] = json!({});
            } else {
                row["measurements"] = stage["measurements"].clone();
            }
            Ok(row)
        })
        .collect()
}

#[expect(
    clippy::too_many_lines,
    reason = "serial fixture lifecycle and result publication stay in one auditable sequence"
)]
async fn matrix(
    args: &Args,
    binary: &Path,
    manifest: &Value,
    directory: &Path,
) -> Result<Vec<Value>> {
    let cases = args.cases();
    let placements = args
        .placements
        .as_deref()
        .map(|path| crate::placement::Placement::load(Some(path)))
        .transpose()?;
    let mut rows = vec![];
    let id = manifest["run_id"].as_str().unwrap();
    println!(
        "RUN {id}: {} cases; results {}",
        cases.len() * args.repetitions as usize,
        directory.display()
    );
    for repetition in 1..=args.repetitions {
        let mut order: Vec<_> = (0..cases.len()).collect();
        if repetition % 2 == 0 {
            order.reverse();
        }
        for (index, case_index) in order.into_iter().enumerate() {
            check_canceled()?;
            source::require_unchanged(&root(), &manifest["fixture_source"])?;
            if source::sha256(binary)? != manifest["executable_sha256"] {
                return Err("benchmark executable changed".into());
            }
            isolation::require_idle()?;
            if let Some(placements) = &placements {
                super::distributed::idle(placements)?;
            }
            capture(Command::new("sync").args(["-f", SSD]))?;
            let settle = super::device::settle(Path::new(SSD)).await?;
            let selected = &cases[case_index];
            let implementation = selected["impl"].as_str().unwrap();
            let name = format!(
                "r{repetition}-{index}-{implementation}-{}-{}-{}",
                selected["mode"].as_str().unwrap(),
                selected["size"],
                selected["codec"].as_str().unwrap()
            );
            let case_dir = directory.join(&name);
            fs::create_dir(&case_dir)?;
            let server_dir = directory.join(format!("{implementation}-{repetition}-{index}-{id}"));
            let mut server = if implementation == "ozzy" {
                None
            } else if let Some(placements) = &placements {
                Some(External::distributed(&server_dir, placements)?)
            } else {
                let mode = selected["mode"].as_str().unwrap();
                let brokers = if super::cluster_mode(mode) { 3 } else { 1 };
                Some(External::start(
                    implementation,
                    &server_dir,
                    mode,
                    &args.broker_cpus.brokers(brokers),
                )?)
            };
            capture(Command::new("sync").args(["-f", SSD]))?;
            if let Some(placements) = &placements {
                for p in placements {
                    super::distributed::run(
                        p,
                        &format!(
                            "sync -f {}",
                            super::distributed::quote(
                                &p.storage_dir.as_ref().unwrap().to_string_lossy()
                            )
                        ),
                    )?;
                }
            }
            println!("CASE {name}");
            let measured = case(args, binary, selected, &case_dir, server.as_ref()).await;
            let identity = server.as_ref().map(External::identity).transpose()?;
            let stopped = server.as_mut().map(External::stop).transpose();
            let mut row = measured?;
            stopped?;
            let after = isolation::require_idle()?;
            if let Some(placements) = &placements {
                row["remote_after_shutdown"] = super::distributed::idle(placements)?;
            }
            source::require_unchanged(&root(), &manifest["fixture_source"])?;
            if source::sha256(binary)? != manifest["executable_sha256"] {
                return Err("benchmark executable changed during measurement".into());
            }
            for key in [
                "source",
                "fixture_source",
                "executable_sha256",
                "host",
                "workload_sha256",
                "environment",
            ] {
                row[key] = manifest[key].clone();
            }
            row["kind"] = json!("measurement");
            row["device_settle"] = settle;
            row["run_id"] = json!(id);
            row["repetition"] = json!(repetition);
            row["configuration"] = args.case_configuration(selected);
            row["artifact_dir"] = json!(case_dir);
            row["manifest"] = json!(directory.join("manifest.json"));
            row["recorded_at"] = json!(capture(
                Command::new("date").args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
            )?);
            row["server_identity"] = identity.unwrap_or(Value::Null);
            row["server_manifest"] = server
                .as_ref()
                .map_or(Value::Null, |s| json!(s.root().join("inspect.json")));
            row["process_isolation"]["after_shutdown"] = after;
            for row in stage_rows(row)? {
                records::append(&cache(), implementation, &row)?;
                println!(
                    "VALID {}",
                    json!({"case":row["case"],"measurements":row["measurements"],"failure":row.get("failure")})
                );
                rows.push(row);
            }
        }
    }
    source::require_unchanged(&root(), &manifest["fixture_source"])?;
    Ok(rows)
}

/// Qualify binaries, run fresh isolated servers serially, and append verified result rows.
pub async fn run(mut args: Args) -> Result<()> {
    install_signals()?;
    args.validate()?;
    if args.dry_run {
        for case in args.cases() {
            println!(
                "{}",
                json!(args.command(&super::worker_binary(), &case, Some("127.0.0.1:1234"))?)
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
    if capture(Command::new("findmnt").args(["-n", "-o", "TARGET", "-T", SSD]))? != "/mnt/ssd" {
        return Err("external SSD is not mounted".into());
    }
    isolation::require_idle()?;
    let id = run_id()?;
    let directory = super::artifacts().join("runs").join(&id);
    fs::create_dir_all(&directory)?;
    if let Some(path) = &args.placements {
        args.placements = Some(super::distributed::freeze(path, &directory)?);
    }
    let binary = build(&args, &directory)?;
    if args.cases().iter().any(|case| case["impl"] == "iggy") {
        super::server::prepare(args.no_build, &directory)?;
    }
    if args.cases().iter().any(|case| case["impl"] == "redpanda") {
        super::server::redpanda::prepare(args.no_build)?;
    }
    if args.check_only {
        return Ok(());
    }
    let mut manifest = manifest(&args, &id, &binary)?;
    if let Some(path) = &args.placements {
        let placements = crate::placement::Placement::load(Some(path))?;
        super::distributed::deploy(
            &placements,
            &binary,
            args.cases().iter().any(|c| c["impl"] == "iggy"),
        )?;
        super::distributed::idle(&placements)?;
        manifest["environment"]["deployment"] = super::distributed::environment(&placements)?;
    }
    json_file(&directory.join("manifest.json"), &manifest)?;
    isolation::pin(None, &args.client_cpus)?;
    let implementations = args
        .cases()
        .iter()
        .filter_map(|c| c["impl"].as_str().map(str::to_owned))
        .collect::<BTreeSet<_>>();
    let rows = match matrix(&args, &binary, &manifest, &directory).await {
        Ok(rows) => rows,
        Err(error) => {
            for implementation in &implementations {
                records::append(
                    &cache(),
                    implementation,
                    &json!({"kind":"run-failed","run_id":id,"reason":error.to_string()}),
                )?;
            }
            fs::write(directory.join("FAILED"), error.to_string())?;
            return Err(error);
        }
    };
    for implementation in implementations {
        records::append(
            &cache(),
            &implementation,
            &json!({"kind":"run-complete","run_id":id}),
        )?;
    }
    json_file(&directory.join("summary.json"), &records::summarize(&rows)?)?;
    println!("COMPLETE {id}");
    Ok(())
}
