//! Repeatable comparisons with a fresh, isolated server for every case.
mod build;
mod command;
mod config;
mod matrix;
mod validate;
use super::cpus::BrokerCpus;
use super::{Result, cache, install_signals, isolation, json_file, records, run_id};
use clap::Parser;
use serde_json::json;
use std::{collections::BTreeSet, fs, path::PathBuf};

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
    #[command(flatten)]
    /// Deterministic redundant event feeds and partition skew.
    pub feeds: crate::dedup::Feeds,
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
    #[arg(long, value_delimiter = ',', default_value = "durable,disk-quorum")]
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
    /// Independent native consumer groups; zero uses ordinary topic readers.
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=32))]
    pub groups: u32,
    /// Group members hosted on each group's reader process.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..=32))]
    pub group_members: u32,
    /// Individual acknowledgments in flight per source; one is serial.
    #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u32).range(1..=65536))]
    pub ack_window: u32,
    /// Per-partition dedup identity window; requires strict entry and byte limits.
    #[arg(long, requires_all = ["dedup_max_entries", "dedup_max_bytes"])]
    pub dedup_window_secs: Option<u64>,
    /// Live plus pending identities per partition.
    #[arg(long, requires = "dedup_window_secs")]
    pub dedup_max_entries: Option<u64>,
    /// Identity index allocation ceiling per partition.
    #[arg(long, requires = "dedup_window_secs")]
    pub dedup_max_bytes: Option<u64>,
    /// Ozzy holds every record this many milliseconds past its send time.
    /// The case drain grows by the delay.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=100_000))]
    pub delay_millis: Option<u64>,
    /// Held records per partition before admission backs off.
    #[arg(long, requires = "delay_millis")]
    pub delay_max_entries: Option<u64>,
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
    /// Ozzy brokers keep journals in bounded memory images; no disk I/O.
    #[arg(long)]
    pub memory_storage: bool,
    /// Memory image bound per broker in MiB, counting dirty and durable copies.
    #[arg(long, requires = "memory_storage")]
    pub memory_mib: Option<u64>,
    /// Run every Ozzy role as a thread of one process over OMQ inproc.
    #[arg(long, requires = "memory_storage")]
    pub inproc: bool,
    /// Ozzy broker endpoint transport; `lz4` and `zstd` bind compressed TCP.
    #[arg(long, default_value = "tcp", value_parser = ["tcp", "lz4", "zstd"])]
    pub transport: String,
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
    /// Time limits of one case.
    fn budget(&self) -> crate::budget::RunBudget {
        crate::budget::RunBudget::new(self.warmup, self.duration, self.drain_secs())
    }

    /// The default drain, plus the delay that holds the last records.
    fn drain_secs(&self) -> u64 {
        crate::budget::DEFAULT_DRAIN_SECS + self.delay_millis.map_or(0, |ms| ms.div_ceil(1000))
    }

    fn local_shards(&self) -> u64 {
        self.shards
            .unwrap_or(self.partitions.min(self.broker_cpus.pool().len() as u64))
    }

    /// Reader connections, each in its own process.
    const fn readers(&self) -> u64 {
        if self.groups == 0 {
            self.partitions.saturating_mul(self.readers_per_partition)
        } else {
            self.groups as u64
        }
    }

    const fn writer_processes(&self) -> u64 {
        self.partitions.div_ceil(PARTITIONS_PER_WRITER_PROCESS)
    }
}

/// Qualify binaries, run fresh isolated servers serially, and append verified result rows.
pub async fn run(mut args: Args) -> Result<()> {
    install_signals()?;
    args.validate(&isolation::cpus(None)?)?;
    if args.dry_run {
        for case in args.cases() {
            println!(
                "{}",
                json!(args.command(&super::worker_binary(), &case, Some("127.0.0.1:1234"))?)
            );
        }
        return Ok(());
    }
    fs::create_dir_all(cache()?)?;
    let _lock = super::lock_runner(&super::artifacts())?;
    super::require_artifact_disk()?;
    isolation::require_idle()?;
    let id = run_id()?;
    let directory = super::artifacts().join("runs").join(&id);
    fs::create_dir_all(&directory)?;
    if let Some(path) = &args.placements {
        args.placements = Some(super::distributed::freeze(path, &directory)?);
    }
    let binary = build::build(&args, &directory)?;
    if args.cases().iter().any(|case| case["impl"] == "iggy") {
        super::server::prepare(args.no_build, &directory)?;
    }
    if args.cases().iter().any(|case| case["impl"] == "redpanda") {
        super::server::redpanda::prepare(args.no_build)?;
    }
    if args.check_only {
        return Ok(());
    }
    let manifest = build::deploy_run(&args, &id, &binary)?;
    json_file(&directory.join("manifest.json"), &manifest)?;
    isolation::pin(None, &args.client_cpus)?;
    let implementations = args
        .cases()
        .iter()
        .filter_map(|c| c["impl"].as_str().map(str::to_owned))
        .collect::<BTreeSet<_>>();
    let outcome = matrix::matrix(&args, &binary, &manifest, &directory).await;
    let failure = outcome.as_ref().err().map(ToString::to_string);
    let closed = records::close_run(&implementations, &id, &directory, outcome);
    if let Some(reason) = failure {
        fs::write(directory.join("FAILED"), reason)?;
    }
    closed
}
