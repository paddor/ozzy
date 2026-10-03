use clap::{Parser, ValueEnum};

mod lanes;
mod metrics;
mod processes;
mod timed;

use ozzy_bench::workload;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message.into()).into()
}

/// Who writes segment data: the device writer pool or the journal owner
/// through kernel AIO (needs `--direct-io true`, the default).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum IoBackend {
    Pool,
    Aio,
}

impl IoBackend {
    const fn name(self) -> &'static str {
        match self {
            Self::Pool => "pool",
            Self::Aio => "aio",
        }
    }
}

/// Writer payload compression. `Off` sends, replicates, and stores plain bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PayloadCompression {
    Adaptive,
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum System {
    SingleDurable,
    DiskQuorum,
    ReplicatedPersisting,
}

#[derive(Debug, Clone, Parser)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent CLI experiment switches"
)]
#[command(about = "Production Ozzy and external timed writer/reader workloads")]
struct Args {
    #[arg(long, value_enum, default_value = "replicated-persisting")]
    system: System,
    /// Opt-in external single-broker comparison. Server lifecycle stays external.
    #[cfg(feature = "comparisons")]
    #[arg(long, value_enum, requires = "external_endpoint")]
    external_system: Option<timed::external::System>,
    #[cfg(feature = "comparisons")]
    #[arg(long, value_enum, requires = "external_system")]
    external_policy: Option<timed::external::Policy>,
    #[cfg(feature = "comparisons")]
    #[arg(long, requires = "external_system")]
    external_endpoint: Option<String>,
    /// Owned external storage filesystem to flush after verification, before logout.
    #[cfg(feature = "comparisons")]
    #[arg(long, requires = "external_system")]
    external_storage_dir: Option<std::path::PathBuf>,
    /// Explicit disk-backed parent. Every broker owns a temporary child directory.
    #[arg(long, default_value = "target/native-bench-storage")]
    storage_dir: std::path::PathBuf,
    /// Timed native disk segment capacity, independent of the total history cap.
    #[arg(long, default_value_t = 64)]
    segment_mib: usize,
    /// Decoded bytes retained per active segment, also forcing a roll at this limit.
    #[arg(long, default_value_t = 64)]
    segment_decoded_mib: usize,
    #[command(flatten)]
    native: timed::native::Options,
    /// Total disk threads: owners, one reader per shard, the writer pool and
    /// one maintenance thread. Zero selects that count; others must match it.
    #[arg(skip = 0usize)]
    disk_workers: usize,
    /// Fixed local journal-owner threads within the total disk-worker budget.
    #[arg(skip = 1usize)]
    disk_owner_threads: usize,
    /// Write replicated segment groups with `O_DIRECT` (disk-backed groups).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    direct_io: bool,
    /// Who writes segment data in disk-backed groups.
    #[arg(long, value_enum, default_value = "aio")]
    io_backend: IoBackend,
    /// Kernel AIO data writes in flight per broker device controller (`--io-backend aio`).
    #[arg(long, default_value_t = 1)]
    aio_depth: usize,
    /// Maximum unfinished local disk reads per shard, including unconsumed replies.
    #[arg(skip = 8usize)]
    read_depth: usize,
    /// Timed native broker send/receive HWM, in socket messages, not records.
    #[arg(skip = 8192u32)]
    broker_hwm: u32,
    /// Local disk append commands per fanring producer lane, not records.
    #[arg(skip = 1024usize)]
    storage_lane_capacity: usize,
    /// Local disk records sharing one physical append/sync; not a writer batch.
    #[arg(skip = 4096usize)]
    storage_group_records: usize,
    /// Local operation payload target, independent of SDK batching and writes.
    #[arg(skip = 4096usize)]
    operation_target_kib: usize,
    /// Local physical write-group payload target, independent of operation size.
    #[arg(skip = 4096usize)]
    write_group_target_kib: usize,
    /// Replicated background bytes per write syscall; omitted drains all ready bytes.
    #[arg(skip)]
    write_call_kib: Option<usize>,
    /// Canonical bytes awaiting background writes per broker, independent of repair cache.
    #[arg(skip)]
    persistence_backlog_mib: Option<usize>,
    /// Recent replication bytes retained for RAM gap repair per broker.
    #[arg(skip)]
    replication_cache_mib: Option<usize>,
    /// Recently written disk-group operations kept in RAM for readers, per
    /// broker. Older reads load the segment file. Default 512 MiB.
    #[arg(skip)]
    resident_read_mib: Option<usize>,
    /// Raw local append allocation blocks, independent of physical write groups.
    #[arg(skip = 64usize)]
    packing_block_kib: usize,
    /// Local write-group payload cap. Defaults to storage-group-records * record-bytes.
    #[arg(skip)]
    storage_group_kib: Option<usize>,
    #[command(flatten)]
    control: ozzy_bench::control::Args,
    /// Owned OMQ background I/O threads per process.
    #[arg(long, default_value_t = 1)]
    io_threads: usize,
    /// Broker processes only: replaces `--io-threads`; clients keep theirs.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=32))]
    broker_io_threads: Option<u64>,
    /// Independent local partition owners, each on its own current-thread runtime.
    /// Replicated modes currently have one ordered group and require one owner.
    #[arg(long, default_value_t = 1)]
    app_threads: usize,
    /// Choose partition IDs whose hashes distribute lanes evenly across local shards.
    #[arg(long)]
    balanced_partitions: bool,
    /// Deterministic incompressible payloads, preserving the submission timestamp.
    #[arg(long)]
    random_payload: bool,
    /// Newline-separated JSON events of the OMQ compression benchmark, cut at the record size.
    #[arg(long, conflicts_with = "random_payload")]
    json_payload: bool,
    /// Fixed 16-byte binary event: submission clock, four byte fields, u32 value.
    #[arg(long, requires = "duration", conflicts_with_all = ["random_payload", "json_payload"])]
    binary_payload: bool,
    /// Run three independent voter processes, locally or at explicit placements.
    #[arg(long)]
    processes: bool,
    /// Native TCP appends; one partition per writer, with optional streaming.
    #[arg(long, requires = "processes")]
    network_ingress: bool,
    /// Split total --window writers across this many independent local processes.
    /// Default: one process when timed; coordinator-hosted for finite diagnostics.
    #[arg(long, requires = "network_ingress")]
    producer_workers: Option<usize>,
    #[arg(
        long,
        hide = true,
        requires = "producer_workers",
        conflicts_with = "worker_index"
    )]
    producer_worker: Option<usize>,
    /// Submit individual native records; brokers choose replication groups.
    #[arg(long, requires = "network_ingress")]
    streaming: bool,
    /// Maximum records per protocol request; Iggy also uses this batch size.
    #[arg(long, requires = "streaming", default_value_t = 1024)]
    request_records: usize,
    /// Maximum records per reader delivery or external poll.
    #[arg(long, default_value_t = 16384)]
    reader_records: usize,
    /// Maximum payload MiB per reader delivery or external poll.
    #[arg(long, default_value_t = 128)]
    reader_payload_mib: usize,
    /// Native APPEND requests awaiting full confirmation per writer connection.
    #[arg(long, default_value_t = 3)]
    writer_inflight_appends: usize,
    /// Optional timed SDK request bound. Zero drains available in-flight credit.
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=65536))]
    writer_batch_records: u32,
    /// Timed SDK collection delay, independent of confirmation latency.
    #[arg(skip = 0u64)]
    writer_linger_us: u64,
    /// SDK APPEND payload packing. Broker storage encoding is independent.
    #[arg(long, value_enum, default_value = "adaptive")]
    payload_compression: PayloadCompression,
    /// JSON array of three {bind[, ssh, executable]} placements.
    #[arg(long, requires = "processes")]
    placements: Option<std::path::PathBuf>,
    #[arg(long, hide = true)]
    worker_index: Option<usize>,
    #[arg(long, hide = true, default_value = "127.0.0.1")]
    bind: std::net::IpAddr,
    #[arg(long, hide = true)]
    group: Option<uuid::Uuid>,
    #[arg(long, default_value_t = 128)]
    record_bytes: usize,
    /// Timed native throughput window in seconds. Default: 3.
    #[arg(long, requires = "network_ingress")]
    duration: Option<f64>,
    /// Total scheduled records/s across native writers. Omit for saturation.
    #[arg(long, requires_all = ["network_ingress", "streaming"],
        value_parser = clap::value_parser!(u64).range(1..=1_000_000_000))]
    records_per_second: Option<u64>,
    /// Continuous offered-load ramp, `RATE:SECONDS,...` with rising rates.
    /// Sets the measurement duration; an overloaded stage ends the ramp.
    #[arg(long, requires_all = ["network_ingress", "streaming"], conflicts_with = "records_per_second")]
    ramp: Option<ozzy_bench::schedule::Ramp>,
    /// Continuous warmup before the timed native measurement, in seconds.
    #[arg(long, default_value_t = 0.25)]
    warmup: f64,
    /// Timed reader processes; default min(4, partitions), independent of writers.
    #[arg(long)]
    reader_workers: Option<usize>,
    #[arg(long, hide = true)]
    reader_worker: Option<usize>,
    /// Native readers subscribed to each partition; each verifies every record.
    #[arg(long, default_value_t = 1)]
    readers_per_partition: usize,
    /// Disk groups: the leader publishes confirmed records once per partition
    /// and readers take live records from that publication, history by replay.
    #[arg(long)]
    live_readers: bool,
    /// Timed history descriptor cap per broker; exhaustion invalidates the run.
    #[arg(long, default_value_t = 131072)]
    history_operations: usize,
    /// Final confirmation and verified-delivery drain deadline, in seconds.
    #[arg(long, default_value_t = 10)]
    drain_timeout_secs: u64,
    /// Native writers, one partition each. Default: 4.
    #[arg(long)]
    window: Option<usize>,
    /// Maximum complete canonical history per broker, before the recovery copy.
    #[arg(long, default_value_t = 512)]
    history_mib: usize,
}

impl Args {
    /// Offered arrivals follow a schedule instead of completions.
    fn scheduled(&self) -> bool {
        self.records_per_second.is_some() || self.ramp.is_some()
    }

    fn reader_workers(&self) -> usize {
        self.reader_workers
            .unwrap_or(self.window.unwrap_or(4).min(4))
    }
}

pub(crate) async fn run() -> Result<()> {
    let args = Args::parse();
    ozzy_bench::control::run(
        &args.control,
        args.io_threads,
        Box::pin(run_args(args.clone())),
    )
    .await
}

async fn run_args(mut args: Args) -> Result<()> {
    if (args.worker_index.is_some()
        || args.producer_worker.is_some()
        || args.reader_worker.is_some())
        && args.control.control_endpoint.is_none()
    {
        return Err(error("worker roles require benchmark OMQ control"));
    }
    if let Some(ramp) = &args.ramp {
        if args
            .duration
            .is_some_and(|duration| (duration * 1e9).round() as u64 != ramp.duration_ns())
        {
            return Err(error("--duration must match the ramp"));
        }
        args.duration = Some(ramp.duration_ns() as f64 / 1e9);
    }
    if args.network_ingress {
        args.duration.get_or_insert(3.0);
        args.producer_workers.get_or_insert(1);
        args.streaming = true;
    }
    timed::run(args).await
}
