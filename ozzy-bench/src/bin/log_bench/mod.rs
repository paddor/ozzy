//! Options and the report of the single-log benchmark.

mod cluster;
mod load;

use clap::{Parser, ValueEnum};
use std::path::PathBuf;
use std::time::Duration;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Brokers that hold a copy of the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Mode {
    /// One broker, one copy.
    Single,
    /// Three brokers in this process, three copies.
    Three,
}

/// Which kind of log takes the appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Kind {
    /// Partition 0 of a topic the deployment names.
    Topic,
    /// A log of the `[logs]` class, created at run time.
    Runtime,
}

/// How brokers reach each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Transport {
    /// One shared OMQ context, no socket.
    Inproc,
    /// Loopback TCP, one OMQ context per broker.
    Tcp,
}

/// One log, one writer, appends through the in-process log API.
#[derive(Debug, Clone, Parser)]
pub(crate) struct Options {
    /// Empty or absent directory the brokers store below. Removed first.
    #[arg(long)]
    pub root: PathBuf,
    #[arg(long, value_enum, default_value = "single")]
    pub mode: Mode,
    #[arg(long, value_enum, default_value = "runtime")]
    pub kind: Kind,
    #[arg(long, value_enum, default_value = "tcp")]
    pub transport: Transport,
    /// Bytes of one record's only part.
    #[arg(long, default_value_t = 128)]
    pub size: usize,
    /// Records one append holds at most.
    #[arg(long, default_value_t = 64)]
    pub batch: usize,
    /// Records offered per second. 0 offers a full batch at any time.
    #[arg(long, default_value_t = 0)]
    pub rate: u64,
    /// Appends the writer keeps in flight.
    #[arg(long, default_value_t = 4)]
    pub window: usize,
    /// Measured seconds.
    #[arg(long, default_value_t = 10)]
    pub seconds: u64,
    /// Seconds before the measurement starts.
    #[arg(long, default_value_t = 2)]
    pub warmup: u64,
    /// Bytes per segment file.
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    pub segment_bytes: u64,
    /// Kernel-outstanding direct writes of each device.
    #[arg(long, default_value_t = 1)]
    pub aio_depth: usize,
    /// First loopback port of the TCP transport.
    #[arg(long, default_value_t = 47_100)]
    pub port: u16,
}

impl Options {
    /// Bound of one encoded append: the batch with room to spare.
    pub(crate) fn append_bytes(&self) -> u64 {
        let batch = 80 + self.batch * (self.size + 24);
        (batch as u64).next_multiple_of(4096).max(64 * 1024)
    }
}

/// What one run measured.
#[derive(Debug)]
pub(crate) struct Report {
    pub elapsed: Duration,
    pub appends: u64,
    pub records: u64,
    /// Confirmation latency of every measured record, in microseconds,
    /// from its arrival, or from its append's submission without a rate.
    pub micros: Vec<u32>,
    /// CPU time of the process over the measurement.
    pub cpu: Duration,
    /// The log ran out of segments before the time was up.
    pub full: bool,
}

fn percentile(sorted: &[u32], per_mille: usize) -> u32 {
    let last = sorted.len().saturating_sub(1);
    sorted
        .get((last * per_mille).div_ceil(1000))
        .copied()
        .unwrap_or(0)
}

fn print(options: &Options, mut report: Report) {
    report.micros.sort_unstable();
    let seconds = report.elapsed.as_secs_f64();
    let rate = report.records as f64 / seconds;
    println!(
        "mode={:?} kind={:?} transport={:?} size={} batch={} rate={} window={} \
         segment_bytes={} aio_depth={}",
        options.mode,
        options.kind,
        options.transport,
        options.size,
        options.batch,
        options.rate,
        options.window,
        options.segment_bytes,
        options.aio_depth,
    );
    println!(
        "seconds={seconds:.2} records={} records_per_s={rate:.0} appends_per_s={:.0} \
         mib_per_s={:.1} cpu_s_per_s={:.2}{}",
        report.records,
        report.appends as f64 / seconds,
        rate * options.size as f64 / (1024.0 * 1024.0),
        report.cpu.as_secs_f64() / seconds,
        if report.full { " log_full=true" } else { "" },
    );
    println!(
        "latency_us p50={} p90={} p99={} p999={} max={}",
        percentile(&report.micros, 500),
        percentile(&report.micros, 900),
        percentile(&report.micros, 990),
        percentile(&report.micros, 999),
        report.micros.last().copied().unwrap_or(0),
    );
}

pub(crate) async fn run() -> Result<()> {
    let options = Options::parse();
    if options.size == 0 || options.batch == 0 || options.window == 0 {
        return Err("size, batch and window must be positive".into());
    }
    let cluster = cluster::start(&options).await?;
    let log = cluster.leader(Duration::from_secs(60)).await?;
    let report = load::run(&options, &log).await;
    cluster.stop().await?;
    print(&options, report?);
    Ok(())
}
