//! Shared process benchmark controls.
use super::Error;
use clap::Parser;
use std::{collections::BTreeSet, path::PathBuf};
#[derive(Debug, Parser)]
#[command(group(clap::ArgGroup::new("language").required(true).multiple(false)))]
pub(super) struct Args {
    #[arg(long, group = "language")]
    pub(super) python: Option<PathBuf>,
    #[arg(long, group = "language")]
    pub(super) java: Option<PathBuf>,
    #[arg(long, requires = "java")]
    pub(super) java_classpath: Option<String>,
    #[arg(long, requires = "java")]
    pub(super) java_library: Option<PathBuf>,
    #[arg(long)]
    pub(super) java_agent: Option<PathBuf>,
    #[arg(long, default_value = "single", value_parser = ["single", "quorum"])]
    pub(super) mode: String,
    #[arg(long, default_value = "lz4", value_parser = ["raw", "lz4"])]
    pub(super) codec: String,
    #[arg(long, default_value = "blocking", value_parser = ["blocking", "asyncio", "async"])]
    pub(super) api: String,
    #[arg(long, default_value = "tcp", value_parser = ["tcp", "lz4+tcp"])]
    pub(super) transport: String,
    #[arg(long, default_value_t = 1024)]
    pub(super) size: usize,
    #[arg(long, default_value_t = 2.0)]
    pub(super) warmup: f64,
    #[arg(long, default_value_t = 8.0)]
    pub(super) duration: f64,
    #[arg(long, default_value_t = 2)]
    pub(super) repetitions: usize,
    #[arg(long, default_value = "0,1")]
    pub(super) broker_cpus: String,
    #[arg(long, default_value = "2,3")]
    pub(super) producer_cpus: String,
    #[arg(long, default_value_t = 4)]
    pub(super) consumer_cpu: usize,
    #[arg(long, default_value_t = 32)]
    pub(super) reader_queue_messages: usize,
    #[arg(long, default_value_t = 1024)]
    pub(super) memory_mib: usize,
    #[arg(long, default_value_t = 16)]
    pub(super) segment_mib: usize,
    #[arg(long, default_value_t = 64)]
    pub(super) retention_mib: usize,
    #[arg(long, default_value_t = 8192)]
    pub(super) window: usize,
    #[arg(long, default_value = "flush", value_parser = ["each", "flush"])]
    pub(super) confirmation: String,
    #[arg(long, default_value = "candidate")]
    pub(super) label: String,
    #[arg(long)]
    pub(super) profile: Option<PathBuf>,
    #[arg(long)]
    pub(super) no_ledger: bool,
    #[arg(long, hide = true)]
    pub(super) broker_child: bool,
}

pub(super) fn cpus(value: &str) -> Result<Vec<usize>, Error> {
    Ok(value.split(',').map(str::parse).collect::<Result<_, _>>()?)
}

pub(super) fn validate(args: &Args) -> Result<(), Error> {
    if (args.java.is_some() && (args.java_classpath.is_none() || args.api == "asyncio"))
        || (args.python.is_some() && (args.api == "async" || args.java_agent.is_some()))
    {
        return Err("invalid language options".into());
    }
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
