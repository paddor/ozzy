//! Isolated codec experiment; does not implement dictionary-backed storage.
#![forbid(unsafe_code)]
mod dict_bench;

use clap::Parser;
use ozzy_bench::automation::{self, Result, isolation, records, source, supervise};
use serde_json::json;
use std::{collections::BTreeMap, fs, fs::OpenOptions, time::Duration};

#[derive(Clone, Debug, Parser)]
#[command(about = "LZ4 dictionary CPU/size experiment on held-out canonical Append bodies")]
struct Args {
    #[arg(long, value_delimiter = ',', default_value = "128,1024,8192")]
    record_bytes: Vec<usize>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "256,512,1024,2048,4096,8192,16384,65536"
    )]
    body_targets: Vec<usize>,
    #[arg(long, value_delimiter = ',', default_value = "0,2048,8192")]
    dict_bytes: Vec<usize>,
    #[arg(long, default_value_t = 128)]
    samples: usize,
    #[arg(long, default_value_t = 200)]
    duration_ms: u64,
    #[arg(long, default_value_t = 100)]
    warmup_ms: u64,
    #[arg(long, default_value_t = 2)]
    repetitions: usize,
    #[arg(long, default_value_t = 0)]
    cpu: usize,
    #[arg(long, default_value_t = 2)]
    controller_cpu: usize,
    #[arg(long, hide = true)]
    worker: bool,
}

impl Args {
    fn validate(&self) -> Result<()> {
        if self.record_bytes.is_empty()
            || self.body_targets.is_empty()
            || self.dict_bytes.is_empty()
            || self.record_bytes.len() > 3
            || self.body_targets.len() > 8
            || self.dict_bytes.len() > 3
            || self
                .record_bytes
                .iter()
                .any(|n| ![128, 1024, 8192].contains(n))
            || self.body_targets.iter().any(|n| !(256..=65536).contains(n))
            || self.dict_bytes.iter().any(|n| ![0, 2048, 8192].contains(n))
            || !(16..=256).contains(&self.samples)
            || !self.samples.is_multiple_of(16)
            || !(50..=1000).contains(&self.duration_ms)
            || !(50..=1000).contains(&self.warmup_ms)
            || !(1..=3).contains(&self.repetitions)
        {
            return Err("invalid or excessive experiment bounds".into());
        }
        let cells = self.record_bytes.len()
            * self.body_targets.len()
            * self.dict_bytes.len()
            * self.repetitions
            * 2;
        if cells as u64 * 2 * (self.duration_ms + self.warmup_ms) > 480_000 {
            return Err("experiment exceeds the bounded timing budget".into());
        }
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    args.validate()?;
    if args.worker {
        isolation::pin(None, &[args.cpu])?;
        println!("{}", dict_bench::run(&args)?);
        return Ok(());
    }
    automation::install_signals()?;
    isolation::require_idle()?;
    fs::create_dir_all(automation::cache())?;
    let lock = OpenOptions::new()
        .create(true)
        .append(true)
        .open(automation::cache().join("runner.lock"))?;
    lock.try_lock()?;
    let cpus = isolation::cpus(None)?;
    if args.cpu == args.controller_cpu
        || !cpus.contains(&args.cpu)
        || !cpus.contains(&args.controller_cpu)
    {
        return Err("worker/controller CPUs must be distinct and inside the inherited mask".into());
    }
    isolation::pin(None, &[args.controller_cpu])?;
    let id = format!("{}-lz4-dictionary", automation::run_id()?);
    let directory = automation::cache().join("experiments").join(&id);
    fs::create_dir_all(&directory)?;
    let identity = source::identity(&automation::root())?;
    let exe = std::env::current_exe()?;
    let mut command = vec![exe.display().to_string()];
    command.extend(std::env::args().skip(1));
    command.push("--worker".into());
    let manifest = json!({"kind":"lz4-dictionary-experiment","run_id":id,"source":identity,
        "executable_sha256":source::sha256(&exe)?,"command":command,
        "cpu":args.cpu,"controller_cpus":isolation::cpus(None)?,"comparison_timing":false,
        "cpuinfo":fs::read_to_string("/proc/cpuinfo")?,"meminfo":fs::read_to_string("/proc/meminfo")?});
    automation::json_file(&directory.join("manifest.json"), &manifest)?;
    println!("EXPERIMENT {}", directory.display());
    let mut guard = isolation::Guard::new("lz4-dictionary", None);
    let mut result = supervise::execute(
        &command,
        &directory,
        Duration::from_secs(600),
        &BTreeMap::new(),
        None,
        Some(&mut guard),
    )
    .await?;
    source::require_unchanged(&automation::root(), &identity)?;
    if source::sha256(&exe)? != manifest["executable_sha256"] {
        return Err("experiment executable changed".into());
    }
    result["manifest"] = manifest;
    result["process_isolation"] = guard.report()?;
    result["after_shutdown"] = isolation::require_idle()?;
    automation::json_file(&directory.join("result.json"), &result)?;
    records::append(&automation::cache().join("experiments"), "ozzy", &result)?;
    println!(
        "VERIFIED {} codec measurements; results appended under ~/.cache/ozzy/experiments/",
        result["measurements"]
            .as_array()
            .ok_or("missing measurements")?
            .len()
    );
    Ok(())
}
