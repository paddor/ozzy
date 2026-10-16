//! Replay one verified native case under a profiler, outside comparison ledgers.
#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

mod profile;

use clap::Parser;
use ozzy_bench::automation::{self, Result, isolation, source, supervise};
use profile::{counters::Counters, report, verify, workload::workload};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

/// What the profiled run records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum Kind {
    // Physical write and kernel AIO syscalls under strace.
    Syscall,
    // User-space CPU samples under perf.
    Cpu,
    // User and kernel CPU samples under perf.
    CpuKernel,
    // Opt-in application stage timing over live counters.
    Stages,
}

impl Kind {
    /// The command-line value, also used in run IDs and the manifest.
    fn name(self) -> &'static str {
        match self {
            Self::Syscall => "syscall",
            Self::Cpu => "cpu",
            Self::CpuKernel => "cpu-kernel",
            Self::Stages => "stages",
        }
    }
}

const MAX_STACK_FREQUENCY: u32 = 999;
/// Stays below the kernel's default `perf_event_max_sample_rate` of 5000.
const MAX_FLAT_FREQUENCY: u32 = 4999;

#[derive(Debug, Parser)]
struct Args {
    /// Saved native workload containing command.json.
    #[arg(long)]
    case_dir: PathBuf,
    /// Physical syscalls, CPU samples, or opt-in application stage timing.
    #[arg(long, value_enum)]
    kind: Kind,
    #[arg(long, default_value_t = 5.0)]
    duration: f64,
    #[arg(long, default_value_t = 2.0)]
    warmup: f64,
    /// CPU samples per second per active thread. Profiling overhead is excluded from comparisons.
    #[arg(long, default_value_t = 99, value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_FLAT_FREQUENCY)))]
    frequency: u32,
    /// Sample instruction pointers only. Without stack copies, rates up to
    /// 4999 Hz resolve instruction-level hot spots.
    #[arg(long)]
    no_stacks: bool,
    /// Replay the workload with the current verified build instead of the case binary.
    #[arg(long)]
    current_build: bool,
    /// Retain received SUB snapshots in an SSD artifact for time correlation.
    #[arg(long)]
    counter_trace: bool,
}

impl Args {
    /// Time limits of the profiled run. It runs with the default drain.
    fn budget(&self) -> ozzy_bench::budget::RunBudget {
        ozzy_bench::budget::RunBudget::new(
            self.warmup,
            self.duration,
            ozzy_bench::budget::DEFAULT_DRAIN_SECS,
        )
    }

    /// Profiles stay short, and only stage profiles collect counter traces.
    fn check(&self) -> Result<()> {
        if !(0.1..=30.0).contains(&self.duration) || !(0.0..=10.0).contains(&self.warmup) {
            return Err("invalid bounded profile duration".into());
        }
        if self.counter_trace && self.kind != Kind::Stages {
            return Err("counter traces require --kind stages".into());
        }
        if self.no_stacks && !matches!(self.kind, Kind::Cpu | Kind::CpuKernel) {
            return Err("--no-stacks requires a CPU profile".into());
        }
        // Each stack sample copies 64 KiB of user stack.
        if !self.no_stacks && self.frequency > MAX_STACK_FREQUENCY {
            return Err("stack samples are limited to 999 Hz; use --no-stacks".into());
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    args.check()?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(profile(&args))
}

/// Replay the case under the profiler, verify the run, and save its reports.
async fn profile(args: &Args) -> Result<()> {
    automation::install_signals()?;
    isolation::require_idle()?;
    automation::require_artifact_disk()?;
    let case_manifest = automation::read_json(
        &args
            .case_dir
            .parent()
            .ok_or("missing case parent")?
            .join("manifest.json"),
    )?;
    let client_cpus: Vec<usize> =
        serde_json::from_value(case_manifest["arguments"]["client_cpus"].clone())?;
    isolation::pin(None, &client_cpus)?;
    let directory = profile_directory(args.kind)?;
    let build = verify::Build::verify(&case_manifest, args.current_build)?;
    let command = workload(args, &directory)?;
    save_manifest(args, &directory, &command, &build, &case_manifest)?;
    // Drain earlier filesystem writes before starting the profiled workload.
    automation::capture(
        Command::new("sync")
            .arg("-f")
            .arg(automation::artifact_root()),
    )?;
    let mut guard = isolation::Guard::new("ozzy", None).with_profiling(true)?;
    monitor_remote(&command, &directory, args.budget().lifetime(), &mut guard)?;
    println!("PROFILE {}", directory.display());
    let row = execute_profile(args, &command, &directory, &mut guard).await?;
    let remote_audit = guard.finish_remote()?;
    automation::json_file(&directory.join("remote-audit-proof.json"), &remote_audit)?;
    verify::verify_run(&row, args.kind)?;
    automation::validation::production_cpu_placement(&row, &case_manifest["arguments"])?;
    source::require_unchanged(&automation::root(), &build.identity)?;
    automation::json_file(&directory.join("result.json"), &row)?;
    report::save(args.kind, &directory, &row)?;
    println!("VERIFIED {} records", row["total_verified_records"]);
    Ok(())
}

/// A new directory for this run under the artifact root's profiles.
fn profile_directory(kind: Kind) -> Result<PathBuf> {
    let profiles = automation::artifact_root().join("ozzy-profiles");
    let directory = profiles.join(format!("{}-{}", automation::run_id()?, kind.name()));
    fs::create_dir_all(&profiles)?;
    fs::create_dir(&directory)?;
    Ok(directory)
}

/// Save the replayed command, and what it replays with which build.
fn save_manifest(
    args: &Args,
    directory: &Path,
    command: &[String],
    build: &verify::Build,
    case_manifest: &Value,
) -> Result<()> {
    automation::json_file(&directory.join("command.json"), &json!(command))?;
    automation::json_file(
        &directory.join("manifest.json"),
        &json!({"source":build.identity,"executable_sha256":build.executable_sha256,
        "case_executable_sha256":case_manifest["executable_sha256"], "current_build":args.current_build,
        "case_dir":args.case_dir, "kind":args.kind.name(), "frequency":args.frequency,
        "call_stacks":!args.no_stacks, "comparison_timing":false,
        "aslr_disabled":matches!(args.kind, Kind::Cpu | Kind::CpuKernel),
        "capture_scope":"coordinator host only; remote brokers participate but are not sampled",
        "controller_cpus":isolation::cpus(None)?, "case_configuration":case_manifest["arguments"]}),
    )
}

fn monitor_remote(
    command: &[String],
    directory: &Path,
    lifetime: Duration,
    guard: &mut isolation::Guard,
) -> Result<()> {
    if let Some([_, path]) = command.windows(2).find(|pair| pair[0] == "--placements") {
        let placements = ozzy_bench::placement::Placement::load(Some(Path::new(path)))?;
        automation::distributed::idle(&placements)?;
        automation::distributed::deploy(&placements, &automation::worker_binary(), false)?;
        if let Some(remote) = placements.iter().find(|p| p.remote.is_some()) {
            guard.set_remote(automation::distributed::Audit::start(
                remote, "ozzy", directory, lifetime,
            )?);
        }
    }
    Ok(())
}

/// Run the workload; stage profiles collect live counters alongside it.
async fn execute_profile(
    args: &Args,
    command: &[String],
    directory: &Path,
    guard: &mut isolation::Guard,
) -> Result<Value> {
    let mut environment =
        BTreeMap::from([("OZZY_BENCH_PROFILE".into(), args.kind.name().to_owned())]);
    let counters = if args.kind == Kind::Stages {
        let counters = Counters::bind(directory).await?;
        environment.insert(
            "OZZY_TELEMETRY_ENDPOINT".into(),
            counters.endpoint().to_owned(),
        );
        Some(counters)
    } else {
        None
    };
    let run = supervise::execute(
        command,
        directory,
        args.budget().lifetime(),
        &environment,
        supervise::Monitors {
            server: None,
            isolation: Some(guard),
        },
    );
    match counters {
        Some(counters) => counters.collect(run, directory, args.counter_trace).await,
        None => run.await,
    }
}
