//! Sustained full-product simulation runner.
use clap::{Parser, ValueEnum};
use ozzy_config::Confirmation;
use ozzy_sim::soak::{self, Action, Config, Resources, Time};
use std::{path::PathBuf, time::Duration};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Durable,
    DiskQuorum,
    ReplicatedPersisting,
}

#[derive(Debug, Parser)]
#[command(about = "Run production SDKs and brokers over OMQ inproc and bounded memory storage")]
struct Args {
    #[arg(long, value_enum, default_value = "replicated-persisting")]
    mode: Mode,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Number of fresh clusters, each using the next seed and a separate artifact directory.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    scenarios: u32,
    #[arg(long, default_value_t = 60)]
    duration: u64,
    #[arg(long, default_value_t = usize::MAX)]
    waves: usize,
    #[arg(long, default_value_t = 10)]
    interval_ms: u64,
    #[arg(long, default_value_t = 30)]
    progress_timeout: u64,
    #[arg(long)]
    artifacts: PathBuf,
    #[arg(long)]
    replay: Option<PathBuf>,
    /// Stop generation at this simulated time as well as the wall-clock limit.
    #[arg(long)]
    simulated_duration: Option<u64>,
    #[arg(long, default_value_t = 2)]
    clock_tick_ms: u64,
    #[arg(long, default_value_t = 10)]
    clock_step_ms: u64,
    #[arg(long, default_value_t = 4)]
    partitions: u32,
    #[arg(long, default_value_t = 256)]
    resident_mib: u64,
    #[arg(long, default_value_t = 256)]
    physical_jobs: usize,
    #[arg(long, default_value_t = 128)]
    physical_mib: u64,
    #[arg(long, default_value_t = 2)]
    retained_mib: u64,
    #[arg(long, default_value_t = 120)]
    retained_age: u64,
    /// Comma-separated actions; repeats weight their frequency. Replay takes precedence.
    #[arg(long, value_enum, value_delimiter = ',')]
    actions: Vec<Action>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Args::parse();
    let config = Config {
        policy: match args.mode {
            Mode::Durable => Confirmation::LocalDurable,
            Mode::DiskQuorum => Confirmation::DiskQuorum,
            Mode::ReplicatedPersisting => Confirmation::ReplicatedPersisting,
        },
        seed: args.seed,
        duration: Duration::from_secs(args.duration),
        waves: args.waves,
        interval: Duration::from_millis(args.interval_ms),
        progress_timeout: Duration::from_secs(args.progress_timeout),
        artifacts: args.artifacts,
        replay: args.replay,
        time: Time {
            duration: args.simulated_duration.map(Duration::from_secs),
            tick: Duration::from_millis(args.clock_tick_ms),
            step: Duration::from_millis(args.clock_step_ms),
        },
        resources: Resources {
            partitions: args.partitions,
            resident_bytes: args.resident_mib.saturating_mul(1024 * 1024),
            physical_jobs: args.physical_jobs,
            physical_bytes: args.physical_mib.saturating_mul(1024 * 1024),
            retained_bytes: args.retained_mib.saturating_mul(1024 * 1024),
            retained_age_secs: args.retained_age,
        },
        actions: if args.actions.is_empty() {
            soak::default_actions()
        } else {
            args.actions
        },
    };
    for scenario in 0..args.scenarios {
        let mut scenario_config = config.clone();
        scenario_config.seed = config.seed.wrapping_add(u64::from(scenario));
        if args.scenarios > 1 {
            scenario_config.artifacts = config
                .artifacts
                .join(format!("scenario-{scenario}-seed-{}", scenario_config.seed));
        }
        match soak::run(&scenario_config).await {
            Ok(report) => println!("{}", serde_json::to_string(&report).expect("soak report")),
            Err(error) => {
                eprintln!("{error}");
                std::process::exit(1);
            }
        }
    }
}
