//! Sustained full-product simulation runner.
use clap::{Parser, ValueEnum};
use ozzy_config::Confirmation;
use ozzy_sim::soak::{self, Config};
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
    };
    match soak::run(&config).await {
        Ok(report) => println!("{report:?}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
