//! Broker serving, configuration, and explicit storage initialization.

mod recovery;
mod serve;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use ozzy_broker::{
    check_config, format_partition_journals, host_resources, initialize_broker_identity,
    initialize_identity, initialize_volumes, load_broker_identity, load_deployment,
};

#[derive(Debug, Parser)]
#[command(
    name = "ozzy_broker",
    about = "Ozzy broker serving and explicit deployment provisioning"
)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve established segment journals. Never creates missing identity or history.
    Serve(ServingArgs),
    /// Serve with explicitly selected nonvoting partition recovery.
    Recover {
        #[command(flatten)]
        serving: ServingArgs,
        #[command(flatten)]
        recovery: recovery::RecoveryArgs,
    },
    /// Check deployment and this broker's effective host restrictions. No writes.
    Validate {
        #[arg(long)]
        broker: String,
    },
    /// Create shared deployment identity once. Does not initialize segment stores.
    Init {
        #[arg(long)]
        identity: PathBuf,
    },
    /// Create local volume/store bindings once, before explicit segment formatting.
    InitBroker {
        #[arg(long)]
        broker: String,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        local_identity: PathBuf,
    },
    /// Bind existing device directories to this broker's persistent volumes.
    InitVolumes {
        #[arg(long)]
        broker: String,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        local_identity: PathBuf,
    },
    /// Initialize absent segment journals on this broker's configured shards.
    Format {
        #[arg(long)]
        broker: String,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        local_identity: PathBuf,
    },
    /// Check existing identity and local topology. Missing state is never created.
    Check {
        #[arg(long)]
        broker: String,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        local_identity: Option<PathBuf>,
    },
}

#[derive(Debug, clap::Args)]
struct ServingArgs {
    #[arg(long)]
    broker: String,
    #[arg(long)]
    identity: PathBuf,
    #[arg(long)]
    local_identity: PathBuf,
    /// Native client IDs are admission identities. Transport must already be trusted.
    #[arg(long, required = true, action = clap::ArgAction::SetTrue)]
    trusted_transport: bool,
}

impl ServingArgs {
    fn run(
        self,
        deployment: ozzy_config::ValidatedDeployment,
        selections: &[ozzy_broker::RecoverySelection],
    ) -> Result<(), ozzy_broker::StartupError> {
        let Self {
            broker,
            identity,
            local_identity,
            trusted_transport: _,
        } = self;
        let checked = check_config(deployment, &identity, &broker, &host_resources()?)?;
        let local = load_broker_identity(&checked, &local_identity)?;
        runtime()?.block_on(serve::run(checked, local, selections))
    }
}

fn run(args: Args) -> Result<(), ozzy_broker::StartupError> {
    let deployment = load_deployment(&args.config)?;
    match args.command {
        Command::Serve(serving) => serving.run(deployment, &[])?,
        Command::Recover { serving, recovery } => {
            serving.run(deployment, &recovery.into_selections())?;
        }
        Command::Validate { broker } => {
            let plan = deployment.broker_plan(&broker, &host_resources()?)?;
            println!(
                "Valid: {} partitions, {} shards, {} OMQ I/O workers; broker {}",
                plan.partitions.len(),
                plan.shards.len(),
                plan.omq_io_threads,
                broker
            );
        }
        Command::Init { identity } => {
            let initialized = initialize_identity(&deployment, &identity, uuid::Uuid::now_v7)?;
            println!(
                "Created shared identity {} at {}. Segment stores unchanged.",
                initialized.cluster,
                identity.display()
            );
        }
        Command::InitBroker {
            broker,
            identity,
            local_identity,
        } => {
            let checked = check_config(deployment, &identity, &broker, &host_resources()?)?;
            initialize_broker_identity(&checked, &local_identity, uuid::Uuid::now_v7)?;
            println!(
                "Created broker storage bindings at {}. Segment stores unchanged.",
                local_identity.display()
            );
        }
        Command::Check {
            broker,
            identity,
            local_identity,
        } => {
            let checked = check_config(deployment, &identity, &broker, &host_resources()?)?;
            if let Some(local_identity) = local_identity {
                load_broker_identity(&checked, &local_identity)?;
            }
            println!(
                "Identity and placement valid: broker {}, cluster {}. Segment stores not opened.",
                broker, checked.identity.cluster
            );
        }
        Command::InitVolumes {
            broker,
            identity,
            local_identity,
        } => {
            let checked = check_config(deployment, &identity, &broker, &host_resources()?)?;
            let local = load_broker_identity(&checked, &local_identity)?;
            initialize_volumes(&checked, &local)?;
            println!("Initialized volume bindings for broker {broker}.");
        }
        Command::Format {
            broker,
            identity,
            local_identity,
        } => {
            let checked = check_config(deployment, &identity, &broker, &host_resources()?)?;
            let local = load_broker_identity(&checked, &local_identity)?;
            runtime()?.block_on(format_partition_journals(&checked, &local))?;
            println!(
                "Initialized {} segment journals for broker {broker}.",
                checked.plan.partitions.len()
            );
        }
    }
    Ok(())
}

fn runtime() -> Result<tokio::runtime::Runtime, ozzy_broker::StartupError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| ozzy_broker::StartupError::Runtime(error.to_string()))
}

fn main() -> std::process::ExitCode {
    match run(Args::parse()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}
