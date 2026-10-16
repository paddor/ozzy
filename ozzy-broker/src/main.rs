//! Broker serving, configuration, and explicit storage initialization.
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

mod recovery;
mod serve;

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use ozzy_broker::{
    CheckedConfig, StartupError, check_config, format_partition_journals, host_resources,
    initialize_broker_identity, initialize_identity, initialize_volumes, load_broker_identity,
    load_deployment,
};
use ozzy_config::{BrokerIdentity, ValidatedDeployment};

#[derive(Debug, Parser)]
#[command(
    name = "ozzy",
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
    InitBroker(BrokerArgs),
    /// Bind existing device directories to this broker's persistent volumes.
    InitVolumes(BrokerArgs),
    /// Initialize absent segment journals on this broker's configured shards.
    Format(BrokerArgs),
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

// One broker, the shared deployment identity, and its local store bindings.
#[derive(Debug, clap::Args)]
struct BrokerArgs {
    #[arg(long)]
    broker: String,
    #[arg(long)]
    identity: PathBuf,
    #[arg(long)]
    local_identity: PathBuf,
}

impl BrokerArgs {
    fn checked(&self, deployment: ValidatedDeployment) -> Result<CheckedConfig, StartupError> {
        check_config(deployment, &self.identity, &self.broker, &host_resources()?)
    }

    fn load(
        &self,
        deployment: ValidatedDeployment,
    ) -> Result<(CheckedConfig, BrokerIdentity), StartupError> {
        let checked = self.checked(deployment)?;
        let local = load_broker_identity(&checked, &self.local_identity)?;
        Ok((checked, local))
    }
}

#[derive(Debug, clap::Args)]
struct ServingArgs {
    #[command(flatten)]
    broker: BrokerArgs,
    /// Native client IDs are admission identities. Transport must already be trusted.
    #[arg(long, required = true, action = clap::ArgAction::SetTrue)]
    trusted_transport: bool,
}

impl ServingArgs {
    fn run(
        self,
        deployment: ValidatedDeployment,
        selections: &[ozzy_broker::RecoverySelection],
    ) -> Result<(), StartupError> {
        let (checked, local) = self.broker.load(deployment)?;
        runtime()?.block_on(serve::run(checked, local, selections))
    }
}

fn run(args: Args) -> Result<(), StartupError> {
    let deployment = load_deployment(&args.config)?;
    match args.command {
        Command::Serve(serving) => serving.run(deployment, &[]),
        Command::Recover { serving, recovery } => {
            serving.run(deployment, &recovery.into_selections())
        }
        Command::Validate { broker } => validate(&deployment, &broker),
        Command::Init { identity } => init(&deployment, &identity),
        Command::InitBroker(broker) => init_broker(deployment, &broker),
        Command::InitVolumes(broker) => init_volumes(deployment, &broker),
        Command::Format(broker) => format_journals(deployment, &broker),
        Command::Check {
            broker,
            identity,
            local_identity,
        } => check_broker(deployment, &broker, &identity, local_identity.as_deref()),
    }
}

fn validate(deployment: &ValidatedDeployment, broker: &str) -> Result<(), StartupError> {
    let plan = deployment.broker_plan(broker, &host_resources()?)?;
    println!(
        "Valid: {} partitions, {} shards, {} OMQ I/O workers; broker {}",
        plan.partitions.len(),
        plan.shards.len(),
        plan.omq_io_threads,
        broker
    );
    Ok(())
}

fn init(deployment: &ValidatedDeployment, identity: &Path) -> Result<(), StartupError> {
    let initialized = initialize_identity(deployment, identity, uuid::Uuid::now_v7)?;
    println!(
        "Created shared identity {} at {}. Segment stores unchanged.",
        initialized.cluster,
        identity.display()
    );
    Ok(())
}

fn init_broker(deployment: ValidatedDeployment, args: &BrokerArgs) -> Result<(), StartupError> {
    let checked = args.checked(deployment)?;
    initialize_broker_identity(&checked, &args.local_identity, uuid::Uuid::now_v7)?;
    println!(
        "Created broker storage bindings at {}. Segment stores unchanged.",
        args.local_identity.display()
    );
    Ok(())
}

fn init_volumes(deployment: ValidatedDeployment, args: &BrokerArgs) -> Result<(), StartupError> {
    let (checked, local) = args.load(deployment)?;
    initialize_volumes(&checked, &local)?;
    println!("Initialized volume bindings for broker {}.", args.broker);
    Ok(())
}

fn format_journals(deployment: ValidatedDeployment, args: &BrokerArgs) -> Result<(), StartupError> {
    let (checked, local) = args.load(deployment)?;
    runtime()?.block_on(format_partition_journals(&checked, &local))?;
    println!(
        "Initialized {} segment journals for broker {}.",
        checked.plan.partitions.len(),
        args.broker
    );
    Ok(())
}

fn check_broker(
    deployment: ValidatedDeployment,
    broker: &str,
    identity: &Path,
    local_identity: Option<&Path>,
) -> Result<(), StartupError> {
    let checked = check_config(deployment, identity, broker, &host_resources()?)?;
    if let Some(local_identity) = local_identity {
        load_broker_identity(&checked, local_identity)?;
    }
    println!(
        "Identity and placement valid: broker {}, cluster {}. Segment stores not opened.",
        broker, checked.identity.cluster
    );
    Ok(())
}

fn runtime() -> Result<tokio::runtime::Runtime, StartupError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| StartupError::Runtime(error.to_string()))
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
