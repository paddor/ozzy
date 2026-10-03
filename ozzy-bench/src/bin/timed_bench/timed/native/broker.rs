//! Production broker lifecycle under the existing bounded process supervisor.

use super::super::{Config, command, launch, measurement::BrokerMeter};
use crate::bench::{Result, error};
use ozzy_bench::native::{BrokerWorker, PreparedBroker};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::mpsc;

pub(in crate::bench::timed) async fn run(config: Config) -> Result<()> {
    let index = config
        .args
        .worker_index
        .filter(|&index| index < config.args.system.brokers())
        .ok_or_else(|| error("invalid production broker index"))?;
    let mut input = launch::input();
    let (mut resources, worker) = prepare(&config, index, &mut input).await?;
    command(&mut input, "serve").await?;
    resources.release_endpoints();
    let broker = worker
        .start_trusted_with_context(ozzy_bench::control::context())
        .await
        .map_err(|cause| error(format!("production broker startup: {cause}")))?;
    let result = tokio::select! {
        result = measure(&config, &worker, &broker, &mut input) => result,
        result = broker.closed() => {
            result?;
            Err(error("production broker stopped before drain"))
        },
    };
    // Errors and a closed command channel still request and observe physical drain.
    let drained: Result<()> = tokio::time::timeout(
        Duration::from_secs(config.args.drain_timeout_secs),
        broker.shutdown(),
    )
    .await
    .map_err(|cause| error(format!("production broker drain: {cause}")))?
    .map_err(Into::into);
    let (mut meter, identity) = result?;
    drained?;
    let mut usage = meter
        .finish()
        .map_err(|cause| error(format!("production broker drained usage: {cause}")))?;
    // These process-global counters belong to the legacy journal workers.
    // Production persistence observations come from the actual owners.
    usage.as_object_mut().unwrap().remove("journal_reads");
    usage.as_object_mut().unwrap().remove("journal_writes");
    launch::reply(&json!({
        "event": "drained", "identity": identity,
        "usage": usage,
        "drain_boundary": "partition journals and broker sockets closed; shared device workers settled and stopped",
    }))?;
    // Preserve the supervisor's normal completion handshake after its report.
    // No running broker remains while the controller closes this channel.
    if let Some(request) = input.recv().await {
        request?;
        return Err(error("production broker command after drain"));
    }
    Ok(())
}

async fn prepare(
    config: &Config,
    index: usize,
    input: &mut mpsc::Receiver<Result<Value>>,
) -> Result<(PreparedBroker, BrokerWorker)> {
    command(input, "prepare").await?;
    let resources = PreparedBroker::new(
        &config.args.storage_dir,
        index,
        config.args.bind,
        config.args.system.brokers(),
    )?;
    launch::reply(&json!({
        "event": "prepared", "index": index, "pid": std::process::id(),
        "broker": format!("broker-{index}"), "directory": resources.directory(),
        "root": resources.root(), "endpoints": {
            "peer": resources.endpoints().peer, "reader_pub": resources.endpoints().reader_pub,
            "follower_pub": resources.endpoints().follower_pub,
        },
        "storage": ozzy_bench::placement::storage(&config.args.storage_dir)?,
        "execution": ozzy_bench::placement::execution()?,
        "executable_xxh3_128": ozzy_bench::provenance::digest(&std::env::current_exe()?)?,
    }))?;
    let request = command(input, "initialize").await?;
    let text = |name| {
        request[name]
            .as_str()
            .ok_or_else(|| error("missing shared deployment record"))
    };
    let worker = resources.install(text("configuration")?, text("identity")?)?;
    worker
        .initialize()
        .await
        .map_err(|cause| error(format!("production broker initialize: {cause}")))?;
    launch::reply(&json!({"event": "initialized", "index": index,
        "configuration_xxh3_128": ozzy_bench::provenance::digest(&worker.deployment.configuration)?,
        "identity_xxh3_128": ozzy_bench::provenance::digest(&worker.deployment.identity)?,
    }))?;
    Ok((resources, worker))
}

async fn measure(
    config: &Config,
    worker: &BrokerWorker,
    broker: &ozzy_broker::Broker,
    input: &mut mpsc::Receiver<Result<Value>>,
) -> Result<(BrokerMeter, Value)> {
    let identity = ready(config, worker, broker)
        .map_err(|cause| error(format!("production broker ready: {cause}")))?;
    launch::reply(&identity)?;
    command(input, "start").await?;
    let mut meter = BrokerMeter::default();
    meter.start()?;
    command(input, "drain").await?;
    Ok((meter, identity))
}

fn ready(config: &Config, worker: &BrokerWorker, broker: &ozzy_broker::Broker) -> Result<Value> {
    let checked = worker.deployment.checked(&worker.broker)?;
    let local = ozzy_broker::load_broker_identity(&checked, &worker.identity)?;
    let configured = &checked.deployment.deployment().brokers[&worker.broker];
    let execution = ozzy_bench::placement::execution()?;
    let threads = execution["threads"]
        .as_array()
        .ok_or_else(|| error("missing production broker thread observation"))?;
    let count = |matches: fn(&str) -> bool| {
        threads
            .iter()
            .filter(|thread| thread["name"].as_str().is_some_and(matches))
            .count()
    };
    Ok(json!({
        "event": "ready", "index": config.args.worker_index,
        "pid": std::process::id(), "broker": worker.broker, "node": local.broker,
        "endpoint": configured.endpoints.peer,
        "reader_publication": configured.endpoints.reader_pub,
        "follower_publication": configured.endpoints.follower_pub,
        "host": std::fs::read_to_string("/proc/sys/kernel/hostname")?.trim(),
        "executable_xxh3_128": ozzy_bench::provenance::digest(&std::env::current_exe()?)?,
        "storage": configured.devices.iter().map(|(name, device)| {
            Ok(json!({"device": name, "root": device.root,
                "placement": ozzy_bench::placement::storage(device.root.parent()
                    .ok_or_else(|| error("production device root has no parent"))?)?}))
        }).collect::<Result<Vec<_>>>()?,
        "topology": {
            "application_threads": broker.application_threads(),
            "dispatcher_threads": broker.dispatcher_threads(),
            "omq_io_threads": broker.io_threads(),
            "observed_threads": {
                "application": count(|name| name.starts_with("ozzy_app-")),
                "dispatcher": count(|name| name == "ozzy_dispatch"),
                "omq_io": count(|name| name.starts_with("ozy/omq/IO/")),
                "omq_control": count(|name| name == "ozy/omq/Control"),
                "backend": count(|name| name.starts_with("ozzy_io-")),
                "process_total": threads.len(),
            },
            "controllers": checked.plan.controllers.iter().map(|controller| {
                let workers = &controller.workers;
                json!({"name": controller.name, "shards": controller.shards,
                    "workers": {
                        "backend": match workers.backend {
                            ozzy_config::IoBackend::Aio => "aio",
                            ozzy_config::IoBackend::Pool => "pool",
                        },
                        "write_threads": workers.write_threads, "cpus": workers.cpus,
                        "progress_cpu": workers.progress_cpu, "aio_cpu": workers.aio_cpu,
                        "aio_depth": workers.aio_depth, "max_inflight": workers.max_inflight,
                        "queued_jobs": workers.queued_jobs, "queued_bytes": workers.queued_bytes,
                        "progress_jobs": workers.progress_jobs, "progress_bytes": workers.progress_bytes,
                        "open_handles": workers.open_handles,
                    }})
            }).collect::<Vec<_>>(),
            "partitions": checked.plan.partitions.iter().map(|partition|
                json!({"topic": partition.topic, "partition": partition.partition,
                    "shard": partition.shard, "device": partition.device,
                    "directory": partition.directory})).collect::<Vec<_>>(),
        },
        "execution": execution,
    }))
}
