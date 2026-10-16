//! Production broker lifecycle under the existing bounded process supervisor.

use super::super::{Config, command, command_with_timeout, launch, measurement::BrokerMeter};
use crate::bench::{Result, error};
use ozzy_bench::native::{BrokerWorker, PreparedBroker};
use ozzy_io::simulation::Image;
use ozzy_sim::broker::Memory;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::mpsc;

pub(in crate::bench::timed) async fn run(config: Config) -> Result<()> {
    let index = config
        .args
        .worker_index
        .filter(|&index| index < config.args.system.brokers())
        .ok_or_else(|| error("invalid production broker index"))?;
    let mut input = launch::input()?;
    let (mut resources, worker, memory) = prepare(&config, index, &mut input).await?;
    command(&mut input, "serve").await?;
    resources.release_endpoints();
    let (broker, image) = start(&worker, memory).await?;
    let mut result = tokio::select! {
        result = measure(&config, &worker, &broker, &mut input) => result,
        result = broker.closed() => {
            result?;
            Err(error("production broker stopped before drain"))
        },
    };
    // Inproc broker threads exit during shutdown; meter them first.
    let inproc_usage = match &mut result {
        Ok((meter, _)) if config.args.inproc => Some(meter.finish()),
        _ => None,
    };
    let memory = crate::bench::metrics::memory_snapshot();
    #[cfg(feature = "comparisons")]
    let live = ozzy_runtime::profiling::totals();
    // Errors and a closed command channel still request and observe physical drain.
    let drained: Result<()> = tokio::time::timeout(
        Duration::from_secs(config.args.drain_timeout_secs),
        broker.shutdown(),
    )
    .await
    .map_err(|cause| error(format!("production broker drain: {cause}")))?
    .map_err(Into::into);
    let (meter, identity) = result?;
    drained?;
    if let Some(image) = image {
        // The pump returns the final image after physical drain; drop it here.
        image
            .await
            .map_err(|cause| error(format!("memory broker storage drain: {cause}")))?;
    }
    let mut usage = drained_usage(&config, inproc_usage, meter).await?;
    annotate_memory(&mut usage, &config, &memory);
    #[cfg(feature = "comparisons")]
    report_record_deliveries(&mut usage, &live);
    if config.args.inproc {
        scope_process_counters(&mut usage, index);
    }
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

/// The broker's usage after drain: metered before shutdown when inproc,
/// otherwise once its own threads are gone.
async fn drained_usage(
    config: &Config,
    inproc_usage: Option<Result<Value>>,
    mut meter: BrokerMeter,
) -> Result<Value> {
    if let Some(usage) = inproc_usage {
        return usage.map_err(|cause| error(format!("inproc broker usage: {cause}")));
    }
    let execution = tokio::time::timeout(
        Duration::from_secs(config.args.drain_timeout_secs),
        observe_drained_threads(),
    )
    .await
    .map_err(|cause| error(format!("production broker thread drain: {cause}")))??;
    let mut usage = meter
        .finish()
        .map_err(|cause| error(format!("production broker drained usage: {cause}")))?;
    usage["execution"] = execution;
    Ok(usage)
}

/// Memory taken after workload drain and before shutdown, with its scope.
fn annotate_memory(usage: &mut Value, config: &Config, memory: &Value) {
    usage["rss_bytes"] = memory["rss_bytes"].clone();
    usage["memory_scope"] = json!(if config.args.inproc {
        "inproc process shared by every broker and client role: RSS after workload drain, before broker shutdown; peak RSS over process lifetime including setup; Linux procfs"
    } else {
        "RSS after workload drain, before broker shutdown; peak RSS over process lifetime including setup; Linux procfs"
    });
    // Legacy resident-selection counters omit production live publication.
    remove_fields(usage, &["journal_reads"]);
}

/// Counters are process globals inproc; only broker-0 reports their total.
fn scope_process_counters(usage: &mut Value, index: usize) {
    if index != 0 {
        remove_fields(
            usage,
            &[
                "journal_writes",
                "group_progress",
                "dedup_index",
                "record_deliveries",
            ],
        );
    }
    usage["process_counter_scope"] = json!(
        "inproc: journal, delivery, progress, and dedup counters are process-wide and reported by broker-0 only"
    );
}

/// Drop report fields this broker does not own.
fn remove_fields(usage: &mut Value, keys: &[&str]) {
    if let Some(fields) = usage.as_object_mut() {
        for key in keys {
            fields.remove(*key);
        }
    }
}

#[cfg(feature = "comparisons")]
fn report_record_deliveries(usage: &mut Value, live: &ozzy_runtime::profiling::Totals) {
    use ozzy_runtime::profiling::Event;
    let counters = ozzy_runtime::profiling::totals();
    let (live_bytes, live_entries) = (
        live.get(Event::DedupLiveBytes),
        live.get(Event::DedupLiveEntries),
    );
    usage["dedup_index"] = json!({
        "installed_images": counters.get(Event::DedupIndexImage),
        "installed_reserved_bytes": counters.get(Event::DedupIndexBytes),
        "installed_configured_entries": counters.get(Event::DedupIndexEntries),
        "reserved_bytes": live_bytes,
        "configured_entries": live_entries,
        "bytes_per_configured_entry": if live_entries == 0 { Value::Null } else { json!(live_bytes as f64 / live_entries as f64) },
        "scope":"live installed index images before broker shutdown; table, queue, and scratch allocations; excludes returned outcomes and SDK payloads; installed_* counters are cumulative including recovery rebuilds",
    });
    usage["record_deliveries"] = json!({
        "scope": "process lifetime, including warmup, measurement, and drain",
        "background_resident_reads": counters.get(Event::BackgroundResidentRead),
        "direct_resident_reads": counters.get(Event::DirectResidentRead),
        "historical_read_deliveries": counters.get(Event::HistoricalReadDelivery),
        "shared_reader_bytes": counters.get(Event::SharedReaderBytes),
        "copied_reader_bytes": counters.get(Event::CopiedReaderBytes),
        "prepared_operations": counters.get(Event::PreparedOperation),
        "prepared_payload_bytes_max": counters.get(Event::PreparedPayloadBytesMax),
    });
    usage["publications"] = json!({
        "scope": "process lifetime; frames a full PUB refused and sent once it had space, not per-subscriber drops",
        "reader_frames": counters.get(Event::ReaderPublication),
        "reader_send_waits": counters.get(Event::ReaderPublicationSendWait),
        "follower_send_waits": counters.get(Event::ReplicaPublicationSendWait),
    });
}

/// pthread join can finish before Linux removes its final proc task entry.
/// Report physical drain only after those entries disappear. A surviving
/// worker still fails the caller's bounded deadline.
async fn observe_drained_threads() -> Result<Value> {
    loop {
        let execution = ozzy_bench::placement::execution()?;
        let owned = execution["threads"]
            .as_array()
            .ok_or_else(|| error("missing broker thread observation"))?
            .iter()
            .any(|thread| {
                thread["name"].as_str().is_some_and(|name| {
                    name.starts_with("ozzy/shard/")
                        || name == "ozzy/dispatch"
                        || name.starts_with("ozzy/disk/")
                })
            });
        if !owned {
            return Ok(execution);
        }
        tokio::task::yield_now().await;
    }
}

async fn start(
    worker: &BrokerWorker,
    memory: Option<Memory>,
) -> Result<(ozzy_broker::Broker, Option<tokio::task::JoinHandle<Image>>)> {
    let context = ozzy_bench::control::context();
    let Some(memory) = memory else {
        let broker = worker
            .start_trusted_with_context(context)
            .await
            .map_err(|cause| error(format!("production broker startup: {cause}")))?;
        return Ok((broker, None));
    };
    let checked = worker.deployment.checked(&worker.broker)?;
    let local = ozzy_broker::load_broker_identity(&checked, &worker.identity)?;
    let (broker, _, image) =
        ozzy_sim::broker::start_image_sized(context, checked, local, &[], memory)
            .await
            .map_err(|cause| error(format!("memory broker startup: {cause}")))?;
    Ok((broker, Some(image)))
}

/// Format the broker's journals through production jobs into a fresh image.
async fn provision(config: &Config, worker: &BrokerWorker) -> Result<Memory> {
    let checked = worker.deployment.checked(&worker.broker)?;
    let local =
        ozzy_broker::initialize_broker_identity(&checked, &worker.identity, uuid::Uuid::now_v7)?;
    let limits = config.memory_image()?;
    let image = Box::pin(ozzy_sim::broker::provision_sized(
        &checked, &local, false, limits,
    ))
    .await;
    Ok(Memory {
        image,
        limits,
        reverse: false,
    })
}

/// An inproc broker observes only threads sharing its role's CPU mask.
fn execution(config: &Config) -> Result<Value> {
    Ok(if config.args.inproc {
        ozzy_bench::placement::mask_execution()?
    } else {
        ozzy_bench::placement::execution()?
    })
}

async fn prepare(
    config: &Config,
    index: usize,
    input: &mut mpsc::Receiver<Result<Value>>,
) -> Result<(PreparedBroker, BrokerWorker, Option<Memory>)> {
    command(input, "prepare").await?;
    let brokers = config.args.system.brokers();
    let resources = if config.args.inproc {
        PreparedBroker::inproc(&config.args.storage_dir, index, brokers)?
    } else {
        PreparedBroker::with_transport(
            &config.args.storage_dir,
            index,
            config.args.bind,
            brokers,
            config.args.transport,
        )?
    };
    launch::reply(&json!({
        "event": "prepared", "index": index, "pid": std::process::id(),
        "broker": format!("broker-{index}"), "directory": resources.directory(),
        "root": resources.root(), "endpoints": {
            "peer": resources.endpoints().peer, "data_peer": resources.endpoints().data_peer, "reader_pub": resources.endpoints().reader_pub,
            "follower_pub": resources.endpoints().follower_pub,
        },
        "storage": ozzy_bench::placement::storage(&config.args.storage_dir)?,
        "execution": execution(config)?,
        "executable_xxh3_128": ozzy_bench::provenance::digest(&std::env::current_exe()?)?,
    }))?;
    let request = command(input, "initialize").await?;
    let text = |name| {
        request[name]
            .as_str()
            .ok_or_else(|| error("missing shared deployment record"))
    };
    let worker = resources.install(text("configuration")?, text("identity")?)?;
    let memory = if config.args.memory_storage {
        Some(provision(config, &worker).await?)
    } else {
        worker
            .initialize()
            .await
            .map_err(|cause| error(format!("production broker initialize: {cause}")))?;
        None
    };
    launch::reply(&json!({"event": "initialized", "index": index,
        "configuration_xxh3_128": ozzy_bench::provenance::digest(&worker.deployment.configuration)?,
        "identity_xxh3_128": ozzy_bench::provenance::digest(&worker.deployment.identity)?,
    }))?;
    Ok((resources, worker, memory))
}

async fn measure(
    config: &Config,
    worker: &BrokerWorker,
    broker: &ozzy_broker::Broker,
    input: &mut mpsc::Receiver<Result<Value>>,
) -> Result<(BrokerMeter, Value)> {
    // Startup returns before partition recovery and leader bootstrap finish.
    tokio::time::timeout(ozzy_bench::budget::CONTROL_STEP, broker.ready())
        .await
        .map_err(|cause| error(format!("production broker serving: {cause}")))?
        .map_err(|cause| error(format!("production broker serving: {cause}")))?;
    let identity = ready(config, worker, broker)
        .map_err(|cause| error(format!("production broker ready: {cause}")))?;
    launch::reply(&identity)?;
    command(input, "start").await?;
    let mut meter = BrokerMeter::default();
    meter.start(config.args.inproc)?;
    // The controller waits for writer and reader reports before requesting
    // drain, within its own watchdog.
    command_with_timeout(input, "drain", config.args.budget().watchdog()).await?;
    Ok((meter, identity))
}

fn ready(config: &Config, worker: &BrokerWorker, broker: &ozzy_broker::Broker) -> Result<Value> {
    let checked = worker.deployment.checked(&worker.broker)?;
    let local = ozzy_broker::load_broker_identity(&checked, &worker.identity)?;
    let configured = &checked.deployment.deployment().brokers[&worker.broker];
    let execution = execution(config)?;
    let threads = execution["threads"]
        .as_array()
        .ok_or_else(|| error("missing production broker thread observation"))?;
    Ok(json!({
        "event": "ready", "index": config.args.worker_index,
        "pid": std::process::id(), "broker": worker.broker, "node": local.broker,
        "data_endpoint": configured.endpoints.data_peer, "endpoint": configured.endpoints.peer,
        "reader_publication": configured.endpoints.reader_pub,
        "follower_publication": configured.endpoints.follower_pub,
        "host": std::fs::read_to_string("/proc/sys/kernel/hostname")?.trim(),
        "executable_xxh3_128": ozzy_bench::provenance::digest(&std::env::current_exe()?)?,
        "storage": storage_placements(config, configured)?,
        "topology": topology(broker, &checked, threads),
        "execution": execution,
    }))
}

/// Where each configured device keeps its files.
fn storage_placements(config: &Config, configured: &ozzy_config::Broker) -> Result<Vec<Value>> {
    configured
        .devices
        .iter()
        .map(|(name, device)| {
            let placement = if config.args.memory_storage {
                json!({"medium": "memory image"})
            } else {
                ozzy_bench::placement::storage(
                    device
                        .root
                        .parent()
                        .ok_or_else(|| error("production device root has no parent"))?,
                )?
            };
            Ok(json!({"device": name, "root": device.root, "placement": placement}))
        })
        .collect()
}

/// Configured threads, workers and partitions next to the threads observed.
fn topology(
    broker: &ozzy_broker::Broker,
    checked: &ozzy_broker::CheckedConfig,
    threads: &[Value],
) -> Value {
    let count = |matches: fn(&str) -> bool| {
        threads
            .iter()
            .filter(|thread| thread["name"].as_str().is_some_and(matches))
            .count()
    };
    json!({
            "application_threads": broker.application_threads(),
            "dispatcher_threads": broker.dispatcher_threads(),
            "omq_io_threads": broker.io_threads(),
            "observed_threads": {
                "application": count(|name| name.starts_with("ozzy/shard/")),
                "dispatcher": count(|name| name == "ozzy/dispatch"),
                // Published OMQ 0.25 shortens the prefix for multiple IO threads.
                "omq_io": count(|name| name.starts_with("ozzy/omq/IO/")
                    || name.starts_with("ozzy/om/IO/")),
                "omq_control": count(|name| matches!(name,
                    "ozzy/omq/Ctrl" | "ozzy/om/Control")),
                "backend": count(|name| name.starts_with("ozzy/disk/")),
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
    })
}
