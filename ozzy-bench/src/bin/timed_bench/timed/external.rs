//! Optional comparison adapters. Shared clock, lanes, verification, and OMQ
//! process supervision; no substitute Ozzy broker or fabricated server metrics.

mod iggy;
mod kafka;
#[cfg(test)]
mod tests;

use super::super::{Result, error, metrics, workload};
use super::reader::oracle::{Oracle, Record as ObservedRecord};
use super::{
    Config, Window, await_connect, command, config, launch, measurement::Counts, producer, report,
};
use bytes::Bytes;
use futures::future::try_join_all;
use ozzy_proto::MessageId;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// External topic provisioning or cleanup completes within this.
const ADMIN: Duration = Duration::from_secs(30);
/// Pause after an empty external read.
const EMPTY_READ_PAUSE: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum System {
    Kafka,
    Redpanda,
    Iggy,
}

impl System {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Kafka => "kafka",
            Self::Redpanda => "redpanda",
            Self::Iggy => "iggy",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Policy {
    Buffered,
    Durable,
    DiskQuorum,
}

impl Policy {
    pub(super) fn configuration_system(self) -> super::super::System {
        match self {
            Self::Buffered | Self::Durable => super::super::System::SingleDurable,
            Self::DiskQuorum => super::super::System::DiskQuorum,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Durable => "durable",
            Self::DiskQuorum => "disk-quorum",
        }
    }

    const fn durable(self) -> bool {
        matches!(self, Self::Durable | Self::DiskQuorum)
    }

    const fn brokers(self) -> usize {
        if matches!(self, Self::DiskQuorum) {
            3
        } else {
            1
        }
    }
}

/// The external system of a comparison run.
fn system(config: &Config) -> Result<System> {
    config
        .args
        .external_system
        .ok_or_else(|| error("missing external system"))
}

/// The confirmation policy of a comparison run.
fn policy(config: &Config) -> Result<Policy> {
    config
        .args
        .external_policy
        .ok_or_else(|| error("missing external confirmation policy"))
}

/// The external broker endpoint of a comparison run.
fn endpoint(config: &Config) -> Result<&str> {
    config
        .args
        .external_endpoint
        .as_deref()
        .ok_or_else(|| error("missing external endpoint"))
}

pub(super) fn diagnostics() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init()?;
    Ok(())
}

pub(super) fn validate(config: &Config) -> Result<()> {
    let endpoint: std::net::SocketAddr = endpoint(config)?.parse()?;
    let requested = policy(config)?;
    let policy_supported = match config.args.external_system {
        Some(System::Kafka) => requested == Policy::Buffered,
        Some(System::Iggy | System::Redpanda) => true,
        None => false,
    };
    if endpoint.ip().is_unspecified()
        || endpoint.ip().is_multicast()
        || endpoint.port() == 0
        || !policy_supported
        || config.args.readers_per_partition != 1
        || config.args.disk_workers != 1
        || config.args.disk_owner_threads != 1
        || !config.args.direct_io
        || config.args.io_backend != super::super::IoBackend::Aio
        || config.args.aio_depth != 1
        || config.args.read_depth != 8
        || config.args.broker_hwm != 8192
        || config.args.storage_lane_capacity != 1024
        || config.args.writer_inflight_appends != 3
        || config.args.app_threads != 1
        || config.args.worker_index.is_some()
        || config.args.placements.is_some()
    {
        return Err(error(
            "external comparisons require a concrete test server, supported confirmation policy, and no compression",
        ));
    }
    Ok(())
}

pub(super) async fn coordinate(
    config: &Config,
    writers: &mut [launch::Process],
    readers: &mut [launch::Process],
) -> Result<Value> {
    let name = format!("ozzy-bench-{}", uuid::Uuid::now_v7().simple());
    let system = system(config)?;
    let topology = tokio::time::timeout(ADMIN, async {
        match system {
            System::Kafka | System::Redpanda => kafka::provision(config, &name).await,
            System::Iggy => iggy::provision(config, &name).await,
        }
    })
    .await??;
    let connect = json!({"command":"connect", "name":name});
    if system == System::Iggy {
        // Registration mutates the metadata consensus group. Complete setup
        // one client at a time; the shared start barrier still starts all timed
        // writer/reader work together. Connection storms are a separate test.
        for process in writers.iter_mut().chain(readers.iter_mut()) {
            let one = std::slice::from_mut(process);
            super::send_all(one, &connect)?;
            super::receive_all(one, "connected").await?;
        }
    } else {
        super::send_all(writers, &connect)?;
        super::send_all(readers, &connect)?;
        super::receive_all(writers, "connected").await?;
        super::receive_all(readers, "connected").await?;
    }
    let (window, writers, readers) = super::measure(config, &mut [], writers, readers).await?;
    let mut row = super::results::summarize(config, window, writers, readers, &[])?;
    if let Some(path) = &config.args.external_storage_dir {
        // All counts/digests are validated and all timing windows have ended.
        // Drain background writes before unrelated durable metadata logout.
        let output = tokio::process::Command::new("sync")
            .arg("-f")
            .arg(path)
            .output()
            .await?;
        if !output.status.success() || !output.stderr.is_empty() {
            return Err(error(format!(
                "external teardown sync: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        row["teardown_storage_sync"] = json!({"path":path,"phase":"after measured traffic and complete reader verification, before logout"});
    }
    describe(config, &name, &mut row)?;
    for key in [
        "logical_stream_count",
        "topic_count",
        "partitions_per_topic",
        "partition_count",
    ] {
        row[key] = topology[key].clone();
    }
    row["effective_topic_topology"] = topology;
    Ok(row)
}

fn topic_topology(topics: usize, partitions: usize) -> Value {
    super::results::topic_topology(topics, partitions)
}

pub(super) fn topology(config: &Config) -> Value {
    json!({
        "producer_workers": config.workers,
        "reader_workers": config.args.reader_workers(),
        "producer_writers": config.writers,
        "request_records": config.args.request_records,
        "reader_records_max": config.reader_records(),
        "reader_payload_bytes_max": config.reader_limits().envelope.max_payload_bytes,
        "control_transport": if config.args.control.control_bind.is_some() {
            "explicit TCP"
        } else {
            "Linux abstract IPC"
        },
        "allocation_counted": cfg!(feature = "allocation-counting"),
        "profiled": std::env::var_os("OZZY_BENCH_PROFILE").is_some()
            || std::env::var_os("OZZY_BENCH_HEAPTRACK").is_some(),
        "profile_kind": std::env::var("OZZY_BENCH_PROFILE").ok(),
    })
}

fn describe(config: &Config, name: &str, row: &mut Value) -> Result<()> {
    let system = system(config)?;
    let policy = policy(config)?;
    row["system"] = json!(format!("{}-{}", system.name(), policy.name()));
    row["native_client_protocol"] = json!(false);
    row["writer_protocol"] = json!(if system == System::Iggy {
        "iggy-send-messages"
    } else {
        "kafka-records"
    });
    row["client_application_threads_per_process"] = json!(1);
    row["external_topic"] = json!(name);
    row["partition_assignment"] = json!(
        "writer lane modulo declared partition count; shared partition offsets and independent writer order"
    );
    row["topic_organization"] = json!(if system == System::Iggy {
        "one Iggy stream namespace containing one records topic"
    } else {
        "one Kafka-protocol topic"
    });
    row["transport"] = json!(format!("{} TCP", system.name()));
    row["reader_transport"] = json!(if system == System::Iggy {
        "explicit partition offset polling; concurrent full verification; 1 ms timer after an empty poll"
    } else {
        "librdkafka StreamConsumer; explicit partition assignment from offset zero; no group rebalancing; concurrent full verification"
    });
    row["placement"] = json!(
        "external broker deployment; independent writer/reader processes; server CPU and topology recorded separately"
    );
    row["confirmation_boundary"] = json!(confirmation_boundary(system, policy));
    row["streaming"] = json!(system != System::Iggy);
    if system == System::Redpanda {
        row["write_caching"] = json!(!policy.durable());
    }
    describe_client(config, system, row);
    Ok(())
}

/// What a confirmed send proves for each system and policy.
fn confirmation_boundary(system: System, policy: Policy) -> &'static str {
    let durable = policy.durable();
    let cluster = policy.brokers() == 3;
    match system {
        System::Kafka => {
            "every delivery future succeeded; acks=all, replication factor 1; OS-buffered, not fsync"
        }
        System::Redpanda if cluster => {
            "every delivery future succeeded; acks=all, replication factor 3, write.caching=false; fsync on a Raft majority of 2 of 3"
        }
        System::Redpanda if durable => {
            "every delivery future succeeded; acks=all, replication factor 1, write.caching=false; local fsync"
        }
        System::Redpanda => {
            "every delivery future succeeded; acks=all, replication factor 1, write.caching=true; not fsync"
        }
        System::Iggy if durable => {
            "Iggy persisted topic: send reply after recoverable stable-storage copies at replication quorum; singleton quorum 1, cluster quorum 2 of 3"
        }
        System::Iggy => {
            "Iggy replicated topic: send reply after VSR commit and application; writes storage without an additional stable-storage barrier; singleton quorum 1"
        }
    }
}

/// The client SDK settings, request shape and retry guarantee.
fn describe_client(config: &Config, system: System, row: &mut Value) {
    if system != System::Iggy {
        row["reader_records_max"] = json!(1);
        row["reader_payload_bytes_max"] = json!(config.args.record_bytes);
        row["client_sdk"] = json!({"implementation":"rust-rdkafka 0.38 / compiled librdkafka",
            "acks":"all","idempotence":true,"compression":"none","linger_ms":0,
            "batch_records_max":config.args.request_records,"batch_bytes_max":4 * 1024 * 1024,
            "unconfirmed_records_per_writer":config.args.request_records,
            "max_partition_fetch_bytes":8 * 1024 * 1024,
            "submission":"individual records; SDK batches without intentional delay"});
    }
    if system == System::Iggy {
        row["client_registration"] = json!("serial, before the shared measurement start barrier");
        row["outstanding_requests_per_connection"] = json!(1);
        row["request_concurrency_boundary"] =
            json!("Iggy TCP SDK holds its connection lock until each send reply");
    }
    row["records_per_append"] = json!(if system == System::Iggy {
        config.args.request_records
    } else {
        1
    });
    row["records_per_submission"] = row["records_per_append"].clone();
    row["retry_guarantee"] = json!(match system {
        System::Kafka | System::Redpanda =>
            "idempotent SDK producer; every delivered offset and final record identity verified",
        System::Iggy =>
            "no benchmark retries; errors invalidate run; not native Ozzy retry guarantees",
    });
}

pub(super) async fn cleanup(config: &Config, name: &str) -> Result<()> {
    let system = system(config)?;
    tokio::time::timeout(ADMIN, async {
        match system {
            System::Kafka | System::Redpanda => kafka::cleanup(config, name).await,
            System::Iggy => iggy::cleanup(config, name).await,
        }
    })
    .await?
}

pub(super) async fn worker(config: Config) -> Result<()> {
    let share = Share::of(&config)?;
    let mut input = launch::input()?;
    let connect = await_connect(&mut input).await?;
    let name = connect["name"]
        .as_str()
        .ok_or_else(|| error("missing external topic"))?;
    let mut clients = connect_clients(&config, name, &share).await?;
    launch::reply(&json!({"event":"connected","index":share.index,"pid":std::process::id()}))?;
    let start = command(&mut input, "start").await?;
    let window = super::window(&start, &config.args)?;
    window.wait().await;
    let meter = metrics::Meter::start(!config.args.inproc);
    let rows = run_lanes(&config, &share, &mut clients, window, &mut input).await?;
    report(
        &config,
        &json!({"lanes":rows,"usage":meter.finish(),"index":share.index,"pid":std::process::id()}),
    )?;
    if input.recv().await.is_some() {
        return Err(error("unexpected external worker command"));
    }
    // Parent stops workers one at a time after every reader has verified its
    // complete stream. Keep connections alive until that untimed teardown.
    for client in &clients {
        if let Client::Iggy(client) = client {
            client.close().await?;
        }
    }
    Ok(())
}

/// One external worker: its role, index, and the writer lanes or reader
/// partitions it serves.
struct Share {
    reader: bool,
    index: usize,
    lanes: std::ops::Range<usize>,
}

impl Share {
    fn of(config: &Config) -> Result<Self> {
        let reader = config.args.reader_worker.is_some();
        let index = config
            .args
            .reader_worker
            .or(config.args.producer_worker)
            .ok_or_else(|| error("external worker needs a reader or producer index"))?;
        let (count, workers) = if reader {
            (config.partitions(), config.args.reader_workers())
        } else {
            (config.writers, config.workers)
        };
        Ok(Self {
            reader,
            index,
            lanes: workload::worker_share(count, workers, index)?,
        })
    }
}

/// One client per lane; writer lanes also build their body pools.
async fn connect_clients(config: &Config, name: &str, share: &Share) -> Result<Vec<Client>> {
    let system = system(config)?;
    let mut clients = Vec::new();
    for lane in share.lanes.clone() {
        clients.push(match system {
            System::Kafka | System::Redpanda => {
                Client::Kafka(kafka::Client::connect(config, name, lane, share.reader)?)
            }
            System::Iggy => Client::Iggy(Box::new(
                iggy::Client::connect(config, name, lane, share.reader).await?,
            )),
        });
        if !share.reader {
            producer::build_payload_pool(config, lane)?;
        }
    }
    Ok(clients)
}

/// Run every lane from the shared start. Writers stop at the deadline;
/// readers once they cover the final writer counts the coordinator sends.
async fn run_lanes(
    config: &Config,
    share: &Share,
    clients: &mut [Client],
    window: Window,
    input: &mut mpsc::Receiver<Result<Value>>,
) -> Result<Vec<Value>> {
    let reader = share.reader;
    let (finish, finished) = watch::channel(None);
    let work = try_join_all(
        clients
            .iter_mut()
            .zip(share.lanes.clone())
            .map(|(client, lane)| {
                let finished = finished.clone();
                async move {
                    if reader {
                        return read(client, config, lane, window, finished).await;
                    }
                    let row = tokio::time::timeout_at(window.deadline(&config.args), async {
                        match client {
                            Client::Kafka(client) => client.write(config, lane, window).await,
                            Client::Iggy(client) => (*client).write(config, lane, window).await,
                        }
                    })
                    .await
                    .map_err(|cause| error(format!("external writer lane={lane}: {cause}")))??;
                    Ok(vec![row])
                }
            }),
    );
    let completion = async {
        if !reader {
            return Ok(());
        }
        let request = command(input, "finish").await?;
        let totals: Vec<u64> = serde_json::from_value(request["counts"].clone())?;
        if totals.len() != config.writers {
            return Err(error("invalid final writer counts"));
        }
        finish
            .send(Some(totals))
            .map_err(|_| error("external readers exited early"))?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (rows, ()) = tokio::try_join!(work, completion)?;
    Ok(rows.into_iter().flatten().collect())
}

enum Client {
    Kafka(kafka::Client),
    Iggy(Box<iggy::Client>),
}

struct Record {
    offset: u64,
    id: MessageId,
    payload: Bytes,
}

async fn read(
    client: &mut Client,
    config: &Config,
    partition: usize,
    window: Window,
    mut finished: watch::Receiver<Option<Vec<u64>>>,
) -> Result<Vec<Value>> {
    let group = config::group(&config.args)?;
    let mut oracle = Oracle::new(config, config.partitions(), &[partition as u32], 0, window)?;
    let mut offset = 0;
    tokio::time::timeout_at(window.deadline(&config.args), async {
        loop {
            let target = finished.borrow().clone();
            if let Some(target) = &target
                && oracle.complete(target)?
            {
                break;
            }
            let records = match &mut *client {
                Client::Kafka(client) => tokio::select! {
                    records = client.read() => records?,
                    changed = finished.changed(), if target.is_none() => {
                        changed.map_err(|_| error("final counts unavailable"))?;
                        continue;
                    }
                },
                // An Iggy RPC is not canceled mid-frame when final counts arrive.
                Client::Iggy(client) => client.read(offset).await?,
            };
            if records.is_empty() {
                tokio::time::sleep(EMPTY_READ_PAUSE).await;
            }
            // One clock per poll reply, as in the native reader lane.
            let complete = metrics::monotonic_ns();
            for record in records {
                oracle.observe(
                    config,
                    group,
                    ObservedRecord {
                        partition: partition as u32,
                        offset: record.offset,
                        id: record.id,
                        payload: &record.payload,
                    },
                    complete,
                )?;
                offset += 1;
                if matches!(client, Client::Kafka(_)) {
                    kafka::cooperate(offset).await;
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await
    .map_err(|cause| {
        error(format!(
            "external reader partition={partition} verified={offset} pending={:?}: {cause}",
            oracle.pending(finished.borrow().as_deref()),
        ))
    })??;
    Ok(oracle.rows())
}
