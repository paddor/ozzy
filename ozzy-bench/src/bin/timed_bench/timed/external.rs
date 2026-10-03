//! Optional comparison adapters. Shared clock, lanes, verification, and OMQ
//! process supervision; no substitute Ozzy broker or fabricated server metrics.

mod iggy;
mod kafka;

use super::super::{Result, error, metrics, workload};
use super::reader::oracle::{Oracle, Record as ObservedRecord};
use super::{Config, Window, command, config, launch, measurement::Counts, producer, report};
use bytes::Bytes;
use futures::future::try_join_all;
use ozzy_proto::MessageId;
use serde_json::{Value, json};
use tokio::sync::watch;

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
    ReplicatedPersisting,
}

impl Policy {
    pub(super) fn configuration_system(self) -> super::super::System {
        match self {
            Self::Buffered | Self::Durable => super::super::System::SingleDurable,
            Self::DiskQuorum => super::super::System::DiskQuorum,
            Self::ReplicatedPersisting => super::super::System::ReplicatedPersisting,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Durable => "durable",
            Self::DiskQuorum => "disk-quorum",
            Self::ReplicatedPersisting => "replicated-persisting",
        }
    }

    const fn durable(self) -> bool {
        matches!(self, Self::Durable | Self::DiskQuorum)
    }

    const fn brokers(self) -> usize {
        if matches!(self, Self::DiskQuorum | Self::ReplicatedPersisting) {
            3
        } else {
            1
        }
    }
}

fn policy(config: &Config) -> Policy {
    config
        .args
        .external_policy
        .expect("validated external policy")
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
    let endpoint: std::net::SocketAddr = config
        .args
        .external_endpoint
        .as_deref()
        .ok_or_else(|| error("missing external endpoint"))?
        .parse()?;
    let requested = config
        .args
        .external_policy
        .ok_or_else(|| error("missing external confirmation policy"))?;
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
    let system = config.args.external_system.unwrap();
    let topology = tokio::time::timeout(std::time::Duration::from_secs(30), async {
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
    describe(config, &name, &mut row);
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

fn describe(config: &Config, name: &str, row: &mut Value) {
    let system = config.args.external_system.unwrap();
    let policy = policy(config);
    let durable = policy.durable();
    let cluster = policy.brokers() == 3;
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
    row["confirmation_boundary"] = json!(match system {
        System::Kafka =>
            "every delivery future succeeded; acks=all, replication factor 1; OS-buffered, not fsync",
        System::Redpanda if cluster && durable =>
            "every delivery future succeeded; acks=all, replication factor 3, write.caching=false; fsync on a Raft majority of 2 of 3",
        System::Redpanda if cluster =>
            "every delivery future succeeded; acks=all, replication factor 3, write.caching=true; in memory on a Raft majority of 2 of 3, background flush",
        System::Redpanda if durable =>
            "every delivery future succeeded; acks=all, replication factor 1, write.caching=false; local fsync",
        System::Redpanda =>
            "every delivery future succeeded; acks=all, replication factor 1, write.caching=true; not fsync",
        System::Iggy if durable =>
            "Iggy persisted topic: send reply after recoverable stable-storage copies at replication quorum; singleton quorum 1, cluster quorum 2 of 3",
        System::Iggy =>
            "Iggy replicated topic: send reply after VSR commit and application; writes storage without an additional stable-storage barrier; singleton quorum 1, cluster quorum 2 of 3",
    });
    row["streaming"] = json!(system != System::Iggy);
    if system == System::Redpanda {
        row["write_caching"] = json!(!durable);
    }
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
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        match config.args.external_system.unwrap() {
            System::Kafka | System::Redpanda => kafka::cleanup(config, name).await,
            System::Iggy => iggy::cleanup(config, name).await,
        }
    })
    .await?
}

pub(super) async fn worker(config: Config) -> Result<()> {
    let reader = config.args.reader_worker.is_some();
    let index = config
        .args
        .reader_worker
        .or(config.args.producer_worker)
        .unwrap();
    let workers = if reader {
        config.args.reader_workers()
    } else {
        config.workers
    };
    let count = if reader {
        config.partitions()
    } else {
        config.writers
    };
    let lanes = workload::worker_share(count, workers, index)?;
    let mut input = launch::input();
    let connect = command(&mut input, "connect").await?;
    let name = connect["name"]
        .as_str()
        .ok_or_else(|| error("missing external topic"))?;
    let mut clients = Vec::new();
    for lane in lanes.clone() {
        clients.push(match config.args.external_system.unwrap() {
            System::Kafka | System::Redpanda => {
                Client::Kafka(kafka::Client::connect(&config, name, lane, reader)?)
            }
            System::Iggy => Client::Iggy(Box::new(
                iggy::Client::connect(&config, name, lane, reader).await?,
            )),
        });
        if !reader {
            producer::build_payload_pool(&config, lane)?;
        }
    }
    launch::reply(&json!({"event":"connected","index":index,"pid":std::process::id()}))?;
    let start = command(&mut input, "start").await?;
    let window = super::window(&start, &config.args)?;
    window.wait().await;
    let meter = metrics::Meter::start();
    let (finish, finished) = watch::channel(None);
    let work = try_join_all(clients.iter_mut().zip(lanes).map(|(client, lane)| {
        let finished = finished.clone();
        let config = &config;
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
            .map_err(|cause| {
                error(format!(
                    "external {} lane={lane}: {cause}",
                    if reader { "reader" } else { "writer" }
                ))
            })??;
            Ok(vec![row])
        }
    }));
    let completion = async {
        if !reader {
            return Ok(());
        }
        let request = command(&mut input, "finish").await?;
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
    let rows = rows.into_iter().flatten().collect::<Vec<_>>();
    report(
        &config,
        &json!({"lanes":rows,"usage":meter.finish(),"index":index,"pid":std::process::id()}),
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
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use ozzy_proto::GroupId;

    #[test]
    fn external_topology_has_adapter_bounds_without_native_claims() {
        let config = Config::new(super::super::super::Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "5",
            "--window",
            "1",
            "--external-policy",
            "disk-quorum",
            "--external-system",
            "redpanda",
            "--external-endpoint",
            "127.0.0.1:18090",
        ]))
        .unwrap();
        validate(&config).unwrap();
        let window = Window {
            start: 1,
            measured: 10,
            end: 20,
        };
        let report = |finished| {
            let mut counts = Counts::new(window);
            counts.submission(12, 1);
            counts.complete(12, finished, 1).unwrap();
            json!({"lanes": [counts.report(0)]})
        };
        let mut row = super::super::results::summarize(
            &config,
            window,
            vec![report(15)],
            vec![report(16)],
            &[],
        )
        .unwrap();
        describe(&config, "records", &mut row);
        assert_eq!(row["request_records"], config.args.request_records);
        assert_eq!(row["reader_records_max"], 1);
        assert_eq!(row["reader_payload_bytes_max"], config.args.record_bytes);
        assert_eq!(row["system"], "redpanda-disk-quorum");
        assert_eq!(row["native_client_protocol"], false);
        for field in [
            "commit_policy",
            "ack_copies",
            "replica_voters",
            "journal_writes",
            "segment_write_mode",
            "broker_io_threads",
            "disk_io_backend",
            "native_reader_api",
        ] {
            assert!(row.get(field).is_none(), "unexpected native field {field}");
        }
    }

    #[test]
    fn external_policy_sets_labels_copies_and_disk_boundary() {
        for (mode, name, brokers, durable) in [
            ("buffered", "buffered", 1, false),
            ("durable", "durable", 1, true),
            ("disk-quorum", "disk-quorum", 3, true),
            ("replicated-persisting", "replicated-persisting", 3, false),
        ] {
            let config = Config::new(super::super::super::Args::parse_from([
                "bench",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "5",
                "--external-policy",
                mode,
                "--external-system",
                "redpanda",
                "--external-endpoint",
                "127.0.0.1:18090",
            ]))
            .unwrap();
            validate(&config).unwrap();
            let mut row = json!({});
            describe(&config, "records", &mut row);
            assert_eq!(row["system"], format!("redpanda-{name}"));
            assert_eq!(row["write_caching"], !durable);
            assert_eq!(kafka::replicas(&config), brokers);
            assert_eq!(
                kafka::write_caching(&config),
                Some(if durable { "false" } else { "true" })
            );
        }
    }

    #[test]
    fn external_policies_do_not_silently_weaken_durability() {
        for (system, policy, valid) in [
            ("kafka", "buffered", true),
            ("kafka", "durable", false),
            ("iggy", "buffered", true),
            ("iggy", "durable", true),
            ("iggy", "volatile", false),
            ("iggy", "disk-quorum", true),
            ("iggy", "replicated-persisting", true),
        ] {
            let accepted = super::super::super::Args::try_parse_from([
                "bench",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "5",
                "--external-policy",
                policy,
                "--external-system",
                system,
                "--external-endpoint",
                "127.0.0.1:18090",
            ])
            .ok()
            .and_then(|args| Config::new(args).ok())
            .is_some_and(|config| validate(&config).is_ok());
            assert_eq!(accepted, valid, "{system} {policy}");
        }
    }

    #[test]
    fn external_comparisons_reject_native_queue_overrides() {
        let config = Config::new(super::super::super::Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "5",
            "--external-policy",
            "buffered",
            "--external-system",
            "iggy",
            "--external-endpoint",
            "127.0.0.1:18090",
            "--writer-inflight-appends",
            "2",
        ]))
        .unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn shared_external_verifier_rejects_offsets_identities_and_payload_corruption() {
        let config = Config::new(super::super::super::Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "5",
            "--external-policy",
            "buffered",
            "--external-system",
            "kafka",
            "--external-endpoint",
            "127.0.0.1:19092",
        ]))
        .unwrap();
        validate(&config).unwrap();
        let group = GroupId::new();
        let bytes = producer::payload(&config, 0, 0, 100).unwrap();
        let id = config::message_id(group, 0, 0);
        let verify = |record: &Record| -> Result<()> {
            let window = Window {
                start: 0,
                measured: 0,
                end: 5_000_000_000,
            };
            let mut oracle = Oracle::new(&config, config.partitions(), &[0], 0, window)?;
            oracle.observe(
                &config,
                group,
                ObservedRecord {
                    partition: 0,
                    offset: record.offset,
                    id: record.id,
                    payload: &record.payload,
                },
                200,
            )?;
            let mut writer = Counts::new(window);
            writer.record_bytes(id, &bytes);
            writer.complete(100, 200, 1)?;
            super::super::results::verify_digests(
                &[json!({"lanes":[writer.report(0)]})],
                &[json!({"lanes":oracle.rows()})],
                1,
            )
        };
        assert!(
            verify(&Record {
                offset: 0,
                id,
                payload: bytes.clone()
            },)
            .is_ok()
        );
        assert!(
            verify(&Record {
                offset: 1,
                id,
                payload: bytes.clone()
            },)
            .is_err()
        );
        assert!(
            verify(&Record {
                offset: 0,
                id: config::message_id(group, 1, 0),
                payload: bytes.clone()
            },)
            .is_err()
        );
        // Every original byte is joined through the final writer/reader digest.
        for index in 0..bytes.len() {
            let mut corrupt = bytes.to_vec();
            corrupt[index] ^= 1;
            assert!(
                verify(&Record {
                    offset: 0,
                    id,
                    payload: corrupt.into()
                },)
                .is_err()
            );
        }
    }
}
