//! Labels derived from production owners and the actual shared SDK adapters.

use super::{Config, Result, error};
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub(in crate::bench::timed) fn topology(config: &Config, brokers: &[Value]) -> Result<Value> {
    let settings = config
        .native
        .as_ref()
        .ok_or_else(|| error("missing production topology"))?;
    let observed = |name: &str| -> Result<usize> {
        let first = brokers
            .first()
            .and_then(|broker| broker["identity"]["topology"][name].as_u64())
            .ok_or_else(|| error(format!("missing production {name} observation")))?;
        if brokers
            .iter()
            .any(|broker| broker["identity"]["topology"][name] != first)
        {
            return Err(error(
                "production broker topology differs from uniform benchmark settings",
            ));
        }
        Ok(usize::try_from(first)?)
    };
    let topics = effective_topics(config, brokers)?;
    let mut row = json!({
        "system": config.args.system.name(), "broker_runtime": "production-deployment",
        "native_client_protocol": true, "topic_count": 1,
        "partition_count": settings.partitions, "partitions_per_topic": settings.partitions,
        "producer_workers": config.workers, "reader_workers": config.args.reader_workers(),
        "producer_writers": config.writers, "records_per_submission": 1, "records_per_append": null,
        "streaming": true, "request_records": config.args.request_records,
        "writer_batch_records_max": config.writer_limits().max_records,
        "writer_batch_payload_bytes_max": config.writer_limits().envelope.max_payload_bytes,
        "writer_inflight_appends": config.args.writer_inflight_appends,
        "writer_linger_us": config.args.writer_linger_us,
        "reader_records_max": config.reader_records(),
        "reader_payload_bytes_max": config.reader_limits().envelope.max_payload_bytes,
        "reader_queue_messages": config.live_queue_messages(),
        "readers_per_partition": config.args.readers_per_partition, "live_readers": true,
        "reader_transport": "TCP PUB/SUB live records; PEER subscriptions, replay, gap repair",
    });
    row.as_object_mut().unwrap().extend(json!({
        "commit_policy": match config.args.system {
            crate::bench::System::SingleDurable => "local_durable",
            crate::bench::System::DiskQuorum => "quorum_durable",
            crate::bench::System::ReplicatedPersisting => "quorum_replicated_persisting",
        },
        "ack_copies": if config.args.system.brokers() == 1 { 1 } else { 2 },
        "replica_voters": config.args.system.brokers(), "storage_copies": config.args.system.brokers(),
        "journals_per_broker": settings.partitions,
        "replication_groups_per_broker": if config.args.system.brokers() == 3 { settings.partitions } else { 0 },
        "actor_runtime": "application",
        "broker_application_shards": observed("application_threads")?,
        "broker_application_threads": observed("application_threads")?,
        "broker_dispatch_threads": observed("dispatcher_threads")?,
        "broker_omq_io_threads": observed("omq_io_threads")?, "broker_omq_mode": "dedicated-io",
        "broker_application_runtime": "one Tokio current_thread runtime per application shard",
        "broker_peer_receive_queues": "ordinary socket receive; dispatcher and bounded shard fanrings",
        "broker_peer_payload_budget_scope": "local writer/follower queues and resident owners are bounded per shard; follower progress and control capacity are reserved",
        "partition_assignment": "shared topic writers; keyed XXH3-64 modulo persisted partition count",
        "client_application_threads_per_process": 1,
        "client_control_omq_io_threads": config.args.io_threads,
        "control_transport": if config.args.control.control_bind.is_some() { "explicit TCP" } else { "Linux abstract IPC" },
        "segment_bytes": config.segment_bytes(),
        "compression": "none",
        "allocation_counted": cfg!(feature = "allocation-counting"),
        "profiled": std::env::var_os("OZZY_BENCH_PROFILE").is_some() || std::env::var_os("OZZY_BENCH_HEAPTRACK").is_some(),
        "profile_kind": std::env::var("OZZY_BENCH_PROFILE").ok(),
        "placement": if config.args.placements.is_some() {
            "explicit broker host placements; coordinator-local writers and readers; actual masks and roots recorded per broker"
        } else {
            "local broker, writer, and reader processes; broker-local shards and shared device workers; actual masks and roots recorded per broker"
        },
        "memory_reservation_bytes": settings.reservation_bytes,
        "memory_reservation_scope": "declared SDK, shard, device, and decode headroom; not measured peak memory",
    }).as_object().unwrap().clone());
    super::super::results::writer_protocol(&mut row, config);
    row.as_object_mut()
        .unwrap()
        .extend(topics.as_object().unwrap().clone());
    row["effective_topic_topology"] = topics;
    row["topic_organization"] = json!("one Ozzy topic");
    row["writer_batch_target_bytes"] = json!(super::writer_settings(config).batch_target_bytes);
    row["payload_compression_scope"] = json!("SDK APPEND payload packing");
    Ok(row)
}

fn effective_topics(config: &Config, brokers: &[Value]) -> Result<Value> {
    let mut previous = None;
    for broker in brokers {
        let partitions = broker["identity"]["topology"]["partitions"]
            .as_array()
            .ok_or_else(|| error("missing actual production partitions"))?;
        let entries = partitions
            .iter()
            .map(|partition| {
                Ok((
                    partition["topic"]
                        .as_str()
                        .ok_or_else(|| error("missing actual topic"))?,
                    partition["partition"]
                        .as_u64()
                        .ok_or_else(|| error("missing actual partition"))?,
                ))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        if entries.len() != config.partitions()
            || entries
                != (0..config.partitions() as u64)
                    .map(|number| ("benchmark", number))
                    .collect()
            || previous
                .as_ref()
                .is_some_and(|previous| previous != &entries)
        {
            return Err(error(
                "actual production topic topology differs from requested counts",
            ));
        }
        previous = Some(entries);
    }
    let entries = previous.ok_or_else(|| error("missing actual production brokers"))?;
    Ok(super::super::results::topic_topology(1, entries.len()))
}
