//! SDK owner declarations computed before broker initialization.

use super::{Config, DIRECTORY_METADATA_BYTES, Result, Setup, error, writer_settings};
use ozzy_proto::NodeId;
use ozzy_runtime::replicated::{
    AppendLinkLimits, BrokerAddress, BrokerLinksConfig, ReaderLinkLimits,
};
use serde_json::{Value, json};

#[derive(Debug, Clone)]
pub(in crate::bench::timed) struct Clients {
    pub(in crate::bench::timed) writers: Vec<Value>,
    pub(in crate::bench::timed) readers: Vec<Value>,
    pub(in crate::bench::timed) bytes: u64,
}

impl Clients {
    pub(in crate::bench::timed) fn new(config: &Config) -> Result<Self> {
        let settings = config
            .native
            .as_ref()
            .ok_or_else(|| error("missing native deployment settings"))?;
        // Local validation only. These identities/endpoints create no sockets.
        let setup = Setup {
            topic: "benchmark".into(),
            partitions: settings.partitions,
            brokers: (0..config.args.system.brokers())
                .map(|index| {
                    Ok(BrokerAddress {
                        node: NodeId::from_bytes([index as u8 + 1; 16]),
                        endpoint: format!("inproc://native-preflight-{index}").parse()?,
                        data_endpoint: format!("inproc://native-preflight-data-{index}").parse()?,
                    })
                })
                .collect::<Result<_>>()?,
        };
        let mut bytes = 0_usize;
        let mut writers = Vec::new();
        for index in 0..config.workers {
            let lanes =
                ozzy_bench::workload::worker_share(config.writers, config.workers, index)?.len();
            let append = writer_limits(config, lanes, settings.partitions)?;
            let links = setup.writer_config(config, append)?;
            bytes = bytes
                .checked_add(append.bytes)
                .and_then(|bytes| bytes.checked_add(links.control_bytes))
                .and_then(|bytes| bytes.checked_add(links.routing_bytes))
                .ok_or_else(|| error("native SDK owner reservation overflow"))?;
            let mut profile = profile(&links);
            profile["append"] = json!({
                "writers": append.writers, "requests": append.requests,
                "records": append.records, "bytes": append.bytes,
            });
            writers.push(profile);
        }
        let mut readers = Vec::new();
        for index in 0..config.args.reader_workers() {
            let subscriptions = ozzy_bench::workload::worker_share(
                config.reader_slots(),
                config.args.reader_workers(),
                index,
            )?
            .len();
            let mut reader = ReaderLinkLimits {
                subscriptions,
                bytes: 0,
                queue_messages: config.live_queue_messages(),
            };
            reader.bytes = reader
                .reservation_bytes(Setup::reader_limits(config), setup.brokers.len())
                .ok_or_else(|| error("native SDK reader reservation overflow"))?;
            let links = setup.reader_config(config, reader)?;
            bytes = bytes
                .checked_add(reader.bytes)
                .and_then(|bytes| bytes.checked_add(links.control_bytes))
                .and_then(|bytes| bytes.checked_add(links.routing_bytes))
                .ok_or_else(|| error("native SDK owner reservation overflow"))?;
            let mut profile = profile(&links);
            profile["reader"] = json!({
                "subscriptions": reader.subscriptions, "bytes": reader.bytes,
                "queue_messages": reader.queue_messages,
            });
            readers.push(profile);
        }
        Ok(Self {
            writers,
            readers,
            bytes: u64::try_from(bytes)?,
        })
    }
}

fn writer_limits(config: &Config, lanes: usize, partitions: usize) -> Result<AppendLinkLimits> {
    let settings = writer_settings(config);
    let reservation = settings
        .link_reservation(DIRECTORY_METADATA_BYTES)
        .ok_or_else(|| error("native SDK writer reservation overflow"))?;
    let writers = lanes
        .checked_mul(partitions)
        .ok_or_else(|| error("native SDK partition writer count overflow"))?;
    // The benchmark fixes one destination per writer. Idle partition writers
    // still reserve their complete storage. Concurrent transmissions share the
    // SDK owner's finite request and record windows.
    let requests = lanes
        .checked_mul(settings.inflight_appends)
        .ok_or_else(|| error("native SDK request window overflow"))?;
    let records = requests
        .checked_mul(settings.limits.max_records)
        .ok_or_else(|| error("native SDK record window overflow"))?;
    let bytes = writers
        .checked_mul(reservation.idle_bytes)
        .and_then(|bytes| {
            bytes.checked_add(
                requests
                    .checked_add(1)?
                    .checked_mul(reservation.request_bytes)?,
            )
        })
        .ok_or_else(|| error("native SDK APPEND reservation overflow"))?;
    Ok(AppendLinkLimits {
        writers,
        requests,
        records,
        bytes,
    })
}

fn profile(config: &BrokerLinksConfig) -> Value {
    json!({
        "control": {"requests": config.requests, "bytes": config.control_bytes},
        "routing_bytes": config.routing_bytes,
        "receive": {"records": config.parameters.receive.max_records,
            "record_bytes": config.parameters.receive.max_record_bytes,
            "metadata_bytes": config.parameters.receive.envelope.max_metadata_bytes,
            "payload_bytes": config.parameters.receive.envelope.max_payload_bytes},
    })
}

#[cfg(test)]
mod tests;
