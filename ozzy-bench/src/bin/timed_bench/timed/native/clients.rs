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

/// One SDK owner's declared profile and the bytes it reserves.
struct Owner {
    profile: Value,
    bytes: usize,
}

impl Owner {
    /// A profile for `links` that also holds `reserved` role bytes.
    fn new(links: &BrokerLinksConfig, reserved: usize) -> Result<Self> {
        let bytes = reserved
            .checked_add(links.control_bytes)
            .and_then(|bytes| bytes.checked_add(links.routing_bytes))
            .ok_or_else(|| error("native SDK owner reservation overflow"))?;
        Ok(Self {
            profile: profile(links),
            bytes,
        })
    }
}

impl Clients {
    pub(in crate::bench::timed) fn new(config: &Config) -> Result<Self> {
        let settings = config
            .native
            .as_ref()
            .ok_or_else(|| error("missing native deployment settings"))?;
        let setup = preflight_setup(config, settings.partitions)?;
        let writers = (0..config.workers)
            .map(|index| writer_owner(config, &setup, index))
            .collect::<Result<Vec<_>>>()?;
        let readers = (0..config.args.reader_workers())
            .map(|index| reader_owner(config, &setup, index))
            .collect::<Result<Vec<_>>>()?;
        let bytes = writers
            .iter()
            .chain(&readers)
            .try_fold(0_usize, |total, owner| total.checked_add(owner.bytes))
            .ok_or_else(|| error("native SDK owner reservation overflow"))?;
        let profiles = |owners: Vec<Owner>| owners.into_iter().map(|owner| owner.profile).collect();
        Ok(Self {
            writers: profiles(writers),
            readers: profiles(readers),
            bytes: u64::try_from(bytes)?,
        })
    }
}

/// Local validation only. These identities and endpoints create no sockets.
fn preflight_setup(config: &Config, partitions: usize) -> Result<Setup> {
    Ok(Setup {
        topic: "benchmark".into(),
        partitions,
        brokers: (0..config.args.system.brokers())
            .map(|index| {
                Ok(BrokerAddress {
                    node: NodeId::from_bytes([index as u8 + 1; 16]),
                    endpoint: format!("inproc://native-preflight-{index}").parse()?,
                    data_endpoint: format!("inproc://native-preflight-data-{index}").parse()?,
                })
            })
            .collect::<Result<_>>()?,
    })
}

/// The writer process `index` with its share of writer lanes.
fn writer_owner(config: &Config, setup: &Setup, index: usize) -> Result<Owner> {
    let lanes = ozzy_bench::workload::worker_share(config.writers, config.workers, index)?.len();
    let append = writer_limits(config, lanes, setup.partitions)?;
    let links = setup.writer_config(config, append)?;
    let mut owner = Owner::new(&links, append.bytes)?;
    owner.profile["append"] = json!({
        "writers": append.writers, "requests": append.requests,
        "records": append.records, "bytes": append.bytes,
    });
    Ok(owner)
}

/// The reader process `index` with its share of subscriptions.
fn reader_owner(config: &Config, setup: &Setup, index: usize) -> Result<Owner> {
    let mut reader = ReaderLinkLimits {
        subscriptions: reader_subscriptions(config, setup.partitions, index)?,
        bytes: 0,
        queue_messages: config.live_queue_messages(),
    };
    reader.bytes = reader
        .reservation_bytes(Setup::reader_limits(config), setup.brokers.len())
        .ok_or_else(|| error("native SDK reader reservation overflow"))?;
    let links = setup.reader_config(config, reader)?;
    let mut owner = Owner::new(&links, reader.bytes)?;
    owner.profile["reader"] = json!({
        "subscriptions": reader.subscriptions, "bytes": reader.bytes,
        "queue_messages": reader.queue_messages,
    });
    Ok(owner)
}

/// Topic reader slots, or every partition of each assigned group per member.
fn reader_subscriptions(config: &Config, partitions: usize, index: usize) -> Result<usize> {
    let workers = config.args.reader_workers();
    let slots = ozzy_bench::workload::worker_share(config.reader_slots(), workers, index)?.len();
    if config.args.native.groups == 0 {
        return Ok(slots);
    }
    ozzy_bench::workload::worker_share(config.args.native.groups as usize, workers, index)?
        .len()
        .checked_mul(config.args.native.group_members as usize)
        .and_then(|n| n.checked_mul(partitions))
        .ok_or_else(|| error("group reader subscription overflow"))
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
