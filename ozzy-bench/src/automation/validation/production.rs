//! Production deployment contracts: CPU masks, policy, topology, SDK bounds,
//! and deduplication.
use super::{
    Result, append_window, derived_rates, feeds, measurements, requested_window, requests, schedule,
};
use serde_json::{Value, json};

/// Check observed broker thread masks against the requested deployment budget.
pub fn production_cpu_placement(row: &Value, config: &Value) -> Result<()> {
    let brokers = row["brokers"].as_array().ok_or("missing brokers")?;
    if !matches!(brokers.len(), 1 | 3) {
        return Err("invalid production broker count".into());
    }
    let cpus: Vec<Vec<usize>> = if let Some(placements) = config["deployment"].as_array() {
        placements
            .iter()
            .take(brokers.len())
            .map(|entry| serde_json::from_value(entry["cpus"].clone()))
            .collect::<std::result::Result<_, _>>()?
    } else if brokers.len() == 1 {
        vec![serde_json::from_value(config["broker_cpus"].clone())?]
    } else {
        let sets: Vec<Vec<usize>> = serde_json::from_value(config["broker_cpu_sets"].clone())?;
        if sets.len() == 1 {
            vec![sets[0].clone(); brokers.len()]
        } else {
            sets
        }
    };
    if cpus.len() != brokers.len() || cpus.iter().any(Vec::is_empty) {
        return Err("missing production broker CPU masks".into());
    }
    for (broker, cpus) in brokers.iter().zip(cpus) {
        let placement = crate::placement::Placement {
            bind: "127.0.0.1".parse()?,
            remote: None,
            storage_dir: None,
            cpus: Some(cpus),
        };
        placement.verify_execution(&broker["identity"]["execution"])?;
        placement.verify_execution(&broker["usage"]["execution"])?;
    }
    Ok(())
}

/// The confirmation contract a production mode runs under.
struct Policy {
    brokers: u64,
    copies: u64,
    /// Commit policy name the broker reports.
    commit: &'static str,
    /// Cluster mode in the effective deployment.
    cluster: &'static str,
}

impl Policy {
    fn for_mode(mode: &str) -> Result<Self> {
        let (brokers, copies, commit, cluster) = match mode {
            "durable" => (1, 1, "local_durable", "single"),
            "disk-quorum" => (3, 2, "quorum_durable", "three"),
            _ => return Err("unsupported production confirmation policy".into()),
        };
        Ok(Self {
            brokers,
            copies,
            commit,
            cluster,
        })
    }
}

pub(super) fn production_comparison(
    case: &Value,
    row: &Value,
    config: &Value,
    mode: &str,
) -> Result<Value> {
    production_cpu_placement(row, config)?;
    requests::payload_encoding(row, config)?;
    requests::broker_omq_ownership(row, config)?;
    production_append_bounds(case, row, config)?;
    let policy = Policy::for_mode(mode)?;
    let profile = if policy.brokers == 1 {
        "single-durable"
    } else {
        mode
    };
    let mut result = measurements(
        &json!({"profile":profile,"partitions":config["partitions"]}),
        row,
    )?;
    let size = case["size"].as_u64().ok_or("missing record size")?;
    let partitions = config["partitions"].as_u64().ok_or("missing partitions")?;
    production_bounds(row, config, &policy, size, partitions)?;
    let append_window_limit = config["writer_inflight_appends"]
        .as_u64()
        .ok_or("missing writer confirmation window")?;
    append_window(row, append_window_limit, &config["partitions"])?;
    production_topology(row, config, partitions, policy.brokers)?;
    dedup_controls(row, config)?;
    delay_controls(row, config)?;
    requested_window(row, config)?;
    derived_rates(&mut result, row, size as f64)?;
    schedule::scheduled_measurements(case, row, config, &mut result)?;
    Ok(result)
}

/// Policy, storage, and SDK request and reader bounds of a production run.
fn production_bounds(
    row: &Value,
    config: &Value,
    policy: &Policy,
    size: u64,
    partitions: u64,
) -> Result<()> {
    let request = config["request_records"]
        .as_u64()
        .ok_or("missing request bound")?;
    let readers = config["reader_records"]
        .as_u64()
        .ok_or("missing reader bound")?;
    let reader_bytes = config["reader_payload_mib"]
        .as_u64()
        .ok_or("missing reader byte bound")?
        * 1024
        * 1024;
    let segment = config["segment_mib"]
        .as_u64()
        .unwrap_or_else(|| crate::automation::compare::default_segment_mib(size))
        * 1024
        * 1024;
    let reader_effective = row["reader_records_max"]
        .as_u64()
        .filter(|count| *count > 0 && *count <= readers.min(2048))
        .ok_or("invalid production reader bound")?;
    if row["commit_policy"] != policy.commit
        || row["ack_copies"] != policy.copies
        || row["replica_voters"] != policy.brokers
        || row["storage_copies"] != policy.brokers
        || row["journals_per_broker"] != partitions
        || row["segment_bytes"] != segment
        || row["request_records"] != request
        || row["writer_batch_records_max"]
            != request.min(ozzy_runtime::replicated::MAX_APPEND_RECORDS as u64)
        || row["writer_protocol"] != "peer-appends"
        || row["writer_batch_configuration_applies"] != true
        || row["native_reader_api"] != "decoded-records"
        || row["reader_payload_bytes_max"] != reader_effective * size
        || reader_effective * size > reader_bytes
        || row["live_readers"] != true
        || row["broker_omq_mode"] != "dedicated-io"
        || row["broker_dispatch_threads"] != 1
        || row["effective_deployment"]["cluster"]["mode"] != policy.cluster
        || row["effective_deployment"]["topics"]["benchmark"]["partitions"] != partitions
        || row["effective_deployment"]["topics"]["benchmark"]["segment_bytes"] != segment
    {
        return Err(
            "production policy, storage, or SDK bounds differ from requested configuration".into(),
        );
    }
    Ok(())
}

fn production_append_bounds(case: &Value, row: &Value, config: &Value) -> Result<()> {
    let size = case["size"].as_u64().ok_or("missing record size")?;
    let segment = config["segment_mib"]
        .as_u64()
        .unwrap_or_else(|| crate::automation::compare::default_segment_mib(size))
        * 1024
        * 1024;
    let expected = crate::native::AppendBudget {
        writers: feeds(config)?.writers(usize::try_from(
            config["partitions"].as_u64().ok_or("missing partitions")?,
        )?),
        records: usize::try_from(
            config["request_records"]
                .as_u64()
                .ok_or("missing request bound")?,
        )?,
        record_bytes: usize::try_from(size)?,
        metadata: crate::native::AppendMetadata {
            resolved: !config["dedup_window_secs"].is_null(),
            scheduled: !config["delay_millis"].is_null(),
        },
        target_bytes: usize::try_from(
            config["native_batch_target_bytes"]
                .as_u64()
                .ok_or("missing SDK batch-byte target")?,
        )?,
    }
    .effective_payload(usize::try_from(segment)?, 64 * 1024 * 1024)?;
    if row["writer_batch_payload_bytes_max"] != expected
        || row["writer_batch_target_bytes"] != expected
    {
        return Err("production SDK byte cap differs from requested configuration".into());
    }
    if row["reader_queue_messages"]
        != crate::native::reader_queue_messages(usize::try_from(
            row["reader_payload_bytes_max"]
                .as_u64()
                .ok_or("missing reader byte cap")?,
        )?)
    {
        return Err("production reader queue exceeds its comparison byte window".into());
    }
    Ok(())
}

fn production_topology(row: &Value, config: &Value, partitions: u64, brokers: u64) -> Result<()> {
    let cpus = config["broker_cpus"]
        .as_array()
        .ok_or("missing broker CPU pool")?;
    let owners = if brokers == 1 {
        config["native_shards"]
            .as_u64()
            .unwrap_or_else(|| partitions.min(cpus.len() as u64))
    } else {
        1
    };
    let io_threads = config["broker_io_threads"].as_u64().unwrap_or(1);
    // Inproc brokers share the process context; its I/O thread is outside
    // every broker's CPU mask and observed thread set.
    let observed_io = if config["worker_placement"] == "inproc-threads" {
        0
    } else {
        io_threads
    };
    let backend = config["disk_io_backend"].as_str().unwrap_or("aio");
    let observed = row["brokers"]
        .as_array()
        .ok_or("missing production brokers")?;
    if owners == 0
        || owners > partitions
        || observed.len() as u64 != brokers
        || row["broker_application_shards"] != owners
        || row["broker_application_threads"] != owners
        || row["broker_omq_io_threads"] != io_threads
    {
        return Err("unexpected production broker topology".into());
    }
    let expected = Topology {
        partitions,
        owners,
        io_threads,
        observed_io,
        backend,
    };
    for broker in observed {
        expected.check(row, broker)?;
    }
    Ok(())
}

/// The topology every production broker must report.
struct Topology<'a> {
    partitions: u64,
    /// Application shards per broker.
    owners: u64,
    io_threads: u64,
    /// OMQ I/O threads seen in the broker's own thread set.
    observed_io: u64,
    backend: &'a str,
}

impl Topology<'_> {
    /// One broker's partitions, threads, and storage workers, as reported and
    /// as declared in the effective deployment.
    fn check(&self, row: &Value, broker: &Value) -> Result<()> {
        let identity = &broker["identity"];
        let topology = &identity["topology"];
        let name = identity["broker"]
            .as_str()
            .ok_or("missing broker identity")?;
        let declared = &row["effective_deployment"]["brokers"][name];
        let entries = topology["partitions"]
            .as_array()
            .ok_or("missing broker partitions")?;
        let mut ids = entries
            .iter()
            .map(|entry| {
                let id = entry["partition"]
                    .as_u64()
                    .ok_or("invalid broker partition")?;
                let shard = entry["shard"].as_u64().ok_or("invalid partition shard")?;
                if entry["topic"] != "benchmark" || shard >= self.owners {
                    return Err("unexpected partition placement".into());
                }
                Ok(id)
            })
            .collect::<Result<Vec<_>>>()?;
        ids.sort_unstable();
        let controllers = topology["controllers"]
            .as_array()
            .ok_or("missing storage controller")?;
        if ids != (0..self.partitions).collect::<Vec<_>>()
            || topology["application_threads"] != self.owners
            || topology["dispatcher_threads"] != 1
            || topology["omq_io_threads"] != self.io_threads
            || topology["observed_threads"]["application"] != self.owners
            || topology["observed_threads"]["dispatcher"] != 1
            || topology["observed_threads"]["omq_io"] != self.observed_io
            || controllers.len() != 1
            || controllers[0]["workers"]["backend"] != self.backend
            || declared["topology"]["omq"]["io_threads"] != self.io_threads
            || declared["topology"]["shards"]
                .as_array()
                .is_none_or(|shards| shards.len() as u64 != self.owners)
            || declared["devices"]["storage"]["workers"]["backend"] != self.backend
            || declared["devices"]["storage"]["workers"]["write_threads"]
                != controllers[0]["workers"]["write_threads"]
        {
            return Err("production broker partition or worker topology differs".into());
        }
        Ok(())
    }
}

/// The topic holds records exactly when the case asked for a delay.
fn delay_controls(row: &Value, config: &Value) -> Result<()> {
    let delay = &row["effective_deployment"]["topics"]["benchmark"]["delay"];
    let requested = config["delay_millis"].as_u64();
    let matches = match requested {
        Some(millis) => {
            delay["max_secs"] == millis / 1000 + 1
                && config["delay_max_entries_per_partition"]
                    .as_u64()
                    .is_none_or(|entries| delay["max_entries"] == entries)
                && row["delay"]["delay_millis"] == millis
        }
        None => delay.is_null() && row["delay"].is_null(),
    };
    if !matches {
        return Err("delay policy differs from requested configuration".into());
    }
    Ok(())
}

fn dedup_controls(row: &Value, config: &Value) -> Result<()> {
    if let Some(window) = config["dedup_window_secs"].as_u64() {
        let policy = &row["effective_deployment"]["topics"]["benchmark"]["deduplication"];
        if policy["window_millis"] != window * 1000
            || policy["max_entries"] != config["dedup_max_entries_per_partition"]
            || policy["max_bytes"] != config["dedup_max_bytes_per_partition"]
        {
            return Err("dedup policy differs from requested configuration".into());
        }
    }
    let feeds = feeds(config)?;
    if !feeds.enabled() {
        if !row["dedup_ingest"].is_null() {
            return Err("unexpected redundant feed measurements".into());
        }
        return Ok(());
    }
    let physical = feeds.writers(usize::try_from(
        config["partitions"].as_u64().ok_or("missing partitions")?,
    )?);
    let expected = feeds.logical_rows(
        physical,
        row["writers"].as_array().ok_or("missing physical feeds")?,
    )?;
    let stored: u64 = expected[0]["lanes"]
        .as_array()
        .ok_or("missing logical feeds")?
        .iter()
        .map(|lane| lane["total"].as_u64().ok_or("missing logical feed total"))
        .sum::<std::result::Result<u64, _>>()?;
    let ingest = &row["dedup_ingest"];
    let attempts = row["total_confirmed_records"]
        .as_u64()
        .ok_or("missing attempts")?;
    if ingest["redundant_producers"] != feeds.redundant_producers
        || ingest["redundant_feeds"] != feeds.redundant_feeds
        || ingest["physical_feeds"] != physical
        || ingest["hot_partition_percent"] != feeds.hot_partition_percent
        || ingest["confirmed_attempts"] != attempts
        || ingest["stored_records"] != stored
        || row["total_verified_records"] != stored
        || ingest["duplicate_submissions"]
            != attempts
                .checked_sub(stored)
                .ok_or("invalid duplicate count")?
    {
        return Err("dedup counts differ from independently checked feeds".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_topology_checks_observed_partition_owners_and_backend() {
        let config = json!({"broker_cpus":[0,1],"native_shards":2,"broker_io_threads":1});
        let mut row = json!({
            "broker_application_shards":2,"broker_application_threads":2,"broker_omq_io_threads":1,
            "effective_deployment":{"brokers":{"broker-0":{"topology":{"omq":{"io_threads":1},"shards":[{},{}]},"devices":{"storage":{"workers":{"backend":"aio","write_threads":2}}}}}},
            "brokers":[{"identity":{"broker":"broker-0","topology":{
                "application_threads":2,"dispatcher_threads":1,"omq_io_threads":1,
                "observed_threads":{"application":2,"dispatcher":1,"omq_io":1},
                "controllers":[{"workers":{"backend":"aio","write_threads":2}}],
                "partitions":[{"topic":"benchmark","partition":0,"shard":0},{"topic":"benchmark","partition":1,"shard":1}]
            }}}]
        });
        production_topology(&row, &config, 2, 1).unwrap();
        row["brokers"][0]["identity"]["topology"]["partitions"][1]["shard"] = json!(2);
        assert!(production_topology(&row, &config, 2, 1).is_err());
        row["brokers"][0]["identity"]["topology"]["partitions"][1]["shard"] = json!(1);
        row["effective_deployment"]["brokers"]["broker-0"]["devices"]["storage"]["workers"]["backend"] =
            json!("pool");
        assert!(production_topology(&row, &config, 2, 1).is_err());
    }

    #[test]
    fn production_append_caps_require_effective_requested_geometry() {
        for (size, target, expected) in [
            (128, 4 * 1024 * 1024, 256 * 1024),
            (8192, 832 * 1024, 832 * 1024),
            (8192, 4 * 1024 * 1024, 4 * 1024 * 1024),
        ] {
            let case = json!({"size": size});
            let config = json!({"partitions":8,"request_records":2048,
                "segment_mib":1024,"native_batch_target_bytes":target});
            let mut row = json!({"writer_batch_payload_bytes_max":expected,
                "writer_batch_target_bytes":expected,"reader_payload_bytes_max":expected,
                "reader_queue_messages":crate::native::reader_queue_messages(expected)});
            production_append_bounds(&case, &row, &config).unwrap();
            row["writer_batch_payload_bytes_max"] = json!(expected - 1);
            assert!(production_append_bounds(&case, &row, &config).is_err());
        }
    }
}
