//! Verification and residency are mandatory before any result enters a ledger.
use super::{Result, finite};
use serde_json::{Value, json};

// Archived local-owner rows used two device writer threads.
const LEGACY_WRITE_THREADS: u64 = 2;

pub fn measurements(case: &Value, row: &Value) -> Result<Value> {
    let profile = case["profile"].as_str().ok_or("missing profile")?;
    let external = profile.starts_with("iggy-")
        || profile.starts_with("kafka-")
        || profile.starts_with("redpanda-");
    if row["system"] != profile || row["native_client_protocol"] != !external {
        return Err("unexpected workload mode or client protocol".into());
    }
    topic_topology(case, row)?;
    measurements_checked(case, row, external)
}

fn topic_topology(case: &Value, row: &Value) -> Result<()> {
    if let Some(partitions) = case.get("partitions")
        && (row["topic_count"] != 1
            || row["partition_count"] != *partitions
            || row["partitions_per_topic"] != *partitions)
    {
        return Err("unexpected topic/partition topology".into());
    }
    if let Some(effective) = row.get("effective_topic_topology")
        && (row["logical_stream_count"] != 1
            || effective
                != &json!({
                    "logical_stream_count":1, "topic_count":1,
                    "partitions_per_topic":row["partitions_per_topic"],
                    "partition_count":row["partition_count"],
                }))
    {
        return Err("effective topic/partition metadata differs from reported topology".into());
    }
    Ok(())
}

fn measurements_checked(case: &Value, row: &Value, external: bool) -> Result<Value> {
    if let Some(shards) = case["app_threads"].as_u64() {
        let hashes = row["partition_hashes"]
            .as_array()
            .ok_or("missing partition hashes")?;
        if shards == 0 {
            return Err("zero application shards".into());
        }
        let assigned = hashes
            .iter()
            .map(|v| {
                v.as_u64()
                    .map(|h| h % shards)
                    .ok_or("invalid partition hash")
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if row["partition_assignment_hash"] != "xxh3-64"
            || hashes.len() as u64 != case["partitions"].as_u64().unwrap_or(0)
            || row["broker_application_shards"] != shards
            || row["broker_application_threads"] != shards
            || row["broker_supervisor_threads"] != u64::from(shards > 1)
            || row["broker_dispatch_threads"] != 0
            || row["partition_to_application_shard"] != json!(assigned)
        {
            return Err("unexpected native shard topology".into());
        }
    }
    for (requested, actual, scale) in [
        ("io_threads", "omq_io_threads", 1),
        ("writer_batch_records", "writer_batch_records_max", 1),
        ("writer_linger_us", "writer_linger_us", 1),
        ("writer_inflight_appends", "writer_inflight_appends", 1),
        ("segment_mib", "segment_bytes", 1024 * 1024),
        ("disk_workers", "disk_io_threads_per_broker", 1),
        ("disk_owner_threads", "disk_owner_threads_per_broker", 1),
        ("read_depth", "local_read_depth", 1),
        (
            "storage_group_records",
            "local_storage_group_records_max",
            1,
        ),
        ("packing_block_kib", "local_packing_block_bytes", 1024),
        (
            "storage_group_kib",
            "local_storage_group_payload_bytes_max",
            1024,
        ),
    ] {
        if let Some(value) = case[requested].as_u64()
            && row[actual] != value * scale
        {
            return Err(format!("unexpected {actual}").into());
        }
    }
    if row["total_verified_records"].as_u64().is_none()
        || row["total_verified_records"] != row["total_confirmed_records"]
    {
        return Err("reader delivery did not match confirmations".into());
    }
    for field in ["producer_records_per_second", "consumer_records_per_second"] {
        if finite(&row[field])? <= 0.0 {
            return Err("no positive in-window throughput samples".into());
        }
    }
    for field in ["producer_ack", "reader_delivery"] {
        if row[field]["samples"].as_u64().unwrap_or(0) == 0 {
            return Err("empty latency cohort".into());
        }
    }
    if !external && let Some(limit) = case["writer_inflight_appends"].as_u64() {
        append_window(row, limit, &case["partitions"])?;
    }
    let mut result = json!({"confirmed_s":finite(&row["producer_records_per_second"])?,"delivered_s":finite(&row["consumer_records_per_second"])?,"ack_p50_us":finite(&row["producer_ack"]["p50_us"])?,"ack_p99_us":finite(&row["producer_ack"]["p99_us"])?,"delivery_p50_us":finite(&row["reader_delivery"]["p50_us"])?,"delivery_p99_us":finite(&row["reader_delivery"]["p99_us"])?});
    for (field, key) in [
        ("producer_ack", "ack_p999_us"),
        ("reader_delivery", "delivery_p999_us"),
    ] {
        if let Some(value) = row[field].get("p999_us") {
            result[key] = json!(checked_p999(&row[field], value)?);
        }
    }
    Ok(result)
}

fn checked_p999(report: &Value, value: &Value) -> Result<f64> {
    let value = finite(value)?;
    if value < finite(&report["p99_us"])? || value > finite(&report["max_us"])? {
        return Err("p99.9 outside p99/maximum bounds".into());
    }
    Ok(value)
}

fn broker_omq_ownership(row: &Value, config: &Value) -> Result<()> {
    let borrowed = config["broker_omq_on_shard"] == true;
    if row["broker_omq_mode"]
        != if borrowed {
            "application-shard"
        } else {
            "dedicated-io"
        }
        || borrowed && row["broker_omq_io_threads"] != 0
    {
        return Err("unexpected broker OMQ ownership".into());
    }
    Ok(())
}

pub fn comparison(case: &Value, row: &Value, config: &Value) -> Result<Value> {
    if row["profiled"] == true || row["allocation_counted"] == true {
        return Err("instrumented timings cannot enter comparison results".into());
    }
    if row["record_bytes"] != case["size"] {
        return Err("measured record size differs from requested case".into());
    }
    validate_corpus(case, row)?;
    let mode = case["mode"].as_str().ok_or("missing mode")?;
    let implementation = case["impl"].as_str().ok_or("missing implementation")?;
    let native = implementation == "ozzy";
    if native && row["broker_runtime"] == "production-deployment" {
        return production_comparison(case, row, config, mode);
    }
    request_bounds(implementation, mode, row, config)?;
    if native {
        broker_omq_ownership(row, config)?;
    }
    let profile = if native && super::cluster_mode(mode) {
        mode.to_owned()
    } else {
        let prefix = if native { "single" } else { implementation };
        format!("{prefix}-{mode}")
    };
    let mut result = measurements(
        &json!({"profile":profile,"partitions":config["partitions"]}),
        row,
    )?;
    let warmup = &config["warmup"];
    let duration = &config["duration"];
    if (finite(&row["warmup_seconds"])? - finite(warmup)?).abs() > 1e-6
        || (finite(&row["measurement_seconds"])? - finite(duration)?).abs() > 1e-6
    {
        return Err("warmup or measured window differs from requested configuration".into());
    }
    let bytes = finite(&case["size"])?;
    result["confirmed_mib_s"] = json!(finite(&result["confirmed_s"])? * bytes / 1_048_576.0);
    result["verified_mib_s"] = json!(finite(&result["delivered_s"])? * bytes / 1_048_576.0);
    result["reader_drain_seconds"] = json!(finite(&row["reader_drain_seconds"])?);
    result["delivery_fraction"] =
        json!(finite(&result["delivered_s"])? / finite(&result["confirmed_s"])?);
    let brokers = row["brokers"].as_array().ok_or("missing brokers")?;
    if native && mode == "replicated-persisting" {
        validate_background_persistence(row, brokers)?;
        validate_replication_memory(row, config)?;
        // One owner, one reader, the writer pool and one maintenance thread.
        if row["disk_io_threads_per_broker"] != 3 + LEGACY_WRITE_THREADS
            || row["disk_owner_threads_per_broker"] != 1
        {
            return Err("unexpected replicated background worker configuration".into());
        }
        if row["background_persistence"]["write_call_bytes"] != config["native_write_call_bytes"] {
            return Err("write syscall limit differs from requested configuration".into());
        }
        if let Some(workers) = config["compression_workers"].as_u64() {
            let expected = if case["codec"] == "raw" { 0 } else { workers };
            let background = &row["background_persistence"];
            if background["compression_workers_per_shard"] != workers
                || background["effective_compression_workers_per_shard"] != expected
            {
                return Err("compression worker count differs from requested configuration".into());
            }
        }
    }
    if native {
        let expected = if matches!(mode, "buffered" | "replicated-persisting") {
            "buffered"
        } else {
            "odsync"
        };
        if row["segment_write_mode"] != expected || config["durable_segment_io"] != "odsync" {
            return Err("unexpected segment write mode".into());
        }
        result["encoded_bytes_per_payload_byte"] = json!(journal_ratio(
            brokers,
            mode,
            finite(&row["total_confirmed_records"])? * bytes
        )?);
    }
    if native && !super::cluster_mode(mode) {
        validate_local_storage(case, row, config)?;
        let records = brokers
            .iter()
            .map(|b| {
                b["local_read_cache"]["journal_records"]
                    .as_u64()
                    .ok_or("missing reader counter")
            })
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>();
        if row["total_verified_records"] != records {
            return Err("journal counters omit verified records".into());
        }
    }
    scheduled_measurements(case, row, config, &mut result)?;
    Ok(result)
}

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

fn production_comparison(case: &Value, row: &Value, config: &Value, mode: &str) -> Result<Value> {
    production_cpu_placement(row, config)?;
    payload_encoding(true, row, config)?;
    broker_omq_ownership(row, config)?;
    let (brokers, copies, policy, confirmation) = match mode {
        "durable" => (1, 1, "local_durable", "local-durable"),
        "disk-quorum" => (3, 2, "quorum_durable", "disk-quorum"),
        "replicated-persisting" => (
            3,
            2,
            "quorum_replicated_persisting",
            "replicated-persisting",
        ),
        _ => return Err("unsupported production confirmation policy".into()),
    };
    let profile = if brokers == 1 { "single-durable" } else { mode };
    let mut result = measurements(
        &json!({"profile":profile,"partitions":config["partitions"]}),
        row,
    )?;
    let size = case["size"].as_u64().ok_or("missing record size")?;
    let partitions = config["partitions"].as_u64().ok_or("missing partitions")?;
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
        .unwrap_or_else(|| super::compare::default_segment_mib(size))
        * 1024
        * 1024;
    let reader_effective = row["reader_records_max"]
        .as_u64()
        .filter(|count| *count > 0 && *count <= readers.min(2048))
        .ok_or("invalid production reader bound")?;
    if row["commit_policy"] != policy
        || row["ack_copies"] != copies
        || row["replica_voters"] != brokers
        || row["storage_copies"] != brokers
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
        || row["effective_deployment"]["topics"]["benchmark"]["confirmation"] != confirmation
        || row["effective_deployment"]["topics"]["benchmark"]["partitions"] != partitions
        || row["effective_deployment"]["topics"]["benchmark"]["segment_bytes"] != segment
    {
        return Err(
            "production policy, storage, or SDK bounds differ from requested configuration".into(),
        );
    }
    let append_window_limit = config["writer_inflight_appends"]
        .as_u64()
        .ok_or("missing writer confirmation window")?;
    append_window(row, append_window_limit, &config["partitions"])?;
    production_topology(row, config, partitions, brokers)?;
    if (finite(&row["warmup_seconds"])? - finite(&config["warmup"])?).abs() > 1e-6
        || (finite(&row["measurement_seconds"])? - finite(&config["duration"])?).abs() > 1e-6
    {
        return Err("warmup or measured window differs from requested configuration".into());
    }
    result["confirmed_mib_s"] = json!(finite(&result["confirmed_s"])? * size as f64 / 1_048_576.0);
    result["verified_mib_s"] = json!(finite(&result["delivered_s"])? * size as f64 / 1_048_576.0);
    result["reader_drain_seconds"] = json!(finite(&row["reader_drain_seconds"])?);
    result["delivery_fraction"] =
        json!(finite(&result["delivered_s"])? / finite(&result["confirmed_s"])?);
    scheduled_measurements(case, row, config, &mut result)?;
    Ok(result)
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
    for broker in observed {
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
                if entry["topic"] != "benchmark" || shard >= owners {
                    return Err("unexpected partition placement".into());
                }
                Ok(id)
            })
            .collect::<Result<Vec<_>>>()?;
        ids.sort_unstable();
        let controllers = topology["controllers"]
            .as_array()
            .ok_or("missing storage controller")?;
        if ids != (0..partitions).collect::<Vec<_>>()
            || topology["application_threads"] != owners
            || topology["dispatcher_threads"] != 1
            || topology["omq_io_threads"] != io_threads
            || topology["observed_threads"]["application"] != owners
            || topology["observed_threads"]["dispatcher"] != 1
            || topology["observed_threads"]["omq_io"] != io_threads
            || controllers.len() != 1
            || controllers[0]["workers"]["backend"] != backend
            || declared["topology"]["omq"]["io_threads"] != io_threads
            || declared["topology"]["shards"]
                .as_array()
                .is_none_or(|shards| shards.len() as u64 != owners)
            || declared["devices"]["storage"]["workers"]["backend"] != backend
            || declared["devices"]["storage"]["workers"]["write_threads"]
                != controllers[0]["workers"]["write_threads"]
        {
            return Err("production broker partition or worker topology differs".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod production_tests {
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
}

fn validate_replication_memory(row: &Value, config: &Value) -> Result<()> {
    for (requested, observed) in [
        (
            "native_persistence_backlog_bytes",
            &row["background_persistence"]["max_pending_body_bytes"],
        ),
        (
            "native_replication_cache_bytes",
            &row["replication_replay_cache"]["max_body_bytes"],
        ),
    ] {
        if let Some(bytes) = config.get(requested)
            && bytes != observed
        {
            return Err(format!("{requested} differs from requested configuration").into());
        }
    }
    Ok(())
}

fn validate_local_storage(case: &Value, row: &Value, config: &Value) -> Result<()> {
    let partitions = config["partitions"].as_u64().ok_or("missing partitions")?;
    let default_owners = partitions.min(
        config["broker_cpus"]
            .as_array()
            .ok_or("missing CPUs")?
            .len() as u64,
    );
    let owners = match config.get("native_shards") {
        Some(value) => value.as_u64().ok_or("invalid native shard count")?,
        None => default_owners,
    };
    if owners == 0 || owners > partitions {
        return Err("invalid native shard count".into());
    }
    let workers = owners * 2 + LEGACY_WRITE_THREADS + 1;
    if row["disk_owner_threads_per_broker"] != owners
        || row["disk_io_threads_per_broker"] != workers
        || row["broker_application_shards"] != owners
        || row["broker_application_threads"] != owners
    {
        return Err("unexpected local disk worker configuration".into());
    }
    storage_group_bounds(case, row, config)?;
    if row["partition_to_application_shard"]
        != json!((0..partitions).map(|p| p % owners).collect::<Vec<_>>())
    {
        return Err("unbalanced native partition placement".into());
    }
    Ok(())
}

fn validate_corpus(case: &Value, row: &Value) -> Result<()> {
    let corpus = match case["pattern"].as_str() {
        Some("events") if case["size"] == 16 => crate::workload::BINARY_EVENT_CORPUS,
        Some("events" | "json") => crate::workload::JSON_EVENT_CORPUS,
        Some("random") => {
            "8-byte monotonic submission clock followed by deterministic SplitMix64 bytes"
        }
        Some("structured") => "8-byte monotonic submission clock followed by structured-v1",
        _ => return Err("unknown record corpus".into()),
    };
    if row["record_corpus"] != corpus {
        return Err("unexpected record corpus".into());
    }
    Ok(())
}

fn scheduled_measurements(
    case: &Value,
    row: &Value,
    config: &Value,
    result: &mut Value,
) -> Result<()> {
    let scheduled = &row["scheduled_load"];
    if let Some(ramp) = config["ramp"].as_str() {
        return ramp_measurements(case, row, ramp, result);
    }
    if case["rate"].is_null() {
        if !scheduled.is_null() || !config["records_per_second"].is_null() {
            return Err("saturation case contains scheduled traffic".into());
        }
        return Ok(());
    }
    let rate = case["rate"].as_u64().ok_or("invalid offered rate")?;
    if !matches!(case["impl"].as_str(), Some("ozzy" | "iggy" | "redpanda"))
        || config["records_per_second"] != rate
        || scheduled["records_per_second"] != rate
    {
        return Err("scheduled rate differs from the requested workload".into());
    }
    let plan = crate::schedule::Load {
        records_per_second: Some(rate),
        ..Default::default()
    }
    .plan(1)?
    .ok_or("missing schedule")?;
    // Match the workload's half-open window, including a fractional warmup.
    let warmup = (finite(&row["warmup_seconds"])? * 1e9) as u64;
    let duration = (finite(&row["measurement_seconds"])? * 1e9) as u64;
    let total = plan.count(warmup.checked_add(duration).ok_or("window overflow")?)? as u64;
    let measured = total - plan.count(warmup)? as u64;
    if measured == 0
        || scheduled["planned_total_records"] != total
        || scheduled["planned_measurement_records"] != measured
        || row["total_confirmed_records"] != total
        || row["total_verified_records"] != total
    {
        return Err("incomplete scheduled arrival cohort".into());
    }
    scheduled_percentiles(scheduled, row, measured, result)?;
    result["offered_s"] = json!(rate);
    result["submitted_s"] = json!(finite(&row["submitted_records_per_second"])?);
    Ok(())
}

/// Latency percentiles of one scheduled cohort, prefixed per boundary.
fn scheduled_percentiles(
    scheduled: &Value,
    row: &Value,
    measured: u64,
    result: &mut Value,
) -> Result<()> {
    for (field, prefix) in [
        ("producer_ack", "scheduled_ack"),
        ("reader_delivery", "scheduled_delivery"),
        ("scheduling_lag", "scheduling_lag"),
    ] {
        // Every reader of a partition reports each scheduled arrival.
        let copies = if field == "reader_delivery" {
            row["readers_per_partition"].as_u64().unwrap_or(1)
        } else {
            1
        };
        if scheduled[field]["samples"] != measured * copies {
            return Err("incomplete scheduled latency samples".into());
        }
        for percentile in ["p50_us", "p99_us"] {
            let value = finite(&scheduled[field][percentile])?;
            if value < 0.0 {
                return Err("negative scheduled latency".into());
            }
            result[format!("{prefix}_{percentile}")] = json!(value);
        }
        if let Some(value) = scheduled[field].get("p999_us") {
            result[format!("{prefix}_p999_us")] = json!(checked_p999(&scheduled[field], value)?);
        }
    }
    result["scheduled_samples"] = json!(measured);
    Ok(())
}

/// Every stage below the first overloaded one holds exactly its planned
/// arrivals. Later stages carry only the failure; the ramp stopped there.
fn ramp_measurements(case: &Value, row: &Value, ramp: &str, result: &mut Value) -> Result<()> {
    let scheduled = &row["scheduled_load"];
    let parsed: crate::schedule::Ramp = ramp.parse()?;
    let stages = scheduled["stages"]
        .as_array()
        .ok_or("missing ramp stages")?;
    if case["ramp"] != ramp
        || !case["rate"].is_null()
        || scheduled["ramp"] != ramp
        || stages.len() != parsed.stages().len()
        || (finite(&row["measurement_seconds"])? * 1e9).round() as u64 != parsed.duration_ns()
    {
        return Err("ramp differs from the requested workload".into());
    }
    let warmup = (finite(&row["warmup_seconds"])? * 1e9) as u64;
    let plan = parsed.plan(warmup)?;
    let (mut start, mut failed) = (warmup, false);
    let mut output = vec![];
    for ((rate, ns), stage) in parsed.stages().iter().zip(stages) {
        let end = start + ns;
        let planned = (plan.count(end)? - plan.count(start)?) as u64;
        start = end;
        if stage["records_per_second"] != *rate || stage["planned_records"] != planned {
            return Err("ramp stage differs from its planned arrivals".into());
        }
        let seconds = *ns as f64 / 1e9;
        if let Some(reason) = stage["overloaded"].as_str() {
            failed = true;
            output.push(json!({"rate":rate,"seconds":seconds,"failure":reason}));
            continue;
        }
        if failed || planned == 0 {
            return Err("ramp continued after an overloaded stage".into());
        }
        let mut measurements = json!({"offered_s":rate});
        scheduled_percentiles(stage, row, planned, &mut measurements)?;
        output.push(json!({"rate":rate,"seconds":seconds,"measurements":measurements}));
    }
    let total = plan.count(warmup + parsed.duration_ns())? as u64;
    if (!failed && row["total_confirmed_records"] != total)
        || row["total_confirmed_records"] != row["total_verified_records"]
    {
        return Err("incomplete scheduled arrival cohort".into());
    }
    result["stages"] = json!(output);
    Ok(())
}

fn journal_ratio(brokers: &[Value], mode: &str, payload: f64) -> Result<f64> {
    let mut resident = 0_u64;
    let mut persisted = 0_u64;
    let mut encoded = 0_u64;
    for broker in brokers {
        let read = &broker["usage"]["journal_reads"];
        resident += read["resident_selections"]
            .as_u64()
            .ok_or("missing resident read counter")?;
        persisted += read["persisted_operation_loads"]
            .as_u64()
            .ok_or("missing persisted read counter")?;
        encoded += broker["usage"]["journal_writes"]["encoded_group_bytes"]
            .as_u64()
            .ok_or("missing encoded write counter")?;
    }
    if resident == 0 || persisted != 0 {
        return Err(format!(
            "live-reader residency gate failed: resident={resident}, persisted_loads={persisted}"
        )
        .into());
    }
    let copies = if super::cluster_mode(mode) { 3 } else { 1 };
    if brokers.len() != copies || encoded == 0 || payload <= 0.0 {
        return Err("invalid journal byte accounting".into());
    }
    Ok(encoded as f64 / copies as f64 / payload)
}

fn validate_background_persistence(row: &Value, brokers: &[Value]) -> Result<()> {
    let bounds = &row["background_persistence"];
    let operations = bounds["max_pending_operations"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or("missing persistence operation bound")?;
    let bytes = bounds["max_pending_body_bytes"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or("missing persistence byte bound")?;
    if row["commit_policy"] != "quorum_replicated_persisting"
        || row["ack_copies"] != 2
        || row["journal_writes"] != 3
        || brokers.len() != 3
    {
        return Err("unexpected RAM-confirmed persistence policy".into());
    }
    for broker in brokers {
        let progress = &broker["persistence"];
        if progress["samples"].as_u64().is_none_or(|n| n == 0)
            || progress["operation_limit"] != operations
            || progress["body_bytes_limit"] != bytes
            || progress["pending_operations_max_observed"]
                .as_u64()
                .is_none_or(|n| n > operations)
            || progress["accepted_lag_operations_max_observed"]
                .as_u64()
                .is_none_or(|n| n > operations)
            || progress["confirmed_lag_operations_max_observed"]
                .as_u64()
                .is_none_or(|n| n > operations)
            || progress["pending_body_bytes_max_observed"]
                .as_u64()
                .is_none_or(|n| n > bytes)
            || broker["operation"].as_u64().is_none_or(|n| n == 0)
            || broker["disk_written"] != broker["operation"]
            || broker["disk_durable"]
                .as_u64()
                .is_none_or(|n| n > broker["disk_written"].as_u64().unwrap_or(0))
            || progress["completion_boundary"] != "buffered_write"
            || finite(&broker["persistence_drain_seconds"])? < 0.0
        {
            return Err("background persistence exceeded bounds or did not drain".into());
        }
    }
    Ok(())
}

fn storage_group_bounds(case: &Value, row: &Value, config: &Value) -> Result<()> {
    if let Some(target) = config["native_operation_target_bytes"].as_u64() {
        let record_bytes = case["size"].as_u64().ok_or("missing payload size")?;
        for broker in row["brokers"].as_array().ok_or("missing brokers")? {
            let observed = &broker["local_read_cache"];
            if observed["prepared_operations"]
                .as_u64()
                .is_none_or(|n| n == 0)
                || observed["prepared_payload_bytes_max"]
                    .as_u64()
                    .is_none_or(|n| n == 0 || n > target.max(record_bytes))
            {
                return Err("canonical operation sizes missing or exceed requested target".into());
            }
        }
    }
    if let Some(zero) = config["disk_zero_ahead"].as_bool()
        && row["disk_zero_ahead"] != zero
    {
        return Err("disk zero-ahead differs from requested configuration".into());
    }
    if let Some(direct) = config["disk_direct_io"].as_bool()
        && row["disk_direct_io"] != direct
    {
        return Err("disk direct I/O differs from requested configuration".into());
    }
    if let Some(backend) = config["disk_io_backend"].as_str()
        && row["disk_io_backend"] != backend
    {
        return Err("disk I/O backend differs from requested configuration".into());
    }
    if let Some(depth) = config["disk_aio_depth"].as_u64()
        && row["disk_aio_depth"] != depth
    {
        return Err("disk AIO depth differs from requested configuration".into());
    }
    if let Some(records) = config["storage_group_records"].as_u64()
        && (row["local_storage_group_records_max"] != records
            || row["local_storage_group_payload_bytes_max"]
                != records * case["size"].as_u64().ok_or("missing payload size")?)
    {
        return Err("storage group bounds differ from requested configuration".into());
    }
    Ok(())
}

fn payload_encoding(native: bool, row: &Value, config: &Value) -> Result<()> {
    if native
        && config.get("native_reader_api").is_some()
        && (row["native_reader_api"] != config["native_reader_api"]
            || row["payload_compression"] != config["payload_compression"]
            || row["payload_compression_threshold"] != config["payload_compression_threshold"])
    {
        return Err(
            "SDK payload encoding or reader API differs from requested configuration".into(),
        );
    }
    Ok(())
}

fn decoded_segment_bytes(config: &Value, record_bytes: u64) -> u64 {
    config["segment_decoded_mib"]
        .as_u64()
        .unwrap_or_else(|| super::compare::default_segment_mib(record_bytes))
        * 1024
        * 1024
}

fn request_bounds(implementation: &str, mode: &str, row: &Value, config: &Value) -> Result<()> {
    let native = implementation == "ozzy";
    payload_encoding(native, row, config)?;
    if native && let Some(protocol) = config["writer_protocol"].as_str() {
        if row["writer_protocol"] != protocol || row["writer_batch_configuration_applies"] != true {
            return Err("native writer protocol differs from requested configuration".into());
        }
    } else if native && let Some(batching) = config["writer_batching"].as_bool() {
        let expected = if batching {
            "peer-appends"
        } else {
            "push-records"
        };
        if row["writer_protocol"] != expected
            || row["writer_batch_configuration_applies"] != batching
        {
            return Err("native writer protocol differs from requested configuration".into());
        }
    }
    if native && let Some(target) = config["native_batch_target_bytes"].as_u64() {
        for field in [
            "writer_batch_target_bytes",
            "operation_target_bytes",
            "local_write_group_target_bytes",
        ] {
            let expected = match field {
                "operation_target_bytes" => config["native_operation_target_bytes"].as_u64(),
                "local_write_group_target_bytes" => {
                    config["native_write_group_target_bytes"].as_u64()
                }
                _ => None,
            };
            if row[field] != expected.unwrap_or(target) {
                return Err(format!("unexpected {field}").into());
            }
        }
        if row["max_record_bytes"] != config["native_max_record_bytes"] {
            return Err("unexpected hard record bound".into());
        }
    }
    if implementation == "redpanda" {
        if row["write_caching"] != matches!(mode, "buffered" | "replicated-persisting")
            || row["reader_records_max"] != 1
            || row["reader_payload_bytes_max"] != row["record_bytes"]
            || row["client_sdk"]["acks"] != "all"
            || row["client_sdk"]["unconfirmed_records_per_writer"] != config["request_records"]
        {
            return Err("unexpected Redpanda confirmation or SDK bounds".into());
        }
    } else if let Some(records) = config["reader_records"].as_u64() {
        let bytes = row["record_bytes"]
            .as_u64()
            .filter(|bytes| *bytes > 0)
            .ok_or("missing reader record size")?;
        let budget = config["reader_payload_mib"]
            .as_u64()
            .filter(|mib| (1..=128).contains(mib))
            .ok_or("missing reader payload bound")?
            * 1024
            * 1024;
        let records = records.min(budget / bytes);
        let records = if matches!(mode, "disk-quorum" | "replicated-persisting") {
            // Cluster comparisons use the default 4096-record storage-group
            // budget; local storage overrides cannot reach these modes. Read
            // payloads must also fit the group's bounded journal read arena.
            let segment_bytes = decoded_segment_bytes(config, bytes);
            let arena_bytes = (8 * 1024 * 1024)
                .max(4096 * (bytes + 100) + 1024)
                .min(segment_bytes);
            records
                .min(
                    config["request_records"]
                        .as_u64()
                        .ok_or("missing request bound")?,
                )
                .min(arena_bytes / bytes)
        } else {
            records
        };
        if records == 0
            || row["reader_records_max"] != records
            || row["reader_payload_bytes_max"] != records * bytes
        {
            return Err("reader request bounds differ from requested configuration".into());
        }
    }
    if native && let Some(limit) = config["writer_inflight_appends"].as_u64() {
        append_window(row, limit, &config["partitions"])?;
    }
    if let Some(records) = config["request_records"].as_u64()
        && (row["request_records"] != records
            || native && row["writer_batch_records_max"] != records
            || !native
                && row["records_per_append"]
                    != if implementation == "redpanda" {
                        1
                    } else {
                        records
                    })
    {
        return Err("request bounds differ from requested configuration".into());
    }
    Ok(())
}

fn append_window(row: &Value, limit: u64, partitions: &Value) -> Result<()> {
    if limit == 0 || row["writer_inflight_appends"] != limit {
        return Err("APPEND window differs from requested configuration".into());
    }
    let writers = row["writers"].as_array().ok_or("missing writer reports")?;
    let mut lanes = 0;
    for writer in writers {
        for lane in writer["lanes"].as_array().ok_or("missing writer lanes")? {
            let peak = lane["protocol_requests"]["max_inflight_appends"]
                .as_u64()
                .ok_or("missing observed APPEND window")?;
            let stats = &lane["protocol_requests"];
            if stats["data_messages"].as_u64().unwrap_or(0) > 0 {
                let records = stats["max_inflight_records"]
                    .as_u64()
                    .ok_or("missing record window observation")?;
                let window = stats["record_window"]
                    .as_u64()
                    .ok_or("missing negotiated record window")?;
                let configured = row["request_records"]
                    .as_u64()
                    .ok_or("missing record queue capacity")?
                    .max(1024);
                if peak != 0
                    || records == 0
                    || records > window
                    || window > configured
                    || stats["data_messages"] != stats["requests"]
                    || stats["max_records"] != 1
                {
                    return Err("invalid individual-record window observation".into());
                }
            } else if !(1..=limit).contains(&peak) {
                return Err("observed APPEND window exceeded configured limit".into());
            }
            lanes += 1;
        }
    }
    if *partitions != lanes {
        return Err("missing APPEND window observations".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_omq_ownership_matches_requested_mode() {
        let dedicated = json!({"broker_omq_mode":"dedicated-io","broker_omq_io_threads":1});
        let borrowed = json!({"broker_omq_mode":"application-shard","broker_omq_io_threads":0});
        assert!(broker_omq_ownership(&dedicated, &json!({})).is_ok());
        assert!(broker_omq_ownership(&borrowed, &json!({"broker_omq_on_shard":true})).is_ok());
        assert!(broker_omq_ownership(&dedicated, &json!({"broker_omq_on_shard":true})).is_err());
        assert!(broker_omq_ownership(&borrowed, &json!({})).is_err());
    }

    #[test]
    fn record_lane_reports_its_own_window_and_rejects_mixed_profiles() {
        let mut row = json!({"writer_inflight_appends":2, "request_records":4096,
        "writers":[{"lanes":[{"protocol_requests":{
            "max_inflight_appends":0, "data_messages":100, "requests":100,
            "max_records":1, "max_inflight_records":32, "record_window":4096
        }}]}]});
        assert!(append_window(&row, 2, &json!(1)).is_ok());
        row["writers"][0]["lanes"][0]["protocol_requests"]["max_inflight_records"] = json!(4097);
        assert!(append_window(&row, 2, &json!(1)).is_err());
        row["writers"][0]["lanes"][0]["protocol_requests"]["max_inflight_records"] = json!(32);
        row["writers"][0]["lanes"][0]["protocol_requests"]["requests"] = json!(101);
        assert!(append_window(&row, 2, &json!(1)).is_err());
    }

    #[test]
    fn cluster_reader_credit_is_bounded_by_its_arena_for_both_adapters() {
        let config = json!({
            "reader_records":16384,"reader_payload_mib":128,"request_records":8192
        });
        for implementation in ["ozzy", "iggy"] {
            let mut row = json!({
                "record_bytes":8192,"request_records":8192,
                "writer_batch_records_max":8192,"records_per_append":8192,
                "reader_records_max":4146,"reader_payload_bytes_max":4146 * 8192
            });
            request_bounds(implementation, "replicated-persisting", &row, &config).unwrap();
            row["reader_records_max"] = json!(8192);
            row["reader_payload_bytes_max"] = json!(8192 * 8192);
            assert!(
                request_bounds(implementation, "replicated-persisting", &row, &config).is_err()
            );
        }
    }
}
