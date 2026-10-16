//! Verification and residency are mandatory before any result enters a ledger.
mod production;
mod requests;
mod schedule;
use super::{Result, finite};
pub use production::production_cpu_placement;
use serde_json::{Value, json};

/// Validate worker counts, throughput, latency, and verification evidence for one case.
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
    if let Some(shards) = case["app_threads"].as_u64()
        && (shards == 0 || row["broker_application_shards"] != shards)
    {
        return Err("unexpected native application shard count".into());
    }
    requested_controls(case, row)?;
    if row["total_verified_records"].as_u64().is_none() || !crate::dedup::Feeds::attempts_match(row)
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

/// Each requested workload control the worker reports back, scaled to its unit.
fn requested_controls(case: &Value, row: &Value) -> Result<()> {
    for (requested, actual, scale) in [
        ("writer_batch_records", "writer_batch_records_max", 1),
        ("writer_inflight_appends", "writer_inflight_appends", 1),
        ("segment_mib", "segment_bytes", 1024 * 1024),
    ] {
        if let Some(value) = case[requested].as_u64()
            && row[actual] != value * scale
        {
            return Err(format!("unexpected {actual}").into());
        }
    }
    Ok(())
}

fn checked_p999(report: &Value, value: &Value) -> Result<f64> {
    let value = finite(value)?;
    if value < finite(&report["p99_us"])? || value > finite(&report["max_us"])? {
        return Err("p99.9 outside p99/maximum bounds".into());
    }
    Ok(value)
}

/// Validate comparison policy and resource contracts against the recorded run configuration.
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
    if implementation == "ozzy" {
        if row["broker_runtime"] != "production-deployment" {
            return Err("native rows must come from the production deployment".into());
        }
        return production::production_comparison(case, row, config, mode);
    }
    requests::request_bounds(implementation, mode, row, config)?;
    let mut result = measurements(
        &json!({"profile":format!("{implementation}-{mode}"),"partitions":config["partitions"]}),
        row,
    )?;
    requested_window(row, config)?;
    derived_rates(&mut result, row, finite(&case["size"])?)?;
    schedule::scheduled_measurements(case, row, config, &mut result)?;
    Ok(result)
}

/// The worker measured exactly the requested warmup and window.
fn requested_window(row: &Value, config: &Value) -> Result<()> {
    if (finite(&row["warmup_seconds"])? - finite(&config["warmup"])?).abs() > 1e-6
        || (finite(&row["measurement_seconds"])? - finite(&config["duration"])?).abs() > 1e-6
    {
        return Err("warmup or measured window differs from requested configuration".into());
    }
    Ok(())
}

/// Byte rates, reader drain, and delivery fraction of `bytes`-sized records.
fn derived_rates(result: &mut Value, row: &Value, bytes: f64) -> Result<()> {
    result["confirmed_mib_s"] = json!(finite(&result["confirmed_s"])? * bytes / 1_048_576.0);
    result["verified_mib_s"] = json!(finite(&result["delivered_s"])? * bytes / 1_048_576.0);
    result["reader_drain_seconds"] = json!(finite(&row["reader_drain_seconds"])?);
    result["delivery_fraction"] =
        json!(finite(&result["delivered_s"])? / finite(&result["confirmed_s"])?);
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
    let [physical, logical] = crate::dedup::Feeds::reported(row);
    if partitions
        .as_u64()
        .and_then(|n| n.checked_mul(physical))
        .map(|n| n / logical)
        != Some(lanes)
    {
        return Err("missing APPEND window observations".into());
    }
    Ok(())
}

fn feeds(config: &Value) -> Result<crate::dedup::Feeds> {
    Ok(crate::dedup::Feeds {
        redundant_producers: u32::try_from(config["redundant_producers"].as_u64().unwrap_or(1))?,
        hot_partition_percent: u32::try_from(
            config["hot_partition_percent"].as_u64().unwrap_or(0),
        )?,
        redundant_feeds: u32::try_from(config["redundant_feeds"].as_u64().unwrap_or(0))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
