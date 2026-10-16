//! Scheduled-load cohorts: fixed offered rates and continuous ramps.
use super::{Result, checked_p999, finite};
use serde_json::{Value, json};

pub(super) fn scheduled_measurements(
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
        || !crate::dedup::Feeds::attempts_match(row)
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
        let [physical, logical] = if field == "reader_delivery" {
            crate::dedup::Feeds::reported(row)
        } else {
            [1, 1]
        };
        if scheduled[field]["samples"] != measured * copies * logical / physical {
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
