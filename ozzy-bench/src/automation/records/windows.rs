//! Fixed-load measurement windows: each offered rate's duration, warmup, and
//! planned arrivals.
use super::{Result, finite};
use serde_json::{Value, json};

/// Whether two fixed-load windows share their duration and warmup.
pub(super) fn same_fixed_load_timing(left: &Value, right: &Value) -> bool {
    ["duration", "warmup"]
        .iter()
        .all(|key| !left[key].is_null() && left[key] == right[key])
}

/// Seconds and exact planned arrivals of one offered rate: a whole fixed-rate
/// run, or one stage of a continuous ramp.
fn stage_window(config: &Value, rate: u64, warmup: f64) -> Result<(f64, u64)> {
    let warmup = (warmup * 1e9) as u64;
    let (plan, start, ns) = if let Some(ramp) = config["ramp"].as_str() {
        let ramp: crate::schedule::Ramp = ramp.parse()?;
        let index = ramp
            .stages()
            .iter()
            .position(|(stage, _)| *stage == rate)
            .ok_or("offered rate is not a ramp stage")?;
        let before: u64 = ramp.stages()[..index].iter().map(|(_, ns)| ns).sum();
        (ramp.plan(warmup)?, warmup + before, ramp.stages()[index].1)
    } else {
        let plan = crate::schedule::Load {
            records_per_second: Some(rate),
            ..Default::default()
        }
        .plan(1)?
        .ok_or("missing schedule")?;
        (plan, warmup, (finite(&config["duration"])? * 1e9) as u64)
    };
    let end = start.checked_add(ns).ok_or("window overflow")?;
    Ok((
        ns as f64 / 1e9,
        (plan.count(end)? - plan.count(start)?) as u64,
    ))
}

/// Each offered rate's window; every measured row must have its full
/// scheduled cohort and positive scheduled latencies.
pub(super) fn fixed_load_windows(rows: &[Value]) -> Result<Value> {
    let mut windows = serde_json::Map::new();
    for row in rows {
        let case = &row["case"];
        let rate = case["rate"]
            .as_u64()
            .filter(|r| *r > 0)
            .ok_or("positive offered load required")?;
        let config = &row["configuration"];
        if !matches!(case["impl"].as_str(), Some("ozzy" | "iggy" | "redpanda"))
            || !matches!(
                case["mode"].as_str(),
                Some("buffered" | "durable" | "disk-quorum")
            )
            || case["size"].as_u64().is_none_or(|size| size == 0)
            || (config["records_per_second"] != rate && config["ramp"].is_null())
            || config["repetitions"] != rows[0]["configuration"]["repetitions"]
        {
            return Err("fixed-load charts require record sizes, supported modes, and matching configured rates".into());
        }
        let warmup = finite(&config["warmup"])?;
        let (duration, expected) = stage_window(config, rate, warmup)?;
        let window =
            json!({"duration":duration,"warmup":warmup,"repetitions":config["repetitions"]});
        if let Some(prior) = windows.insert(rate.to_string(), window.clone())
            && prior != window
        {
            return Err("one offered load has mismatched measurement windows".into());
        }
        // An overloaded ramp stage has a window but no scheduled cohort.
        if row.get("failure").is_some() {
            continue;
        }
        if duration <= 0.0
            || warmup < 0.0
            || expected == 0
            || row["measurements"]["scheduled_samples"] != expected
        {
            return Err("missing or incomplete scheduled arrivals".into());
        }
        for metric in [
            "scheduled_ack_p50_us",
            "scheduled_ack_p99_us",
            "scheduled_delivery_p50_us",
            "scheduled_delivery_p99_us",
        ] {
            if finite(&row["measurements"][metric])? <= 0.0 {
                return Err("missing positive scheduled latency".into());
            }
        }
    }
    Ok(Value::Object(windows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_repetitions_may_differ_but_timing_must_match() {
        let measured = json!({"duration":8.0,"warmup":2.0,"repetitions":2});
        assert!(same_fixed_load_timing(
            &measured,
            &json!({"duration":8.0,"warmup":2.0,"repetitions":1})
        ));
        assert!(!same_fixed_load_timing(
            &measured,
            &json!({"duration":10.0,"warmup":2.0,"repetitions":1})
        ));
        assert!(!same_fixed_load_timing(
            &measured,
            &json!({"duration":8.0,"warmup":5.0,"repetitions":1})
        ));
    }
}
