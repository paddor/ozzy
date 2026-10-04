//! Matched release measurements; no averaging across cells or lost coverage.
use ozzy_bench::automation::Result;
use serde_json::{Value, json};
use std::collections::BTreeMap;

type Case = (String, u64, u64);
const LATENCIES: [&str; 6] = [
    "ack_p50_us",
    "ack_p99_us",
    "ack_p999_us",
    "delivery_p50_us",
    "delivery_p99_us",
    "delivery_p999_us",
];

fn rows(data: &Value, fixed: bool) -> Result<BTreeMap<Case, &Value>> {
    let mut rows = BTreeMap::new();
    for row in data["summary"].as_array().ok_or("missing summary")? {
        let case = &row["case"];
        if case["impl"] != "ozzy" {
            continue;
        }
        let mode = case["mode"].as_str().ok_or("missing mode")?;
        let size = case["size"].as_u64().ok_or("missing size")?;
        let rate = case["rate"].as_u64().unwrap_or(0);
        if !matches!(mode, "durable" | "disk-quorum" | "replicated-persisting")
            || !matches!(size, 128 | 1024 | 8192)
            || case["codec"] != "raw"
            || case["pattern"] != "events"
            || fixed != (rate != 0)
            || (fixed
                && (!matches!(rate, 100 | 1000 | 10000 | 100_000)
                    || (size == 8192 && rate > 10000)))
        {
            return Err("regression gate requires the release chart workload".into());
        }
        if rows.insert((mode.into(), size, rate), row).is_some() {
            return Err("duplicate performance cell".into());
        }
    }
    if rows.is_empty() {
        return Err("no Ozzy performance cells".into());
    }
    Ok(rows)
}

fn measurement(row: &Value, metric: &str) -> Result<(f64, f64, f64)> {
    let stats = &row["measurements"][metric];
    let number = |field: &str| -> Result<f64> {
        stats[field]
            .as_f64()
            .filter(|value| value.is_finite() && *value > 0.0)
            .ok_or_else(|| format!("missing or invalid {metric}.{field}").into())
    };
    let (min, median, max) = (number("minimum")?, number("median")?, number("maximum")?);
    if min > median || median > max || row["repetitions"].as_u64().unwrap_or(0) < 2 {
        return Err("performance gate needs two complete repetitions and valid ranges".into());
    }
    Ok((min, median, max))
}

pub(crate) fn compare(baseline: &Value, candidate: &Value, fixed: bool) -> Result<Value> {
    let prior = rows(baseline, fixed)?;
    let current = rows(candidate, fixed)?;
    if prior.keys().ne(current.keys()) {
        return Err("performance cell coverage changed".into());
    }
    for (mode, _, _) in prior.keys() {
        for size in [128, 1024, 8192] {
            let rates: &[u64] = if !fixed {
                &[0]
            } else if size == 8192 {
                &[100, 1000, 10000]
            } else {
                &[100, 1000, 10000, 100_000]
            };
            for rate in rates {
                if !prior.contains_key(&(mode.clone(), size, *rate)) {
                    return Err(format!("missing release cell {mode}/{size}/{rate}").into());
                }
            }
        }
    }
    let compatible = !baseline["compatibility"].is_null()
        && baseline["compatibility"] == candidate["compatibility"]
        && baseline["fixed_load_windows"] == candidate["fixed_load_windows"]
        && baseline["batch_records"] == candidate["batch_records"]
        && baseline["writer_payload_caps"] == candidate["writer_payload_caps"]
        && baseline["payload_compression"] == candidate["payload_compression"];
    let compatible = compatible && baseline["writer_protocols"] == candidate["writer_protocols"];
    let mut comparisons = Vec::new();
    let mut regressed = false;
    for (case, before) in &prior {
        let after = current[case];
        let metrics = LATENCIES
            .iter()
            .map(|key| {
                if fixed {
                    format!("scheduled_{key}")
                } else {
                    (*key).into()
                }
            })
            .chain(
                (!fixed)
                    .then_some(["confirmed_s", "delivered_s"])
                    .into_iter()
                    .flatten()
                    .map(str::to_owned),
            );
        for metric in metrics {
            let (before_min, before_median, before_max) = measurement(before, &metric)?;
            let (after_min, after_median, after_max) = measurement(after, &metric)?;
            let throughput = matches!(metric.as_str(), "confirmed_s" | "delivered_s");
            let ratio = after_median / before_median;
            let regression = if throughput {
                ratio <= 0.95
            } else {
                ratio >= 1.05
            };
            regressed |= regression;
            comparisons.push(json!({
                "mode":case.0,"size":case.1,"rate":case.2,"metric":metric,
                "baseline":before["measurements"][&metric],"candidate":after["measurements"][&metric],
                "delta_percent":(ratio-1.0)*100.0,"regression":regression,
                "overlapping_ranges":before_min <= after_max && after_min <= before_max
            }));
        }
    }
    Ok(json!({
        "status":if !compatible { "incomparable" } else if regressed { "regression" } else { "pass" },
        "threshold_percent":5,"compatible":compatible,"comparisons":comparisons,
        "baseline":baseline,"candidate":candidate
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> Value {
        let mut rows = Vec::new();
        for size in [128, 1024, 8192] {
            let mut measurements = serde_json::Map::new();
            for key in LATENCIES.into_iter().chain(["confirmed_s", "delivered_s"]) {
                measurements.insert(
                    key.into(),
                    json!({"minimum":100,"median":100,"maximum":100}),
                );
            }
            rows.push(json!({"case":{"impl":"ozzy","mode":"durable","size":size,
                "codec":"raw","pattern":"events","rate":null},"repetitions":2,"measurements":measurements}));
        }
        json!({"summary":rows,"compatibility":{"compiler":"same"}})
    }

    #[test]
    fn any_cell_at_five_percent_fails_even_when_other_cells_improve() {
        let baseline = data();
        let mut candidate = baseline.clone();
        candidate["summary"][0]["measurements"]["confirmed_s"] =
            json!({"minimum":95,"median":95,"maximum":95});
        candidate["summary"][1]["measurements"]["confirmed_s"] =
            json!({"minimum":200,"median":200,"maximum":200});
        assert_eq!(
            compare(&baseline, &candidate, false).unwrap()["status"],
            "regression"
        );
        candidate = baseline.clone();
        candidate["summary"][2]["measurements"]["delivery_p99_us"] =
            json!({"minimum":105,"median":105,"maximum":105});
        assert_eq!(
            compare(&baseline, &candidate, false).unwrap()["status"],
            "regression"
        );
        assert_eq!(
            compare(&baseline, &baseline, false).unwrap()["status"],
            "pass"
        );
    }

    #[test]
    fn incompatible_or_incomplete_evidence_never_passes() {
        let baseline = data();
        let mut candidate = baseline.clone();
        candidate["compatibility"]["compiler"] = json!("different");
        assert_eq!(
            compare(&baseline, &candidate, false).unwrap()["status"],
            "incomparable"
        );
        candidate["summary"].as_array_mut().unwrap().pop();
        assert!(compare(&baseline, &candidate, false).is_err());
        assert!(compare(&data_with_one_repeat(), &baseline, false).is_err());
    }

    fn data_with_one_repeat() -> Value {
        let mut data = data();
        data["summary"][0]["repetitions"] = json!(1);
        data
    }

    #[test]
    fn fixed_load_gates_scheduled_latency_and_rejects_removed_rates() {
        let mut fixed = data();
        let mut rows = Vec::new();
        for row in fixed["summary"].as_array().unwrap() {
            let size = row["case"]["size"].as_u64().unwrap();
            for rate in [100, 1000, 10000, 100_000] {
                if size == 8192 && rate > 10000 {
                    continue;
                }
                let mut row = row.clone();
                row["case"]["rate"] = json!(rate);
                for metric in LATENCIES {
                    row["measurements"][format!("scheduled_{metric}")] =
                        row["measurements"][metric].clone();
                }
                rows.push(row);
            }
        }
        fixed["summary"] = json!(rows);
        assert_eq!(
            compare(&fixed, &fixed, true).unwrap()["comparisons"]
                .as_array()
                .unwrap()
                .len(),
            66
        );
        let mut candidate = fixed.clone();
        candidate["summary"][0]["measurements"]["scheduled_ack_p999_us"] =
            json!({"minimum":105,"median":105,"maximum":105});
        assert_eq!(
            compare(&fixed, &candidate, true).unwrap()["status"],
            "regression"
        );
        candidate["summary"][0]["case"]["rate"] = json!(1_000_000);
        assert!(compare(&candidate, &candidate, true).is_err());
    }
}
