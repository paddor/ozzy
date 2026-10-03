use super::super::{Result, error};
use super::{Config, Window, measurement::Histogram};
use serde_json::{Value, json};

pub(in crate::bench::timed) fn topic_topology(topics: usize, partitions: usize) -> Value {
    json!({"logical_stream_count":topics, "topic_count":topics,
        "partitions_per_topic":partitions, "partition_count":topics * partitions})
}

pub(super) fn totals(rows: &[Value], writers: usize) -> Result<Vec<u64>> {
    let mut totals = vec![None; writers];
    for worker in rows {
        for lane in worker["lanes"]
            .as_array()
            .ok_or_else(|| error("missing lane reports"))?
        {
            let index = lane["lane"].as_u64().ok_or_else(|| error("missing lane"))? as usize;
            let total = lane["total"]
                .as_u64()
                .ok_or_else(|| error("missing record count"))?;
            if index >= writers || totals[index].replace(total).is_some() {
                return Err(error("duplicate or invalid writer lane"));
            }
        }
    }
    totals
        .into_iter()
        .map(|n| n.ok_or_else(|| error("missing writer lane")))
        .collect()
}

/// Reports of one reader per partition; every copy must cover every partition.
pub(super) fn reader_copy(rows: &[Value], copy: usize) -> Result<Vec<Value>> {
    rows.iter()
        .map(|worker| {
            let lanes: Vec<_> = worker["lanes"]
                .as_array()
                .ok_or_else(|| error("missing lane reports"))?
                .iter()
                .filter(|lane| lane["copy"].as_u64().unwrap_or(0) == copy as u64)
                .cloned()
                .collect();
            Ok(json!({ "lanes": lanes }))
        })
        .collect()
}

pub(super) fn verify_digests(writers: &[Value], readers: &[Value], count: usize) -> Result<()> {
    let extract = |rows: &[Value]| -> Result<Vec<[u8; 16]>> {
        let mut digests = vec![None; count];
        for row in rows {
            for lane in row["lanes"]
                .as_array()
                .ok_or_else(|| error("missing lanes"))?
            {
                let index = lane["lane"]
                    .as_u64()
                    .ok_or_else(|| error("missing lane index"))?
                    as usize;
                let digest = serde_json::from_value::<[u8; 16]>(lane["digest_xxh3_128"].clone())?;
                if index >= count || digests[index].replace(digest).is_some() {
                    return Err(error("duplicate or invalid lane digest"));
                }
            }
        }
        digests
            .into_iter()
            .map(|d| d.ok_or_else(|| error("missing lane digest")))
            .collect()
    };
    if extract(writers)? != extract(readers)? {
        return Err(error("writer/reader full-byte digest mismatch"));
    }
    Ok(())
}

fn merge(rows: &mut [Value], field: &str, bins: usize) -> Result<(Vec<u64>, Value, u64)> {
    let mut counts = vec![0; bins];
    let mut hist = Histogram::new();
    let mut last = 0;
    for worker in rows {
        for lane in worker["lanes"]
            .as_array_mut()
            .ok_or_else(|| error("missing lanes"))?
        {
            let values = lane[field]
                .as_array()
                .ok_or_else(|| error("missing interval counts"))?;
            if values.len() != bins {
                return Err(error("incorrect interval count"));
            }
            for (sum, n) in counts.iter_mut().zip(values) {
                *sum += n.as_u64().ok_or_else(|| error("invalid interval count"))?;
            }
            hist.merge(&lane["latency"])?;
            last = last.max(
                lane["last_ns"]
                    .as_u64()
                    .ok_or_else(|| error("missing final clock"))?,
            );
        }
    }
    Ok((counts, hist.report(), last))
}

pub(super) fn summarize(
    config: &Config,
    window: Window,
    mut writers: Vec<Value>,
    mut readers: Vec<Value>,
    brokers: &[Value],
) -> Result<Value> {
    let seconds = (window.end - window.measured) as f64 / 1e9;
    let bins = seconds.ceil() as usize;
    let (confirmed, mut ack, writer_end) = merge(&mut writers, "completed_per_second", bins)?;
    let (submitted, _, _) = merge(&mut writers, "submitted_per_second", bins)?;
    let (mut delivered, mut e2e, reader_end) = merge(&mut readers, "completed_per_second", bins)?;
    // Every reader copy verifies every record; report the mean rate per copy.
    let copies = config.args.readers_per_partition as u64;
    let delivered_per_second = delivered.iter().sum::<u64>() as f64 / copies as f64 / seconds;
    for count in &mut delivered {
        *count /= copies;
    }
    ack["unit"] = json!(if config.args.streaming {
        "record"
    } else {
        "writer batch"
    });
    e2e["unit"] = json!("record");
    let total: u64 = totals(&writers, config.writers)?.iter().sum();
    if ack["samples"].as_u64() == Some(0) || total == 0 {
        return Err(error("empty timed measurement cohort"));
    }
    if confirmed.iter().all(|count| *count == 0) || delivered.iter().all(|count| *count == 0) {
        return Err(error(format!(
            "empty timed confirmation or delivery window: submitted={submitted:?}, confirmed={confirmed:?}, delivered={delivered:?}, total={total}, measurement_start_ns={}, measurement_end_ns={}, last_confirmation_ns={writer_end}, last_delivery_ns={reader_end}",
            window.measured, window.end,
        )));
    }
    let scheduled = scheduled(config, window, &writers, &readers)?;
    for worker in writers.iter_mut().chain(&mut readers) {
        worker["usage"]["cpu_scope"] =
            json!("worker process: continuous warmup, measurement, and drain; all threads");
        for lane in worker["lanes"].as_array_mut().unwrap() {
            lane.as_object_mut().unwrap().remove("latency");
            lane.as_object_mut().unwrap().remove("scheduled_latency");
            lane.as_object_mut().unwrap().remove("scheduling_lag");
        }
    }
    let intervals: Vec<_> = (0..bins)
        .map(|i| {
            let elapsed = (seconds - i as f64).min(1.0);
            json!({"second":i,"seconds":elapsed,"submitted_records":submitted[i],
            "confirmed_records":confirmed[i],"verified_records":delivered[i]})
        })
        .collect();
    let mut row = json!({"mode":"timed-throughput","record_bytes":config.args.record_bytes,
        "record_corpus":if config.args.binary_payload { ozzy_bench::workload::BINARY_EVENT_CORPUS } else if config.args.random_payload { "8-byte monotonic submission clock followed by deterministic SplitMix64 bytes" } else if config.args.json_payload { ozzy_bench::workload::JSON_EVENT_CORPUS } else { "8-byte monotonic submission clock followed by structured-v1" },
        "generation_cost":"included","verification_cost":"full bytes, ordered unique IDs, offsets, final writer counts and XXH3-128 digest; included",
        "measurement_seconds":seconds,"warmup_seconds":config.args.warmup,
        "measurement_start_ns":window.measured,"measurement_end_ns":window.end,
        "submitted_records_per_second":submitted.iter().sum::<u64>() as f64 / seconds,
        "producer_records_per_second":confirmed.iter().sum::<u64>() as f64 / seconds,
        "consumer_records_per_second":delivered_per_second,
        "total_confirmed_records":total,"total_verified_records":total,"intervals":intervals,
        "producer_ack":ack,"reader_delivery":e2e,
        "latency_cohort":"submissions in measurement window; includes final drain",
        "throughput_boundary":"confirmation observed / full record verified inside exact measurement window",
        "writer_drain_seconds":writer_end.saturating_sub(window.end) as f64 / 1e9,
        "reader_drain_seconds":reader_end.saturating_sub(window.end) as f64 / 1e9,
        "writers":writers,"readers":readers,"brokers":brokers,
        "scheduled_load":scheduled});
    let topology = if config.native.is_some() {
        super::native::topology(config, brokers)?
    } else {
        #[cfg(feature = "comparisons")]
        {
            if config.args.external_system.is_some() {
                super::external::topology(config)
            } else {
                return Err(error("missing production or external benchmark topology"));
            }
        }
        #[cfg(not(feature = "comparisons"))]
        {
            return Err(error("missing production benchmark topology"));
        }
    };
    row.as_object_mut()
        .unwrap()
        .extend(topology.as_object().unwrap().clone());
    Ok(row)
}

fn all_lanes(workers: &[Value]) -> Result<Vec<Value>> {
    let mut lanes = vec![];
    for worker in workers {
        lanes.extend(
            worker["lanes"]
                .as_array()
                .ok_or_else(|| error("missing scheduled lanes"))?
                .iter()
                .cloned(),
        );
    }
    Ok(lanes)
}

fn scheduled(
    config: &Config,
    window: Window,
    writers: &[Value],
    readers: &[Value],
) -> Result<Value> {
    if !config.args.scheduled() {
        return Ok(Value::Null);
    }
    let actual = totals(writers, config.writers)?;
    let copies = config.args.readers_per_partition;
    let delivered = (0..copies)
        .map(|copy| totals(&reader_copy(readers, copy)?, config.writers))
        .collect::<Result<Vec<_>>>()?;
    let (writer_lanes, reader_lanes) = (all_lanes(writers)?, all_lanes(readers)?);
    // A ramp ends at the first stage any writer lane could not admit.
    let overloaded = writer_lanes
        .iter()
        .filter_map(|lane| lane["overloaded_stage"].as_u64())
        .min()
        .map(usize::try_from)
        .transpose()?;
    let mut plans = vec![];
    for lane in 0..config.writers {
        let plan = super::pacing::Plan::new(config, window, lane)?.expect("scheduled load");
        if (overloaded.is_none() && actual[lane] != plan.total())
            || delivered.iter().any(|copy| copy[lane] != actual[lane])
        {
            return Err(error(
                "scheduled arrival count differs from confirmed/delivered count",
            ));
        }
        plans.push(plan);
    }
    let stage_count = plans[0].bounds().len() - 1;
    let merge = |lanes: &[Value], field: &str, stage: usize, expected: u64| -> Result<Value> {
        let mut histogram = Histogram::new();
        for lane in lanes {
            histogram.merge(&lane[field][stage])?;
        }
        let report = histogram.report();
        if report["samples"].as_u64() != Some(expected) {
            return Err(error("incomplete scheduled latency cohort"));
        }
        Ok(report)
    };
    let mut stages = vec![];
    for stage in 0..stage_count {
        let planned = plans
            .iter()
            .map(|plan| plan.stage_total(stage))
            .sum::<Result<u64>>()?;
        let bounds = plans[0].bounds();
        let seconds = (bounds[stage + 1] - bounds[stage]) as f64 / 1e9;
        let rate = config.args.ramp.as_ref().map_or_else(
            || config.args.records_per_second.expect("scheduled rate"),
            |ramp| ramp.stages()[stage].0,
        );
        stages.push(if overloaded.is_some_and(|failed| stage >= failed) {
            json!({"records_per_second":rate,"seconds":seconds,"planned_records":planned,
                "overloaded":if Some(stage) == overloaded { "scheduled backlog exceeded" } else { "not reached; an earlier stage overloaded" }})
        } else {
            json!({"records_per_second":rate,"seconds":seconds,"planned_records":planned,
                "producer_ack":merge(&writer_lanes, "scheduled_latency", stage, planned)?,
                "reader_delivery":merge(&reader_lanes, "scheduled_latency", stage, planned * copies as u64)?,
                "scheduling_lag":merge(&writer_lanes, "scheduling_lag", stage, planned)?})
        });
    }
    let mut row = json!({
        "scope":"total across writer connections; global ordinal modulo connections",
        "release_quantum_ns":0,
        "release_timer":"CLOCK_MONOTONIC timerfd with absolute deadlines; no additional quantization",
        "maximum_admission_lag_ns":super::pacing::MAX_ADMISSION_LAG_NS,
        "overload":if config.args.ramp.is_some() {
            "an arrival unadmitted for the maximum lag ends the ramp at its stage; admitted records drain"
        } else {
            "an arrival unadmitted for the maximum lag fails the run; preserve due times and drain every arrival scheduled before window end"
        },
        "planned_total_records":plans.iter().map(|plan| plan.total()).sum::<u64>(),
        "latency_cohort":"scheduled arrivals in measurement window, including admission and completion during drain",
        "submission_clock":"actual record generation starts; excludes implicit unadmitted scheduled backlog",
    });
    if let Some(ramp) = &config.args.ramp {
        row["ramp"] = json!(ramp.to_string());
        row["stages"] = json!(stages);
    } else {
        // One steady stage keeps the fixed-rate report shape.
        let [stage] =
            <[Value; 1]>::try_from(stages).map_err(|_| error("steady load has one stage"))?;
        row["records_per_second"] = stage["records_per_second"].clone();
        row["planned_measurement_records"] = stage["planned_records"].clone();
        for field in ["producer_ack", "reader_delivery", "scheduling_lag"] {
            row[field] = stage[field].clone();
        }
    }
    Ok(row)
}

pub(in crate::bench::timed) fn writer_protocol(row: &mut Value, config: &Config) {
    let payload_compression =
        if config.args.payload_compression == super::super::PayloadCompression::Off {
            "none"
        } else {
            "adaptive-lz4"
        };
    row["payload_compression"] = json!(payload_compression);
    row["payload_compression_threshold"] = if payload_compression == "none" {
        Value::Null
    } else {
        json!(ozzy_runtime::replicated::PAYLOAD_COMPRESSION_THRESHOLD)
    };
    row["native_reader_api"] = json!("decoded-records");
    row["writer_sdk_threads_per_process"] = json!(2);
    row["writer_sdk_omq_io_threads"] = json!(1);
    row["writer_sdk_runtime"] =
        json!("one SDK-owner Tokio current_thread runtime plus one owned OMQ I/O runtime");
    row["writer_intake"] = json!("typed fanring MPSC with one SPSC lane per producer handle");

    // Each native writer verifies bounded APPENDs against measured counters.
    row["transport"] = json!("TCP PEER APPEND batches, control, and confirmations");
    row["writer_protocol"] = json!("peer-appends");
    row["writer_effective_linger_us"] = json!(config.args.writer_linger_us);
    row["writer_wire_records_per_message"] = Value::Null;
    row["writer_batch_configuration_applies"] = json!(true);
    row["writer_batch_target_bytes"] = json!(4 * 1024 * 1024);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "comparisons")]
    use clap::Parser;

    #[cfg(feature = "comparisons")]
    #[test]
    fn drain_only_confirmations_or_delivery_cannot_publish_a_comparison() {
        let config = Config::new(super::super::super::Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "5",
            "--window",
            "1",
            "--external-system",
            "redpanda",
            "--external-policy",
            "durable",
            "--external-endpoint",
            "127.0.0.1:18090",
        ]))
        .unwrap();
        let window = Window {
            start: 1,
            measured: 10,
            end: 20,
        };
        let report = |finished| {
            let mut counts = super::super::measurement::Counts::new(window);
            counts.submission(12, 1);
            counts.complete(12, finished, 1).unwrap();
            json!({"lanes": [counts.report(0)]})
        };
        for (confirmed, delivered) in [(30, 30), (15, 30), (30, 15)] {
            let result = summarize(
                &config,
                window,
                vec![report(confirmed)],
                vec![report(delivered)],
                &[],
            );
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("empty timed confirmation or delivery window")
            );
        }
        assert!(summarize(&config, window, vec![report(15)], vec![report(16)], &[]).is_ok());
    }

    #[test]
    fn counts_and_digests_require_exact_lane_coverage_and_matching_bytes() {
        let lane = json!({"lane":0,"total":7,"digest_xxh3_128":vec![1_u8;16]});
        let rows = [json!({"lanes":[lane.clone()]})];
        assert_eq!(totals(&rows, 1).unwrap(), [7]);
        verify_digests(&rows, &rows, 1).unwrap();
        let mut changed = rows.clone();
        changed[0]["lanes"][0]["digest_xxh3_128"][0] = json!(2);
        assert!(verify_digests(&rows, &changed, 1).is_err());
        assert!(totals(&rows, 2).is_err());
        assert!(verify_digests(&rows, &rows, 2).is_err());
        let duplicate = [json!({"lanes":[lane.clone(), lane]})];
        assert!(totals(&duplicate, 1).is_err());
        assert!(verify_digests(&duplicate, &duplicate, 1).is_err());
        changed[0]["lanes"][0]["digest_xxh3_128"] = json!(vec![1_u8; 32]);
        assert!(verify_digests(&rows, &changed, 1).is_err());
    }
}
