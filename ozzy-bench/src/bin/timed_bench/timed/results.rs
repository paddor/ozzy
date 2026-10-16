mod dedup;
mod groups;
mod scheduled;

use super::super::{Args, Result, error};
use super::{Config, Window, measurement::Histogram};
use scheduled::scheduled;
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
    let cohort = Cohort::merge(config, window, &mut writers, &mut readers)?;
    let scheduled = scheduled(config, window, &writers, &readers)?;
    finish_worker_reports(writers.iter_mut().chain(&mut readers))?;
    let seconds = cohort.seconds;
    let mut row = json!({"mode":"timed-throughput","record_bytes":config.args.record_bytes,
        "record_corpus":record_corpus(&config.args),
        "generation_cost":"included","verification_cost":"full bytes, ordered unique IDs, offsets, final writer counts and XXH3-128 digest; included",
        "measurement_seconds":seconds,"warmup_seconds":config.args.warmup,
        "measurement_start_ns":window.measured,"measurement_end_ns":window.end,
        "submitted_records_per_second":cohort.submitted.iter().sum::<u64>() as f64 / seconds,
        "producer_records_per_second":cohort.confirmed.iter().sum::<u64>() as f64 / seconds,
        "consumer_records_per_second":cohort.delivered_per_second,
        "total_confirmed_records":cohort.total,"total_verified_records":cohort.unique,"intervals":cohort.intervals(),
        "producer_ack":cohort.ack,"reader_delivery":cohort.e2e,
        "latency_cohort":"submissions in measurement window; includes final drain",
        "throughput_boundary":"confirmation observed / full record verified inside exact measurement window",
        "writer_drain_seconds":cohort.writer_end.saturating_sub(window.end) as f64 / 1e9,
        "reader_drain_seconds":cohort.reader_end.saturating_sub(window.end) as f64 / 1e9,
        "writers":writers,"readers":readers,"brokers":brokers,
        "scheduled_load":scheduled});
    groups::summarize(&mut row, config)?;
    dedup::summarize(&mut row, config)?;
    extend_object(&mut row, topology(config, brokers)?)?;
    Ok(row)
}

/// Per-second counts, latency reports and totals merged over every lane of
/// the measured cohort.
struct Cohort {
    seconds: f64,
    submitted: Vec<u64>,
    confirmed: Vec<u64>,
    /// Verified records per reader copy.
    delivered: Vec<u64>,
    delivered_per_second: f64,
    ack: Value,
    e2e: Value,
    writer_end: u64,
    reader_end: u64,
    total: u64,
    unique: u64,
}

impl Cohort {
    /// Merge the measured lanes. An empty cohort or window fails the run.
    fn merge(
        config: &Config,
        window: Window,
        writers: &mut [Value],
        readers: &mut [Value],
    ) -> Result<Self> {
        let seconds = (window.end - window.measured) as f64 / 1e9;
        let bins = seconds.ceil() as usize;
        let (confirmed, mut ack, writer_end) = merge(writers, "completed_per_second", bins)?;
        let (submitted, _, _) = merge(writers, "submitted_per_second", bins)?;
        let (mut delivered, mut e2e, reader_end) = merge(readers, "completed_per_second", bins)?;
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
        let total: u64 = totals(writers, config.writers)?.iter().sum();
        let unique: u64 = totals(
            &config
                .args
                .native
                .feeds
                .logical_rows(config.writers, writers)?,
            config.logical_writers(),
        )?
        .iter()
        .sum();
        if ack["samples"].as_u64() == Some(0) || total == 0 {
            return Err(error("empty timed measurement cohort"));
        }
        if confirmed.iter().all(|count| *count == 0) || delivered.iter().all(|count| *count == 0) {
            return Err(error(format!(
                "empty timed confirmation or delivery window: submitted={submitted:?}, confirmed={confirmed:?}, delivered={delivered:?}, total={total}, measurement_start_ns={}, measurement_end_ns={}, last_confirmation_ns={writer_end}, last_delivery_ns={reader_end}",
                window.measured, window.end,
            )));
        }
        Ok(Self {
            seconds,
            submitted,
            confirmed,
            delivered,
            delivered_per_second,
            ack,
            e2e,
            writer_end,
            reader_end,
            total,
            unique,
        })
    }

    /// One row per measured second; the last one may be partial.
    fn intervals(&self) -> Vec<Value> {
        (0..self.confirmed.len())
            .map(|i| {
                let elapsed = (self.seconds - i as f64).min(1.0);
                json!({"second":i,"seconds":elapsed,"submitted_records":self.submitted[i],
                "confirmed_records":self.confirmed[i],"verified_records":self.delivered[i]})
            })
            .collect()
    }
}

fn record_corpus(args: &Args) -> &'static str {
    if args.binary_payload {
        ozzy_bench::workload::BINARY_EVENT_CORPUS
    } else if args.random_payload {
        "8-byte monotonic submission clock followed by deterministic SplitMix64 bytes"
    } else if args.json_payload {
        ozzy_bench::workload::JSON_EVENT_CORPUS
    } else {
        "8-byte monotonic submission clock followed by structured-v1"
    }
}

/// Scope each worker report's usage to the whole worker process, and drop the
/// per-lane histograms the cohort already merged.
fn finish_worker_reports<'a>(workers: impl Iterator<Item = &'a mut Value>) -> Result<()> {
    for worker in workers {
        worker["usage"]["cpu_scope"] =
            json!("worker process: continuous warmup, measurement, and drain; all threads");
        for lane in worker["lanes"]
            .as_array_mut()
            .ok_or_else(|| error("missing lane reports"))?
        {
            let lane = lane
                .as_object_mut()
                .ok_or_else(|| error("lane report is not an object"))?;
            for field in ["latency", "scheduled_latency", "scheduling_lag"] {
                lane.remove(field);
            }
        }
    }
    Ok(())
}

/// The deployment fields of a production or external run.
fn topology(config: &Config, brokers: &[Value]) -> Result<Value> {
    if config.native.is_some() {
        return super::native::topology(config, brokers);
    }
    #[cfg(feature = "comparisons")]
    if config.args.external_system.is_some() {
        return Ok(super::external::topology(config));
    }
    Err(error(if cfg!(feature = "comparisons") {
        "missing production or external benchmark topology"
    } else {
        "missing production benchmark topology"
    }))
}

/// Encoded journal group bytes over every broker copy.
pub(super) fn encoded_bytes(row: &Value) -> Result<u64> {
    Ok(row["brokers"]
        .as_array()
        .ok_or_else(|| error("missing broker reports"))?
        .iter()
        .map(|broker| {
            broker["usage"]["journal_writes"]["encoded_group_bytes"]
                .as_u64()
                .unwrap_or(0)
        })
        .sum())
}

/// Add every field of the JSON object `fields` to the JSON object `row`.
pub(in crate::bench::timed) fn extend_object(row: &mut Value, fields: Value) -> Result<()> {
    let (Some(row), Value::Object(fields)) = (row.as_object_mut(), fields) else {
        return Err(error(
            "benchmark rows and their fields must be JSON objects",
        ));
    };
    row.extend(fields);
    Ok(())
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
    // Recorded only when selected, so plain TCP rows keep their shape.
    if config.args.transport != ozzy_bench::native::Transport::Tcp {
        row["endpoint_transport"] = json!(config.args.transport.scheme().as_str());
    }
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
