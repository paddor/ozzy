//! The chart footer: one key per series, the measured topology, and captions
//! for each system's batching, disk and reader setup.
use super::{
    Region, XAxis, fixed, id,
    style::{self, SERIES, Series},
};
use ozzy_bench::automation::Result;
use plotters::prelude::*;
use serde_json::Value;
use std::fmt::Write;

pub(super) fn draw(area: &Region<'_>, data: &Value, rows: &[&Value], mode: &str) -> Result<()> {
    let present: Vec<_> = SERIES
        .into_iter()
        .filter(|s| rows.iter().any(|r| id(r) == s.id))
        .collect();
    // Fixed-load panels draw writer and reader latency together.
    let fixed = rows.iter().any(|r| r["case"]["rate"].as_u64().is_some());
    series_keys(area, data, &present, fixed)?;
    let lines_below_keys = i32::try_from(present.len())? * 16;
    if fixed {
        area.draw_text(
            &fixed_note(data)?,
            &style::muted(10),
            (78, 4 + lines_below_keys),
        )?;
    }
    let config = configuration(data, mode);
    area.draw_text(&topology(&config)?, &style::text(11), (380, 4))?;
    let mut reference = data["references"]["iggy"]["compatibility"]["configuration"].clone();
    if let Some(records) = data["batch_records"]["iggy"].as_u64() {
        reference["request_records"] = records.into();
    }
    let records = data["writer_protocols"][mode] == "push-records";
    let captions_y = 4 + lines_below_keys + if fixed { 16 } else { 0 };
    for (index, label) in batch_labels(&config, &reference, &present, records)?
        .iter()
        .enumerate()
    {
        area.draw_text(
            label,
            &style::muted(10),
            (78, captions_y + 16 * i32::try_from(index)?),
        )?;
    }
    Ok(())
}

/// A line sample and name per present series; fixed-load charts add the
/// lighter reader sample.
fn series_keys(area: &Region<'_>, data: &Value, present: &[Series], fixed: bool) -> Result<()> {
    let label_x = if fixed { 118 } else { 98 };
    for (index, series) in present.iter().enumerate() {
        let y = 4 + i32::try_from(index)? * 16;
        area.draw(&PathElement::new(
            vec![(78, y + 6), (92, y + 6)],
            series.color.stroke_width(2),
        ))?;
        if fixed {
            area.draw(&PathElement::new(
                vec![(98, y + 6), (112, y + 6)],
                fixed::reader_color(series.color).stroke_width(2),
            ))?;
        }
        let label = series_label(data, *series)?;
        // Fixed-load panels name their own backlog failures.
        area.draw_text(&label, &style::text(11), (label_x, y))?;
    }
    Ok(())
}

/// How fixed-load shades read, and which loads only Ozzy measured.
fn fixed_note(data: &Value) -> Result<String> {
    let mut note = "Writer confirmation: full color; verified reader: lighter shade".to_owned();
    for rate in super::ozzy_only_rates(data)? {
        write!(note, "; {} Ozzy only", XAxis::Rate.label(rate))?;
    }
    Ok(note)
}

/// The measured configuration with the batch, compression and payload cap
/// that the captions describe.
fn configuration(data: &Value, mode: &str) -> Value {
    let mut config = data["compatibility"]["configuration"].clone();
    if let Some(records) = data["batch_records"]["ozzy"].as_u64() {
        config["request_records"] = records.into();
    }
    if data["payload_compression"].is_string() {
        config["payload_compression"] = data["payload_compression"].clone();
    }
    config["observed_batch_payload_cap"] = data["writer_payload_caps"][mode].clone();
    config
}

fn topology(config: &Value) -> Result<String> {
    let mut topology = format!(
        "{} partitions",
        config["partitions"]
            .as_u64()
            .ok_or("missing partition count")?
    );
    if let Some(workers) = config["producer_workers"].as_u64() {
        write!(topology, ", {workers} writer processes")?;
    }
    if let Some(mib) = config["segment_mib"].as_u64() {
        write!(topology, ", {mib} MiB segments")?;
    }
    Ok(topology)
}

fn series_label(data: &Value, series: Series) -> Result<String> {
    let mut label = if let Some(implementation) = series.id.strip_suffix("/raw")
        && implementation != "ozzy"
    {
        let version = data["versions"][implementation]
            .as_str()
            .ok_or("missing recorded broker version")?;
        let name = if implementation == "iggy" {
            "Iggy"
        } else {
            "Redpanda"
        };
        let client = if implementation == "redpanda" {
            " (librdkafka)"
        } else {
            ""
        };
        format!("{name} {version}{client}")
    } else {
        series.label.to_owned()
    };
    if series.id == "ozzy/raw"
        && data["compatibility"]["configuration"]["broker_omq_on_shard"] == true
    {
        label.push_str(" (shard-owned OMQ)");
    }
    Ok(label)
}

fn batch_labels(
    config: &Value,
    reference: &Value,
    present: &[Series],
    record_lane: bool,
) -> Result<Vec<String>> {
    let records = config["request_records"].as_u64().unwrap_or(1024);
    let mut labels = vec![];
    if record_lane && present.iter().any(|s| s.id.starts_with("ozzy/")) {
        labels.push(format!(
            "Ozzy: 1 record/message; {records} queue slots/writer"
        ));
    } else if present.iter().any(|s| s.id.starts_with("ozzy/")) {
        labels.push(ozzy_batches(config, records)?);
        labels.extend(ozzy_storage_and_reader(config));
    }
    if present.iter().any(|s| s.id == "iggy/raw") {
        let records = reference["request_records"].as_u64().unwrap_or(records);
        labels.push(format!("Iggy: explicit batches up to {records} records"));
    }
    if present.iter().any(|s| s.id == "redpanda/raw") {
        let records = reference["request_records"].as_u64().unwrap_or(records);
        labels.push(format!(
            "Redpanda: per record; SDK batches up to {records} records or 4 MiB"
        ));
    }
    Ok(labels)
}

/// How the Ozzy writers batched records into APPENDs.
fn ozzy_batches(config: &Value, records: u64) -> Result<String> {
    let sdk = if config["payload_compression"] == "adaptive-lz4" {
        "adaptive LZ4 APPENDs"
    } else {
        "SDK batches"
    };
    let mut label = format!("Ozzy: per record; {sdk} up to {records} records");
    if let Some(bytes) = config["observed_batch_payload_cap"].as_u64() {
        write!(label, "; <= {} KiB payload", bytes / 1024)?;
    } else if let Some(bytes) = config["native_batch_target_bytes"].as_u64() {
        write!(label, "; {} MiB target", bytes / (1024 * 1024))?;
    }
    if let Some(requests) = config["writer_inflight_appends"].as_u64() {
        write!(label, "; {requests} in flight")?;
    }
    Ok(label)
}

/// The Ozzy disk and reader captions, when the run recorded them.
fn ozzy_storage_and_reader(config: &Value) -> Vec<String> {
    let mut labels = vec![];
    if let Some(depth) = config["disk_aio_depth"].as_u64() {
        let mut disk = if config["disk_io_backend"] == "pool" {
            "Ozzy disk: bounded pool".to_owned()
        } else {
            format!("Ozzy disk: AIO depth {depth}")
        };
        if config["disk_direct_io"] == true {
            disk.push_str("; direct writes");
        }
        labels.push(disk);
    }
    if config["live_readers"] == true {
        labels.push("Ozzy readers: live PUB; PEER repair".to_owned());
    }
    labels
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn worker_payload_cap_replaces_requested_target_in_caption() {
        let labels = batch_labels(
            &json!({"request_records":2048,"native_batch_target_bytes":4 * 1024 * 1024,
                "observed_batch_payload_cap":832 * 1024}),
            &Value::Null,
            &SERIES,
            false,
        )
        .unwrap();
        assert!(labels[0].contains("<= 832 KiB payload"));
        assert!(!labels[0].contains("4 MiB target"));
    }

    #[test]
    fn individual_record_lane_does_not_claim_sdk_batching() {
        let labels = batch_labels(
            &json!({"request_records":4096}),
            &Value::Null,
            &SERIES,
            true,
        )
        .unwrap();
        assert_eq!(labels[0], "Ozzy: 1 record/message; 4096 queue slots/writer");
        assert!(
            labels
                .iter()
                .any(|label| label.contains("Iggy: explicit batches"))
        );
        let labels = batch_labels(
            &json!({"request_records":1024,"payload_compression":"adaptive-lz4",
                "writer_inflight_appends":1,"disk_aio_depth":8}),
            &Value::Null,
            &SERIES,
            false,
        )
        .unwrap();
        assert_eq!(
            labels[0],
            "Ozzy: per record; adaptive LZ4 APPENDs up to 1024 records; 1 in flight"
        );
        assert_eq!(labels[1], "Ozzy disk: AIO depth 8");
    }
}
