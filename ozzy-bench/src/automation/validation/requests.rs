//! Request, reader, and writer bounds every adapter reports back.
use super::Result;
use serde_json::Value;

pub(super) fn broker_omq_ownership(row: &Value, config: &Value) -> Result<()> {
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

/// SDK payload encoding and reader API of a native run.
pub(super) fn payload_encoding(row: &Value, config: &Value) -> Result<()> {
    if config.get("native_reader_api").is_some()
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
        .unwrap_or_else(|| crate::automation::compare::default_segment_mib(record_bytes))
        * 1024
        * 1024
}

/// Request and reader bounds of an external adapter run.
pub(super) fn request_bounds(
    implementation: &str,
    mode: &str,
    row: &Value,
    config: &Value,
) -> Result<()> {
    if implementation == "redpanda" {
        if row["write_caching"] != (mode == "buffered")
            || row["reader_records_max"] != 1
            || row["reader_payload_bytes_max"] != row["record_bytes"]
            || row["client_sdk"]["acks"] != "all"
            || row["client_sdk"]["unconfirmed_records_per_writer"] != config["request_records"]
        {
            return Err("unexpected Redpanda confirmation or SDK bounds".into());
        }
    } else {
        reader_bounds(mode, row, config)?;
    }
    if let Some(records) = config["request_records"].as_u64()
        && (row["request_records"] != records
            || row["records_per_append"]
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

/// Reader record and byte bounds. Cluster reads also fit the group request
/// and its bounded journal read arena.
fn reader_bounds(mode: &str, row: &Value, config: &Value) -> Result<()> {
    let Some(records) = config["reader_records"].as_u64() else {
        return Ok(());
    };
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
    let records = if mode == "disk-quorum" {
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
    fn cluster_reader_capacity_is_bounded_by_its_arena() {
        let config = json!({
            "reader_records":16384,"reader_payload_mib":128,"request_records":8192
        });
        let mut row = json!({
            "record_bytes":8192,"request_records":8192,"records_per_append":8192,
            "reader_records_max":4146,"reader_payload_bytes_max":4146 * 8192
        });
        request_bounds("iggy", "disk-quorum", &row, &config).unwrap();
        row["reader_records_max"] = json!(8192);
        row["reader_payload_bytes_max"] = json!(8192 * 8192);
        assert!(request_bounds("iggy", "disk-quorum", &row, &config).is_err());
    }
}
