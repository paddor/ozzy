//! Verified unique records and redundant attempt accounting.

use super::{Config, Result, Value, error, json};

pub(super) fn summarize(row: &mut Value, config: &Config) -> Result<()> {
    if !config.args.native.feeds.enabled() {
        return Ok(());
    }
    let total = row["total_confirmed_records"]
        .as_u64()
        .ok_or_else(|| error("missing verified attempt count"))?;
    let unique = row["total_verified_records"]
        .as_u64()
        .ok_or_else(|| error("missing verified unique record count"))?;
    let dispositions = confirmed_dispositions(row)?;
    if dispositions != [unique, total - unique, 0] {
        return Err(error(
            "SDK dispositions disagree with independently verified unique records",
        ));
    }
    let encoded = super::encoded_bytes(row)?;
    let destinations = feeds_per_partition(config)?;
    let rate = config
        .args
        .records_per_second
        .ok_or_else(|| error("redundant feeds require a fixed offered rate"))?;
    let logical = config.logical_writers() as u64;
    row["dedup_ingest"] = json!({
        "redundant_producers":config.args.native.feeds.redundant_producers,
        "redundant_feeds":config.args.native.feeds.redundant_feeds,
        "physical_feeds":config.writers, "logical_feeds":logical,
        "hot_partition_percent":config.args.native.feeds.hot_partition_percent,
        "logical_feeds_per_partition":destinations,
        "confirmed_attempts":total, "stored_records":unique,
        "confirmed_stored": dispositions[0], "confirmed_duplicate": dispositions[1],
        "duplicate_submissions":total - unique,
        "offered_unique_events_per_second": rate * logical / config.writers as u64,
        "duplicate_fraction": (total - unique) as f64 / total as f64,
        "verification":"independently generated matching IDs/bytes and confirmed offsets across every redundant feed; each unique ID and stored byte verified once by readers",
        "disposition_scope":"per-attempt SDK receipt dispositions, checked against independently verified unique records and all redundant feed counts",
        "encoded_storage_bytes":if cfg!(feature="comparisons") { json!(encoded) } else { Value::Null },
        "encoded_storage_amplification": if encoded == 0 { Value::Null } else { json!(encoded as f64 / (unique as f64 * config.args.record_bytes as f64)) },
        "storage_scope":"all broker copies during warmup, measurement, and drain; includes duplicate-result operations, seals, padding; excludes file headers, indexes, manifests and unused capacity"
    });
    Ok(())
}

/// Stored, duplicate, and scheduled SDK receipt dispositions over every
/// writer lane.
pub(super) fn confirmed_dispositions(row: &Value) -> Result<[u64; 3]> {
    let mut dispositions = [0_u64; 3];
    for worker in row["writers"]
        .as_array()
        .ok_or_else(|| error("missing writers"))?
    {
        for lane in worker["lanes"]
            .as_array()
            .ok_or_else(|| error("missing writer lanes"))?
        {
            for (total, key) in dispositions
                .iter_mut()
                .zip(["stored", "duplicate", "scheduled"])
            {
                *total += lane["confirmed_dispositions"][key]
                    .as_u64()
                    .ok_or_else(|| error("missing confirmed dispositions"))?;
            }
        }
    }
    Ok(dispositions)
}

/// How many logical feeds land on each native partition.
fn feeds_per_partition(config: &Config) -> Result<Vec<u64>> {
    let native = config
        .native
        .as_ref()
        .ok_or_else(|| error("redundant feeds require native settings"))?;
    let mut destinations = vec![0; native.partitions];
    for lane in 0..config.logical_writers() {
        let part = config.feed_partition(lane, destinations.len());
        destinations[part] += 1;
    }
    Ok(destinations)
}
