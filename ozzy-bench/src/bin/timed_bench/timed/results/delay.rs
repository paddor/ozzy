//! Held records: every receipt scheduled, and lateness past each due time.

use super::{Config, Result, Value, dedup::confirmed_dispositions, error, json};

pub(super) fn summarize(row: &mut Value, config: &Config) -> Result<()> {
    let Some(millis) = config.args.native.delay_millis else {
        return Ok(());
    };
    let total = row["total_confirmed_records"]
        .as_u64()
        .ok_or_else(|| error("missing confirmed record count"))?;
    if confirmed_dispositions(row)? != [0, 0, total] {
        return Err(error("a held record's receipt was not scheduled"));
    }
    let delivery = &row["reader_delivery"];
    let mut lateness = json!({});
    for field in ["samples", "p50_us", "p99_us", "p999_us", "max_us"] {
        lateness[field] = delivery[field].clone();
    }
    row["delay"] = json!({
        "delay_millis": millis,
        "max_entries": config.args.native.delay_max_entries,
        "confirmed_scheduled": total,
        "lateness": lateness,
        "lateness_scope": "reader_delivery: verified delivery after send clock plus delay, which each due time never precedes; reader window, throughput, intervals, and latency cohort move by the delay",
    });
    Ok(())
}
