//! Processing measurements retain the independently verified delivery cohort.

use super::{Config, Histogram, Result, Value, error, json};

pub(super) fn summarize(row: &mut Value, config: &Config) -> Result<()> {
    if config.args.native.groups == 0 {
        return Ok(());
    }
    let members = Members::sum(row)?;
    let brokers = BrokerProgress::sum(row)?;
    let total = row["total_confirmed_records"]
        .as_u64()
        .ok_or_else(|| error("missing stored count"))?;
    let BrokerProgress {
        updates,
        queries,
        update_rate,
    } = brokers;
    let bytes = super::encoded_bytes(row)?;
    row["processing_confirmation"] = members.histogram.report();
    row["processing_progress"] = json!({
        "confirmed_records":members.confirmed, "confirmed_records_per_second":members.rate,
        "handoff_redelivered":members.redelivered, "fenced_acknowledgments":members.fenced,
        "update_requests": if cfg!(feature = "comparisons") { json!(updates) } else { Value::Null },
        "update_requests_per_second": if cfg!(feature = "comparisons") { json!(update_rate) } else { Value::Null },
        "query_requests": if cfg!(feature = "comparisons") { json!(queries) } else { Value::Null },
        "acknowledgments_per_update_request": if updates == 0 { Value::Null } else { json!(members.confirmed as f64 / updates as f64) },
        "request_scope":"warmup, measurement, and drain; decoded source RPCs including retries and refusals; distinct from committed journal operations",
        "confirmation_latency_scope":"deliveries during measurement, through confirmed individual acknowledgment; includes final drain",
        "stored_records":total,"duplicate_submissions":0,
        "identity_workload":"all producer event IDs are unique",
        "encoded_storage_bytes": if cfg!(feature = "comparisons") { json!(bytes) } else { Value::Null },
        "encoded_storage_amplification": if bytes == 0 || total == 0 { Value::Null } else { json!(bytes as f64 / (total as f64 * config.args.record_bytes as f64)) },
        "storage_scope":"all broker copies, including progress, group plans, seals, and padding; excludes headers, indexes, manifests, and unused capacity",
    });
    Ok(())
}

/// Processing confirmation totals over every group member report.
struct Members {
    histogram: Histogram,
    confirmed: u64,
    rate: f64,
    redelivered: u64,
    fenced: u64,
}

impl Members {
    fn sum(row: &Value) -> Result<Self> {
        let mut members = Self {
            histogram: Histogram::new(),
            confirmed: 0,
            rate: 0.0,
            redelivered: 0,
            fenced: 0,
        };
        for worker in row["readers"]
            .as_array()
            .ok_or_else(|| error("missing reader reports"))?
        {
            for member in worker["group_members"]
                .as_array()
                .ok_or_else(|| error("missing group members"))?
            {
                members.add(member)?;
            }
        }
        Ok(members)
    }

    fn add(&mut self, member: &Value) -> Result<()> {
        self.histogram
            .merge(&member["processing_confirmation"]["histogram"])?;
        self.confirmed += member["confirmed_records"]
            .as_u64()
            .ok_or_else(|| error("missing confirmed processing count"))?;
        self.rate += member["confirmed_records_per_second"]
            .as_f64()
            .ok_or_else(|| error("missing processing rate"))?;
        self.redelivered += member["handoff_redelivered"]
            .as_u64()
            .ok_or_else(|| error("missing redelivery count"))?;
        self.fenced += member["fenced_acknowledgments"]
            .as_u64()
            .ok_or_else(|| error("missing fenced count"))?;
        Ok(())
    }
}

/// Group progress requests over every broker report; a broker without them
/// counts zero.
#[derive(Default)]
struct BrokerProgress {
    updates: u64,
    queries: u64,
    update_rate: f64,
}

impl BrokerProgress {
    fn sum(row: &Value) -> Result<Self> {
        let mut progress = Self::default();
        for broker in row["brokers"]
            .as_array()
            .ok_or_else(|| error("missing brokers"))?
        {
            let usage = &broker["usage"];
            progress.updates += usage["group_progress"]["update_requests"]
                .as_u64()
                .unwrap_or(0);
            progress.queries += usage["group_progress"]["query_requests"]
                .as_u64()
                .unwrap_or(0);
            progress.update_rate += usage["group_progress"]["update_requests_per_second"]
                .as_f64()
                .unwrap_or(0.0);
        }
        Ok(progress)
    }
}
