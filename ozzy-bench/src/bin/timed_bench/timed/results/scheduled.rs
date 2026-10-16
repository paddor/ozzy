//! Scheduled load summary: per-lane arrival plans, per-stage latency
//! cohorts, and where an overloaded ramp ended.

use super::super::pacing::{MAX_ADMISSION_LAG_NS, Plan};
use super::{Args, Config, Histogram, Result, Value, Window, error, json, reader_copy, totals};

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

pub(super) fn scheduled(
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
    let expected = totals(
        &config
            .args
            .native
            .feeds
            .logical_rows(config.writers, writers)?,
        config.logical_writers(),
    )?;
    let delivered = (0..copies)
        .map(|copy| totals(&reader_copy(readers, copy)?, config.logical_writers()))
        .collect::<Result<Vec<_>>>()?;
    let stages = Stages::new(config, window, writers, readers, &actual)?;
    if delivered.iter().any(|copy| *copy != expected) {
        return Err(error(
            "scheduled unique arrivals differ from verified reader prefixes",
        ));
    }
    let reports = (0..stages.count())
        .map(|stage| stages.report(stage))
        .collect::<Result<Vec<_>>>()?;
    scheduled_row(config, &stages.plans, reports)
}

/// The arrival plans and lane reports behind one scheduled load summary.
struct Stages<'a> {
    config: &'a Config,
    plans: Vec<Plan>,
    /// First stage a writer lane could not admit; a ramp ends there.
    overloaded: Option<usize>,
    writer_lanes: Vec<Value>,
    reader_lanes: Vec<Value>,
}

impl<'a> Stages<'a> {
    /// Without overload, every writer lane confirms exactly its planned
    /// arrivals.
    fn new(
        config: &'a Config,
        window: Window,
        writers: &[Value],
        readers: &[Value],
        actual: &[u64],
    ) -> Result<Self> {
        let (writer_lanes, reader_lanes) = (all_lanes(writers)?, all_lanes(readers)?);
        let overloaded = writer_lanes
            .iter()
            .filter_map(|lane| lane["overloaded_stage"].as_u64())
            .min()
            .map(usize::try_from)
            .transpose()?;
        let mut plans = vec![];
        for (lane, &actual) in actual.iter().enumerate() {
            let plan = Plan::new(config, window, lane)?
                .ok_or_else(|| error("scheduled load lane has no arrival plan"))?;
            if overloaded.is_none() && actual != plan.total() {
                return Err(error(
                    "scheduled arrival count differs from confirmed/delivered count",
                ));
            }
            plans.push(plan);
        }
        Ok(Self {
            config,
            plans,
            overloaded,
            writer_lanes,
            reader_lanes,
        })
    }

    fn count(&self) -> usize {
        self.plans[0].bounds().len() - 1
    }

    /// One stage's planned load and, unless overload ended the ramp at or
    /// before it, its merged latency reports.
    fn report(&self, stage: usize) -> Result<Value> {
        let config = self.config;
        let planned = self
            .plans
            .iter()
            .map(|plan| plan.stage_total(stage))
            .sum::<Result<u64>>()?;
        let bounds = self.plans[0].bounds();
        let seconds = (bounds[stage + 1] - bounds[stage]) as f64 / 1e9;
        let rate = stage_rate(&config.args, stage)?;
        if self.overloaded.is_some_and(|failed| stage >= failed) {
            return Ok(
                json!({"records_per_second":rate,"seconds":seconds,"planned_records":planned,
                "overloaded":if Some(stage) == self.overloaded { "scheduled backlog exceeded" } else { "not reached; an earlier stage overloaded" }}),
            );
        }
        let copies = config.args.readers_per_partition as u64;
        let delivered = planned * copies * config.logical_writers() as u64 / config.writers as u64;
        Ok(
            json!({"records_per_second":rate,"seconds":seconds,"planned_records":planned,
            "producer_ack":merge_stage(&self.writer_lanes, "scheduled_latency", stage, planned)?,
            "reader_delivery":merge_stage(&self.reader_lanes, "scheduled_latency", stage, delivered)?,
            "scheduling_lag":merge_stage(&self.writer_lanes, "scheduling_lag", stage, planned)?}),
        )
    }
}

/// Offered records per second in one stage: the ramp stage's or the fixed
/// rate.
fn stage_rate(args: &Args, stage: usize) -> Result<u64> {
    match &args.ramp {
        Some(ramp) => Ok(ramp.stages()[stage].0),
        None => args
            .records_per_second
            .ok_or_else(|| error("scheduled load lacks an offered rate")),
    }
}

/// Merge one stage histogram over lanes; it must hold every expected sample.
fn merge_stage(lanes: &[Value], field: &str, stage: usize, expected: u64) -> Result<Value> {
    let mut histogram = Histogram::new();
    for lane in lanes {
        histogram.merge(&lane[field][stage])?;
    }
    let report = histogram.report();
    if report["samples"].as_u64() != Some(expected) {
        return Err(error("incomplete scheduled latency cohort"));
    }
    Ok(report)
}

/// The scheduled load summary: every ramp stage, or the one steady stage
/// flattened into the fixed-rate report shape.
fn scheduled_row(config: &Config, plans: &[Plan], stages: Vec<Value>) -> Result<Value> {
    let mut row = json!({
        "scope":"total across writer connections; global ordinal modulo connections",
        "release_quantum_ns":0,
        "release_timer":"CLOCK_MONOTONIC timerfd with absolute deadlines; no additional quantization",
        "maximum_admission_lag_ns":MAX_ADMISSION_LAG_NS,
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
