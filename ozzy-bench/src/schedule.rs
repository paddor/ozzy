//! Deterministic offered arrivals, independent of transport completion speed.

use std::ops::Range;
use std::process::Command;

use serde_json::{Value, json};

use crate::{BenchResult, bench_error};

/// Opt-in rate control shared by benchmark adapters. Rates are total records,
/// not per connection or process. Each batch arrives as one scheduling unit.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct Load {
    /// Total offered records per second. Omit for completion-paced saturation.
    #[arg(long)]
    pub records_per_second: Option<u64>,
    /// Repeat period for bursts; requires --burst-active-ms and a target rate.
    #[arg(long, requires_all = ["records_per_second", "burst_active_ms"])]
    pub burst_period_ms: Option<u64>,
    /// Active part of each burst period. Rate is the whole-period average.
    #[arg(long, requires_all = ["records_per_second", "burst_period_ms"])]
    pub burst_active_ms: Option<u64>,
}

impl Load {
    pub fn plan(&self, batch: usize) -> BenchResult<Option<Schedule>> {
        let Some(rate) = self.records_per_second else {
            if self.burst_period_ms.is_some() || self.burst_active_ms.is_some() {
                return Err(bench_error("bursts require an offered record rate"));
            }
            return Ok(None);
        };
        if !(1..=1_000_000_000).contains(&rate) || !(1..=1024).contains(&batch) {
            return Err(bench_error(
                "scheduled rate must be 1..1000000000 records/s and batch 1..1024",
            ));
        }
        let (period, active) = match (self.burst_period_ms, self.burst_active_ms) {
            (None, None) => (1, 1),
            (Some(period), Some(active))
                if (1..=60_000).contains(&period) && active > 0 && active <= period =>
            {
                (period, active)
            }
            _ => {
                return Err(bench_error(
                    "burst times must satisfy 1 <= active <= period <= 60000 ms",
                ));
            }
        };
        Ok(Some(Schedule {
            stages: [Stage {
                rate,
                start: 0,
                first: 0,
            }; MAX_STAGES],
            count: 1,
            batch,
            period: period * 1_000_000,
            active: active * 1_000_000,
        }))
    }

    pub fn append_args(&self, command: &mut Command) {
        for (flag, value) in [
            ("--records-per-second", self.records_per_second),
            ("--burst-period-ms", self.burst_period_ms),
            ("--burst-active-ms", self.burst_active_ms),
        ] {
            if let Some(value) = value {
                command.arg(flag).arg(value.to_string());
            }
        }
    }

    pub fn label(&self) -> &'static str {
        if self.burst_period_ms.is_some() {
            "bursty"
        } else if self.records_per_second.is_some() {
            "steady"
        } else {
            "saturation"
        }
    }

    pub fn report(&self) -> Value {
        json!({"shape":self.label(), "offered_records_per_second":self.records_per_second,
            "burst_period_ms":self.burst_period_ms, "burst_active_ms":self.burst_active_ms,
            "rate_scope":"total across all writers; bursts use whole-period average",
            "arrival_unit":"all records in one input batch arrive together",
            "assignment":self.records_per_second.map(|_| "global arrival ordinal modulo total connections"),
            "overload":if self.records_per_second.is_some() {"preserve scheduled arrivals and drain under deadline; no silent drops or clock reset"} else {"completion-paced; stop new submissions at window end"}})
    }
}

/// Maximum stages of one continuous ramp.
pub const MAX_STAGES: usize = 8;

/// Continuous offered-load ramp: each stage holds one total rate for a fixed
/// time. Arrivals keep one absolute clock across stages; nothing restarts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ramp(Vec<(u64, u64)>);

impl std::str::FromStr for Ramp {
    type Err = String;

    /// `RATE:SECONDS,...`, rates strictly increasing, whole seconds.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let stages = text
            .split(',')
            .map(|stage| {
                let (rate, seconds) = stage
                    .split_once(':')
                    .ok_or_else(|| format!("ramp stage {stage:?} is not RATE:SECONDS"))?;
                let rate: u64 = rate.parse().map_err(|_| format!("invalid rate {rate:?}"))?;
                let seconds: u64 = seconds
                    .parse()
                    .map_err(|_| format!("invalid seconds {seconds:?}"))?;
                Ok((rate, seconds * 1_000_000_000))
            })
            .collect::<Result<Vec<_>, String>>()?;
        if !(1..=MAX_STAGES).contains(&stages.len())
            || stages.iter().any(|(rate, ns)| {
                !(1..=1_000_000_000).contains(rate) || !(1..=3600 * 1_000_000_000).contains(ns)
            })
            || stages.windows(2).any(|w| w[1].0 <= w[0].0)
        {
            return Err(format!(
                "a ramp has 1..={MAX_STAGES} stages of 1..=3600 s with rising rates of 1..=1000000000 records/s"
            ));
        }
        Ok(Self(stages))
    }
}

impl std::fmt::Display for Ramp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stages: Vec<_> = self
            .0
            .iter()
            .map(|(rate, ns)| format!("{rate}:{}", ns / 1_000_000_000))
            .collect();
        f.write_str(&stages.join(","))
    }
}

impl Ramp {
    /// `(records_per_second, duration_ns)` per stage.
    pub fn stages(&self) -> &[(u64, u64)] {
        &self.0
    }

    pub fn duration_ns(&self) -> u64 {
        self.0.iter().map(|(_, ns)| ns).sum()
    }

    /// One arrival per ordinal. `warmup_ns` extends the first stage.
    pub fn plan(&self, warmup_ns: u64) -> BenchResult<Schedule> {
        let mut stages = [Stage {
            rate: self.0[0].0,
            start: 0,
            first: 0,
        }; MAX_STAGES];
        let (mut start, mut first) = (0_u64, 0_u64);
        for (index, (rate, ns)) in self.0.iter().enumerate() {
            stages[index] = Stage {
                rate: *rate,
                start,
                first,
            };
            let ns = if index == 0 { ns + warmup_ns } else { *ns };
            // Ordinals j with j * 1e9 / rate < ns, exactly.
            first = first
                .checked_add(u64::try_from(
                    (u128::from(ns) * u128::from(*rate)).div_ceil(1_000_000_000),
                )?)
                .ok_or_else(|| bench_error("ramp arrival count overflow"))?;
            start = start
                .checked_add(ns)
                .ok_or_else(|| bench_error("ramp clock overflow"))?;
        }
        Ok(Schedule {
            stages,
            count: self.0.len(),
            batch: 1,
            period: 1,
            active: 1,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct Stage {
    rate: u64,
    /// Stage start relative to the origin.
    start: u64,
    /// First arrival ordinal of the stage.
    first: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Schedule {
    stages: [Stage; MAX_STAGES],
    count: usize,
    batch: usize,
    period: u64,
    active: u64,
}

impl Schedule {
    fn relative(self, ordinal: u64) -> u128 {
        let stage = self.stages[..self.count]
            .iter()
            .rev()
            .find(|stage| stage.first <= ordinal)
            .expect("the first stage starts at ordinal zero");
        let steady = u128::from(stage.start)
            + u128::from(ordinal - stage.first) * self.batch as u128 * 1_000_000_000
                / u128::from(stage.rate);
        if self.active == self.period {
            return steady;
        }
        let period = u128::from(self.period);
        steady / period * period + steady % period * u128::from(self.active) / period
    }

    pub fn due(self, origin: u64, ordinal: u64) -> BenchResult<u64> {
        origin
            .checked_add(u64::try_from(self.relative(ordinal))?)
            .ok_or_else(|| bench_error("scheduled arrival clock overflow"))
    }

    /// Exact count in [origin, origin + duration), including integer rounding.
    pub fn count(self, duration_ns: u64) -> BenchResult<usize> {
        let (mut low, mut high) = (0_u64, u64::MAX);
        while low < high {
            let middle = low + (high - low) / 2;
            if self.relative(middle) < u128::from(duration_ns) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if self.relative(low) < u128::from(duration_ns) {
            return Err(bench_error("scheduled arrival count overflow"));
        }
        Ok(usize::try_from(low)?)
    }
}

/// Count arrivals owned by contiguous connection lanes, without allocating jobs.
pub fn assigned(count: usize, connections: usize, lanes: Range<usize>) -> usize {
    count / connections * lanes.len()
        + (count % connections)
            .min(lanes.end)
            .saturating_sub(lanes.start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_duty_cycle_preserves_exact_steady_rounding_and_wide_ordinals() {
        for rate in [1, 7, 1001, 1_000_000_000] {
            for batch in [1, 1024] {
                for period in [1, 100, 60_000] {
                    let plan = Load {
                        records_per_second: Some(rate),
                        burst_period_ms: Some(period),
                        burst_active_ms: Some(period),
                    }
                    .plan(batch)
                    .unwrap()
                    .unwrap();
                    for ordinal in [0, 1, 999, 1_000_000_000, u64::MAX] {
                        let expected =
                            u128::from(ordinal) * batch as u128 * 1_000_000_000 / u128::from(rate);
                        assert_eq!(plan.relative(ordinal), expected);
                        assert_eq!(
                            plan.due(1, ordinal).ok(),
                            u64::try_from(expected).ok().and_then(|n| n.checked_add(1))
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn rate_and_bursts_preserve_absolute_arrivals_and_exact_half_open_counts() {
        let steady = Load {
            records_per_second: Some(1000),
            ..Load::default()
        }
        .plan(10)
        .unwrap()
        .unwrap();
        let burst = Load {
            records_per_second: Some(1000),
            burst_period_ms: Some(100),
            burst_active_ms: Some(10),
        }
        .plan(10)
        .unwrap()
        .unwrap();
        assert_eq!(
            (0..12)
                .map(|i| steady.due(0, i).unwrap())
                .collect::<Vec<_>>(),
            (0..12).map(|i| i * 10_000_000).collect::<Vec<_>>()
        );
        assert_eq!(
            (0..12)
                .map(|i| burst.due(0, i).unwrap())
                .collect::<Vec<_>>(),
            vec![
                0,
                1_000_000,
                2_000_000,
                3_000_000,
                4_000_000,
                5_000_000,
                6_000_000,
                7_000_000,
                8_000_000,
                9_000_000,
                100_000_000,
                101_000_000
            ]
        );
        for plan in [steady, burst] {
            for duration in [
                0,
                1,
                10_000_000,
                50_000_000,
                100_000_000,
                101_000_000,
                1_000_000_000,
            ] {
                let count = plan.count(duration).unwrap();
                assert!(plan.due(0, count as u64).unwrap() >= duration);
                if count > 0 {
                    assert!(plan.due(0, count as u64 - 1).unwrap() < duration);
                }
            }
            assert_eq!(plan.count(1_000_000_000).unwrap(), 100);
        }
    }

    #[test]
    fn ramp_keeps_one_clock_and_exact_stage_counts() {
        let ramp: Ramp = "10:2,1000:1".parse().unwrap();
        assert_eq!(ramp.to_string(), "10:2,1000:1");
        assert_eq!(ramp.duration_ns(), 3_000_000_000);
        let plan = ramp.plan(1_000_000_000).unwrap();
        // 30 arrivals in the warmed 3 s first stage, then 1000 in one second.
        assert_eq!(plan.count(3_000_000_000).unwrap(), 30);
        assert_eq!(plan.due(0, 29).unwrap(), 2_900_000_000);
        assert_eq!(plan.due(0, 30).unwrap(), 3_000_000_000);
        assert_eq!(plan.due(0, 31).unwrap(), 3_001_000_000);
        assert_eq!(plan.count(4_000_000_000).unwrap(), 1030);
        for text in [
            "",
            "10",
            "10:0",
            "0:1",
            "100:1,10:1",
            "10:1,10:1",
            "1:1,2:1,3:1,4:1,5:1,6:1,7:1,8:1,9:1",
        ] {
            assert!(text.parse::<Ramp>().is_err(), "accepted {text:?}");
        }
    }

    #[test]
    fn worker_assignment_never_multiplies_or_loses_arrivals() {
        for count in 0..40 {
            for connections in 1..=16 {
                for workers in 1..=connections {
                    let counts: usize = (0..workers)
                        .map(|index| {
                            assigned(
                                count,
                                connections,
                                crate::workload::worker_share(connections, workers, index).unwrap(),
                            )
                        })
                        .sum();
                    assert_eq!(counts, count);
                }
            }
        }
    }

    #[test]
    fn invalid_schedule_and_clock_overflow_fail() {
        for load in [
            Load {
                records_per_second: Some(0),
                ..Load::default()
            },
            Load {
                burst_period_ms: Some(1),
                ..Load::default()
            },
            Load {
                records_per_second: Some(1),
                burst_period_ms: Some(1),
                burst_active_ms: Some(2),
            },
        ] {
            assert!(load.plan(1).is_err());
        }
        let plan = Load {
            records_per_second: Some(1),
            ..Load::default()
        }
        .plan(1)
        .unwrap()
        .unwrap();
        assert!(plan.due(u64::MAX, 1).is_err());
    }
}
