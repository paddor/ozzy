//! Bounded log-linear duration histograms and their summaries.

use crate::STAGE_NAMES;

/// Histogram buckets: eight per power of two above sixteen nanoseconds.
pub(crate) const BINS: usize = 512;

/// One merged histogram; quantiles are inclusive bucket upper bounds.
#[derive(Clone, Copy, Debug)]
pub struct Snapshot {
    /// Stable stage name.
    pub stage: &'static str,
    /// Number of observations, not necessarily records.
    pub samples: u64,
    /// Sum of measured nanoseconds across concurrent tasks.
    pub total_ns: u64,
    /// Median upper bound; at most 12.5 percent rounding above a sample.
    pub p50_ns: u64,
    /// 99th percentile upper bound, with the same rounding.
    pub p99_ns: u64,
    /// Largest exact observed duration.
    pub max_ns: u64,
}

/// Rebuild one process summary from thread-owned sparse histogram snapshots.
pub fn summarize_stage(
    stage: &str,
    bins: &[(usize, u64)],
    total: u64,
    maximum: u64,
) -> Option<Snapshot> {
    let stage = *STAGE_NAMES.iter().find(|name| **name == stage)?;
    let mut dense = [0_u64; BINS];
    for &(index, count) in bins {
        let entry = dense.get_mut(index)?;
        *entry = entry.checked_add(count)?;
    }
    Some(Metric::summarize(stage, &dense, total, maximum))
}

#[derive(Clone)]
pub(crate) struct Metric {
    pub(crate) bins: [u64; BINS],
    pub(crate) total: u64,
    pub(crate) maximum: u64,
}
impl Metric {
    pub(crate) const fn new() -> Self {
        Self {
            bins: [0; BINS],
            total: 0,
            maximum: 0,
        }
    }
    pub(crate) fn observe(&mut self, ns: u64) {
        let shift = (64 - ns.leading_zeros()).saturating_sub(4);
        let index = shift as usize * 8 + (ns >> shift) as usize;
        self.bins[index] += 1;
        self.total += ns;
        self.maximum = self.maximum.max(ns);
    }
    pub(crate) fn samples(&self) -> u64 {
        self.bins.iter().sum()
    }
    pub(crate) fn snapshot(&self, stage: &'static str) -> Snapshot {
        Self::summarize(stage, &self.bins, self.total, self.maximum)
    }
    pub(crate) fn summarize(
        stage: &'static str,
        bins: &[u64; BINS],
        total: u64,
        maximum: u64,
    ) -> Snapshot {
        let samples = bins.iter().sum::<u64>();
        let quantile = |percent: u64| {
            if samples == 0 {
                return 0;
            }
            let rank = (samples * percent).div_ceil(100);
            let mut count = 0;
            // `rank <= samples`, so some bin reaches it.
            let i = bins
                .iter()
                .position(|n| {
                    count += n;
                    count >= rank
                })
                .unwrap_or(BINS - 1);
            if i < 16 {
                i as u64
            } else {
                (((9 + i % 8) as u128) << (i / 8 - 1))
                    .saturating_sub(1)
                    .min(u128::from(u64::MAX)) as u64
            }
        };
        Snapshot {
            stage,
            samples,
            total_ns: total,
            p50_ns: quantile(50),
            p99_ns: quantile(99),
            max_ns: maximum,
        }
    }
}
