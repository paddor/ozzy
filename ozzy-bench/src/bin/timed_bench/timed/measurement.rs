use super::super::{Args, Result, error, metrics};
use ozzy_journal::integrity::IntegrityHasher;
use serde_json::{Value, json};

/// Broker counters start before the shared start clock so scheduler skew cannot
/// omit initial writes. Includes prestart idle, warmup, measurement, and drain;
/// setup and teardown are excluded, and all process threads are counted.
#[derive(Default)]
pub(super) struct BrokerMeter {
    meter: Option<metrics::Meter>,
    network: std::collections::BTreeMap<String, (u64, u64)>,
    #[cfg(feature = "comparisons")]
    reads: (u64, u64, u64),
    #[cfg(feature = "comparisons")]
    deliveries: (u64, u64),
    #[cfg(feature = "comparisons")]
    delivery_bytes: (u64, u64),
    #[cfg(feature = "comparisons")]
    encoded_bytes: u64,
}

impl BrokerMeter {
    pub(super) fn start(&mut self) -> Result<()> {
        if self.meter.is_some() {
            return Err(error("duplicate broker measurement start"));
        }
        self.network = network_bytes()?;
        self.meter = Some(metrics::Meter::start());
        #[cfg(feature = "comparisons")]
        {
            self.reads = (
                ozzy_journal_segment::read_metrics::resident_selections(),
                ozzy_journal_segment::read_metrics::persisted_loads(),
                ozzy_runtime::storage_metrics::snapshot().background_resident_reads,
            );
            self.encoded_bytes = ozzy_journal_segment::write_metrics::encoded_group_bytes();
            let reads = ozzy_runtime::storage_metrics::snapshot();
            self.deliveries = (
                reads.direct_resident_reads,
                reads.historical_read_deliveries,
            );
            self.delivery_bytes = (reads.shared_reader_bytes, reads.copied_reader_bytes);
        }
        super::launch::reply(&json!({"event":"started"}))
    }

    pub(super) fn finish(&mut self) -> Result<Value> {
        let mut usage = self
            .meter
            .take()
            .ok_or_else(|| error("broker meter not started"))?
            .finish();
        usage["execution"] = ozzy_bench::placement::execution()?;
        usage["host_network_bytes"] = json!(network_bytes()?.into_iter().filter_map(|(name, (rx, tx))| {
            let (old_rx, old_tx) = self.network.get(&name)?;
            Some(json!({"interface":name, "rx":rx.saturating_sub(*old_rx), "tx":tx.saturating_sub(*old_tx)}))
        }).collect::<Vec<_>>());
        usage["cpu_scope"] = json!(
            "broker process: start command through final snapshot, including prestart idle, warmup, measurement, and drain; all threads"
        );
        #[cfg(feature = "comparisons")]
        {
            let memory =
                ozzy_runtime::storage_metrics::snapshot().background_resident_reads - self.reads.2;
            let reads = ozzy_runtime::storage_metrics::snapshot();
            usage["journal_reads"] = json!({
                "resident_selections": ozzy_journal_segment::read_metrics::resident_selections() - self.reads.0 + memory,
                "background_resident_selections": memory,
                "direct_resident_deliveries": reads.direct_resident_reads - self.deliveries.0,
                "historical_worker_deliveries": reads.historical_read_deliveries - self.deliveries.1,
                "shared_payload_bytes": reads.shared_reader_bytes - self.delivery_bytes.0,
                "copied_payload_bytes": reads.copied_reader_bytes - self.delivery_bytes.1,
                "persisted_operation_loads": ozzy_journal_segment::read_metrics::persisted_loads() - self.reads.1,
                "scope": "same broker interval; startup recovery excluded; all owner journals"
            });
            usage["journal_writes"] = json!({
                "encoded_group_bytes": ozzy_journal_segment::write_metrics::encoded_group_bytes() - self.encoded_bytes,
                "scope": "same broker interval; complete appended groups including seals and padding; excludes file headers, indexes, manifests, and unused capacity"
            });
        }
        Ok(usage)
    }
}

// Host-wide counters, not per-process attribution. Captured outside timed work.
fn network_bytes() -> Result<std::collections::BTreeMap<String, (u64, u64)>> {
    let mut output = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir("/sys/class/net")? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "lo" {
            continue;
        }
        let stats = entry.path().join("statistics");
        let rx = std::fs::read_to_string(stats.join("rx_bytes"))?
            .trim()
            .parse()?;
        let tx = std::fs::read_to_string(stats.join("tx_bytes"))?
            .trim()
            .parse()?;
        output.insert(name, (rx, tx));
    }
    Ok(output)
}

#[derive(Clone, Copy)]
pub(super) struct Window {
    pub start: u64,
    pub measured: u64,
    pub end: u64,
}

impl Window {
    pub(super) fn new(start: u64, args: &Args) -> Result<Self> {
        let measured = start
            .checked_add((args.warmup * 1e9) as u64)
            .ok_or_else(|| error("clock overflow"))?;
        let end = measured
            .checked_add((args.duration.unwrap() * 1e9) as u64)
            .ok_or_else(|| error("clock overflow"))?;
        Ok(Self {
            start,
            measured,
            end,
        })
    }
    pub(super) fn contains(self, at: u64) -> bool {
        self.measured <= at && at < self.end
    }
    pub(super) async fn wait(self) {
        let delay = self.start.saturating_sub(metrics::monotonic_ns());
        tokio::time::sleep(std::time::Duration::from_nanos(delay)).await;
    }
    pub(super) fn deadline(self, args: &Args) -> tokio::time::Instant {
        tokio::time::Instant::now()
            + std::time::Duration::from_nanos(
                (self.end + args.drain_timeout_secs * 1_000_000_000)
                    .saturating_sub(metrics::monotonic_ns()),
            )
    }
}

/// Fixed-size integer histogram. Quantiles are bucket upper bounds, within
/// 1/64 of the containing power of two. No per-record sample ledger grows.
pub(super) struct Histogram {
    bins: Vec<u64>,
    max: u64,
}

impl Histogram {
    pub(super) fn new() -> Self {
        Self {
            bins: vec![0; 4096],
            max: 0,
        }
    }
    pub(super) fn record(&mut self, ns: u64) {
        let n = ns.max(1);
        let shift = n.ilog2().saturating_sub(6);
        let bucket = shift as usize * 64 + ((n - 1) >> shift) as usize;
        self.bins[bucket] += 1;
        self.max = self.max.max(ns);
    }
    fn upper(bucket: usize) -> u64 {
        if bucket < 128 {
            return bucket as u64 + 1;
        }
        ((bucket % 64 + 65) as u64).saturating_mul(1_u64 << (bucket / 64 - 1))
    }
    pub(super) fn report(&self) -> Value {
        let samples: u64 = self.bins.iter().sum();
        let quantile = |numerator: u64, denominator: u64| -> Option<f64> {
            if samples == 0 {
                return None;
            }
            let target =
                (u128::from(samples) * u128::from(numerator)).div_ceil(u128::from(denominator));
            let mut count = 0;
            for (index, &n) in self.bins.iter().enumerate() {
                count += n;
                if u128::from(count) >= target {
                    return Some(Self::upper(index).min(self.max) as f64 / 1000.0);
                }
            }
            unreachable!("histogram count")
        };
        json!({"samples":samples, "p50_us":quantile(50, 100), "p99_us":quantile(99, 100),
            "p999_us":quantile(999, 1000),
            "max_us":self.max as f64 / 1000.0, "quantile":"nearest-rank logarithmic bucket upper bound; 64 buckets per power of two",
            // Preserve merged bins after timing, so future percentiles do not
            // require another benchmark. Lane histograms are still discarded.
            "histogram":self.raw()})
    }
    pub(super) fn merge(&mut self, row: &Value) -> Result<()> {
        let bins = row["bins"]
            .as_array()
            .ok_or_else(|| error("missing histogram"))?;
        if bins.len() != self.bins.len() {
            return Err(error("invalid histogram size"));
        }
        for (dst, src) in self.bins.iter_mut().zip(bins) {
            *dst = dst
                .checked_add(
                    src.as_u64()
                        .ok_or_else(|| error("invalid histogram count"))?,
                )
                .ok_or_else(|| error("histogram overflow"))?;
        }
        self.max = self.max.max(
            row["max"]
                .as_u64()
                .ok_or_else(|| error("missing histogram maximum"))?,
        );
        Ok(())
    }
    pub(super) fn raw(&self) -> Value {
        json!({"bins":self.bins,"max":self.max})
    }
}

pub(super) struct Counts {
    pub total: u64,
    pub completed: Vec<u64>,
    pub submitted: Vec<u64>,
    pub latency: Histogram,
    pub last: u64,
    window: Window,
    digest: IntegrityHasher,
    /// Absolute measured stage starts, then the window end.
    bounds: Vec<u64>,
    /// Scheduled latency and generation lag per stage.
    scheduled: Option<Vec<(Histogram, Histogram)>>,
    /// First ramp stage whose arrivals exceeded the admission backlog.
    overloaded: Option<usize>,
}

impl Counts {
    pub(super) fn new(window: Window) -> Self {
        let seconds = (window.end - window.measured).div_ceil(1_000_000_000) as usize;
        Self {
            total: 0,
            completed: vec![0; seconds],
            submitted: vec![0; seconds],
            latency: Histogram::new(),
            last: 0,
            window,
            digest: IntegrityHasher::new("ozzy benchmark delivered records v1"),
            bounds: vec![],
            scheduled: None,
            overloaded: None,
        }
    }
    /// `bounds` holds each measured stage start, then the window end.
    pub(super) fn enable_schedule(&mut self, bounds: &[u64]) {
        self.bounds = bounds.to_vec();
        self.scheduled = Some(
            (1..bounds.len())
                .map(|_| (Histogram::new(), Histogram::new()))
                .collect(),
        );
    }
    /// Stop admission: arrivals of `stage` exceeded the backlog.
    pub(super) fn overload(&mut self, stage: usize) {
        self.overloaded = Some(stage);
    }
    pub(super) fn scheduled_complete(&mut self, due: u64, start: u64, finished: u64) -> Result<()> {
        if start < due {
            return Err(error("record generated before its scheduled arrival"));
        }
        self.complete(start, finished, 1)?;
        if self.window.contains(due) {
            let stage = self.bounds[1..]
                .iter()
                .take_while(|end| **end <= due)
                .count();
            let (latency, lag) =
                &mut self.scheduled.as_mut().expect("scheduled counter enabled")[stage];
            latency.record(finished - due);
            lag.record(start - due);
        }
        Ok(())
    }
    pub(super) fn record_bytes(&mut self, id: ozzy_proto::MessageId, bytes: &[u8]) {
        self.digest.update(id.as_bytes());
        self.digest.update(bytes);
    }
    pub(super) fn submission(&mut self, start: u64, records: usize) {
        if self.window.contains(start) {
            self.submitted[((start - self.window.measured) / 1_000_000_000) as usize] +=
                records as u64;
        }
    }
    pub(super) fn complete(&mut self, start: u64, finished: u64, records: usize) -> Result<()> {
        if start < self.window.start || start > finished {
            return Err(error("invalid record timing"));
        }
        self.total += records as u64;
        self.last = self.last.max(finished);
        if self.window.contains(finished) {
            self.completed[((finished - self.window.measured) / 1_000_000_000) as usize] +=
                records as u64;
        }
        if self.window.contains(start) {
            self.latency.record(finished - start);
        }
        Ok(())
    }
    pub(super) fn report(&self, lane: usize) -> Value {
        let mut row = json!({"lane":lane,"total":self.total,"completed_per_second":self.completed,
            "submitted_per_second":self.submitted,"latency":self.latency.raw(),"last_ns":self.last,
            "digest_xxh3_128":&self.digest.finish().as_bytes()[..16]});
        if let Some(stages) = &self.scheduled {
            row["scheduled_latency"] = stages.iter().map(|(latency, _)| latency.raw()).collect();
            row["scheduling_lag"] = stages.iter().map(|(_, lag)| lag.raw()).collect();
            row["overloaded_stage"] = json!(self.overloaded);
        }
        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scheduled_before_end_but_admitted_during_drain_stays_in_latency_cohort() {
        let mut counts = Counts::new(Window {
            start: 1,
            measured: 10,
            end: 20,
        });
        counts.enable_schedule(&[10, 20]);
        counts.submission(22, 1);
        counts.scheduled_complete(19, 22, 30).unwrap();
        assert_eq!(counts.submitted, [0]);
        assert_eq!(counts.completed, [0]);
        assert_eq!(counts.latency.report()["samples"], 0);
        let (latency, lag) = &counts.scheduled.as_ref().unwrap()[0];
        assert_eq!(latency.report()["samples"], 1);
        assert_eq!(latency.report()["max_us"], 0.011);
        assert_eq!(lag.report()["max_us"], 0.003);
        assert!(counts.scheduled_complete(19, 18, 30).is_err());
    }
    #[test]
    fn ramp_stages_split_scheduled_latency_by_due_time() {
        let mut counts = Counts::new(Window {
            start: 1,
            measured: 10,
            end: 30,
        });
        counts.enable_schedule(&[10, 20, 30]);
        counts.scheduled_complete(5, 5, 6).unwrap();
        counts.scheduled_complete(19, 19, 21).unwrap();
        counts.scheduled_complete(20, 20, 23).unwrap();
        counts.overload(1);
        let row = counts.report(0);
        assert_eq!(row["overloaded_stage"], 1);
        let stages = counts.scheduled.as_ref().unwrap();
        assert_eq!(stages[0].0.report()["samples"], 1);
        assert_eq!(stages[1].0.report()["max_us"], 0.003);
    }
    #[test]
    fn record_digest_uses_shared_xxh3_128_profile_and_covers_identity_and_bytes() {
        let window = Window {
            start: 1,
            measured: 2,
            end: 3,
        };
        let id = ozzy_proto::MessageId::from_bytes([7; 16]);
        let mut counts = Counts::new(window);
        counts.record_bytes(id, b"payload");
        let expected = ozzy_journal::integrity::hash(
            "ozzy benchmark delivered records v1",
            &[id.as_bytes().as_slice(), b"payload"].concat(),
        );
        assert_eq!(
            counts.report(0)["digest_xxh3_128"],
            json!(&expected.as_bytes()[..16])
        );
        for (id, bytes) in [
            (id, b"changed".as_slice()),
            (ozzy_proto::MessageId::from_bytes([8; 16]), b"payload"),
        ] {
            let mut changed = Counts::new(window);
            changed.record_bytes(id, bytes);
            assert_ne!(
                changed.report(0)["digest_xxh3_128"],
                counts.report(0)["digest_xxh3_128"]
            );
        }
    }
    #[test]
    fn p999_uses_nearest_rank_and_preserves_reusable_histogram() {
        let mut hist = Histogram::new();
        assert!(hist.report()["p999_us"].is_null());
        for _ in 0..998 {
            hist.record(1_000);
        }
        hist.record(1_000_000);
        hist.record(10_000_000);
        let row = hist.report();
        assert!(row["p99_us"].as_f64().unwrap() < 2.0);
        assert!((1000.0..1016.0).contains(&row["p999_us"].as_f64().unwrap()));
        assert_eq!(row["max_us"], 10_000.0);
        let mut restored = Histogram::new();
        restored.merge(&row["histogram"]).unwrap();
        assert_eq!(restored.report(), row);
    }

    #[test]
    fn histogram_bounds_include_every_sample_and_merge_exact_counts() {
        let mut hist = Histogram::new();
        for n in 0_u64..100_000 {
            hist.record(n);
            let shift = n.max(1).ilog2().saturating_sub(6);
            let index = shift as usize * 64 + ((n.max(1) - 1) >> shift) as usize;
            assert!(Histogram::upper(index) >= n);
            assert!(Histogram::upper(index) - n <= (1 << shift));
        }
        let mut merged = Histogram::new();
        merged.merge(&hist.raw()).unwrap();
        assert_eq!(merged.report(), hist.report());
        assert_eq!(hist.report()["samples"], 100_000);
    }
    #[test]
    fn drain_is_excluded_from_rate_but_included_in_submission_latency() {
        let mut c = Counts::new(Window {
            start: 1,
            measured: 10,
            end: 20,
        });
        c.submission(19, 3);
        c.complete(19, 30, 3).unwrap();
        c.complete(2, 15, 2).unwrap();
        assert_eq!(c.total, 5);
        assert_eq!(c.completed, [2]);
        assert_eq!(c.submitted, [3]);
        assert_eq!(c.latency.report()["samples"], 1);
    }
}
