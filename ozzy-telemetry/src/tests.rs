use super::*;
use crate::histogram::BINS;

#[test]
fn counters_read_the_publication_clock_once_per_batch() {
    let mut local = Local::new();
    let due = Instant::now().checked_sub(PUBLISH_INTERVAL).unwrap();
    local.published = due;
    for _ in 1..PUBLISH_CHECK_OPERATIONS {
        local.publish_if_due();
    }
    assert_eq!(local.published, due);
    local.publish_if_due();
    assert!(local.published > due);
}

#[test]
fn disabled_diagnostics_allocate_no_local_state() {
    std::thread::spawn(|| {
        ENABLED.with(|enabled| enabled.set(false));
        event(Event::IoStarted);
        log(Level::Info, "unused");
        assert!(start().is_none());
        LOCAL.with(|local| assert!(local.borrow().is_none()));
    })
    .join()
    .unwrap();
}

#[test]
fn diagnostic_logs_bound_entries_and_preserve_utf8() {
    std::thread::spawn(|| {
        enable();
        for _ in 0..32 {
            log(Level::Warn, &"\u{20ac}".repeat(256));
        }
        LOCAL.with(|local| {
            let local = local.borrow();
            let logs = &local.as_ref().unwrap().logs;
            assert_eq!(logs.len(), 16);
            assert_eq!(logs[0]["sequence"], 17);
            assert_eq!(logs[15]["sequence"], 32);
            assert_eq!(logs[15]["message"].as_str().unwrap().len(), 255);
        });
    })
    .join()
    .unwrap();
}

#[test]
fn event_counts_stay_with_the_emitting_thread() {
    let first = std::thread::spawn(|| {
        enable();
        count(Event::ReplicaRepair, 7);
        events().unwrap()
    });
    let second = std::thread::spawn(|| {
        enable();
        count(Event::ReplicaRepair, 11);
        events().unwrap()
    });
    let repair = |rows: Vec<(&str, u64)>| {
        rows.into_iter()
            .find(|(name, _)| *name == "replica_repair")
            .unwrap()
            .1
    };
    assert_eq!(repair(first.join().unwrap()), 7);
    assert_eq!(repair(second.join().unwrap()), 11);
}

#[test]
fn merged_histogram_matches_combined_observations() {
    let mut first = Metric::new();
    let mut second = Metric::new();
    let mut combined = Metric::new();
    for ns in [1, 7, 100, 1000] {
        first.observe(ns);
        combined.observe(ns);
    }
    for ns in [20, 50, 5000] {
        second.observe(ns);
        combined.observe(ns);
    }
    let bins = first
        .bins
        .into_iter()
        .chain(second.bins)
        .enumerate()
        .map(|(index, count)| (index % BINS, count))
        .collect::<Vec<_>>();
    let merged = summarize_stage(
        "replica_confirmation",
        &bins,
        first.total + second.total,
        first.maximum.max(second.maximum),
    )
    .unwrap();
    let expected = combined.snapshot("replica_confirmation");
    assert_eq!(merged.samples, expected.samples);
    assert_eq!(merged.total_ns, expected.total_ns);
    assert_eq!(merged.p50_ns, expected.p50_ns);
    assert_eq!(merged.p99_ns, expected.p99_ns);
    assert_eq!(merged.max_ns, expected.max_ns);
}

#[test]
fn histogram_bounds_cover_small_large_and_extreme_durations() {
    for value in (0..1024).chain([12345, 1_000_000, u64::MAX]) {
        let mut metric = Metric::new();
        metric.observe(value);
        let row = metric.snapshot("test");
        assert_eq!(row.samples, 1);
        assert_eq!(row.max_ns, value);
        assert!(row.p99_ns >= value);
        assert!(u128::from(row.p99_ns) <= u128::from(value) * 9 / 8 + 1);
    }
    let mut metric = Metric::new();
    for ns in 1..=100 {
        metric.observe(ns);
    }
    let row = metric.snapshot("test");
    assert_eq!(row.total_ns, 5050);
    assert_eq!(row.p50_ns, 51);
    assert_eq!(row.p99_ns, 103);
}

#[test]
fn totals_combine_threads_by_kind_and_keep_exited_threads() {
    let before = totals();
    std::thread::spawn(|| {
        enable();
        count(Event::IoSync, 5);
        peak(Event::PreparedPayloadBytesMax, 9);
        gauge(Event::DedupLiveBytes, 10);
    })
    .join()
    .unwrap();
    let (lowered, release) = (std::sync::Barrier::new(2), std::sync::Barrier::new(2));
    std::thread::scope(|scope| {
        scope.spawn(|| {
            enable();
            peak(Event::PreparedPayloadBytesMax, 3);
            gauge(Event::DedupLiveBytes, -4);
            lowered.wait();
            release.wait();
        });
        lowered.wait();
        let live = totals();
        release.wait();
        for totals in [live, super::totals()] {
            let since = |event| totals.get(event).wrapping_sub(before.get(event));
            assert_eq!(since(Event::IoSync), 5);
            assert_eq!(since(Event::DedupLiveBytes), 6);
            assert_eq!(totals.get(Event::PreparedPayloadBytesMax), 9);
        }
    });
}

#[test]
fn published_names_map_to_their_kind() {
    assert_eq!(Kind::of("io_sync"), Some(Kind::Count));
    assert_eq!(Kind::of("live_window_peak_bytes"), Some(Kind::Peak));
    assert_eq!(Kind::of("dedup_live_bytes"), Some(Kind::Gauge));
    assert_eq!(Kind::of("unknown"), None);
}
