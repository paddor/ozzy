//! Opt-in thread-owned stage diagnostics. Disabled paths never read the clock.
//!
//! Enable before starting clients or brokers. Thread-owned histograms cover
//! each thread's lifetime, use bounded storage, and measure different work units:
//! record queues, write groups, and confirmation replies. Their percentiles
//! must not be added. No payload bytes or record identities are retained.

use std::{
    cell::{Cell, RefCell},
    time::{Duration, Instant},
};

use omq_tokio::{Context, Message, Options, SocketType, blocking};
use serde_json::json;

const BINS: usize = 512;
const PUBLISH_INTERVAL: Duration = Duration::from_millis(20);

thread_local! {
    static ENABLED: Cell<bool> = Cell::new(
        std::env::var("OZZY_BENCH_PROFILE").as_deref() == Ok("stages")
    );
    static LOCAL: RefCell<Option<Box<Local>>> = const { RefCell::new(None) };
}

// Variant indices and published labels come from one ordered declaration.
macro_rules! counters {
    (
        $(#[$kind_doc:meta])* $kind:ident, $names:ident {
            $($(#[$variant_doc:meta])* $variant:ident => $label:literal),* $(,)?
        }
    ) => {
        $(#[$kind_doc])*
        #[derive(Clone, Copy, Debug)]
        #[repr(usize)]
        pub enum $kind {
            $($(#[$variant_doc])* $variant),*
        }
        const $names: [&str; [$($label),*].len()] = [$($label),*];
    };
}

counters! {
    /// Measured local stages; transport transit between processes is excluded.
    Stage, NAMES {
        /// SDK admission to request packing, per record transmission attempt.
        SdkQueue => "sdk_queue",
        /// Received confirmation decoding and SDK prefix publication, per reply.
        ConfirmationApply => "confirmation_apply",
        /// OMQ admission to complete APPEND confirmation, per request.
        RequestRoundtrip => "request_roundtrip",
        /// Proposal submission to the replication actor starting validation.
        ReplicaQueue => "replica_proposal_queue",
        /// Leader-local admission completion to the successful proposal reply.
        /// Includes waiting for another broker and applying the confirmed prefix.
        ReplicaConfirmation => "replica_confirmation",
        /// Local actor proposal submission through validation completion.
        LocalPropose => "local_propose",
        /// Local actor admission submission through its journal completion.
        LocalAdmit => "local_admit",
        /// Local actor admission through observed physical write completion.
        LocalWrite => "local_write",
        /// Local actor barrier submission through durable evidence publication.
        LocalSyncPublish => "local_sync_publish",
        /// Durable evidence publication through local driver installation.
        LocalSyncInstall => "local_sync_install",
        /// Local actor apply submission through journal completion.
        LocalApply => "local_apply",
        /// Physical data-write future, excluding journal scheduling and installation.
        JournalPhysicalWrite => "journal_physical_write",
        /// Detached data barrier and durability evidence publication future.
        JournalPhysicalSync => "journal_physical_sync",
        /// Reader request admission through observed journal completion, before encoding.
        ReaderJournal => "reader_journal",
        /// Resident or historical delivery encoded into one reader reply.
        ReaderEncode => "reader_encode",
        /// SDK materializes one buffered live or replayed record, excluding socket wait.
        ReaderMaterialize => "reader_materialize",
        /// Whole-APPEND LZ4 attempt in SDK preparation runtime.
        SdkCompression => "sdk_compression",
        /// Broker shard output enqueue through dispatcher reply admission.
        ShardReplyQueue => "shard_reply_queue",
    }
}

counters! {
    /// Opt-in replication events. Counts describe transport policy, never votes.
    Event, EVENT_NAMES {
        /// Fresh data packet reserved and queued to one follower.
        ReplicaSend => "replica_send",
        /// Data packet queued using an existing repair reservation.
        ReplicaRepair => "replica_repair",
        /// Validated packet reached the follower's normal receive policy.
        ReplicaReceive => "replica_receive",
        /// Packet skipped because its predecessor is ahead of the retained prefix.
        ReplicaReceiveGap => "replica_receive_gap",
        /// Packet skipped because its complete suffix exceeds remaining capacity.
        ReplicaReceiveCapacity => "replica_receive_capacity",
        /// Packet was already present in the retained receive prefix.
        ReplicaReceiveDuplicate => "replica_receive_duplicate",
        /// Correlated status query queued to one peer, including retries.
        ReplicaProbe => "replica_probe",
        /// Worker result rejected because its application/authority ticket changed.
        ReplicaStaleValidation => "replica_stale_validation",
        /// Recent replay lookup has no entries in this authority generation.
        ReplayCacheEmpty => "replay_cache_empty",
        /// Applied packets no longer form a contiguous history in the same image.
        ReplayCacheReset => "replay_cache_reset",
        /// Requested predecessor has fallen out of the bounded packet cache.
        ReplayCacheEvicted => "replay_cache_evicted",
        /// Requested predecessor is inside a retained multi-operation packet.
        ReplayCacheInterior => "replay_cache_interior",
        /// Requested predecessor is beyond the retained packet range.
        ReplayCacheAhead => "replay_cache_ahead",
        /// Recent packet found, but send range or flow metadata cannot serve it.
        ReplayPacketMiss => "replay_packet_miss",
        /// Recent packets served replay without reserving journal work.
        ReplayCacheHit => "replay_cache_hit",
        /// Cold replay waits for pending persistence to settle, per actor turn.
        ReplayPersistenceWait => "replay_persistence_wait",
        /// Cold replication fetch submitted to the journal owner.
        ReplayDiskFetch => "replay_disk_fetch",
        /// One live publication queued for the group.
        ReplicaPublication => "replica_publication",
        /// Publication passed leader/scope/integrity checks.
        ReplicaPublicationReceived => "replica_publication_received",
        /// Canonical bytes submitted for targeted PEER replication, including retries.
        ReplicaPayloadBytes => "replica_payload_bytes",
        /// Canonical bytes inspected by receive staging, including held-message retries.
        ReceiveBytes => "receive_bytes",
        /// Canonical bytes in packets wholly covered by the receive cursor.
        DuplicateBytes => "duplicate_bytes",
        /// Canonical bytes newly retained by receive staging.
        RetainedBytes => "retained_bytes",
        /// One publication of confirmed records queued for every live reader.
        ReaderPublication => "reader_publication",
        /// SDK whole-APPEND LZ4 attempts.
        SdkCompressionAttempt => "sdk_compression_attempt",
        /// SDK attempts retained as LZ4.
        SdkCompressionWin => "sdk_compression_win",
        /// SDK attempts retained raw.
        SdkCompressionRaw => "sdk_compression_raw",
        /// Raw bytes presented to SDK LZ4 attempts.
        SdkCompressionInputBytes => "sdk_compression_input_bytes",
        /// Candidate bytes produced by SDK LZ4 attempts.
        SdkCompressionOutputBytes => "sdk_compression_output_bytes",
        /// Journal-owner whole-APPEND LZ4 attempts.
        BrokerCompressionAttempt => "broker_compression_attempt",
        /// Journal-owner attempts retained as LZ4.
        BrokerCompressionWin => "broker_compression_win",
        /// Journal-owner attempts retained raw.
        BrokerCompressionRaw => "broker_compression_raw",
        /// Raw bytes presented to journal-owner LZ4 attempts.
        BrokerCompressionInputBytes => "broker_compression_input_bytes",
        /// Candidate bytes produced by journal-owner LZ4 attempts.
        BrokerCompressionOutputBytes => "broker_compression_output_bytes",
        /// Peak, not a count: live operations awaiting application or persistence.
        LiveWindowPeakOperations => "live_window_peak_operations",
        /// Peak, not a count: live body bytes awaiting application or persistence.
        LiveWindowPeakBytes => "live_window_peak_bytes",
        /// Leader proposals held back until the live window had room.
        LiveWindowFull => "live_window_full",
        /// SDK APPEND refused until broker admission capacity becomes available.
        SdkAdmissionRefusal => "sdk_admission_refusal",
        /// Native APPEND proposal lane refused an otherwise prepared request.
        NativeProposalRefusal => "native_proposal_refusal",
        /// Dispatcher reply queue refused a shard-owned frame.
        ShardReplyRefusal => "shard_reply_refusal",
        /// Local command lane refused a shard-owned reply.
        ShardPortRefusal => "shard_port_refusal",
        /// Same-channel 29-byte receipt report queued to PEER.
        CompactReceipt => "compact_receipt",
        /// Full history/session binding or correlated probe response.
        FullReceipt => "full_receipt",
        /// Partition actor answered a writer request with an admission refusal.
        NativeAdmissionRefusal => "native_admission_refusal",
        /// Dequeued client input discarded without a reply: stale or ignored.
        ShardInputDiscarded => "shard_input_discarded",
        /// One shard scheduler poll (a wakeup of the shard thread).
        ShardTurn => "shard_turn",
        /// Lossy follower PUB socket could not accept a frame.
        ReplicaPublicationSendDrop => "replica_publication_send_drop",
        /// Follower PUB frame could not enter its bounded shard queue.
        ReplicaPublicationQueueDrop => "replica_publication_queue_drop",
        /// Follower PUB frame arrived beyond the contiguous retained prefix.
        ReplicaPublicationGap => "replica_publication_gap",
    }
}

struct Local {
    events: [u64; EVENT_NAMES.len()],
    metrics: Box<[Metric]>,
    socket: Option<blocking::Socket>,
    thread: String,
    thread_name: Option<String>,
    started: Instant,
    published: Instant,
    sequence: u64,
}

impl Local {
    fn new() -> Self {
        let socket = std::env::var("OZZY_BENCH_COUNTERS_ENDPOINT")
            .ok()
            .and_then(|endpoint| {
                let address = endpoint.parse().ok()?;
                let context = Context::new();
                let socket = context.blocking_socket(
                    SocketType::Pub,
                    Options::default().send_hwm(512).linger(Duration::ZERO),
                );
                socket.connect(address).ok()?;
                Some(socket)
            });
        let thread = std::thread::current();
        let started = Instant::now();
        Self {
            events: [0; EVENT_NAMES.len()],
            metrics: vec![Metric::new(); NAMES.len()].into_boxed_slice(),
            socket,
            thread: format!("{:?}", thread.id()),
            thread_name: thread.name().map(str::to_owned),
            started,
            published: started,
            sequence: 0,
        }
    }

    fn publish_if_due(&mut self) {
        if self.published.elapsed() >= PUBLISH_INTERVAL {
            self.publish();
        }
    }

    fn publish(&mut self) {
        self.published = Instant::now();
        let Some(socket) = &self.socket else {
            return;
        };
        self.sequence += 1;
        let events = EVENT_NAMES
            .into_iter()
            .zip(self.events)
            .collect::<std::collections::BTreeMap<_, _>>();
        let stages = self
            .metrics
            .iter()
            .zip(NAMES)
            .filter(|(metric, _)| metric.samples() != 0)
            .map(|(metric, stage)| {
                let bins = metric
                    .bins
                    .iter()
                    .enumerate()
                    .filter(|(_, count)| **count != 0)
                    .collect::<Vec<_>>();
                json!({"stage":stage,"bins":bins,
                    "total_ns":metric.total,"max_ns":metric.maximum})
            })
            .collect::<Vec<_>>();
        let row = json!({
            "pid":std::process::id(),"thread":self.thread,
            "thread_name":self.thread_name,"sequence":self.sequence,
            "elapsed_ms":self.started.elapsed().as_millis(),
            "events":events,"stages":stages,
        });
        if let Ok(bytes) = serde_json::to_vec(&row) {
            let _ = socket.try_send(Message::from(bytes));
        }
    }
}

impl Drop for Local {
    fn drop(&mut self) {
        self.publish();
    }
}

fn with_local(f: impl FnOnce(&mut Local)) {
    if enabled() {
        LOCAL.with(|cell| {
            let mut borrowed = cell.borrow_mut();
            let local = borrowed.get_or_insert_with(|| Box::new(Local::new()));
            f(local);
        });
    }
}

/// Adaptive compression decision location.
#[derive(Clone, Copy, Debug)]
pub enum CompressionSite {
    /// Application-side native writer.
    Sdk,
    /// Leader journal owner fallback.
    Broker,
}

/// Record one attempted adaptive LZ4 decision.
pub fn compression(site: CompressionSite, packed: bool, input: usize, output: usize) {
    let (attempt, win, raw, input_bytes, output_bytes) = match site {
        CompressionSite::Sdk => (
            Event::SdkCompressionAttempt,
            Event::SdkCompressionWin,
            Event::SdkCompressionRaw,
            Event::SdkCompressionInputBytes,
            Event::SdkCompressionOutputBytes,
        ),
        CompressionSite::Broker => (
            Event::BrokerCompressionAttempt,
            Event::BrokerCompressionWin,
            Event::BrokerCompressionRaw,
            Event::BrokerCompressionInputBytes,
            Event::BrokerCompressionOutputBytes,
        ),
    };
    event(attempt);
    event(if packed { win } else { raw });
    count(input_bytes, input as u64);
    count(output_bytes, output as u64);
}

/// Record one event only while diagnostics are enabled. Does not read a clock.
pub fn event(event: Event) {
    count(event, 1);
}

/// Raise a peak gauge only while diagnostics are enabled. These entries hold
/// maxima, not counts.
pub fn peak(event: Event, value: u64) {
    with_local(|local| {
        local.events[event as usize] = local.events[event as usize].max(value);
        local.publish_if_due();
    });
}

/// Add bytes or events only while diagnostics are enabled.
pub fn count(event: Event, amount: u64) {
    with_local(|local| {
        local.events[event as usize] += amount;
        local.publish_if_due();
    });
}

/// This thread's event counts. Cross-thread aggregation belongs to SUB.
pub fn events() -> Option<Vec<(&'static str, u64)>> {
    enabled().then(|| {
        LOCAL.with(|cell| {
            cell.borrow().as_ref().map_or_else(
                || EVENT_NAMES.into_iter().map(|name| (name, 0)).collect(),
                |local| EVENT_NAMES.into_iter().zip(local.events).collect(),
            )
        })
    })
}

/// Enable diagnostics for this thread. Worker threads read the profile env.
pub fn enable() {
    ENABLED.with(|enabled| enabled.set(true));
}

/// Whether this thread enabled stage diagnostics.
pub fn enabled() -> bool {
    ENABLED.with(Cell::get)
}

/// Capture a stage boundary only when explicitly enabled.
pub fn start() -> Option<Instant> {
    enabled().then(Instant::now)
}

/// Record elapsed nanoseconds. Stage profiles also enqueue due PUB snapshots.
pub fn finish(stage: Stage, start: Option<Instant>) {
    if let Some(start) = start {
        let elapsed = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        with_local(|local| {
            local.metrics[stage as usize].observe(elapsed);
            local.publish_if_due();
        });
    }
}

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

/// This thread's stage snapshot. Cross-thread aggregation belongs to SUB.
pub fn snapshot() -> Option<Vec<Snapshot>> {
    enabled().then(|| {
        LOCAL.with(|cell| {
            cell.borrow().as_ref().map_or_else(
                || {
                    NAMES
                        .into_iter()
                        .map(|stage| Metric::new().snapshot(stage))
                        .collect()
                },
                |local| {
                    local
                        .metrics
                        .iter()
                        .zip(NAMES)
                        .map(|(metric, stage)| metric.snapshot(stage))
                        .collect()
                },
            )
        })
    })
}

/// Rebuild one process summary from thread-owned sparse histogram snapshots.
pub fn summarize_stage(
    stage: &str,
    bins: &[(usize, u64)],
    total: u64,
    maximum: u64,
) -> Option<Snapshot> {
    let stage = *NAMES.iter().find(|name| **name == stage)?;
    let mut dense = [0_u64; BINS];
    for &(index, count) in bins {
        let entry = dense.get_mut(index)?;
        *entry = entry.checked_add(count)?;
    }
    Some(Metric::summarize(stage, &dense, total, maximum))
}

#[derive(Clone)]
struct Metric {
    bins: [u64; BINS],
    total: u64,
    maximum: u64,
}
impl Metric {
    const fn new() -> Self {
        Self {
            bins: [0; BINS],
            total: 0,
            maximum: 0,
        }
    }
    fn observe(&mut self, ns: u64) {
        let shift = (64 - ns.leading_zeros()).saturating_sub(4);
        let index = shift as usize * 8 + (ns >> shift) as usize;
        self.bins[index] += 1;
        self.total += ns;
        self.maximum = self.maximum.max(ns);
    }
    fn samples(&self) -> u64 {
        self.bins.iter().sum()
    }
    fn snapshot(&self, stage: &'static str) -> Snapshot {
        Self::summarize(stage, &self.bins, self.total, self.maximum)
    }
    fn summarize(stage: &'static str, bins: &[u64; BINS], total: u64, maximum: u64) -> Snapshot {
        let samples = bins.iter().sum::<u64>();
        let quantile = |percent: u64| {
            if samples == 0 {
                return 0;
            }
            let rank = (samples * percent).div_ceil(100);
            let mut count = 0;
            for (i, n) in bins.iter().enumerate() {
                count += n;
                if count >= rank {
                    return if i < 16 {
                        i as u64
                    } else {
                        (((9 + i % 8) as u128) << (i / 8 - 1))
                            .saturating_sub(1)
                            .min(u128::from(u64::MAX)) as u64
                    };
                }
            }
            unreachable!("histogram rank")
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
