//! Opt-in thread-owned stage diagnostics. Disabled paths never read the clock.
//!
//! Enable before starting clients or brokers. Thread-owned histograms cover
//! each thread's lifetime, use bounded storage, and measure different work units:
//! record queues, write groups, and confirmation replies. Their percentiles
//! must not be added. No payload bytes or record identities are retained.
//!
//! Event values also belong to their thread. [`totals`] combines every
//! thread's values by each event's [`Kind`], including threads that exited.
//! [`enable_counters`] records events on every thread without stage timing.
#![warn(missing_docs)]
#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

use std::{
    cell::{Cell, RefCell},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use omq_tokio::{Context, ContextConfig, Message, Options, SocketType, blocking};
use serde_json::json;

const PUBLISH_INTERVAL: Duration = Duration::from_millis(20);
/// Recorded operations between clock reads for the publication interval.
const PUBLISH_CHECK_OPERATIONS: u32 = 64;
static TRANSPORT: OnceLock<Context> = OnceLock::new();
static COUNTING: AtomicBool = AtomicBool::new(false);

mod counters;
mod histogram;
mod names;

use counters::Values;
pub use counters::{Totals, totals};
use histogram::Metric;
pub use histogram::{Snapshot, summarize_stage};
use names::{EVENT_NAMES, STAGE_NAMES};
pub use names::{Event, Kind, Stage};

thread_local! {
    static ENABLED: Cell<bool> = Cell::new(
        std::env::var("OZZY_BENCH_PROFILE").as_deref() == Ok("stages")
            || std::env::var_os("OZZY_TELEMETRY_ENDPOINT").is_some()
    );
    static LOCAL: RefCell<Option<Box<Local>>> = const { RefCell::new(None) };
}

struct Local {
    events: Arc<Values>,
    /// One per stage, allocated by the first finished stage.
    metrics: Vec<Metric>,
    socket: Option<blocking::Socket>,
    thread: String,
    thread_name: Option<String>,
    started: Instant,
    published: Instant,
    unchecked: u32,
    sequence: u64,
    logs: std::collections::VecDeque<serde_json::Value>,
    log_sequence: u64,
}

impl Local {
    fn new() -> Self {
        let socket = std::env::var("OZZY_TELEMETRY_ENDPOINT")
            .ok()
            .and_then(|endpoint| {
                let address = endpoint.parse().ok()?;
                let context = TRANSPORT.get_or_init(|| {
                    Context::with_config_and_name(ContextConfig { io_threads: 1 }, "ozzy/tele")
                });
                let socket = context.blocking_socket(
                    SocketType::Pub,
                    Options::default().send_hwm(8).linger(Duration::ZERO),
                );
                socket.connect(address).ok()?;
                Some(socket)
            });
        let thread = std::thread::current();
        let started = Instant::now();
        Self {
            events: Values::register(),
            metrics: Vec::new(),
            socket,
            thread: format!("{:?}", thread.id()),
            thread_name: thread.name().map(str::to_owned),
            started,
            published: started,
            unchecked: 0,
            sequence: 0,
            logs: std::collections::VecDeque::with_capacity(16),
            log_sequence: 0,
        }
    }

    /// Busy threads read the clock once per batch of operations instead of
    /// on every counter update. A thread recording fewer operations than one
    /// batch per interval publishes less often.
    fn publish_if_due(&mut self) {
        self.unchecked += 1;
        if self.unchecked >= PUBLISH_CHECK_OPERATIONS {
            self.publish_if_elapsed();
        }
    }

    fn publish_if_elapsed(&mut self) {
        self.unchecked = 0;
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
            .zip(self.events.get())
            .collect::<std::collections::BTreeMap<_, _>>();
        let stages = self
            .metrics
            .iter()
            .zip(STAGE_NAMES)
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
            "logs":self.logs,
        });
        if let Ok(bytes) = serde_json::to_vec(&row) {
            let _ = socket.try_send(Message::from(bytes));
        }
    }
}

impl Drop for Local {
    fn drop(&mut self) {
        self.publish();
        self.events.retire();
    }
}

fn with_local(f: impl FnOnce(&mut Local)) {
    if enabled() {
        local(f);
    }
}

fn local(f: impl FnOnce(&mut Local)) {
    LOCAL.with(|cell| {
        let mut borrowed = cell.borrow_mut();
        let local = borrowed.get_or_insert_with(|| Box::new(Local::new()));
        f(local);
    });
}

/// Severity of a bounded diagnostic entry, independent of application logging.
#[derive(Clone, Copy, Debug)]
pub enum Level {
    /// A notable state transition.
    Info,
    /// Recoverable pressure or degraded execution.
    Warn,
    /// Execution failure.
    Error,
}

/// Publish a diagnostic on this thread's existing PUB socket. At most sixteen
/// entries of 256 UTF-8 bytes are retained per thread. Do not pass payloads,
/// record identities, credentials, or unbounded application details.
pub fn log(level: Level, message: &str) {
    with_local(|local| {
        let mut length = message.len().min(256);
        while !message.is_char_boundary(length) {
            length -= 1;
        }
        let level = match level {
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        };
        if local.logs.len() == 16 {
            local.logs.pop_front();
        }
        local.log_sequence += 1;
        local.logs.push_back(json!({"sequence":local.log_sequence,
            "elapsed_ms":local.started.elapsed().as_millis(),
            "level":level,"message":&message[..length]}));
        local.publish_if_elapsed();
    });
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

/// Record one event while counting, publishing due snapshots.
pub fn event(event: Event) {
    count(event, 1);
}

/// Add bytes or events to a [`Kind::Count`] event while counting.
pub fn count(event: Event, amount: u64) {
    record(event, Kind::Count, |count| count.wrapping_add(amount));
}

/// Raise a [`Kind::Peak`] event to `value` while counting.
pub fn peak(event: Event, value: u64) {
    record(event, Kind::Peak, |peak| peak.max(value));
}

/// Raise or lower a [`Kind::Gauge`] event while counting. Lower only what was
/// raised while counting, on this thread or another.
pub fn gauge(event: Event, change: i64) {
    record(event, Kind::Gauge, |level| {
        level.wrapping_add_signed(change)
    });
}

fn record(event: Event, kind: Kind, change: impl FnOnce(u64) -> u64) {
    debug_assert_eq!(event.kind(), kind, "{event:?} recorded as {kind:?}");
    if counting() {
        local(|local| {
            local.events.update(event, change);
            local.publish_if_due();
        });
    }
}

/// This thread's event values. Cross-thread aggregation belongs to SUB or
/// [`totals`].
pub fn events() -> Option<Vec<(&'static str, u64)>> {
    counting().then(|| {
        LOCAL.with(|cell| {
            cell.borrow().as_ref().map_or_else(
                || EVENT_NAMES.into_iter().map(|name| (name, 0)).collect(),
                |local| EVENT_NAMES.into_iter().zip(local.events.get()).collect(),
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

/// Record events on every thread from now on, without stage timing. Enable
/// before starting clients or brokers.
pub fn enable_counters() {
    COUNTING.store(true, Ordering::Relaxed);
}

/// Whether this thread records events.
pub fn counting() -> bool {
    enabled() || COUNTING.load(Ordering::Relaxed)
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
            if local.metrics.is_empty() {
                local.metrics = vec![Metric::new(); STAGE_NAMES.len()];
            }
            local.metrics[stage as usize].observe(elapsed);
            local.publish_if_due();
        });
    }
}

/// This thread's stage snapshot. Cross-thread aggregation belongs to SUB.
pub fn snapshot() -> Option<Vec<Snapshot>> {
    enabled().then(|| {
        LOCAL.with(|cell| {
            let local = cell.borrow();
            let metrics = local.as_ref().map_or(&[][..], |local| &local.metrics);
            STAGE_NAMES
                .into_iter()
                .enumerate()
                .map(|(index, stage)| {
                    metrics.get(index).map_or_else(
                        || Metric::new().snapshot(stage),
                        |metric| metric.snapshot(stage),
                    )
                })
                .collect()
        })
    })
}

#[cfg(test)]
mod tests;
