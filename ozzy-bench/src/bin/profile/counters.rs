//! Live telemetry counters. Each worker thread publishes snapshots to the
//! collector's local SUB socket; the collector reports repair and refusal
//! bursts while the workload runs, then merges the final counts into the
//! result row.
use omq_tokio::{Context, Message, Options, Socket, SocketType};
use ozzy_bench::automation::Result;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fmt, fs,
    future::Future,
    io::Write,
    path::Path,
    time::{Duration, Instant},
};

/// Minimum gap between two counter-rate alerts of one thread.
const ALERT_INTERVAL: Duration = Duration::from_millis(200);
/// How long the final counter drain waits for each further message.
const DRAIN_WAIT: Duration = Duration::from_millis(20);

/// A counter event whose rate the collector watches.
struct Watched {
    event: &'static str,
    /// The error for a snapshot without this event.
    missing: &'static str,
}

/// Fresh sends, repairs, then the four refusal kinds.
const WATCHED: [Watched; 6] = [
    Watched {
        event: "replica_send",
        missing: "missing fresh sends",
    },
    Watched {
        event: "replica_repair",
        missing: "missing repairs",
    },
    Watched {
        event: "native_proposal_refusal",
        missing: "missing proposal refusal count",
    },
    Watched {
        event: "native_admission_refusal",
        missing: "missing admission refusal count",
    },
    Watched {
        event: "shard_reply_refusal",
        missing: "missing shard reply refusal count",
    },
    Watched {
        event: "shard_port_refusal",
        missing: "missing shard port refusal count",
    },
];

/// The latest snapshot of one worker thread and its rate state.
#[derive(Default)]
struct CounterFeed {
    previous: [u64; 6],
    previous_elapsed_ms: u64,
    sequence: u64,
    samples: u64,
    dropped: u64,
    events: Value,
    stages: Value,
    logs: Value,
    last_alert: Option<Instant>,
}

impl CounterFeed {
    /// Keep `row` as the latest snapshot; sequence gaps count as dropped.
    fn record(&mut self, row: &Value, sequence: u64) {
        self.dropped += sequence.saturating_sub(self.sequence.saturating_add(1));
        self.sequence = sequence;
        self.samples += 1;
        self.events = row["events"].clone();
        self.stages = row["stages"].clone();
        self.logs = row["logs"].clone();
    }

    /// Watched counter increments since the previous snapshot.
    fn advance(&mut self, row: &Value) -> Result<RateDelta> {
        let elapsed_ms = row["elapsed_ms"]
            .as_u64()
            .ok_or("counter elapsed time missing")?;
        let interval_ms = elapsed_ms.saturating_sub(self.previous_elapsed_ms);
        self.previous_elapsed_ms = elapsed_ms;
        let mut current = [0; 6];
        for (count, watched) in current.iter_mut().zip(&WATCHED) {
            *count = row["events"][watched.event]
                .as_u64()
                .ok_or(watched.missing)?;
        }
        let counts = std::array::from_fn(|slot| current[slot].saturating_sub(self.previous[slot]));
        self.previous = current;
        Ok(RateDelta {
            interval_ms,
            counts,
        })
    }

    /// An anomalous interval, at most one alert per `ALERT_INTERVAL`.
    fn should_alert(&self, delta: &RateDelta) -> bool {
        delta.is_anomalous()
            && self
                .last_alert
                .is_none_or(|at| at.elapsed() >= ALERT_INTERVAL)
    }
}

/// Watched counter increments over one snapshot interval.
struct RateDelta {
    interval_ms: u64,
    /// Increments in `WATCHED` order.
    counts: [u64; 6],
}

impl RateDelta {
    fn refused(&self) -> u64 {
        self.counts[2..].iter().sum()
    }

    /// Bulk repair at half the fresh send rate or more, or bulk refusals.
    fn is_anomalous(&self) -> bool {
        let [fresh, repair, ..] = self.counts;
        (repair >= 8 && repair >= fresh / 2) || self.refused() >= 8
    }
}

impl fmt::Display for RateDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [fresh, repair, proposal, admission, reply, port] = self.counts;
        write!(
            f,
            "interval_ms={} fresh={fresh} repair={repair} refused={} (proposal={proposal} admission={admission} reply={reply} port={port})",
            self.interval_ms,
            self.refused()
        )
    }
}

/// The collector's bound SUB socket; its context lives as long as it does.
pub(crate) struct Counters {
    _context: Context,
    socket: Socket,
    endpoint: String,
}

impl Counters {
    /// The collector binds local abstract IPC before any worker starts. Each
    /// application thread owns its PUB socket and enqueues snapshots directly.
    pub(crate) async fn bind(directory: &Path) -> Result<Self> {
        let name = directory
            .file_name()
            .ok_or("profile directory has no name")?;
        let endpoint = format!("ipc://@ozzy-profile-{}", name.to_string_lossy());
        let context = Context::new();
        let socket = context.socket(
            SocketType::Sub,
            Options::default()
                .recv_hwm(8192)
                .max_message_size(256 * 1024)
                .linger(Duration::ZERO),
        );
        socket.subscribe(Vec::<u8>::new()).await?;
        socket.bind(&endpoint).await?;
        Ok(Self {
            _context: context,
            socket,
            endpoint,
        })
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Run `workload` while collecting snapshots, take the remaining ones,
    /// save the trace when kept, and merge the counts into the result row.
    pub(crate) async fn collect(
        self,
        workload: impl Future<Output = Result<Value>>,
        directory: &Path,
        keep_trace: bool,
    ) -> Result<Value> {
        let mut collector = Collector::new(keep_trace);
        let result = tokio::select! {
            result = workload => result,
            result = collector.watch(&self.socket) => {
                result?;
                return Err("counter monitor stopped".into());
            }
        };
        collector.drain(&self.socket).await?;
        let samples = collector.samples();
        let dropped = collector.dropped();
        if collector.feeds.is_empty() {
            return Err("local counter SUB received no worker samples".into());
        }
        if let Some(trace) = &collector.trace {
            save_trace(directory, trace, samples, dropped)?;
        }
        let mut row = result?;
        merge_counters(&mut row, &collector.feeds)?;
        println!(
            "COUNTER_FEED {} threads={} samples={samples} missed={dropped}",
            self.endpoint,
            collector.feeds.len()
        );
        Ok(row)
    }
}

/// The latest snapshot per worker thread, and every snapshot when traced.
struct Collector {
    feeds: BTreeMap<(u64, String), CounterFeed>,
    trace: Option<Vec<Value>>,
    started: Instant,
}

impl Collector {
    fn new(keep_trace: bool) -> Self {
        Self {
            feeds: BTreeMap::new(),
            trace: keep_trace.then(Vec::new),
            started: Instant::now(),
        }
    }

    /// Surface anomalous repair or dispatch refusal while workload still runs.
    async fn watch(&mut self, socket: &Socket) -> Result<()> {
        loop {
            let message = socket.recv().await?;
            self.observe(&message)?;
        }
    }

    /// Take the snapshots still queued after the workload ended.
    async fn drain(&mut self, socket: &Socket) -> Result<()> {
        for _ in 0..4096 {
            let Ok(Ok(message)) = tokio::time::timeout(DRAIN_WAIT, socket.recv()).await else {
                break;
            };
            self.observe(&message)?;
        }
        Ok(())
    }

    fn observe(&mut self, message: &Message) -> Result<()> {
        let received_ns = self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        let mut row: Value = serde_json::from_slice(
            message
                .part_slice(0)
                .ok_or("counter message has no payload")?,
        )?;
        let pid = row["pid"].as_u64().ok_or("counter pid missing")?;
        let thread = row["thread"].as_str().ok_or("counter thread missing")?;
        let sequence = row["sequence"].as_u64().ok_or("counter sequence missing")?;
        let feed = self.feeds.entry((pid, thread.to_owned())).or_default();
        feed.record(&row, sequence);
        let delta = feed.advance(&row)?;
        if feed.should_alert(&delta) {
            println!("COUNTER_RATE pid={pid} thread={thread} {delta}");
            std::io::stdout().flush()?;
            feed.last_alert = Some(Instant::now());
        }
        if let Some(trace) = &mut self.trace {
            row["collector_elapsed_ns"] = json!(received_ns);
            trace.push(row);
        }
        Ok(())
    }

    fn samples(&self) -> u64 {
        self.feeds.values().map(|feed| feed.samples).sum()
    }

    fn dropped(&self) -> u64 {
        self.feeds.values().map(|feed| feed.dropped).sum()
    }
}

/// Save every received snapshot; a trace that lost one is refused.
fn save_trace(directory: &Path, trace: &[Value], samples: u64, dropped: u64) -> Result<()> {
    if dropped != 0 || trace.len() as u64 != samples {
        return Err("counter trace lost PUB snapshots".into());
    }
    let path = directory.join("counter-samples.jsonl");
    let mut file = std::io::BufWriter::new(fs::File::create(&path)?);
    for snapshot in trace {
        serde_json::to_writer(&mut file, snapshot)?;
        file.write_all(b"\n")?;
    }
    file.flush()?;
    println!("COUNTER_TRACE {} samples={samples}", path.display());
    Ok(())
}

/// Merge each worker process's thread counters into its usage.
fn merge_counters(row: &mut Value, feeds: &BTreeMap<(u64, String), CounterFeed>) -> Result<()> {
    for role in ["brokers", "writers", "readers"] {
        let processes = row[role].as_array_mut().ok_or("missing profile workers")?;
        for process in processes {
            let pid = if role == "brokers" {
                process["identity"]["pid"].as_u64()
            } else {
                process["pid"].as_u64()
            }
            .ok_or("missing profile worker pid")?;
            let totals = ProcessTotals::of(feeds, pid)?;
            if !totals.events.is_empty() {
                totals.write(&mut process["usage"])?;
            }
        }
    }
    Ok(())
}

/// Counter totals over the threads of one process.
#[derive(Default)]
struct ProcessTotals {
    events: BTreeMap<String, u64>,
    stages: BTreeMap<String, StageTotals>,
    logs: Vec<Value>,
}

#[derive(Default)]
struct StageTotals {
    bins: Vec<(usize, u64)>,
    total_ns: u64,
    max_ns: u64,
}

impl ProcessTotals {
    fn of(feeds: &BTreeMap<(u64, String), CounterFeed>, pid: u64) -> Result<Self> {
        let mut totals = Self::default();
        for ((source, thread), feed) in feeds {
            if *source != pid {
                continue;
            }
            if let Some(entries) = feed.logs.as_array() {
                totals.logs.push(json!({"thread":thread,"entries":entries}));
            }
            for (name, amount) in feed.events.as_object().ok_or("missing thread events")? {
                let amount = amount.as_u64().ok_or("invalid thread event count")?;
                let kind = ozzy_runtime::profiling::Kind::of(name).ok_or("unknown thread event")?;
                let entry = totals.events.entry(name.clone()).or_default();
                *entry = kind.merge(*entry, amount);
            }
            for stage in feed.stages.as_array().ok_or("missing thread stages")? {
                totals.add_stage(stage)?;
            }
        }
        Ok(totals)
    }

    fn add_stage(&mut self, stage: &Value) -> Result<()> {
        let name = stage["stage"].as_str().ok_or("missing stage name")?;
        let total = self.stages.entry(name.to_owned()).or_default();
        total.total_ns += stage["total_ns"].as_u64().ok_or("missing stage total")?;
        total.max_ns = total
            .max_ns
            .max(stage["max_ns"].as_u64().ok_or("missing stage maximum")?);
        for pair in stage["bins"].as_array().ok_or("missing stage bins")? {
            let bins = pair.as_array().ok_or("invalid stage bin")?;
            if bins.len() != 2 {
                return Err("invalid stage bin".into());
            }
            total.bins.push((
                bins[0].as_u64().ok_or("invalid stage bin index")? as usize,
                bins[1].as_u64().ok_or("invalid stage bin count")?,
            ));
        }
        Ok(())
    }

    /// Store the totals as the process's event counts, logs and stage
    /// summaries.
    fn write(self, usage: &mut Value) -> Result<()> {
        usage["replication_events"] = json!(self.events);
        usage["telemetry_logs"] = json!(self.logs);
        usage["stages"] = Value::Array(
            self.stages
                .into_iter()
                .map(|(name, total)| {
                    let stage = ozzy_runtime::profiling::summarize_stage(
                        &name,
                        &total.bins,
                        total.total_ns,
                        total.max_ns,
                    )
                    .ok_or("invalid merged stage bins")?;
                    Ok(json!({"stage":stage.stage,"samples":stage.samples,
                        "total_ns":stage.total_ns,"p50_ns":stage.p50_ns,
                        "p99_ns":stage.p99_ns,"max_ns":stage.max_ns}))
                })
                .collect::<Result<Vec<_>>>()?,
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_trace_retains_received_snapshot_and_collector_time() {
        ozzy_runtime::profiling::enable();
        ozzy_runtime::profiling::count(ozzy_runtime::profiling::Event::ProcessingArenaRefusal, 3);
        let events: BTreeMap<_, _> = ozzy_runtime::profiling::events().into_iter().collect();
        let row = json!({
            "pid":42,"thread":"shard-a","sequence":1,"elapsed_ms":20,
            "events":events,"stages":[]
        });
        let message = omq_tokio::Message::from(serde_json::to_vec(&row).unwrap());
        let mut collector = Collector::new(true);
        collector.observe(&message).unwrap();
        let trace = collector.trace.as_ref().unwrap();
        assert_eq!(trace.len(), 1);
        assert_eq!(trace[0]["events"], row["events"]);
        assert_eq!(trace[0]["events"]["processing_arena_refusal"], 3);
        assert_eq!(trace[0]["stages"], row["stages"]);
        assert!(trace[0]["collector_elapsed_ns"].as_u64().is_some());
        assert_eq!(collector.feeds[&(42, "shard-a".to_owned())].samples, 1);
    }

    #[test]
    fn merge_sums_counts_and_gauges_but_keeps_largest_peak() {
        let mut row = json!({
            "brokers":[{"identity":{"pid":42},"usage":{}}],
            "writers":[],"readers":[]
        });
        let feeds = BTreeMap::from([
            (
                (42, "shard-a".to_owned()),
                CounterFeed {
                    events: json!({"replica_repair":7,"live_window_peak_operations":11,"dedup_live_bytes":10}),
                    stages: json!([]),
                    ..Default::default()
                },
            ),
            (
                (42, "shard-b".to_owned()),
                CounterFeed {
                    events: json!({"replica_repair":9,"live_window_peak_operations":5,"dedup_live_bytes":u64::MAX - 3}),
                    stages: json!([]),
                    ..Default::default()
                },
            ),
        ]);
        merge_counters(&mut row, &feeds).unwrap();
        assert_eq!(
            row["brokers"][0]["usage"]["replication_events"]["replica_repair"],
            16
        );
        assert_eq!(
            row["brokers"][0]["usage"]["replication_events"]["live_window_peak_operations"],
            11
        );
        assert_eq!(
            row["brokers"][0]["usage"]["replication_events"]["dedup_live_bytes"],
            6
        );
    }
}
