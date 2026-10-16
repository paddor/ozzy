//! One Iggy request at a time. Scheduled arrivals keep their clock while an
//! earlier request waits; batches drain due records without intentional linger.
use super::super::super::pacing::{Arrivals, Plan, Timer};
use super::super::{Config, Counts, Result, Value, Window, config, error, metrics, producer};
use iggy::prelude::{IggyMessage, SendMessagesResponse};
use ozzy_proto::GroupId;

pub(super) async fn produce(
    config: &Config,
    lane: usize,
    window: Window,
    send: impl AsyncFnMut(&mut [IggyMessage]) -> Result<SendMessagesResponse>,
) -> Result<Value> {
    let mut timer = config.args.scheduled().then(Timer::new).transpose()?;
    let clock = Clock {
        now: metrics::monotonic_ns,
        wait_until: async |due| match timer.as_mut() {
            Some(timer) => timer.wait_until(due).await,
            None => Err(error("an unpaced Iggy lane has no arrival timer")),
        },
    };
    produce_with_clock(config, lane, window, send, clock).await
}

/// Where a lane reads the time and waits for an arrival.
struct Clock<N, W> {
    now: N,
    wait_until: W,
}

async fn produce_with_clock<N: Fn() -> u64, W: AsyncFnMut(u64) -> Result<()>>(
    config: &Config,
    lane: usize,
    window: Window,
    mut send: impl AsyncFnMut(&mut [IggyMessage]) -> Result<SendMessagesResponse>,
    mut clock: Clock<N, W>,
) -> Result<Value> {
    let mut run = LaneRun::new(config, lane, window)?;
    loop {
        run.messages.clear();
        run.starts.clear();
        if let Some(arrivals) = &mut run.arrivals {
            if arrivals.exhausted(run.sequence) {
                break;
            }
            if arrivals.lower_on_backlog(&mut run.counts, run.sequence, &clock.now)? {
                continue;
            }
            (clock.wait_until)(arrivals.plan().due(run.sequence)?).await?;
        }
        let first = run.sequence;
        run.fill(&clock.now)?;
        if run.messages.is_empty() {
            break;
        }
        // Do not cancel/recreate a send when another scheduled arrival becomes
        // due. Iggy's SDK serializes sends on the connection until its reply.
        let reply = send(&mut run.messages).await?;
        run.confirm(&reply, first, &clock.now)?;
    }
    Ok(run.report())
}

/// One writer lane: its pacing, counters, confirmations, and the request
/// being built.
struct LaneRun<'a> {
    config: &'a Config,
    lane: usize,
    window: Window,
    group: GroupId,
    arrivals: Option<Arrivals>,
    counts: Counts,
    confirmations: Confirmations,
    messages: Vec<IggyMessage>,
    starts: Vec<u64>,
    /// The next record to add.
    sequence: u64,
    /// Unpaced lanes read the clock once per quantum, as the native writer
    /// lane does; paced lanes need every record's own clock. Every record
    /// gets its own body.
    start: u64,
}

impl<'a> LaneRun<'a> {
    fn new(config: &'a Config, lane: usize, window: Window) -> Result<Self> {
        let group = config::group(&config.args)?;
        let mut counts = Counts::new(window);
        let plan = Plan::new(config, window, lane)?;
        if let Some(plan) = &plan {
            counts.enable_schedule(plan.bounds());
        }
        Ok(Self {
            config,
            lane,
            window,
            group,
            arrivals: plan.as_ref().map(Arrivals::new),
            counts,
            confirmations: Confirmations::new(config, lane),
            messages: Vec::with_capacity(config.args.request_records),
            starts: Vec::with_capacity(config.args.request_records),
            sequence: 0,
            start: 0,
        })
    }

    /// Add due records up to the request size. Stops at the arrival limit,
    /// at a record not due yet, or, unpaced, at the window end.
    fn fill(&mut self, now: &impl Fn() -> u64) -> Result<()> {
        while self.messages.len() < self.config.args.request_records {
            let fresh =
                self.arrivals.is_some() || self.sequence.is_multiple_of(producer::CLOCK_QUANTUM);
            if fresh {
                self.start = now();
            }
            if let Some(arrivals) = &mut self.arrivals {
                if arrivals.exhausted(self.sequence) {
                    break;
                }
                let start = self.start;
                if arrivals.lower_on_backlog(&mut self.counts, self.sequence, || start)? {
                    continue;
                }
                if self.start < arrivals.plan().due(self.sequence)? {
                    break;
                }
            } else if fresh && self.start >= self.window.end {
                break;
            }
            self.push()?;
        }
        Ok(())
    }

    /// Generate the next record into the request.
    fn push(&mut self) -> Result<()> {
        let payload = producer::payload(self.config, self.lane, self.sequence, self.start)?;
        let id = config::message_id(self.group, self.lane, self.sequence);
        self.counts.record_bytes(id, &payload);
        self.messages.push(
            IggyMessage::builder()
                .id(u128::from_be_bytes(*id.as_bytes()))
                .payload(payload)
                .build()?,
        );
        self.starts.push(self.start);
        self.counts.submission(self.start, 1);
        self.sequence += 1;
        Ok(())
    }

    /// Accept the reply to the request that began at record `first`, then
    /// complete each record at the current time.
    fn confirm(
        &mut self,
        reply: &SendMessagesResponse,
        first: u64,
        now: &impl Fn() -> u64,
    ) -> Result<()> {
        self.confirmations
            .accept(reply, first, self.messages.len())?;
        let complete = now();
        let plan = self.arrivals.as_ref().map(Arrivals::plan);
        for (i, &start) in self.starts.iter().enumerate() {
            if let Some(plan) = plan {
                self.counts
                    .scheduled_complete(plan.due(first + i as u64)?, start, complete)?;
            } else {
                self.counts.complete(start, complete, 1)?;
            }
        }
        Ok(())
    }

    fn report(&self) -> Value {
        let mut row = self.counts.report(self.lane);
        row["partition"] = serde_json::json!(self.confirmations.partition);
        row["confirmed_partition_offset_range"] = serde_json::json!(
            self.confirmations
                .first
                .map(|first| [first, self.confirmations.end - 1])
        );
        row
    }
}

struct Confirmations {
    partition: usize,
    shared: bool,
    first: Option<u64>,
    end: u64,
}

impl Confirmations {
    fn new(config: &Config, lane: usize) -> Self {
        let partition = lane % config.partitions();
        Self {
            partition,
            shared: partition + config.partitions() < config.writers,
            first: None,
            end: 0,
        }
    }

    fn accept(
        &mut self,
        reply: &SendMessagesResponse,
        sequence: u64,
        records: usize,
    ) -> Result<()> {
        let [reply] = reply.confirmations.as_slice() else {
            return Err("Iggy confirmation range differs from submitted records".into());
        };
        if reply.partition_id != self.partition as u32
            || if self.shared {
                reply.base_offset < sequence || reply.base_offset < self.end
            } else {
                reply.base_offset != sequence
            }
        {
            return Err("Iggy confirmation range differs from submitted records".into());
        }
        self.end = reply
            .base_offset
            .checked_add(records as u64)
            .ok_or("Iggy confirmation range overflow")?;
        self.first.get_or_insert(reply.base_offset);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use iggy::prelude::SendMessagesConfirmationResponse;

    fn configuration(request_records: &str) -> Config {
        Config::new(super::super::super::super::super::Args::parse_from([
            "bench",
            "--external-policy",
            "buffered",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "0.08",
            "--warmup",
            "0",
            "--window",
            "3",
            "--request-records",
            request_records,
            "--records-per-second",
            "100",
            "--json-payload",
            "--external-system",
            "iggy",
            "--external-endpoint",
            "127.0.0.1:18090",
            "--group",
            "00000000-0000-0000-0000-000000000001",
        ]))
        .unwrap()
    }

    fn reply(lane: usize, offset: u64) -> SendMessagesResponse {
        SendMessagesResponse {
            confirmations: vec![SendMessagesConfirmationResponse {
                stream_id: 1,
                topic_id: 1,
                partition_id: lane as u32,
                base_offset: offset,
            }],
        }
    }

    #[tokio::test]
    async fn sparse_batches_never_wait_to_fill_and_late_arrivals_drain_with_original_clocks() {
        let config = configuration("2");
        let start = 10_000_000_000;
        let clock = std::cell::Cell::new(start);
        let window = Window {
            start,
            measured: start,
            end: start + 80_000_000,
        };
        let lane = 1;
        let plan = Plan::new(&config, window, lane).unwrap().unwrap();
        let group = config::group(&config.args).unwrap();
        let mut sequence = 0;
        let mut batches = Vec::new();
        let row = produce_with_clock(
            &config,
            lane,
            window,
            async |messages| {
                let first = sequence;
                batches.push(messages.len());
                for message in messages.iter() {
                    let generated = u64::from_be_bytes(message.payload[..8].try_into().unwrap());
                    assert!(generated >= plan.due(sequence).unwrap());
                    assert_eq!(
                        message.header.id,
                        u128::from_be_bytes(*config::message_id(group, lane, sequence).as_bytes())
                    );
                    assert_eq!(
                        message.payload,
                        producer::payload(&config, lane, sequence, generated).unwrap()
                    );
                    if first > 0 {
                        assert!(generated > window.end);
                    }
                    sequence += 1;
                }
                if first == 0 {
                    assert_eq!(
                        messages.len(),
                        1,
                        "must send the first due record immediately"
                    );
                    clock.set(window.end + 10_000_000);
                }
                Ok(reply(lane, first))
            },
            Clock {
                now: || clock.get(),
                wait_until: async |due| {
                    clock.set(clock.get().max(due));
                    Ok(())
                },
            },
        )
        .await
        .unwrap();
        assert_eq!(batches, [1, 2]);
        assert_eq!(row["total"], plan.total());
        let samples = |pointer: &str| {
            row.pointer(pointer).unwrap()["bins"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap())
                .sum::<u64>()
        };
        assert_eq!(samples("/scheduled_latency/0"), 3);
        assert_eq!(samples("/scheduling_lag/0"), 3);
        assert_eq!(samples("/latency"), 1);
    }

    #[tokio::test]
    async fn shared_partition_confirmations_allow_gaps_but_reject_overlapping_ranges() {
        let mut config = configuration("1");
        config.args.native.partitions = Some(1);
        config.args.reader_workers = Some(1);
        let config = Config::new(config.args).unwrap();
        for offsets in [[3, 8, 12], [3, 3, 12]] {
            let start = 10_000_000_000;
            let clock = std::cell::Cell::new(start);
            let window = Window {
                start,
                measured: start,
                end: start + 80_000_000,
            };
            let mut sent = 0;
            let result = produce_with_clock(
                &config,
                0,
                window,
                async |messages| {
                    assert_eq!(messages.len(), 1);
                    let confirmation = reply(0, offsets[sent]);
                    sent += 1;
                    clock.set(window.end + 10_000_000);
                    Ok(confirmation)
                },
                Clock {
                    now: || clock.get(),
                    wait_until: async |due| {
                        clock.set(clock.get().max(due));
                        Ok(())
                    },
                },
            )
            .await;
            if offsets[1] == 3 {
                assert!(result.is_err());
            } else {
                let row = result.unwrap();
                assert_eq!(row["total"], 3);
                assert_eq!(row["partition"], 0);
                assert_eq!(
                    row["confirmed_partition_offset_range"],
                    serde_json::json!([3, 12])
                );
            }
        }
    }

    #[tokio::test]
    async fn invalid_confirmations_and_excess_backlog_never_publish_a_result() {
        let config = configuration("1");
        let start = metrics::monotonic_ns().saturating_sub(1_000_000);
        let window = Window {
            start,
            measured: start,
            end: start + 80_000_000,
        };
        for bad in [
            reply(0, 1),
            reply(1, 0),
            SendMessagesResponse {
                confirmations: vec![],
            },
        ] {
            assert!(
                produce(&config, 0, window, async |_| Ok(bad.clone()))
                    .await
                    .is_err()
            );
        }
        let start = metrics::monotonic_ns() - 1_500_000_000;
        let window = Window {
            start,
            measured: start,
            end: start + 2_000_000_000,
        };
        let error = produce(&config, 0, window, async |_| {
            panic!("must reject backlog before sending")
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("scheduled backlog exceeded"));
    }
}
