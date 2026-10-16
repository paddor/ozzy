//! Individual SDK submissions with concurrent delivery observation and pacing.
use super::super::super::pacing::{Arrivals, Plan, Timer};
use super::super::{Config, Counts, Result, Value, Window, config, metrics, producer};
use bytes::Bytes;
use futures::{FutureExt, StreamExt, stream::FuturesOrdered};
use ozzy_proto::{GroupId, MessageId};
use std::future::Future;

pub(super) async fn produce<F: Future<Output = Result<u64>>>(
    config: &Config,
    index: usize,
    window: Window,
    mut send: impl FnMut(MessageId, Bytes, u64) -> Result<F>,
) -> Result<Value> {
    let mut lane = LaneRun::new(config, index, window)?;
    let mut timer = lane.arrivals.as_ref().map(|_| Timer::new()).transpose()?;
    let mut pending = FuturesOrdered::new();
    loop {
        // Completed deliveries must be observed during sparse traffic too.
        // Waiting until the queue fills would inflate latency by seconds.
        while let Some(Some(delivery)) = pending.next().now_or_never() {
            lane.observe(delivery)?;
        }
        let now = metrics::monotonic_ns();
        if lane.finished(now) {
            break;
        }
        if lane.lower_on_backlog(|| now)? {
            continue;
        }
        if pending.len() == config.args.request_records
            && let Some(delivery) = pending.next().await
        {
            lane.observe(delivery)?;
            continue;
        }
        if let (Some(plan), Some(timer)) = (lane.plan(), timer.as_mut()) {
            let due = plan.due(lane.sequence)?;
            if now < due {
                tokio::select! {
                    Some(delivery) = pending.next(), if !pending.is_empty() => lane.observe(delivery)?,
                    result = timer.wait_until(due) => result?,
                }
                continue;
            }
        }
        let (start, id, payload) = lane.record()?;
        let sequence = lane.sequence;
        let future = send(id, payload, sequence)?;
        lane.counts.submission(start, 1);
        pending.push_back(async move {
            let offset = future.await;
            Delivery {
                offset,
                start,
                sequence,
                finished: metrics::monotonic_ns(),
            }
        });
        lane.sequence += 1;
        super::cooperate(lane.sequence).await;
    }
    while let Some(delivery) = pending.next().await {
        lane.observe(delivery)?;
    }
    lane.report()
}

/// One writer lane: its pacing, counters and confirmed partition offsets.
struct LaneRun<'a> {
    config: &'a Config,
    index: usize,
    group: GroupId,
    window: Window,
    arrivals: Option<Arrivals>,
    counts: Counts,
    partition: usize,
    offsets: Offsets,
    /// The next record to submit.
    sequence: u64,
}

impl<'a> LaneRun<'a> {
    fn new(config: &'a Config, index: usize, window: Window) -> Result<Self> {
        let group = config::group(&config.args)?;
        let plan = Plan::new(config, window, index)?;
        let mut counts = Counts::new(window);
        if let Some(plan) = &plan {
            counts.enable_schedule(plan.bounds());
        }
        let partition = index % config.partitions();
        Ok(Self {
            config,
            index,
            group,
            window,
            arrivals: plan.as_ref().map(Arrivals::new),
            counts,
            partition,
            offsets: Offsets {
                shared: partition + config.partitions() < config.writers,
                first: None,
                previous: None,
            },
            sequence: 0,
        })
    }

    /// Whether the lane submitted its last record: its arrival limit, or the
    /// window end for an unpaced lane.
    fn finished(&self, now: u64) -> bool {
        self.arrivals
            .as_ref()
            .map_or(now >= self.window.end, |arrivals| {
                arrivals.exhausted(self.sequence)
            })
    }

    /// The arrival plan of a paced lane.
    fn plan(&self) -> Option<Plan> {
        self.arrivals.as_ref().map(Arrivals::plan)
    }

    /// Whether an overloaded ramp just lowered the arrival limit.
    fn lower_on_backlog(&mut self, now: impl FnOnce() -> u64) -> Result<bool> {
        match &mut self.arrivals {
            Some(arrivals) => arrivals.lower_on_backlog(&mut self.counts, self.sequence, now),
            None => Ok(false),
        }
    }

    /// Confirm one delivery's offset and count its completion.
    fn observe(&mut self, delivery: Delivery) -> Result<()> {
        let plan = self.plan();
        observe(delivery, plan.as_ref(), &mut self.counts, &mut self.offsets)
    }

    /// Generate the next record and count its bytes.
    fn record(&mut self) -> Result<(u64, MessageId, Bytes)> {
        let start = metrics::monotonic_ns();
        let payload = producer::payload(self.config, self.index, self.sequence, start)?;
        let id = config::message_id(self.group, self.index, self.sequence);
        self.counts.record_bytes(id, &payload);
        Ok((start, id, payload))
    }

    /// The lane row, once every submitted record is confirmed.
    fn report(self) -> Result<Value> {
        if self.counts.total != self.sequence {
            return Err("incomplete Kafka confirmations".into());
        }
        let mut row = self.counts.report(self.index);
        row["partition"] = serde_json::json!(self.partition);
        row["confirmed_partition_offset_range"] = serde_json::json!(
            self.offsets
                .first
                .zip(self.offsets.previous)
                .map(|(first, last)| [first, last])
        );
        Ok(row)
    }
}

/// One send outcome: the confirmed offset, the submission and completion
/// clocks, and the record sequence.
struct Delivery {
    offset: Result<u64>,
    start: u64,
    sequence: u64,
    finished: u64,
}

struct Offsets {
    shared: bool,
    first: Option<u64>,
    previous: Option<u64>,
}

impl Offsets {
    fn confirm(&mut self, offset: u64, sequence: u64) -> Result<()> {
        if if self.shared {
            offset < sequence || self.previous.is_some_and(|previous| offset <= previous)
        } else {
            offset != sequence
        } {
            return Err("Kafka confirmation offset mismatch".into());
        }
        self.first.get_or_insert(offset);
        self.previous = Some(offset);
        Ok(())
    }
}

fn observe(
    delivery: Delivery,
    plan: Option<&Plan>,
    counts: &mut Counts,
    offsets: &mut Offsets,
) -> Result<()> {
    offsets.confirm(delivery.offset?, delivery.sequence)?;
    if let Some(plan) = plan {
        counts.scheduled_complete(
            plan.due(delivery.sequence)?,
            delivery.start,
            delivery.finished,
        )
    } else {
        counts.complete(delivery.start, delivery.finished, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::{cell::Cell, rc::Rc, time::Duration};

    fn delivered(offset: u64, sequence: u64) -> Delivery {
        Delivery {
            offset: Ok(offset),
            start: 100,
            sequence,
            finished: 200,
        }
    }

    #[test]
    fn shared_delivery_offsets_allow_gaps_and_reject_reordered_confirmations() {
        let window = Window {
            start: 0,
            measured: 0,
            end: 1_000_000_000,
        };
        let mut counts = Counts::new(window);
        let mut offsets = Offsets {
            shared: true,
            first: None,
            previous: None,
        };
        observe(delivered(3, 0), None, &mut counts, &mut offsets).unwrap();
        observe(delivered(8, 1), None, &mut counts, &mut offsets).unwrap();
        assert!(observe(delivered(7, 2), None, &mut counts, &mut offsets).is_err());
        assert_eq!(counts.total, 2);
        assert_eq!((offsets.first, offsets.previous), (Some(3), Some(8)));
        let mut exclusive = Offsets {
            shared: false,
            first: None,
            previous: None,
        };
        assert!(observe(delivered(1, 0), None, &mut counts, &mut exclusive).is_err());
        assert_eq!(counts.total, 2);
    }

    #[tokio::test]
    async fn sparse_deliveries_are_polled_before_next_arrival_and_late_arrivals_are_preserved() {
        let config = Config::new(super::super::super::super::super::Args::parse_from([
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
            "1",
            "--request-records",
            "2",
            "--records-per-second",
            "100",
            "--json-payload",
            "--external-system",
            "redpanda",
            "--external-endpoint",
            "127.0.0.1:19092",
            "--group",
            "00000000-0000-0000-0000-000000000001",
        ]))
        .unwrap();
        let start = metrics::monotonic_ns() + 10_000_000;
        let window = Window {
            start,
            measured: start,
            end: start + 80_000_000,
        };
        let plan = Plan::new(&config, window, 0).unwrap().unwrap();
        let completed = Rc::new(Cell::new(0));
        let mut sent = 0;
        let row = produce(&config, 0, window, |_, payload, sequence| {
            assert_eq!(sequence, sent);
            let generated = u64::from_be_bytes(payload[..8].try_into().unwrap());
            assert!(generated >= plan.due(sequence).unwrap());
            assert_eq!(
                payload,
                producer::payload(&config, 0, sequence, generated).unwrap()
            );
            if sequence == 1 {
                assert_eq!(completed.get(), 1, "sparse delivery was left unpolled");
            }
            sent += 1;
            let completed = Rc::clone(&completed);
            Ok(async move {
                if sequence == 1 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                completed.set(completed.get() + 1);
                Ok(sequence)
            })
        })
        .await
        .unwrap();
        assert_eq!(sent, plan.total());
        assert_eq!(row["total"], plan.total());
        assert_eq!(
            row["scheduled_latency"][0]["bins"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap())
                .sum::<u64>(),
            plan.total()
        );
        assert!(row["last_ns"].as_u64().unwrap() > window.end);
    }
}
