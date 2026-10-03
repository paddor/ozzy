//! Individual SDK submissions with concurrent delivery observation and pacing.
use super::super::super::pacing::{Plan, Timer};
use super::super::{Config, Counts, Result, Value, Window, config, metrics, producer};
use bytes::Bytes;
use futures::{FutureExt, StreamExt, stream::FuturesOrdered};
use ozzy_proto::MessageId;
use std::future::Future;

pub(super) async fn produce<F: Future<Output = Result<u64>>>(
    config: &Config,
    lane: usize,
    window: Window,
    mut send: impl FnMut(MessageId, Bytes, u64) -> Result<F>,
) -> Result<Value> {
    let group = config::group(&config.args)?;
    let plan = Plan::new(config, window, lane)?;
    let mut timer = plan.map(|_| Timer::new()).transpose()?;
    let mut counts = Counts::new(window);
    if let Some(plan) = &plan {
        counts.enable_schedule(plan.bounds());
    }
    let mut pending = FuturesOrdered::new();
    let mut sequence = 0;
    let partition = lane % config.partitions();
    let shared = partition + config.partitions() < config.writers;
    let mut offsets = Offsets {
        shared,
        first: None,
        previous: None,
    };
    // An overloaded ramp lowers the limit to the arrivals of earlier stages.
    let mut limit = plan.map(Plan::total);
    loop {
        // Completed deliveries must be observed during sparse traffic too.
        // Waiting until the queue fills would inflate latency by seconds.
        while let Some(Some(result)) = pending.next().now_or_never() {
            observe(result, plan.as_ref(), &mut counts, &mut offsets)?;
        }
        let now = metrics::monotonic_ns();
        if limit.map_or(now >= window.end, |limit| sequence >= limit) {
            break;
        }
        if let Some(plan) = plan
            && limit == Some(plan.total())
            && let Some(stage) = plan.check_backlog(sequence, now)?
        {
            counts.overload(stage);
            limit = Some(plan.total_before(stage)?);
            continue;
        }
        if pending.len() == config.args.request_records {
            observe(
                pending.next().await.unwrap(),
                plan.as_ref(),
                &mut counts,
                &mut offsets,
            )?;
            continue;
        }
        if let Some(plan) = plan {
            let due = plan.due(sequence)?;
            if now < due {
                tokio::select! {
                    result = pending.next(), if !pending.is_empty() => observe(result.unwrap(), Some(&plan), &mut counts, &mut offsets)?,
                    result = timer.as_mut().unwrap().wait_until(due) => result?,
                }
                continue;
            }
        }
        let start = metrics::monotonic_ns();
        let payload = producer::payload(config, lane, sequence, start)?;
        let id = config::message_id(group, lane, sequence);
        counts.record_bytes(id, &payload);
        let future = send(id, payload, sequence)?;
        counts.submission(start, 1);
        pending.push_back(async move {
            let result = future.await;
            (result, start, sequence, metrics::monotonic_ns())
        });
        sequence += 1;
        super::cooperate(sequence).await;
    }
    while let Some(result) = pending.next().await {
        observe(result, plan.as_ref(), &mut counts, &mut offsets)?;
    }
    if counts.total != sequence {
        return Err("incomplete Kafka confirmations".into());
    }
    let mut row = counts.report(lane);
    row["partition"] = serde_json::json!(partition);
    row["confirmed_partition_offset_range"] = serde_json::json!(
        offsets
            .first
            .zip(offsets.previous)
            .map(|(first, last)| [first, last])
    );
    Ok(row)
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
    (result, start, sequence, finished): (Result<u64>, u64, u64, u64),
    plan: Option<&Plan>,
    counts: &mut Counts,
    offsets: &mut Offsets,
) -> Result<()> {
    offsets.confirm(result?, sequence)?;
    if let Some(plan) = plan {
        counts.scheduled_complete(plan.due(sequence)?, start, finished)
    } else {
        counts.complete(start, finished, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::{cell::Cell, rc::Rc, time::Duration};

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
        observe((Ok(3), 100, 0, 200), None, &mut counts, &mut offsets).unwrap();
        observe((Ok(8), 100, 1, 200), None, &mut counts, &mut offsets).unwrap();
        assert!(observe((Ok(7), 100, 2, 200), None, &mut counts, &mut offsets).is_err());
        assert_eq!(counts.total, 2);
        assert_eq!((offsets.first, offsets.previous), (Some(3), Some(8)));
        let mut exclusive = Offsets {
            shared: false,
            first: None,
            previous: None,
        };
        assert!(observe((Ok(1), 100, 0, 200), None, &mut counts, &mut exclusive).is_err());
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
