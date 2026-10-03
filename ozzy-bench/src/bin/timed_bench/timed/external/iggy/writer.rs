//! One Iggy request at a time. Scheduled arrivals keep their clock while an
//! earlier request waits; batches drain due records without intentional linger.
use super::super::super::pacing::{Plan, Timer};
use super::super::{Config, Counts, Result, Value, Window, config, metrics, producer};
use iggy::prelude::{IggyMessage, SendMessagesResponse};

pub(super) async fn produce(
    config: &Config,
    lane: usize,
    window: Window,
    send: impl AsyncFnMut(&mut [IggyMessage]) -> Result<SendMessagesResponse>,
) -> Result<Value> {
    let mut timer = config.args.scheduled().then(Timer::new).transpose()?;
    produce_with_clock(
        config,
        lane,
        window,
        send,
        metrics::monotonic_ns,
        async |due| timer.as_mut().unwrap().wait_until(due).await,
    )
    .await
}

async fn produce_with_clock(
    config: &Config,
    lane: usize,
    window: Window,
    mut send: impl AsyncFnMut(&mut [IggyMessage]) -> Result<SendMessagesResponse>,
    now: impl Fn() -> u64,
    mut wait_until: impl AsyncFnMut(u64) -> Result<()>,
) -> Result<Value> {
    let group = config::group(&config.args)?;
    let mut counts = Counts::new(window);
    let plan = Plan::new(config, window, lane)?;
    if let Some(plan) = &plan {
        counts.enable_schedule(plan.bounds());
    }
    let mut sequence = 0;
    let mut confirmations = Confirmations::new(config, lane);
    let mut messages = Vec::with_capacity(config.args.request_records);
    let mut starts = Vec::with_capacity(config.args.request_records);
    // Unpaced lanes read the clock once per quantum, as the native writer lane
    // does; paced lanes need every record's own clock. Every record gets its
    // own body.
    let mut start = 0;
    // An overloaded ramp lowers the limit to the arrivals of earlier stages.
    let mut limit = plan.map(Plan::total);
    loop {
        messages.clear();
        starts.clear();
        if let Some(plan) = plan {
            if Some(sequence) >= limit {
                break;
            }
            if limit == Some(plan.total())
                && let Some(stage) = plan.check_backlog(sequence, now())?
            {
                counts.overload(stage);
                limit = Some(plan.total_before(stage)?);
                continue;
            }
            wait_until(plan.due(sequence)?).await?;
        }
        let first = sequence;
        while messages.len() < config.args.request_records {
            let fresh = plan.is_some() || sequence.is_multiple_of(producer::CLOCK_QUANTUM);
            if fresh {
                start = now();
            }
            if let Some(plan) = plan {
                if Some(sequence) >= limit {
                    break;
                }
                if limit == Some(plan.total())
                    && let Some(stage) = plan.check_backlog(sequence, start)?
                {
                    counts.overload(stage);
                    limit = Some(plan.total_before(stage)?);
                    continue;
                }
                if start < plan.due(sequence)? {
                    break;
                }
            } else if fresh && start >= window.end {
                break;
            }
            let payload = producer::payload(config, lane, sequence, start)?;
            let id = config::message_id(group, lane, sequence);
            counts.record_bytes(id, &payload);
            messages.push(
                IggyMessage::builder()
                    .id(u128::from_be_bytes(*id.as_bytes()))
                    .payload(payload)
                    .build()?,
            );
            starts.push(start);
            counts.submission(start, 1);
            sequence += 1;
        }
        if messages.is_empty() {
            break;
        }
        // Do not cancel/recreate a send when another scheduled arrival becomes
        // due. Iggy's SDK serializes sends on the connection until its reply.
        let reply = send(&mut messages).await?;
        confirmations.accept(&reply, first, messages.len())?;
        let complete = now();
        for (i, &start) in starts.iter().enumerate() {
            if let Some(plan) = plan {
                counts.scheduled_complete(plan.due(first + i as u64)?, start, complete)?;
            } else {
                counts.complete(start, complete, 1)?;
            }
        }
    }
    let mut row = counts.report(lane);
    row["partition"] = serde_json::json!(confirmations.partition);
    row["confirmed_partition_offset_range"] = serde_json::json!(
        confirmations
            .first
            .map(|first| [first, confirmations.end - 1])
    );
    Ok(row)
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
            || clock.get(),
            async |due| {
                clock.set(clock.get().max(due));
                Ok(())
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
                || clock.get(),
                async |due| {
                    clock.set(clock.get().max(due));
                    Ok(())
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
