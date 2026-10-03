use bytes::Bytes;
use futures::future::try_join_all;
use ozzy_proto::{GroupId, append::AppendKey};
use ozzy_runtime::replicated::WriterRuntime;
use ozzy_runtime::replicated::{RecordInput, WriterStats};
use serde_json::{Value, json};
use std::collections::VecDeque;

use super::super::{Result, error, lanes, metrics, workload};
use super::{Config, Window, command, config, launch, measurement::Counts, report};

mod client;
mod paced;
use client::{PendingRecord, ReceiptCheck, Writer};

/// Records sharing one clock reading in unpaced lanes.
pub(super) const CLOCK_QUANTUM: u64 = 64;

/// Distinct body bytes per lane before bodies repeat. Real traffic differs
/// from record to record. The pool exceeds every codec window and request the
/// benchmark uses, so neither a compressor nor a branch predictor finds an
/// earlier copy of a body.
const PAYLOAD_POOL_BYTES: usize = 8 * 1024 * 1024;

/// Bodies after the clock for one lane and shape, stored back to back.
struct Pool {
    lane: usize,
    record_bytes: usize,
    shape: u8,
    bytes: Vec<u8>,
}

thread_local! {
    static POOLS: std::cell::RefCell<Vec<Pool>> = std::cell::RefCell::default();
}

/// Bodies per lane, used in turn by sequence: enough to fill the pool, and
/// never fewer than one request's records.
pub(super) fn payload_bodies(config: &Config) -> u64 {
    let body = config.args.record_bytes.saturating_sub(8).max(1);
    PAYLOAD_POOL_BYTES
        .div_ceil(body)
        .max(config.args.request_records) as u64
}

/// Build one lane's pool on this thread before the measurement window opens.
pub(super) fn build_payload_pool(config: &Config, lane: usize) -> Result<()> {
    encode_payload(&mut Vec::new(), config, lane, 0, 0)
}

/// Memory one lane's pool holds in each writer or reader process.
pub(super) fn payload_pool_bytes(config: &Config) -> u64 {
    if config.args.binary_payload {
        return 0;
    }
    payload_bodies(config) * config.args.record_bytes.saturating_sub(8) as u64
}

pub(super) async fn run(config: Config) -> Result<()> {
    let index = config
        .args
        .producer_worker
        .ok_or_else(|| error("missing producer worker"))?;
    let lanes = workload::worker_share(config.writers, config.workers, index)?;
    let group = config::group(&config.args)?;
    let mut input = launch::input();
    let connect = command(&mut input, "connect").await?;
    let context = WriterRuntime::new()?;
    let setup = connect
        .get("native")
        .ok_or_else(|| error("missing production writer setup"))?;
    let (clients, links) = client::connect_shared(&context, &config, setup, lanes.clone()).await?;
    for lane in lanes.clone() {
        build_payload_pool(&config, lane)?;
    }
    launch::reply(&json!({"event":"connected","index":index,"pid":std::process::id()}))?;
    let start = command(&mut input, "start").await?;
    let window = super::window(&start, &config.args)?;
    window.wait().await;
    let meter = metrics::Meter::start();
    let work = try_join_all(
        clients
            .into_iter()
            .zip(lanes)
            .map(|(client, lane)| run_lane(client, &config, group, lane, window)),
    );
    let rows = tokio::select! {
        result = work => result?,
        _ = input.recv() => return Err(error("producer control closed during workload")),
    };
    let usage = meter.finish();
    links.shutdown().await?;
    report(
        &config,
        &json!({"lanes":rows,"usage":usage,"pid":std::process::id(),"index":index}),
    )?;
    if input.recv().await.is_some() {
        return Err(error("unexpected producer command"));
    }
    Ok(())
}

/// Timestamp plus deterministic structured payload. Creation and verification
/// both belong to the measured workload; no whole-run corpus is retained.
pub(super) fn payload(config: &Config, lane: usize, sequence: u64, start: u64) -> Result<Bytes> {
    let mut bytes = Vec::with_capacity(config.args.record_bytes);
    encode_payload(&mut bytes, config, lane, sequence, start)?;
    Ok(Bytes::from(bytes))
}

pub(super) fn encode_payload(
    bytes: &mut Vec<u8>,
    config: &Config,
    lane: usize,
    sequence: u64,
    start: u64,
) -> Result<()> {
    bytes.clear();
    let bodies = payload_bodies(config);
    let body = sequence % bodies;
    if config.args.binary_payload {
        let number = config::record_number(lane, body)?;
        bytes.extend_from_slice(&workload::binary_event(start, number));
        return Ok(());
    }
    bytes.extend_from_slice(&start.to_be_bytes());
    let record_bytes = config.args.record_bytes;
    let size = record_bytes.saturating_sub(8);
    let shape = if config.args.random_payload {
        0
    } else if config.args.json_payload {
        1
    } else {
        2
    };
    POOLS.with(|pools| {
        let mut pools = pools.borrow_mut();
        let index = if let Some(index) = pools.iter().position(|pool| {
            pool.lane == lane && pool.record_bytes == record_bytes && pool.shape == shape
        }) {
            index
        } else {
            let mut pool = Vec::with_capacity(bodies as usize * size);
            for body in 0..bodies {
                append_body(&mut pool, config, config::record_number(lane, body)?);
            }
            pools.push(Pool {
                lane,
                record_bytes,
                shape,
                bytes: pool,
            });
            pools.len() - 1
        };
        let offset = body as usize * size;
        bytes.extend_from_slice(&pools[index].bytes[offset..offset + size]);
        Ok(())
    })
}

/// Append the record bytes after the 8-byte clock for one body number.
fn append_body(body: &mut Vec<u8>, config: &Config, number: u64) {
    let size = config.args.record_bytes.saturating_sub(8);
    let end = body.len() + size;
    if config.args.random_payload {
        let mut state = number;
        while body.len() < end {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut value = state;
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^= value >> 31;
            let remaining = (end - body.len()).min(8);
            body.extend_from_slice(&value.to_le_bytes()[..remaining]);
        }
    } else if config.args.json_payload {
        workload::append_json_record(body, size, number);
    } else {
        workload::append_record(body, size, number);
    }
    debug_assert_eq!(body.len(), end);
}

fn record_input(
    config: &Config,
    group: GroupId,
    lane: usize,
    sequence: u64,
    start: u64,
    scratch: &mut Vec<u8>,
) -> Result<RecordInput> {
    let id = config::message_id(group, lane, sequence);
    Ok(if config.args.record_bytes <= RecordInput::INLINE_BYTES {
        encode_payload(scratch, config, lane, sequence, start)?;
        RecordInput::copy_from_slice(id, scratch)
    } else {
        RecordInput::single(id, payload(config, lane, sequence, start)?)
    })
}

async fn run_lane(
    mut writer: Writer,
    config: &Config,
    group: GroupId,
    lane: usize,
    window: Window,
) -> Result<Value> {
    let mut counts = Counts::new(window);
    let mut check = writer.receipt_check();
    let number = writer.number();
    let mut payload_scratch = Vec::with_capacity(config.args.record_bytes);
    let mut pending: VecDeque<(PendingRecord, u64, u64)> =
        VecDeque::with_capacity(config.args.request_records);
    // Admission can stay immediately ready with the SDK on its own runtime.
    // Bound caller work so all timed writer lanes get polled before the deadline.
    let turn_records = (2 * 1024 * 1024 / config.args.record_bytes.max(1)).clamp(1, 256) as u64;
    let mut sequence = 0;
    let mut stage = "admission";
    let mut start = 0;
    let deadline = window.deadline(&config.args);
    tokio::time::timeout_at(deadline, async {
        loop {
            if config.args.scheduled() {
                stage = "scheduled admission and confirmations";
                check = paced::produce(
                    &mut writer,
                    config,
                    group,
                    lane,
                    window,
                    &mut counts,
                    &mut sequence,
                )
                .await?;
                break;
            }
            if sequence.is_multiple_of(CLOCK_QUANTUM) {
                start = metrics::monotonic_ns();
                if start >= window.end {
                    break;
                }
            }
            encode_payload(&mut payload_scratch, config, lane, sequence, start)?;
            let id = config::message_id(group, lane, sequence);
            // Small records take the inline path an application would use for
            // events it builds in place; larger ones own their buffer.
            let input = if payload_scratch.len() <= RecordInput::INLINE_BYTES {
                RecordInput::copy_from_slice(id, &payload_scratch)
            } else {
                RecordInput::single(id, Bytes::copy_from_slice(&payload_scratch))
            };
            counts.record_bytes(id, &payload_scratch);
            stage = "admission";
            let record = writer.send(input).await?;
            counts.submission(start, 1);
            pending.push_back((record, start, sequence));
            sequence += 1;
            drain_confirmed(&mut pending, &mut counts, config, group, lane, &mut check)?;
            if sequence.is_multiple_of(turn_records) {
                tokio::task::yield_now().await;
            }
        }
        stage = "final confirmations";
        while !pending.is_empty() {
            observe_front(&mut pending, &mut counts, config, group, lane, &mut check).await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await
    .map_err(|cause| {
        deadline_error(lane, stage, sequence, counts.total, &writer.stats(), &cause)
    })??;
    let stats = writer.stats();
    tokio::time::timeout_at(deadline, writer.close())
        .await
        .map_err(|cause| {
            deadline_error(lane, "writer close", sequence, counts.total, &stats, &cause)
        })??;
    if counts.total != sequence {
        return Err(error("incomplete writer cohort"));
    }
    if sequence == 0 && stats != WriterStats::default() {
        return Err(error("idle writer transmitted an APPEND"));
    }
    if sequence != 0
        && (stats.requests == 0
            || stats.max_records > config.writer_limits().max_records
            || !(1..=config.args.writer_inflight_appends).contains(&stats.max_inflight_appends))
    {
        return Err(error("native writer did not use bounded SDK APPENDs"));
    }
    let mut row = counts.report(lane);
    row["partition"] = json!(number);
    row["confirmed_partition_offset_range"] = json!(check.offset_range());
    row["protocol_requests"] = json!({
        "scope":"warmup, measurement and drain; includes retry transmissions; not confirmations",
        "requests":stats.requests, "records":stats.records, "payload_bytes":stats.payload_bytes,
        "max_inflight_appends":stats.max_inflight_appends,
        "min_records":stats.min_records, "max_records":stats.max_records,
        "min_payload_bytes":stats.min_payload_bytes, "max_payload_bytes":stats.max_payload_bytes,
        "record_count_bucket_upper_bounds":(0..31).map(|i|1_u64 << i).chain([u64::from(u32::MAX)]).collect::<Vec<_>>(),
        "record_count_buckets":stats.record_count_buckets,
    });
    Ok(row)
}

fn deadline_error(
    lane: usize,
    stage: &str,
    admitted: u64,
    confirmed: u64,
    stats: &WriterStats,
    cause: &tokio::time::error::Elapsed,
) -> Box<dyn std::error::Error + Send + Sync> {
    error(format!(
        "writer lane={lane} stage={stage} admitted={admitted} confirmed={confirmed} \
         unconfirmed={} socket_requests={} socket_records={} socket_payload_bytes={}: {cause}",
        admitted.saturating_sub(confirmed),
        stats.requests,
        stats.records,
        stats.payload_bytes,
    ))
}

fn drain_confirmed(
    pending: &mut VecDeque<(PendingRecord, u64, u64)>,
    counts: &mut Counts,
    config: &Config,
    group: GroupId,
    lane: usize,
    check: &mut ReceiptCheck,
) -> Result<()> {
    let mut finished = None;
    while let Some((record, start, sequence)) = pending.front() {
        let Some(receipt) = record.try_confirmed() else {
            break;
        };
        validate_receipt(&receipt?, *sequence, config, group, lane, check)?;
        let completed = *finished.get_or_insert_with(metrics::monotonic_ns);
        counts.complete(*start, completed, 1)?;
        pending.pop_front();
    }
    Ok(())
}

async fn observe_front(
    pending: &mut VecDeque<(PendingRecord, u64, u64)>,
    counts: &mut Counts,
    config: &Config,
    group: GroupId,
    lane: usize,
    check: &mut ReceiptCheck,
) -> Result<()> {
    let (record, start, sequence) = pending.front().expect("pending confirmation");
    let receipt = record.confirmed().await?;
    let completed = metrics::monotonic_ns();
    validate_receipt(&receipt, *sequence, config, group, lane, check)?;
    counts.complete(*start, completed, 1)?;
    pending.pop_front();
    drain_confirmed(pending, counts, config, group, lane, check)
}

fn validate_receipt(
    receipt: &ozzy_runtime::replicated::RecordReceipt,
    sequence: u64,
    config: &Config,
    group: GroupId,
    lane: usize,
    check: &mut ReceiptCheck,
) -> Result<()> {
    if !check.offset(receipt.offset, sequence)
        || receipt.message_id != config::message_id(group, lane, sequence)
        || receipt.partition != check.partition
        || receipt.owner_epoch != 1
        || receipt.key
            != (AppendKey {
                producer_id: lanes::producer(lane),
                producer_epoch: 1,
                first_sequence: sequence,
            })
        || receipt.policy != config.args.system.policy()
    {
        return Err(error("stream confirmation identity mismatch"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn native_and_external_inputs_share_exact_binary_and_json_payloads() {
        for (size, flag) in [
            ("16", "--binary-payload"),
            ("128", "--json-payload"),
            ("1024", "--json-payload"),
            ("8192", "--json-payload"),
        ] {
            let config = Config::new(super::super::super::Args::parse_from([
                "bench",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "10",
                "--record-bytes",
                size,
                flag,
            ]))
            .unwrap();
            let group = GroupId::new();
            let mut scratch = Vec::with_capacity(RecordInput::INLINE_BYTES);
            for lane in 0..4 {
                for sequence in [0, 129, u64::from(u32::MAX) + 1] {
                    let external = payload(&config, lane, sequence, 1234).unwrap();
                    let native =
                        record_input(&config, group, lane, sequence, 1234, &mut scratch).unwrap();
                    assert_eq!(external.len(), config.args.record_bytes);
                    assert_eq!(native.parts().next().unwrap(), external.as_ref());
                    assert_eq!(&external[..8], 1234_u64.to_be_bytes());
                    if config.args.binary_payload {
                        assert_eq!(external[10], lane as u8);
                    } else {
                        // The last event is cut at the record size; all others parse.
                        assert_eq!(external[8], b'{');
                        let text = std::str::from_utf8(&external[8..]).unwrap();
                        let events = text.split_inclusive('\n').filter(|e| e.ends_with('\n'));
                        assert_eq!(events.clone().count() > 0, config.args.record_bytes >= 1024);
                        for event in events {
                            serde_json::from_str::<Value>(event).unwrap();
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn bodies_repeat_only_after_the_pool_and_one_request() {
        for (size, flag) in [("16", "--binary-payload"), ("128", "--json-payload")] {
            let config = Config::new(super::super::super::Args::parse_from([
                "bench",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "10",
                "--record-bytes",
                size,
                flag,
            ]))
            .unwrap();
            let bodies = payload_bodies(&config);
            let body = config.args.record_bytes as u64 - 8;
            assert!(bodies * body >= PAYLOAD_POOL_BYTES as u64);
            assert!(bodies >= config.args.request_records as u64);
            let first = payload(&config, 1, 7, 1).unwrap();
            assert_eq!(first[8..], payload(&config, 1, 7 + bodies, 2).unwrap()[8..]);
            for other in [8, 7 + bodies - 1, 7 + config.args.request_records as u64] {
                assert_ne!(first[8..], payload(&config, 1, other, 1).unwrap()[8..]);
            }
        }
    }
}
