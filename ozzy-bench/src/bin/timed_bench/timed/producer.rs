use bytes::Bytes;
use futures::future::try_join_all;
use ozzy_proto::{GroupId, append::AppendKey};
use ozzy_runtime::replicated::{RecordInput, WriterStats};
use serde_json::{Value, json};
use std::collections::VecDeque;

use super::super::{Result, error, lanes, metrics, workload};
use super::{Config, Window, await_connect, command, config, launch, measurement::Counts, report};

mod client;
mod paced;
use client::{PendingRecord, ReceiptCheck, Writer};

/// Records sharing one clock reading in unpaced lanes.
pub(super) const CLOCK_QUANTUM: u64 = 64;

/// Bound immediately ready caller work by both count and payload bytes.
fn turn_records(config: &Config) -> u64 {
    (2 * 1024 * 1024 / config.args.record_bytes.max(1)).clamp(1, 256) as u64
}

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
    encode_payload(&mut Vec::new(), config, config.logical_lane(lane), 0, 0)
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
    let mut input = launch::input()?;
    let connect = await_connect(&mut input).await?;
    let context = super::native::sdk_runtime(&config)?;
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
    let meter = metrics::Meter::start(!config.args.inproc);
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

/// The stream one timed writer lane produces: its run settings, message
/// group, and lane number.
#[derive(Clone, Copy)]
struct Lane<'a> {
    config: &'a Config,
    group: GroupId,
    index: usize,
}

impl Lane<'_> {
    /// One record of this writer's logical feed lane, ready for the SDK: inline
    /// when small, owned otherwise. Redundant feed copies share its id and bytes.
    fn logical_input(
        self,
        sequence: u64,
        start: u64,
        scratch: &mut Vec<u8>,
    ) -> Result<RecordInput> {
        let lane = self.config.logical_lane(self.index);
        let id = config::message_id(self.group, lane, sequence);
        Ok(
            if self.config.args.record_bytes <= RecordInput::INLINE_BYTES {
                encode_payload(scratch, self.config, lane, sequence, start)?;
                RecordInput::copy_from_slice(id, scratch)
            } else {
                RecordInput::single(id, payload(self.config, lane, sequence, start)?)
            },
        )
    }

    /// Check one confirmation against what this lane sent as `sequence`.
    fn validate_receipt(
        self,
        receipt: &ozzy_runtime::replicated::RecordReceipt,
        sequence: u64,
        check: &mut ReceiptCheck,
    ) -> Result<()> {
        let key = AppendKey {
            producer_id: lanes::producer(self.index),
            producer_epoch: 1,
            first_sequence: sequence,
        };
        let id = config::message_id(self.group, self.config.logical_lane(self.index), sequence);
        if !check.offset(receipt.offset, sequence)
            || receipt.message_id != id
            || receipt.partition != check.partition
            || receipt.owner_epoch != 1
            || receipt.key != key
            || receipt.policy != self.config.args.system.policy()
        {
            return Err(error("stream confirmation identity mismatch"));
        }
        let disposition = match receipt.disposition {
            ozzy_runtime::replicated::AppendDisposition::Stored => 0,
            ozzy_runtime::replicated::AppendDisposition::Duplicate => 1,
            _ => return Err(error("stream confirmation disposition mismatch")),
        };
        check.dispositions[disposition] += 1;
        Ok(())
    }
}

/// A sent record awaiting confirmation, with the clock reading it carries.
struct Sent {
    record: PendingRecord,
    start: u64,
    sequence: u64,
}

/// One lane's confirmation bookkeeping while it runs.
struct LaneRun<'a> {
    lane: Lane<'a>,
    counts: Counts,
    check: ReceiptCheck,
    /// Records admitted so far, and so the next record's sequence.
    sequence: u64,
    /// What the lane waits for, named in deadline errors.
    stage: &'static str,
}

async fn run_lane(
    mut writer: Writer,
    config: &Config,
    group: GroupId,
    lane: usize,
    window: Window,
) -> Result<Value> {
    let mut run = LaneRun {
        lane: Lane {
            config,
            group,
            index: lane,
        },
        counts: Counts::new(window),
        check: writer.receipt_check(config.args.native.feeds.enabled()),
        sequence: 0,
        stage: "admission",
    };
    let deadline = window.deadline(&config.args);
    let produced = tokio::time::timeout_at(deadline, run.produce(&mut writer, window)).await;
    let stats = writer.stats()?;
    produced.map_err(|cause| run.deadline_error(run.stage, &stats, &cause))??;
    let partition = writer.number();
    tokio::time::timeout_at(deadline, writer.close())
        .await
        .map_err(|cause| run.deadline_error("writer close", &stats, &cause))??;
    run.check_requests(&stats)?;
    Ok(run.report(partition, &stats))
}

impl LaneRun<'_> {
    /// Admit records until the window ends, then wait for every confirmation.
    async fn produce(&mut self, writer: &mut Writer, window: Window) -> Result<()> {
        let config = self.lane.config;
        if config.args.scheduled() {
            self.stage = "scheduled admission and confirmations";
            return paced::produce(writer, self, window).await;
        }
        let mut scratch = Vec::with_capacity(config.args.record_bytes);
        let mut pending = VecDeque::with_capacity(config.args.request_records);
        // Admission can stay immediately ready with the SDK on its own runtime.
        // Bound caller work so all timed writer lanes get polled before the deadline.
        let turn_records = turn_records(config);
        let mut start = 0;
        loop {
            if self.sequence.is_multiple_of(CLOCK_QUANTUM) {
                start = metrics::monotonic_ns();
                if start >= window.end {
                    break;
                }
            }
            let input = self.next_raw_input(&mut scratch, start)?;
            self.stage = "admission";
            let record = writer.send(input).await?;
            self.counts.submission(start, 1);
            pending.push_back(Sent {
                record,
                start,
                sequence: self.sequence,
            });
            self.sequence += 1;
            self.drain_confirmed(&mut pending)?;
            if self.sequence.is_multiple_of(turn_records) {
                tokio::task::yield_now().await;
            }
        }
        self.stage = "final confirmations";
        while !pending.is_empty() {
            self.observe_front(&mut pending).await?;
        }
        Ok(())
    }

    /// Encode the next record of this writer's own lane, stamped `start`, and
    /// count its bytes.
    fn next_raw_input(&mut self, scratch: &mut Vec<u8>, start: u64) -> Result<RecordInput> {
        let Lane {
            config,
            group,
            index,
        } = self.lane;
        encode_payload(scratch, config, index, self.sequence, start)?;
        let id = config::message_id(group, index, self.sequence);
        // Small records take the inline path an application would use for
        // events it builds in place; larger ones own their buffer.
        let input = if scratch.len() <= RecordInput::INLINE_BYTES {
            RecordInput::copy_from_slice(id, scratch)
        } else {
            RecordInput::single(id, Bytes::copy_from_slice(scratch))
        };
        self.counts.record_bytes(id, scratch);
        Ok(input)
    }

    /// Complete every already confirmed record at the front, reading the
    /// clock once.
    fn drain_confirmed(&mut self, pending: &mut VecDeque<Sent>) -> Result<()> {
        let mut finished = None;
        while let Some(sent) = pending.front() {
            let Some(receipt) = sent.record.try_confirmed() else {
                break;
            };
            self.lane
                .validate_receipt(&receipt?, sent.sequence, &mut self.check)?;
            let completed = *finished.get_or_insert_with(metrics::monotonic_ns);
            self.counts.complete(sent.start, completed, 1)?;
            pending.pop_front();
        }
        Ok(())
    }

    /// Wait for the oldest confirmation, then complete the ready ones.
    async fn observe_front(&mut self, pending: &mut VecDeque<Sent>) -> Result<()> {
        let Some(sent) = pending.front() else {
            return Ok(());
        };
        let receipt = sent.record.confirmed().await?;
        let completed = metrics::monotonic_ns();
        self.lane
            .validate_receipt(&receipt, sent.sequence, &mut self.check)?;
        self.counts.complete(sent.start, completed, 1)?;
        pending.pop_front();
        self.drain_confirmed(pending)
    }

    /// Every admitted record was confirmed through bounded SDK APPENDs.
    fn check_requests(&self, stats: &WriterStats) -> Result<()> {
        let config = self.lane.config;
        if self.counts.total != self.sequence {
            return Err(error("incomplete writer cohort"));
        }
        if self.sequence == 0 && *stats != WriterStats::default() {
            return Err(error("idle writer transmitted an APPEND"));
        }
        let bounded = stats.requests != 0
            && stats.max_records <= config.writer_limits().max_records
            && (1..=config.args.writer_inflight_appends).contains(&stats.max_inflight_appends);
        if self.sequence != 0 && !bounded {
            return Err(error("native writer did not use bounded SDK APPENDs"));
        }
        Ok(())
    }

    fn report(&self, partition: u32, stats: &WriterStats) -> Value {
        let mut row = self.counts.report(self.lane.index);
        row["partition"] = json!(partition);
        row["confirmed_partition_offset_range"] = json!(self.check.offset_range());
        row["confirmed_dispositions"] =
            json!({"stored": self.check.dispositions[0], "duplicate": self.check.dispositions[1]});
        if self.lane.config.args.native.feeds.enabled() {
            row["confirmed_offset_digest_xxh3_128"] = json!(self.check.offset_digest());
        }
        row["protocol_requests"] = json!({
            "scope":"warmup, measurement and drain; includes retry transmissions; not confirmations",
            "requests":stats.requests, "records":stats.records, "payload_bytes":stats.payload_bytes,
            "max_inflight_appends":stats.max_inflight_appends,
            "min_records":stats.min_records, "max_records":stats.max_records,
            "min_payload_bytes":stats.min_payload_bytes, "max_payload_bytes":stats.max_payload_bytes,
            "record_count_bucket_upper_bounds":(0..31).map(|i|1_u64 << i).chain([u64::from(u32::MAX)]).collect::<Vec<_>>(),
            "record_count_buckets":stats.record_count_buckets,
        });
        row
    }

    fn deadline_error(
        &self,
        stage: &str,
        stats: &WriterStats,
        cause: &tokio::time::error::Elapsed,
    ) -> Box<dyn std::error::Error + Send + Sync> {
        let (admitted, confirmed) = (self.sequence, self.counts.total);
        error(format!(
            "writer lane={} stage={stage} admitted={admitted} confirmed={confirmed} \
             unconfirmed={} socket_requests={} socket_records={} socket_payload_bytes={}: {cause}",
            self.lane.index,
            admitted.saturating_sub(confirmed),
            stats.requests,
            stats.records,
            stats.payload_bytes,
        ))
    }
}

#[cfg(test)]
mod tests;
