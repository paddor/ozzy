//! Topic readers share broker connections and retain per-writer workload rows.

use super::super::native::Setup;
use super::oracle::{Oracle, Record};
use super::{
    Config, GroupId, Result, Value, Window, error, launch, metrics, report, try_join_all, watch,
    workload,
};
use ozzy_runtime::{
    replicated::{BrokerLinks, TopicReader, TopicReaderConfig, TopicRecord, WriterRuntime},
    topic_metadata::TopicMetadata,
};
use serde_json::json;
use std::collections::BTreeMap;

pub(in crate::bench::timed) struct Task {
    reader: TopicReader,
    metadata: TopicMetadata,
    partitions: Vec<u32>,
    copy: usize,
}

pub(super) async fn run(
    config: Config,
    index: usize,
    setup: &Value,
    mut input: tokio::sync::mpsc::Receiver<Result<Value>>,
) -> Result<()> {
    let runtime = super::super::native::sdk_runtime(&config)?;
    let (tasks, links) = connect(&runtime, &config, setup, index).await?;
    launch::reply(&json!({"event":"connected", "index":index, "pid":std::process::id()}))?;
    let start = super::command(&mut input, "start").await?;
    let window = super::super::window(&start, &config.args)?;
    let group = super::config::group(&config.args)?;
    window.wait().await;
    let meter = metrics::Meter::start(!config.args.inproc);
    let (finish, finished) = watch::channel(None);
    let work = try_join_all(
        tasks
            .into_iter()
            .map(|task| run_task(task, &config, group, window, finished.clone())),
    );
    let completion = super::final_counts(&mut input, finish, &config, window);
    let (reports, ()) = tokio::try_join!(work, completion)?;
    let usage = meter.finish();
    links.shutdown().await?;
    let mut lanes = Vec::new();
    let mut readers = Vec::new();
    for (rows, detail) in reports {
        lanes.extend(rows);
        readers.push(detail);
    }
    report(
        &config,
        &json!({"lanes":lanes, "topic_readers":readers, "usage":usage,
        "index":index, "pid":std::process::id()}),
    )?;
    if input.recv().await.is_some() {
        return Err(error("unexpected reader command"));
    }
    Ok(())
}

pub(in crate::bench::timed) async fn connect(
    runtime: &WriterRuntime,
    config: &Config,
    value: &Value,
    index: usize,
) -> Result<(Vec<Task>, BrokerLinks)> {
    let setup = Setup::parse(config, value)?;
    let copies = config.args.readers_per_partition;
    let slots = setup
        .partitions
        .checked_mul(copies)
        .ok_or_else(|| error("reader slot overflow"))?;
    let assigned = workload::worker_share(slots, config.args.reader_workers(), index)?;
    if assigned.is_empty() {
        return Err(error("reader worker has no assigned partitions"));
    }
    let links = setup
        .reader_links(runtime, config, &value["reader"])
        .await?;
    let metadata = links.topic(&setup.topic).await?;
    if metadata.partition_count() != setup.partitions
        || metadata.policy() != config.args.system.policy()
    {
        return Err(error("native topic policy or partition count mismatch"));
    }
    let mut groups: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
    for slot in assigned {
        groups
            .entry(slot % copies)
            .or_default()
            .push(u32::try_from(slot / copies)?);
    }
    let mut tasks = Vec::new();
    for (copy, partitions) in groups {
        let reader = TopicReader::open(
            links.clone(),
            &setup.topic,
            TopicReaderConfig {
                partitions: Some(partitions.clone()),
                ..TopicReaderConfig::default()
            },
        )
        .await?;
        tasks.push(Task {
            reader,
            metadata: metadata.clone(),
            partitions,
            copy,
        });
    }
    Ok((tasks, links))
}

pub(in crate::bench::timed) async fn run_task(
    task: Task,
    config: &Config,
    group: GroupId,
    window: Window,
    mut finished: watch::Receiver<Option<Vec<u64>>>,
) -> Result<(Vec<Value>, Value)> {
    let Task {
        mut reader,
        metadata,
        partitions,
        copy,
    } = task;
    let yield_mask = yield_mask(config.args.record_bytes);
    let count = metadata.partition_count();
    let mut verifier = Verifier {
        oracle: Oracle::new(config, count, &partitions, copy, window)?,
        config,
        group,
        metadata: &metadata,
        // A batch never crosses more than one yield point.
        batch: CLOCK_BATCH.min(yield_mask + 1),
    };
    let mut delivered = 0_u64;
    let mut stage = "records";
    tokio::time::timeout_at(window.deadline(&config.args), async {
        // Borrowing the watch takes its lock, so read the final counts only
        // when they change, not per record.
        let mut target = finished.borrow_and_update().clone();
        loop {
            if let Some(target) = &target
                && verifier.oracle.complete(target)?
            {
                break;
            }
            let record = tokio::select! {
                biased;
                record = reader.next() => record?,
                changed = finished.changed(), if target.is_none() => {
                    changed.map_err(|_| error("final reader counts unavailable"))?;
                    target.clone_from(&finished.borrow_and_update());
                    continue;
                }
            };
            let before = delivered;
            verifier.ready(&mut reader, &record, &mut delivered)?;
            if (before ^ delivered) & !yield_mask != 0 {
                tokio::task::yield_now().await;
            }
        }
        stage = "close";
        reader.close().await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await
    .map_err(|cause| {
        error(format!(
            "topic reader stage={stage} copy={copy} delivered={delivered} pending={:?} stats={:?}: {cause}",
            verifier.oracle.pending(finished.borrow().as_deref()), reader.stats(),
        ))
    })??;
    verify_drained(&reader, &verifier.oracle, delivered)?;
    let stats = reader.stats();
    let detail = json!({"copy":copy, "partitions":partitions, "delivered":delivered,
        "live_records":stats.live_records, "replayed_records":stats.replayed_records, "live_gaps":stats.live_gaps,
        "positions":verifier.oracle.positions()});
    Ok((verifier.oracle.rows(), detail))
}

/// Most ready records verified before one shared completion clock read.
const CLOCK_BATCH: u64 = 64;

/// One task's independent record checks.
struct Verifier<'a> {
    oracle: Oracle,
    config: &'a Config,
    group: GroupId,
    metadata: &'a TopicMetadata,
    /// Most ready records that share one completion clock read.
    batch: u64,
}

impl Verifier<'_> {
    fn verify(&mut self, record: &TopicRecord) -> Result<()> {
        let record = Record::delivered(self.metadata, record)?;
        self.oracle.observe_untimed(self.config, self.group, record)
    }

    /// Verify `first` and the records ready after it, at most `batch` in all.
    /// They share one clock read taken after the last of them, so no latency
    /// is under-reported. `delivered` counts each verified record.
    fn ready(
        &mut self,
        reader: &mut TopicReader,
        first: &TopicRecord,
        delivered: &mut u64,
    ) -> Result<()> {
        let end = *delivered + self.batch;
        self.verify(first)?;
        *delivered += 1;
        while *delivered < end
            && let Some(record) = reader.try_next()?
        {
            self.verify(&record)?;
            *delivered += 1;
        }
        self.oracle.flush(metrics::monotonic_ns())
    }
}

/// Records between yields, less one. A power of two, so the per-record check
/// is a mask, not a division.
fn yield_mask(record_bytes: usize) -> u64 {
    (2 * 1024 * 1024 / record_bytes)
        .clamp(1, 256)
        .next_power_of_two() as u64
        - 1
}

/// The closed reader's checkpoint and transport counts match what the
/// oracle verified independently.
fn verify_drained(reader: &TopicReader, oracle: &Oracle, delivered: u64) -> Result<()> {
    let checkpoint = reader.checkpoint();
    if checkpoint
        .positions
        .iter()
        .map(|&(n, offset)| (n, offset.get()))
        .collect::<Vec<_>>()
        != oracle.positions()
    {
        return Err(error(
            "reader checkpoint differs from independently verified offsets",
        ));
    }
    let stats = reader.stats();
    if stats.live_records.checked_add(stats.replayed_records) != Some(delivered) {
        return Err(error(
            "reader transport counts differ from verified records",
        ));
    }
    Ok(())
}
