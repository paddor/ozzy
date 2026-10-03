//! Topic readers share broker connections and retain per-writer workload rows.

use super::super::native::Setup;
use super::oracle::{Oracle, Record};
use super::{
    Config, GroupId, Result, Value, Window, error, launch, metrics, report, try_join_all, watch,
    workload,
};
use ozzy_runtime::{
    replicated::{BrokerLinks, TopicReader, TopicReaderConfig, WriterRuntime},
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
    let runtime = WriterRuntime::new()?;
    let (tasks, links) = connect(&runtime, &config, setup, index).await?;
    launch::reply(&json!({"event":"connected", "index":index, "pid":std::process::id()}))?;
    let start = super::command(&mut input, "start").await?;
    let window = super::super::window(&start, &config.args)?;
    let group = super::config::group(&config.args)?;
    window.wait().await;
    let meter = metrics::Meter::start();
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
    let mut oracle = Oracle::new(
        config,
        metadata.partition_count(),
        &partitions,
        copy,
        window,
    )?;
    let turn = (2 * 1024 * 1024 / config.args.record_bytes).clamp(1, 256) as u64;
    let mut delivered = 0_u64;
    let mut stage = "records";
    tokio::time::timeout_at(window.deadline(&config.args), async {
        loop {
            let target = finished.borrow().clone();
            if let Some(target) = &target
                && oracle.complete(target)?
            {
                break;
            }
            let record = tokio::select! {
                record = reader.next() => record?,
                changed = finished.changed(), if target.is_none() => {
                    changed.map_err(|_| error("final reader counts unavailable"))?;
                    continue;
                }
            };
            let expected = metadata.partition(record.partition)
                .ok_or_else(|| error("reader returned an unknown partition"))?;
            if record.topic != metadata.id() || record.incarnation != expected.incarnation {
                return Err(error("reader topic or partition identity mismatch"));
            }
            let [payload] = record.payload.as_slice() else {
                return Err(error("invalid record layout"));
            };
            oracle.observe(config, group, Record {
                partition: record.partition, offset: record.offset.get(),
                id: record.message_id, payload,
            }, metrics::monotonic_ns())?;
            delivered += 1;
            if delivered.is_multiple_of(turn) {
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
            oracle.pending(finished.borrow().as_deref()), reader.stats(),
        ))
    })??;
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
    let detail = json!({"copy":copy, "partitions":partitions, "delivered":delivered,
        "live_records":stats.live_records, "replayed_records":stats.replayed_records,
        "positions":oracle.positions()});
    Ok((oracle.rows(), detail))
}
