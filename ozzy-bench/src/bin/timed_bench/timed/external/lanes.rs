//! An external worker process: one client per lane from the shared start.

use super::{
    Config, ObservedRecord, Oracle, Result, System, Value, Window, await_connect, command, config,
    error, iggy, kafka, launch, metrics, producer, report, system, workload,
};
use futures::future::try_join_all;
use serde_json::json;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// Pause after an empty external read.
const EMPTY_READ_PAUSE: Duration = Duration::from_millis(1);

pub(in crate::bench::timed) async fn worker(config: Config) -> Result<()> {
    let share = Share::of(&config)?;
    let mut input = launch::input()?;
    let connect = await_connect(&mut input).await?;
    let name = connect["name"]
        .as_str()
        .ok_or_else(|| error("missing external topic"))?;
    let mut clients = connect_clients(&config, name, &share).await?;
    launch::reply(&json!({"event":"connected","index":share.index,"pid":std::process::id()}))?;
    let start = command(&mut input, "start").await?;
    let window = super::super::window(&start, &config.args)?;
    window.wait().await;
    let meter = metrics::Meter::start(!config.args.inproc);
    let rows = run_lanes(&config, &share, &mut clients, window, &mut input).await?;
    report(
        &config,
        &json!({"lanes":rows,"usage":meter.finish(),"index":share.index,"pid":std::process::id()}),
    )?;
    if input.recv().await.is_some() {
        return Err(error("unexpected external worker command"));
    }
    // Parent stops workers one at a time after every reader has verified its
    // complete stream. Keep connections alive until that untimed teardown.
    for client in &clients {
        if let Client::Iggy(client) = client {
            client.close().await?;
        }
    }
    Ok(())
}

/// One external worker: its role, index, and the writer lanes or reader
/// partitions it serves.
struct Share {
    reader: bool,
    index: usize,
    lanes: std::ops::Range<usize>,
}

impl Share {
    fn of(config: &Config) -> Result<Self> {
        let reader = config.args.reader_worker.is_some();
        let index = config
            .args
            .reader_worker
            .or(config.args.producer_worker)
            .ok_or_else(|| error("external worker needs a reader or producer index"))?;
        let (count, workers) = if reader {
            (config.partitions(), config.args.reader_workers())
        } else {
            (config.writers, config.workers)
        };
        Ok(Self {
            reader,
            index,
            lanes: workload::worker_share(count, workers, index)?,
        })
    }
}

/// One client per lane; writer lanes also build their body pools.
async fn connect_clients(config: &Config, name: &str, share: &Share) -> Result<Vec<Client>> {
    let system = system(config)?;
    let mut clients = Vec::new();
    for lane in share.lanes.clone() {
        clients.push(match system {
            System::Kafka | System::Redpanda => {
                Client::Kafka(kafka::Client::connect(config, name, lane, share.reader)?)
            }
            System::Iggy => Client::Iggy(Box::new(
                iggy::Client::connect(config, name, lane, share.reader).await?,
            )),
        });
        if !share.reader {
            producer::build_payload_pool(config, lane)?;
        }
    }
    Ok(clients)
}

/// Run every lane from the shared start. Writers stop at the deadline;
/// readers once they cover the final writer counts the coordinator sends.
async fn run_lanes(
    config: &Config,
    share: &Share,
    clients: &mut [Client],
    window: Window,
    input: &mut mpsc::Receiver<Result<Value>>,
) -> Result<Vec<Value>> {
    let reader = share.reader;
    let (finish, finished) = watch::channel(None);
    let work = try_join_all(
        clients
            .iter_mut()
            .zip(share.lanes.clone())
            .map(|(client, lane)| {
                let finished = finished.clone();
                async move {
                    if reader {
                        return read(client, config, lane, window, finished).await;
                    }
                    let row = tokio::time::timeout_at(window.deadline(&config.args), async {
                        match client {
                            Client::Kafka(client) => client.write(config, lane, window).await,
                            Client::Iggy(client) => (*client).write(config, lane, window).await,
                        }
                    })
                    .await
                    .map_err(|cause| error(format!("external writer lane={lane}: {cause}")))??;
                    Ok(vec![row])
                }
            }),
    );
    let completion = async {
        if !reader {
            return Ok(());
        }
        let request = command(input, "finish").await?;
        let totals: Vec<u64> = serde_json::from_value(request["counts"].clone())?;
        if totals.len() != config.writers {
            return Err(error("invalid final writer counts"));
        }
        finish
            .send(Some(totals))
            .map_err(|_| error("external readers exited early"))?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (rows, ()) = tokio::try_join!(work, completion)?;
    Ok(rows.into_iter().flatten().collect())
}

enum Client {
    Kafka(kafka::Client),
    Iggy(Box<iggy::Client>),
}

async fn read(
    client: &mut Client,
    config: &Config,
    partition: usize,
    window: Window,
    mut finished: watch::Receiver<Option<Vec<u64>>>,
) -> Result<Vec<Value>> {
    let group = config::group(&config.args)?;
    let mut oracle = Oracle::new(config, config.partitions(), &[partition as u32], 0, window)?;
    let mut offset = 0;
    tokio::time::timeout_at(window.deadline(&config.args), async {
        loop {
            let target = finished.borrow().clone();
            if let Some(target) = &target
                && oracle.complete(target)?
            {
                break;
            }
            let records = match &mut *client {
                Client::Kafka(client) => tokio::select! {
                    records = client.read() => records?,
                    changed = finished.changed(), if target.is_none() => {
                        changed.map_err(|_| error("final counts unavailable"))?;
                        continue;
                    }
                },
                // An Iggy RPC is not canceled mid-frame when final counts arrive.
                Client::Iggy(client) => client.read(offset).await?,
            };
            if records.is_empty() {
                tokio::time::sleep(EMPTY_READ_PAUSE).await;
            }
            // One clock per poll reply, as in the native reader lane.
            let complete = metrics::monotonic_ns();
            for record in records {
                oracle.observe(
                    config,
                    group,
                    ObservedRecord {
                        partition: partition as u32,
                        offset: record.offset,
                        id: record.id,
                        payload: &record.payload,
                    },
                    complete,
                )?;
                offset += 1;
                if matches!(client, Client::Kafka(_)) {
                    kafka::cooperate(offset).await;
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await
    .map_err(|cause| {
        error(format!(
            "external reader partition={partition} verified={offset} pending={:?}: {cause}",
            oracle.pending(finished.borrow().as_deref()),
        ))
    })??;
    Ok(oracle.rows())
}
