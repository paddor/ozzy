//! Write records at a steady rate while one consumer-group member reads and
//! acknowledges them, until Ctrl-C or the group is deleted. Gives the admin
//! console live rates, lag, and a group to look at. With `--due-secs`, each
//! record is due that long after writing, and the demo reports how late
//! records arrive after their due time.
//!
//! ```sh
//! cargo run --release -p ozzy-broker --example ozzy_demo -- \
//!   --config tmp/try/deployment.toml --broker local --identity tmp/try/shared.identity
//! ```

use clap::Parser;
use futures::{StreamExt, stream::FuturesUnordered};
use ozzy_broker::{CheckedConfig, check_config, host_resources, load_deployment};
use ozzy_proto::{
    ConsumerMemberId, EnvelopeLimits, MessageId, NodeId,
    data::{DataLimits, RecordPayload},
    handshake,
};
use ozzy_runtime::replicated::{
    AppendLinkLimits, BrokerLinks, BrokerLinksConfig, GroupReader, GroupReaderConfig,
    GroupReaderError, ReaderLinkLimits, RecordInput, RetryPolicy, SdkClock, SharedTopicWriter,
    SharedTopicWriterConfig, TopicRoutes, WriterRuntime,
};
use std::{
    error::Error,
    path::PathBuf,
    pin::pin,
    time::{Duration, SystemTime},
};
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const LIMITS: DataLimits = DataLimits {
    envelope: EnvelopeLimits {
        max_metadata_bytes: 16 * 1024,
        max_payload_bytes: 4096,
    },
    max_records: 4,
    max_parts: 4,
    max_record_bytes: 1024,
};

#[derive(Debug, Parser)]
struct Args {
    /// Deployment config.
    #[arg(long)]
    config: PathBuf,
    /// A broker in `--config` whose plan fits this host.
    #[arg(long)]
    broker: String,
    /// Shared identity file.
    #[arg(long)]
    identity: PathBuf,
    #[arg(long, default_value = "orders")]
    topic: String,
    #[arg(long, default_value = "workers")]
    group: String,
    /// Records written per second.
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=100_000))]
    rate: u32,
    /// Stop writing after this many records; 0 writes until stopped.
    #[arg(long, default_value_t = 0)]
    records: u64,
    /// Make each record due this many seconds after writing it. The topic
    /// needs `[topics.NAME.delay]`, like quickstart's `delayed`.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    due_secs: Option<u64>,
}

/// Records awaiting confirmation at once. Pacing stops while this many wait.
const IN_FLIGHT: usize = 256;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let deployment = load_deployment(&args.config)?;
    let checked = check_config(deployment, &args.identity, &args.broker, &host_resources()?)?;
    let topic = checked.deployment.deployment().topics.get(&args.topic);
    let topic = topic.ok_or("no such topic")?;
    if let Some(due) = args.due_secs {
        let delay = topic
            .delay
            .ok_or("--due-secs needs a topic with [topics.NAME.delay], like `delayed`")?;
        if due > delay.max_secs {
            return Err(format!(
                "--due-secs is above the topic's max_secs {}",
                delay.max_secs
            )
            .into());
        }
    }
    let partitions = topic.partitions as usize;
    let runtime = WriterRuntime::new()?;

    let config = writer_config(&checked, partitions)?;
    let writer_links = BrokerLinks::connect(&runtime, config.clone()).await?;
    let reader_links = BrokerLinks::connect(&runtime, reader_config(config, partitions)?).await?;
    let config = SharedTopicWriterConfig::new(LIMITS);
    let writer =
        SharedTopicWriter::open(&writer_links, &args.topic, config, RetryPolicy::default()).await?;
    let member = ConsumerMemberId::new();
    let config = GroupReaderConfig::new(member);
    let reader = GroupReader::open(reader_links.clone(), &args.topic, &args.group, config).await?;
    println!(
        "member {member} reads group {}/{}; delete the group or press Ctrl-C",
        args.topic, args.group
    );
    if let Some(due) = args.due_secs {
        println!("records are due {due} s after writing; late counts from the due time");
    }
    run(writer, reader, &args).await?;
    writer_links.shutdown().await?;
    reader_links.shutdown().await?;
    Ok(())
}

/// Links that write records to every configured broker.
fn writer_config(checked: &CheckedConfig, partitions: usize) -> Result<BrokerLinksConfig> {
    let mut parameters = handshake::Parameters::streaming(LIMITS, handshake::PRODUCER)?;
    parameters.capabilities |= handshake::OWNER_ROUTING;
    let mut config = BrokerLinksConfig {
        local: NodeId::from_bytes(*Uuid::now_v7().as_bytes()),
        brokers: checked.broker_addresses()?,
        parameters,
        requests: 12,
        control_bytes: 0,
        routing_bytes: TopicRoutes::reservation_bytes(partitions).ok_or("routing overflow")?,
        append: Some(append_limits(partitions)?),
        reader: None,
        maximum_partitions: partitions,
        request_timeout: Duration::from_secs(5),
        retry_interval: Duration::from_millis(100),
        clock: SdkClock::default(),
    };
    config.control_bytes = config
        .control_reservation_bytes()
        .ok_or("control overflow")?;
    Ok(config)
}

/// Room for one writer, which opens a writer on every partition.
fn append_limits(partitions: usize) -> Result<AppendLinkLimits> {
    let writer = SharedTopicWriterConfig::new(LIMITS);
    let reservation = writer
        .link_reservation(LIMITS.envelope.max_metadata_bytes)
        .ok_or("writer reservation overflow")?;
    let requests = writer.inflight_appends;
    let bytes = partitions
        .checked_mul(reservation.idle_bytes)
        .zip((requests + 1).checked_mul(reservation.request_bytes))
        .and_then(|(idle, sending)| idle.checked_add(sending))
        .ok_or("writer reservation overflow")?;
    Ok(AppendLinkLimits {
        writers: partitions,
        requests,
        records: requests * LIMITS.max_records,
        bytes,
    })
}

/// The writer's links turned into one group member's, under a new client ID.
fn reader_config(mut config: BrokerLinksConfig, partitions: usize) -> Result<BrokerLinksConfig> {
    config.local = NodeId::from_bytes(*Uuid::now_v7().as_bytes());
    config.parameters = handshake::Parameters::group(LIMITS, handshake::CONSUMER)?;
    config.parameters.capabilities |= handshake::OWNER_ROUTING;
    config.append = None;
    config.reader = Some(ReaderLinkLimits {
        subscriptions: partitions,
        bytes: 16 << 20,
        queue_messages: 4,
    });
    config.control_bytes = config
        .control_reservation_bytes()
        .ok_or("control overflow")?;
    Ok(config)
}

/// Records written, confirmed, and read so far.
#[derive(Debug, Default)]
struct Totals {
    written: u64,
    confirmed: u64,
    read: u64,
}

impl std::fmt::Display for Totals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            written,
            confirmed,
            read,
        } = self;
        write!(f, "written {written}, confirmed {confirmed}, read {read}")
    }
}

/// How late records arrived after their due time since the last report, in
/// milliseconds.
#[derive(Debug, Default)]
struct Lateness(Vec<u64>);

impl Lateness {
    /// Count a record whose payload is its due time; skip any other.
    fn arrived(&mut self, payload: &RecordPayload) -> Result<()> {
        if let [part] = payload.as_slice()
            && let Ok(due) = <[u8; 8]>::try_from(part.as_ref())
        {
            self.0
                .push(unix_millis()?.saturating_sub(u64::from_be_bytes(due)));
        }
        Ok(())
    }

    /// Percentiles of the arrivals since the last report, then forget them.
    fn report(&mut self) -> String {
        if self.0.is_empty() {
            return String::new();
        }
        self.0.sort_unstable();
        let at = |q: usize| self.0[(self.0.len() - 1) * q / 100];
        let line = format!(
            "; late p50 {} ms, p99 {} ms, max {} ms",
            at(50),
            at(99),
            at(100)
        );
        self.0.clear();
        line
    }
}

fn unix_millis() -> Result<u64> {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    Ok(u64::try_from(now.as_millis())?)
}

/// A plain record, or one due `due_secs` from now carrying its due time as
/// its payload.
fn record(due_secs: Option<u64>) -> Result<RecordInput> {
    let Some(secs) = due_secs else {
        return Ok(RecordInput::copy_from_slice(
            MessageId::new(),
            b"ozzy demo record",
        ));
    };
    let due = unix_millis()? + secs * 1000;
    Ok(RecordInput::copy_from_slice(MessageId::new(), &due.to_be_bytes()).with_due_millis(due))
}

/// Write at `args.rate` and read the group in one loop. Prints totals once a
/// second.
async fn run(mut writer: SharedTopicWriter, mut reader: GroupReader, args: &Args) -> Result<()> {
    let mut pace = tokio::time::interval(Duration::from_secs(1) / args.rate);
    pace.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut report = tokio::time::interval(Duration::from_secs(1));
    let mut stop = pin!(tokio::signal::ctrl_c());
    let mut pending = FuturesUnordered::new();
    let mut totals = Totals::default();
    let mut lateness = Lateness::default();
    let reader = loop {
        let writing = args.records == 0 || totals.written < args.records;
        tokio::select! {
            stopped = &mut stop => {
                stopped?;
                break Some(reader);
            }
            _ = pace.tick(), if writing && pending.len() < IN_FLIGHT => {
                let record = record(args.due_secs)?;
                let sent = writer.send(record, Some(&totals.written.to_be_bytes())).await?;
                pending.push(async move { sent.confirmed().await });
                totals.written += 1;
            }
            Some(receipt) = pending.next() => {
                receipt?;
                totals.confirmed += 1;
            }
            delivered = reader.next() => match delivered {
                Ok(delivered) => {
                    if args.due_secs.is_some() {
                        lateness.arrived(&delivered.record.payload)?;
                    }
                    drop(reader.acknowledge(delivered.token)?);
                    totals.read += 1;
                }
                Err(GroupReaderError::Deleted) => {
                    println!("the group was deleted");
                    break None;
                }
                Err(error) => return Err(error.into()),
            },
            _ = report.tick() => println!("{totals}{}", lateness.report()),
        }
    };
    // Bounded, so a stopped broker cannot hang the exit.
    let drain = async {
        while let Some(receipt) = pending.next().await {
            receipt?;
            totals.confirmed += 1;
        }
        writer.close().await?;
        if let Some(reader) = reader {
            reader.close().await?;
        }
        Ok::<_, Box<dyn Error>>(())
    };
    match tokio::time::timeout(Duration::from_secs(5), drain).await {
        Ok(drained) => drained?,
        Err(_) => println!("gave up waiting for confirmations"),
    }
    println!("{totals}");
    Ok(())
}
