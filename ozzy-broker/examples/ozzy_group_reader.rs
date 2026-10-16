//! Write a few records, join a consumer group on a running deployment, and
//! read until the group is deleted or Ctrl-C. A way to try the admin
//! console's Groups screen.
//!
//! ```sh
//! cargo run --release -p ozzy-broker --example ozzy_group_reader -- \
//!   --config ozzy.toml --broker laptop --identity shared.identity
//! ```

use clap::Parser;
use ozzy_broker::{CheckedConfig, check_config, host_resources, load_deployment};
use ozzy_proto::{
    ConsumerMemberId, EnvelopeLimits, MessageId, NodeId, data::DataLimits, handshake,
};
use ozzy_runtime::replicated::{
    AppendLinkLimits, BrokerLinks, BrokerLinksConfig, GroupReader, GroupReaderConfig,
    GroupReaderError, ReaderLinkLimits, RecordInput, RetryPolicy, SdkClock, SharedTopicWriter,
    SharedTopicWriterConfig, TopicRoutes, WriterRuntime,
};
use std::{error::Error, path::PathBuf, time::Duration};
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
    /// Records to write before reading.
    #[arg(long, default_value_t = 16)]
    records: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let deployment = load_deployment(&args.config)?;
    let checked = check_config(deployment, &args.identity, &args.broker, &host_resources()?)?;
    let topic = checked.deployment.deployment().topics.get(&args.topic);
    let partitions = topic.ok_or("no such topic")?.partitions as usize;
    let runtime = WriterRuntime::new()?;

    let config = writer_config(&checked, partitions)?;
    let links = BrokerLinks::connect(&runtime, config.clone()).await?;
    write(&links, &args).await?;
    links.shutdown().await?;

    let links = BrokerLinks::connect(&runtime, reader_config(config, partitions)?).await?;
    read(&links, &args).await?;
    links.shutdown().await?;
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

/// Write `args.records` records spread over the partitions by key.
async fn write(links: &BrokerLinks, args: &Args) -> Result<()> {
    let config = SharedTopicWriterConfig::new(LIMITS);
    let mut writer =
        SharedTopicWriter::open(links, &args.topic, config, RetryPolicy::default()).await?;
    for number in 0..args.records {
        let record = RecordInput::copy_from_slice(MessageId::new(), b"group reader example");
        let pending = writer.send(record, Some(&number.to_be_bytes())).await?;
        pending.confirmed().await?;
    }
    writer.close().await?;
    println!("wrote {} records to {}", args.records, args.topic);
    Ok(())
}

/// Join the group and acknowledge every record until the group is deleted
/// or Ctrl-C.
async fn read(links: &BrokerLinks, args: &Args) -> Result<()> {
    let member = ConsumerMemberId::new();
    let config = GroupReaderConfig::new(member);
    let mut reader = GroupReader::open(links.clone(), &args.topic, &args.group, config).await?;
    println!(
        "member {member} reads group {}/{}; delete the group or press Ctrl-C",
        args.topic, args.group
    );
    loop {
        let delivered = tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(reader.close().await?),
            delivered = reader.next() => delivered,
        };
        match delivered {
            Ok(delivered) => {
                let record = &delivered.record;
                println!(
                    "partition {} offset {}",
                    record.partition,
                    record.offset.get()
                );
                drop(reader.acknowledge(delivered.token)?);
            }
            Err(GroupReaderError::Deleted) => {
                println!("the group was deleted");
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        }
    }
}
