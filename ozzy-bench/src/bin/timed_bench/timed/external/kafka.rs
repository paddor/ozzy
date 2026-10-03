mod writer;
use super::{Config, Record, Result, Value, Window, error};
use bytes::Bytes;
use ozzy_proto::MessageId;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

/// librdkafka's ready receive/delivery futures do not consume Tokio's task
/// budget. Bound each turn so one lane cannot starve other lanes or control.
pub(super) async fn cooperate(records: u64) {
    if records.is_multiple_of(1024) {
        tokio::task::yield_now().await;
    }
}

fn settings(config: &Config) -> ClientConfig {
    let mut settings = ClientConfig::new();
    settings.set(
        "bootstrap.servers",
        config.args.external_endpoint.as_deref().unwrap(),
    );
    settings
}

pub(super) async fn provision(config: &Config, name: &str) -> Result<Value> {
    let admin: AdminClient<DefaultClientContext> = settings(config).create()?;
    let segment_bytes = (config.args.segment_mib as u64 * 1024 * 1024).to_string();
    let topic = topic(config, name, &segment_bytes)?;
    let topics = admin.create_topics(&[topic], &AdminOptions::new()).await?;
    if topics.len() != 1 || topics[0].is_err() {
        return Err(error(format!("Kafka topic creation failed: {topics:?}")));
    }
    let metadata = admin
        .inner()
        .fetch_metadata(Some(name), std::time::Duration::from_secs(10))?;
    let copies = replicas(config);
    let actual_topic = metadata
        .topics()
        .first()
        .ok_or_else(|| error("external topic missing from metadata"))?;
    let mut partition_ids = actual_topic
        .partitions()
        .iter()
        .map(rdkafka::metadata::MetadataPartition::id)
        .collect::<Vec<_>>();
    partition_ids.sort_unstable();
    if metadata.brokers().len() != copies
        || metadata.topics().len() != 1
        || actual_topic.partitions().len() != config.partitions()
        || partition_ids != (0..i32::try_from(config.partitions())?).collect::<Vec<_>>()
        || actual_topic
            .partitions()
            .iter()
            .any(|p| p.replicas().len() != copies)
    {
        return Err(error(
            "external topic must have one copy per broker and the requested partitions",
        ));
    }
    {
        let resources = admin
            .describe_configs(&[ResourceSpecifier::Topic(name)], &AdminOptions::new())
            .await?;
        let [Ok(resource)] = resources.as_slice() else {
            return Err(error(format!(
                "external topic configuration unavailable: {resources:?}"
            )));
        };
        if resource
            .get("segment.bytes")
            .and_then(|entry| entry.value.as_deref())
            != Some(segment_bytes.as_str())
        {
            return Err(error(
                "external segment.bytes does not match requested size",
            ));
        }
        if let Some(expected) = write_caching(config)
            && resource
                .get("write.caching")
                .and_then(|entry| entry.value.as_deref())
                != Some(expected)
        {
            return Err(error(
                "Redpanda write.caching does not match confirmation policy",
            ));
        }
    }
    Ok(super::topic_topology(
        metadata.topics().len(),
        metadata.topics()[0].partitions().len(),
    ))
}

fn topic<'a>(config: &Config, name: &'a str, segment_bytes: &'a str) -> Result<NewTopic<'a>> {
    let copies = replicas(config);
    let mut topic = NewTopic::new(
        name,
        i32::try_from(config.partitions())?,
        TopicReplication::Fixed(i32::try_from(copies)?),
    )
    .set("compression.type", "uncompressed")
    .set("cleanup.policy", "delete")
    .set("retention.ms", "-1")
    .set("retention.bytes", "-1")
    .set("segment.bytes", segment_bytes)
    .set("min.insync.replicas", if copies == 1 { "1" } else { "2" });
    // A 4 MiB SDK batch must fit the topic's encoded-batch ceiling.
    topic = topic.set("max.message.bytes", "8388608");
    if let Some(value) = write_caching(config) {
        topic = topic.set("write.caching", value);
    }
    Ok(topic)
}

/// Three copies for group policies, one broker each; one for local policies.
pub(super) fn replicas(config: &Config) -> usize {
    super::policy(config).brokers()
}

/// Disk policies wait for fsync; the others confirm from memory.
pub(super) fn write_caching(config: &Config) -> Option<&'static str> {
    (config.args.external_system == Some(super::System::Redpanda)).then_some(
        if super::policy(config).durable() {
            "false"
        } else {
            "true"
        },
    )
}

pub(super) async fn cleanup(config: &Config, name: &str) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = settings(config).create()?;
    let deleted = admin.delete_topics(&[name], &AdminOptions::new()).await?;
    if deleted.len() != 1 || deleted[0].is_err() {
        return Err(error(format!(
            "Kafka test-topic cleanup failed: {deleted:?}"
        )));
    }
    Ok(())
}

pub(super) struct Client {
    writer: Option<FutureProducer>,
    reader: Option<StreamConsumer>,
    topic: String,
    partition: i32,
}

impl Client {
    pub(super) fn connect(config: &Config, name: &str, lane: usize, reader: bool) -> Result<Self> {
        let mut settings = settings(config);
        let partition = i32::try_from(if reader {
            lane
        } else {
            lane % config.partitions()
        })?;
        let (writer, reader) = if reader {
            let consumer: StreamConsumer = settings
                .set("group.id", format!("{name}-{lane}"))
                .set("enable.auto.commit", "false")
                .set("enable.auto.offset.store", "false")
                .set("fetch.min.bytes", "1")
                .set("fetch.wait.max.ms", "10")
                .set("max.partition.fetch.bytes", "8388608")
                // The SDK default pauses a full second after its local queue
                // fills, even when the reader has since drained that queue.
                .set("fetch.queue.backoff.ms", "10")
                .set("auto.offset.reset", "error")
                .create()?;
            let mut partitions = TopicPartitionList::new();
            partitions.add_partition_offset(name, partition, Offset::Beginning)?;
            consumer.assign(&partitions)?;
            (None, Some(consumer))
        } else {
            (
                Some(
                    settings
                        .set("acks", "all")
                        .set("enable.idempotence", "true")
                        .set("compression.type", "none")
                        .set("linger.ms", "0")
                        .set(
                            "batch.num.messages",
                            config.args.request_records.to_string(),
                        )
                        .set("batch.size", "4194304")
                        .set("message.max.bytes", "8388608")
                        // SDK queue accounting can outlive delivery callbacks.
                        // The delivery queue independently enforces the exact
                        // per-writer unconfirmed-record window.
                        .set(
                            "queue.buffering.max.messages",
                            (config.args.request_records * 2).to_string(),
                        )
                        .set("delivery.timeout.ms", "10000")
                        .create()?,
                ),
                None,
            )
        };
        Ok(Self {
            writer,
            reader,
            topic: name.to_owned(),
            partition,
        })
    }

    pub(super) async fn write(
        &self,
        config: &Config,
        lane: usize,
        window: Window,
    ) -> Result<Value> {
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| error("missing Kafka producer"))?;
        writer::produce(config, lane, window, |id, payload, _sequence| {
            let future = writer
                .send_result(
                    FutureRecord::to(&self.topic)
                        .partition(self.partition)
                        .key(id.as_bytes().as_slice())
                        .payload(payload.as_ref()),
                )
                .map_err(|(cause, _)| cause)?;
            Ok(async move {
                let delivery = future.await?.map_err(|(cause, _)| cause)?;
                if delivery.partition != self.partition {
                    return Err(error("Kafka confirmation partition mismatch"));
                }
                Ok(u64::try_from(delivery.offset)?)
            })
        })
        .await
    }

    pub(super) async fn read(&mut self) -> Result<Vec<Record>> {
        let message = self
            .reader
            .as_ref()
            .ok_or_else(|| error("missing Kafka reader"))?
            .recv()
            .await?;
        if message.partition() != self.partition || message.topic() != self.topic {
            return Err(error("unexpected Kafka partition"));
        }
        Ok(vec![Record {
            offset: u64::try_from(message.offset())?,
            id: MessageId::from_bytes(
                message
                    .key()
                    .ok_or_else(|| error("missing Kafka record identity"))?
                    .try_into()?,
            ),
            payload: Bytes::copy_from_slice(
                message
                    .payload()
                    .ok_or_else(|| error("missing Kafka payload"))?,
            ),
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn redpanda_topics_explicitly_select_write_caching_without_changing_kafka() {
        use clap::Parser;
        for (external, policy, expected) in [
            ("kafka", "buffered", None),
            ("redpanda", "buffered", Some("true")),
            ("redpanda", "durable", Some("false")),
        ] {
            let config = Config::new(
                super::super::super::super::Args::try_parse_from([
                    "bench",
                    "--external-system",
                    external,
                    "--external-endpoint",
                    "127.0.0.1:19094",
                    "--external-policy",
                    policy,
                    "--network-ingress",
                    "--processes",
                    "--streaming",
                    "--duration",
                    "3",
                    "--segment-mib",
                    "1024",
                    "--history-mib",
                    "4096",
                ])
                .unwrap(),
            )
            .unwrap();
            let segment_bytes = (config.args.segment_mib as u64 * 1024 * 1024).to_string();
            let topic = topic(&config, "test", &segment_bytes).unwrap();
            assert!(topic.config.contains(&("segment.bytes", "1073741824")));
            assert_eq!(
                topic
                    .config
                    .iter()
                    .find(|(key, _)| *key == "write.caching")
                    .map(|(_, value)| *value),
                expected
            );
        }
    }

    #[tokio::test]
    async fn kafka_ready_records_cannot_starve_another_lane() {
        let peer_ran = Cell::new(false);
        let ready_records = async {
            for sequence in 1..=2048 {
                cooperate(sequence).await;
                if sequence == 1024 {
                    assert!(peer_ran.get(), "ready SDK futures starved the other lane");
                }
            }
        };
        let peer = async { peer_ran.set(true) };
        futures::future::join(ready_records, peer).await;
    }
}
