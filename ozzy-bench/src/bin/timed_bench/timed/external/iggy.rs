mod writer;

use super::{Config, Record, Result, Value, Window};
use iggy::prelude::{
    Client as _, ClusterClient, ClusterNodeStatus, CompressionAlgorithm, Consumer, Durability,
    Identifier, IggyByteSize, IggyExpiry, MaxTopicSize, MessageClient, Partitioning,
    PollingStrategy, StreamClient, TcpClient, TopicClient, TopicCreateOptions, UserClient,
};
use ozzy_proto::MessageId;

async fn connect(config: &Config) -> Result<TcpClient> {
    // The transport client owns its socket without the high-level wrapper's
    // detached heartbeat task. The test server disables heartbeat requirements.
    let client = TcpClient::from_connection_string(&format!(
        "iggy://iggy:iggy@{}",
        config.args.external_endpoint.as_deref().unwrap()
    ))?;
    client.connect().await?;
    Ok(client)
}

pub(super) async fn provision(config: &Config, name: &str) -> Result<Value> {
    let client = connect(config).await?;
    let expected = super::policy(config).brokers();
    loop {
        let metadata = client.get_cluster_metadata().await?;
        if metadata.nodes.len() != expected {
            return Err("Iggy broker count differs from requested topology".into());
        }
        if metadata
            .nodes
            .iter()
            .all(|node| node.status == ClusterNodeStatus::Healthy)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    client.create_stream(name).await?;
    let requested = TopicCreateOptions {
        partitions_count: Some(config.partitions() as u32),
        compression_algorithm: Some(CompressionAlgorithm::None),
        message_expiry: Some(IggyExpiry::NeverExpire),
        max_topic_size: Some(MaxTopicSize::Unlimited),
        segment_size: Some(IggyByteSize::from(
            config.args.segment_mib as u64 * 1024 * 1024,
        )),
        durability: if super::policy(config).durable() {
            Durability::Persisted
        } else {
            Durability::Replicated
        },
        preallocate_segments: Some(false),
        ..TopicCreateOptions::default()
    };
    let topic = client
        .create_topic(&Identifier::try_from(name)?, "records", &requested)
        .await?;
    let mut partition_ids = topic
        .partitions
        .iter()
        .map(|partition| partition.id)
        .collect::<Vec<_>>();
    partition_ids.sort_unstable();
    let effective = TopicCreateOptions::from_resource_options(&topic.options);
    if effective.durability != requested.durability
        || effective.segment_size != requested.segment_size
        || topic.partitions_count != config.partitions() as u32
        || topic.partitions.len() != config.partitions()
        || partition_ids != (0..config.partitions() as u32).collect::<Vec<_>>()
        || topic.compression_algorithm != CompressionAlgorithm::None
    {
        return Err("Iggy effective topic options differ from benchmark request".into());
    }
    let topics = client.get_topics(&Identifier::try_from(name)?).await?;
    if topics.len() != 1
        || topics[0].id != topic.id
        || topics[0].partitions_count != topic.partitions_count
    {
        return Err("Iggy metadata differs from requested topic topology".into());
    }
    let topology = super::topic_topology(topics.len(), topic.partitions.len());
    client.logout_user().await?;
    client.shutdown().await?;
    Ok(topology)
}

pub(super) async fn cleanup(config: &Config, name: &str) -> Result<()> {
    let client = connect(config).await?;
    client.delete_stream(&Identifier::try_from(name)?).await?;
    client.logout_user().await?;
    client.shutdown().await?;
    Ok(())
}

pub(super) struct Client {
    connection: TcpClient,
    stream: Identifier,
    topic: Identifier,
    partition: u32,
    reader_records: u32,
}

impl Client {
    pub(super) async fn connect(
        config: &Config,
        name: &str,
        lane: usize,
        reader: bool,
    ) -> Result<Self> {
        Ok(Self {
            connection: connect(config).await?,
            stream: Identifier::try_from(name)?,
            topic: Identifier::try_from("records")?,
            partition: if reader {
                lane
            } else {
                lane % config.partitions()
            } as u32,
            reader_records: config.reader_records() as u32,
        })
    }

    pub(super) async fn write(
        &self,
        config: &Config,
        lane: usize,
        window: Window,
    ) -> Result<Value> {
        writer::produce(config, lane, window, async |messages| {
            Ok(self
                .connection
                .send_messages(
                    &self.stream,
                    &self.topic,
                    &Partitioning::partition_id(self.partition),
                    messages,
                )
                .await?)
        })
        .await
    }

    pub(super) async fn close(&self) -> Result<()> {
        // Await metadata logout before closing transport. An abrupt socket
        // close otherwise schedules a detached logout on the server.
        self.connection.logout_user().await?;
        self.connection.shutdown().await?;
        Ok(())
    }

    pub(super) async fn read(&mut self, sequence: u64) -> Result<Vec<Record>> {
        let messages = self
            .connection
            .poll_messages(
                &self.stream,
                &self.topic,
                Some(self.partition),
                &Consumer::default(),
                &PollingStrategy::offset(sequence),
                self.reader_records,
                false,
            )
            .await?;
        Ok(messages
            .messages
            .into_iter()
            .map(|message| Record {
                offset: message.header.offset,
                id: MessageId::from_bytes(message.header.id.to_be_bytes()),
                payload: message.payload,
            })
            .collect())
    }
}
