//! Identity-only attachment, completed before caller admission.

use super::{
    BrokerLinks, Partition, Partitions, ProducerIdentity, RetryPolicy, SharedTopicWriter,
    SharedTopicWriterConfig, TopicWriterError, Writer, session,
};

impl SharedTopicWriter {
    /// Attach using broker-owned epochs and sequences. The previous process
    /// must be gone. Pending records from that process are not reconstructed.
    pub async fn resume(
        links: &BrokerLinks,
        name: &str,
        identity: ProducerIdentity,
        config: SharedTopicWriterConfig,
        retry: RetryPolicy,
    ) -> Result<Self, TopicWriterError> {
        Self::attach(links, name, identity, config, retry, false).await
    }

    /// Fence every partition before admitting records. A failed attempt may
    /// already have fenced some partitions; another takeover is safe.
    pub async fn takeover(
        links: &BrokerLinks,
        name: &str,
        identity: ProducerIdentity,
        config: SharedTopicWriterConfig,
        retry: RetryPolicy,
    ) -> Result<Self, TopicWriterError> {
        Self::attach(links, name, identity, config, retry, true).await
    }

    async fn attach(
        links: &BrokerLinks,
        name: &str,
        identity: ProducerIdentity,
        config: SharedTopicWriterConfig,
        retry: RetryPolicy,
        takeover: bool,
    ) -> Result<Self, TopicWriterError> {
        retry
            .validate()
            .map_err(|_| TopicWriterError::Configuration)?;
        ProducerIdentity::from_bytes(identity.to_bytes())?;
        let metadata = links.topic(name).await?;
        if metadata.id() != identity.topic {
            return Err(TopicWriterError::Configuration);
        }
        config
            .writer(
                metadata
                    .partition(0)
                    .ok_or(TopicWriterError::Configuration)?
                    .incarnation,
                identity.producer,
                1,
                0,
                metadata.policy(),
            )
            .validate_shared()?;
        let routes = links.routes(metadata)?;
        let mut entries = Vec::new();
        for number in 0..routes.metadata().partition_count() {
            let number = u32::try_from(number).map_err(|_| TopicWriterError::Configuration)?;
            let opened =
                session::resolve(&routes, number, identity.producer, takeover, retry).await?;
            let writer = Writer::connect_shared(
                &routes,
                number,
                config.writer(
                    opened.partition,
                    identity.producer,
                    opened.epoch,
                    opened.next_sequence,
                    routes.metadata().policy(),
                ),
                retry,
            )
            .await?;
            entries.push(Partition {
                target: opened.partition,
                writer,
            });
        }
        let seed = routes.metadata().partitioner_seed();
        Ok(Self {
            routes,
            producer: identity.producer,
            state: Partitions::new(entries, seed),
        })
    }
}
