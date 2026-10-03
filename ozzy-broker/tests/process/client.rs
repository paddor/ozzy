//! Actual shared SDK with an independent record/offset oracle across process death.

use bytes::Bytes;
use ozzy_broker::CheckedConfig;
use ozzy_proto::{
    EnvelopeLimits, MessageId, NodeId, Offset, append::Policy, data::DataLimits, handshake,
};
use ozzy_runtime::replicated::{
    AppendLinkLimits, BrokerAddress, BrokerLinks, BrokerLinksConfig, ReaderLinkLimits, RecordInput,
    RetryPolicy, SdkClock, SharedTopicPendingRecord, SharedTopicWriter, SharedTopicWriterConfig,
    TopicCheckpoint, TopicReader, TopicReaderConfig, WriterRuntime,
};
use std::time::Duration;

pub(super) struct Client {
    pub(super) links: BrokerLinks,
    writer: SharedTopicWriter,
    keys: Vec<[u8; 4]>,
    history: Vec<Vec<(MessageId, Bytes)>>,
    next_sequences: Vec<u64>,
    policy: Policy,
}

pub(super) type Pending = (SharedTopicPendingRecord, MessageId, Bytes);

impl Client {
    pub(super) async fn open(checked: &CheckedConfig) -> Self {
        let topic = &checked.deployment.deployment().topics["orders"];
        let partitions = topic.partitions as usize;
        let policy = match topic.confirmation {
            ozzy_config::Confirmation::LocalDurable => Policy::LocalDurable,
            ozzy_config::Confirmation::DiskQuorum => Policy::QuorumDurable,
            ozzy_config::Confirmation::ReplicatedPersisting => Policy::QuorumReplicatedPersisting,
        };
        let limits = DataLimits {
            envelope: EnvelopeLimits {
                max_metadata_bytes: 16 * 1024,
                max_payload_bytes: 4096,
            },
            max_records: 4,
            max_parts: 4,
            max_record_bytes: 1024,
        };
        let mut parameters = handshake::Parameters::streaming(
            limits,
            handshake::PRODUCER | handshake::CONSUMER,
            64,
            64 * 1024,
        )
        .unwrap();
        parameters.capabilities |= handshake::OWNER_ROUTING | handshake::OWNER_READ;
        let runtime = WriterRuntime::new().unwrap();
        let links = BrokerLinks::connect(
            &runtime,
            BrokerLinksConfig {
                local: NodeId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
                brokers: checked
                    .identity
                    .brokers
                    .iter()
                    .map(|(name, id)| BrokerAddress {
                        node: NodeId::from_bytes(*id.as_bytes()),
                        endpoint: checked.deployment.deployment().brokers[name]
                            .endpoints
                            .peer
                            .parse()
                            .unwrap(),
                    })
                    .collect(),
                parameters,
                requests: 12,
                control_bytes: 1024 * 1024,
                routing_bytes: 1024 * 1024,
                append: Some(AppendLinkLimits {
                    writers: 32,
                    requests: 32,
                    records: 128,
                    bytes: 256 * 1024 * 1024,
                }),
                maximum_partitions: partitions,
                reader: Some(ReaderLinkLimits {
                    subscriptions: 8,
                    bytes: 16 * 1024 * 1024,
                    queue_messages: 4,
                }),
                request_timeout: Duration::from_secs(5),
                retry_interval: Duration::from_millis(10),
                clock: SdkClock::default(),
            },
        )
        .await
        .unwrap();
        let writer = SharedTopicWriter::open(
            &links,
            "orders",
            SharedTopicWriterConfig::new(limits),
            RetryPolicy::default(),
        )
        .await
        .unwrap();
        let keys = (0..topic.partitions)
            .map(|number| {
                (0_u32..1000)
                    .map(u32::to_be_bytes)
                    .find(|key| writer.metadata().keyed_partition(key).number == number)
                    .unwrap()
            })
            .collect();
        Self {
            links,
            writer,
            keys,
            history: vec![Vec::new(); partitions],
            next_sequences: vec![0; partitions],
            policy,
        }
    }

    pub(super) async fn queue(&mut self, wave: usize) -> Vec<Pending> {
        self.queue_selected(wave, None).await
    }

    pub(super) async fn queue_except(&mut self, wave: usize, partition: usize) -> Vec<Pending> {
        self.queue_selected(wave, Some(partition)).await
    }

    async fn queue_selected(&mut self, wave: usize, excluded: Option<usize>) -> Vec<Pending> {
        let mut pending = Vec::new();
        for (partition, key) in self.keys.iter().enumerate() {
            if excluded == Some(partition) {
                continue;
            }
            for record in 0..4 {
                let id = MessageId::from_bytes(
                    ((wave * 1000 + partition * 10 + record + 1) as u128).to_be_bytes(),
                );
                let body =
                    Bytes::from(format!("wave-{wave}-partition-{partition}-record-{record}"));
                let admitted = self
                    .writer
                    .send(RecordInput::copy_from_slice(id, &body), Some(key))
                    .await
                    .unwrap();
                assert_eq!(admitted.partition(), partition as u32);
                assert_eq!(admitted.sequence(), self.next_sequences[partition]);
                self.next_sequences[partition] += 1;
                pending.push((admitted, id, body));
            }
        }
        pending
    }

    pub(super) async fn queue_large(&mut self, wave: u32, records: u32) -> Vec<Pending> {
        let mut pending = Vec::new();
        for record in 0..records {
            let value = (1_u128 << 96) | (u128::from(wave) << 32) | u128::from(record);
            let id = MessageId::from_bytes(value.to_be_bytes());
            let mut state = (u64::from(wave) << 32) | (u64::from(record) + 1);
            let mut body = [0; 1024];
            for block in body.as_chunks_mut::<8>().0 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                block.copy_from_slice(&state.to_be_bytes());
            }
            let body = Bytes::copy_from_slice(&body);
            let admitted = self
                .writer
                .send(RecordInput::copy_from_slice(id, &body), Some(&self.keys[0]))
                .await
                .unwrap();
            assert_eq!(admitted.partition(), 0);
            assert_eq!(admitted.sequence(), self.next_sequences[0]);
            self.next_sequences[0] += 1;
            pending.push((admitted, id, body));
        }
        pending
    }

    pub(super) async fn confirm(&mut self, pending: Vec<Pending>) {
        for (pending, id, body) in pending {
            let receipt = pending.confirmed().await.unwrap();
            assert_eq!(receipt.record.policy, self.policy);
            assert_eq!(receipt.partition, pending.partition());
            assert_eq!(receipt.record.message_id, id);
            assert_eq!(receipt.record.key.producer_id, self.writer.producer());
            assert_eq!(receipt.record.key.first_sequence, pending.sequence());
            let history = &mut self.history[receipt.partition as usize];
            assert_eq!(receipt.record.offset, history.len() as u64);
            history.push((id, body));
        }
    }

    pub(super) async fn reader(&self, at_end: bool) -> TopicReader {
        TopicReader::open(
            self.links.clone(),
            "orders",
            TopicReaderConfig {
                checkpoint: at_end.then(|| TopicCheckpoint {
                    topic: self.writer.metadata().id(),
                    positions: self
                        .history
                        .iter()
                        .enumerate()
                        .map(|(n, history)| (n as u32, Offset::new(history.len() as u64)))
                        .collect(),
                }),
                ..TopicReaderConfig::default()
            },
        )
        .await
        .unwrap()
    }

    pub(super) fn positions(&self) -> Vec<usize> {
        self.history.iter().map(Vec::len).collect()
    }

    pub(super) async fn read(&self, reader: &mut TopicReader, mut positions: Vec<usize>) {
        let wanted = self
            .history
            .iter()
            .zip(&positions)
            .map(|(history, &next)| history.len() - next)
            .sum::<usize>();
        for _ in 0..wanted {
            let record = reader.next().await.unwrap();
            let partition = record.partition as usize;
            let next = positions[partition];
            assert_eq!(record.offset, Offset::new(next as u64));
            let (id, body) = &self.history[partition][next];
            assert_eq!(&record.message_id, id);
            assert_eq!(record.payload.as_slice(), std::slice::from_ref(body));
            positions[partition] += 1;
        }
        assert_eq!(positions, self.positions());
        for (number, next) in reader.checkpoint().positions {
            assert_eq!(next.get(), positions[number as usize] as u64);
        }
    }

    pub(super) async fn close(self) {
        self.writer.close().await.unwrap();
        self.links.shutdown().await.unwrap();
    }

    pub(super) async fn replay(&self) {
        let mut reader = self.reader(false).await;
        self.read(&mut reader, vec![0; self.history.len()]).await;
        reader.close().await.unwrap();
    }
}
