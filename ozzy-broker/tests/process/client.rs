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

mod workload;

pub(crate) struct Client {
    pub(super) links: BrokerLinks,
    writer: SharedTopicWriter,
    keys: Vec<[u8; 4]>,
    history: Vec<Vec<(MessageId, Vec<Bytes>)>>,
    bases: Vec<usize>,
    next_sequences: Vec<u64>,
    policy: Policy,
}

pub(super) type Pending = (SharedTopicPendingRecord, MessageId, Vec<Bytes>);

impl Client {
    pub(crate) async fn open(checked: &CheckedConfig) -> Self {
        let runtime = WriterRuntime::new().unwrap();
        Self::open_with_runtime(checked, &runtime).await
    }

    pub(crate) async fn open_with_runtime(
        checked: &CheckedConfig,
        runtime: &WriterRuntime,
    ) -> Self {
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
        let mut parameters =
            handshake::Parameters::streaming(limits, handshake::PRODUCER | handshake::CONSUMER)
                .unwrap();
        parameters.capabilities |= handshake::OWNER_ROUTING | handshake::OWNER_READ;
        let links = BrokerLinks::connect(
            runtime,
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
                        data_endpoint: checked.deployment.deployment().brokers[name]
                            .endpoints
                            .data_peer
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
            bases: vec![0; partitions],
            next_sequences: vec![0; partitions],
            policy,
        }
    }

    pub(super) async fn queue(&mut self, wave: usize) -> Vec<Pending> {
        self.queue_selected(wave, None).await
    }

    pub(crate) async fn reopen_producer(self, takeover: bool) -> Self {
        let trace = std::env::var("OZZY_SOAK_TRACE").is_ok_and(|value| value == "1");
        if trace {
            println!("producer close started, takeover={takeover}");
        }
        let identity = self.writer.identity();
        self.writer.close().await.unwrap();
        if trace {
            println!("producer close finished, attachment started");
        }
        let limits = DataLimits {
            envelope: EnvelopeLimits {
                max_metadata_bytes: 16 * 1024,
                max_payload_bytes: 4096,
            },
            max_records: 4,
            max_parts: 4,
            max_record_bytes: 1024,
        };
        let config = SharedTopicWriterConfig::new(limits);
        let writer = if takeover {
            SharedTopicWriter::takeover(
                &self.links,
                "orders",
                identity,
                config,
                RetryPolicy::default(),
            )
            .await
        } else {
            SharedTopicWriter::resume(
                &self.links,
                "orders",
                identity,
                config,
                RetryPolicy::default(),
            )
            .await
        }
        .unwrap();
        if trace {
            println!("producer attachment finished");
        }
        assert_eq!(writer.producer(), identity.producer);
        Self {
            next_sequences: if takeover {
                vec![0; self.keys.len()]
            } else {
                self.next_sequences
            },
            writer,
            links: self.links,
            keys: self.keys,
            history: self.history,
            bases: self.bases,
            policy: self.policy,
        }
    }

    pub(super) async fn queue_except(&mut self, wave: usize, partition: usize) -> Vec<Pending> {
        self.queue_selected(wave, Some(partition)).await
    }

    async fn queue_selected(&mut self, wave: usize, excluded: Option<usize>) -> Vec<Pending> {
        let mut pending = Vec::new();
        for partition in 0..self.keys.len() {
            if excluded == Some(partition) {
                continue;
            }
            for record in 0..4 {
                let id = MessageId::from_bytes(
                    ((wave * 1000 + partition * 10 + record + 1) as u128).to_be_bytes(),
                );
                let body =
                    Bytes::from(format!("wave-{wave}-partition-{partition}-record-{record}"));
                pending.push(self.admit(partition, id, vec![body]).await);
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
            pending.push(self.admit(0, id, vec![body]).await);
        }
        pending
    }

    pub(crate) async fn confirm(&mut self, pending: Vec<Pending>) {
        for (pending, id, body) in pending {
            let receipt = self.confirm_record(&pending).await;
            assert_eq!(receipt.record.policy, self.policy);
            assert_eq!(receipt.partition, pending.partition());
            assert_eq!(receipt.record.message_id, id);
            assert_eq!(receipt.record.key.producer_id, self.writer.producer());
            assert_eq!(receipt.record.key.first_sequence, pending.sequence());
            let history = &mut self.history[receipt.partition as usize];
            assert_eq!(
                receipt.record.offset,
                (self.bases[receipt.partition as usize] + history.len()) as u64
            );
            history.push((id, body));
        }
    }

    async fn confirm_record(
        &self,
        pending: &SharedTopicPendingRecord,
    ) -> ozzy_runtime::replicated::SharedTopicReceipt {
        let routes = self.links.routes(self.writer.metadata().clone()).unwrap();
        let mut confirmation = std::pin::pin!(pending.confirmed());
        loop {
            tokio::select! {
                result = &mut confirmation => return result.unwrap(),
                () = tokio::time::sleep(Duration::from_secs(5)) => {
                    eprintln!(
                        "waiting for partition {} sequence {}: route={:?}, stats={:?}",
                        pending.partition(), pending.sequence(),
                        routes.route(pending.partition()),
                        self.writer.partition_stats(pending.partition()),
                    );
                }
            }
        }
    }

    pub(crate) async fn reader(&self, at_end: bool) -> TopicReader {
        TopicReader::open(
            self.links.clone(),
            "orders",
            TopicReaderConfig {
                start: if at_end || self.bases.iter().any(|&base| base > 0) {
                    ozzy_runtime::replicated::ReaderStart::Checkpoint(TopicCheckpoint {
                        topic: self.writer.metadata().id(),
                        positions: self
                            .history
                            .iter()
                            .enumerate()
                            .map(|(n, history)| {
                                (
                                    n as u32,
                                    Offset::new(if at_end {
                                        self.bases[n] + history.len()
                                    } else {
                                        self.bases[n]
                                    } as u64),
                                )
                            })
                            .collect(),
                    })
                } else {
                    ozzy_runtime::replicated::ReaderStart::Earliest
                },
                ..TopicReaderConfig::default()
            },
        )
        .await
        .unwrap()
    }

    pub(crate) fn positions(&self) -> Vec<usize> {
        self.history
            .iter()
            .zip(&self.bases)
            .map(|(history, base)| base + history.len())
            .collect()
    }

    pub(super) fn leader(&self, partition: u32) -> NodeId {
        self.links
            .routes(self.writer.metadata().clone())
            .unwrap()
            .route(partition)
            .unwrap()
            .unwrap()
            .leader
            .unwrap()
    }

    pub(super) async fn retained_floor(&self) -> Offset {
        let mut reader = TopicReader::open(
            self.links.clone(),
            "orders",
            TopicReaderConfig {
                partitions: Some(vec![0]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let floor = match reader.next().await {
            Ok(record) => {
                assert_eq!(record.partition, 0);
                record.offset
            }
            Err(ozzy_runtime::replicated::TopicReaderError::RetentionGap {
                partition,
                earliest,
            }) => {
                assert_eq!(partition, 0);
                earliest
            }
            Err(error) => panic!("retained floor lookup: {error:?}"),
        };
        reader.close().await.unwrap();
        floor
    }

    pub(crate) async fn read(&self, reader: &mut TopicReader, mut positions: Vec<usize>) {
        let wanted = self
            .history
            .iter()
            .zip(&positions)
            .zip(&self.bases)
            .map(|((history, &next), &base)| base + history.len() - next)
            .sum::<usize>();
        for _ in 0..wanted {
            let record = reader.next().await.unwrap();
            let partition = record.partition as usize;
            let next = positions[partition];
            assert_eq!(record.offset, Offset::new(next as u64));
            let (id, body) = &self.history[partition][next - self.bases[partition]];
            assert_eq!(&record.message_id, id);
            assert_eq!(record.payload.as_slice(), body.as_slice());
            positions[partition] += 1;
        }
        assert_eq!(positions, self.positions());
        for (number, next) in reader.checkpoint().positions {
            assert_eq!(next.get(), positions[number as usize] as u64);
        }
    }

    pub(crate) async fn close(self) {
        self.writer.close().await.unwrap();
        self.links.shutdown().await.unwrap();
    }

    pub(crate) async fn replay(&self) {
        let mut reader = self.reader(false).await;
        self.read(&mut reader, self.bases.clone()).await;
        reader.close().await.unwrap();
    }

    pub(crate) fn discard_verified(&mut self) {
        for (history, base) in self.history.iter_mut().zip(&mut self.bases) {
            *base += history.len();
            history.clear();
        }
    }
}
