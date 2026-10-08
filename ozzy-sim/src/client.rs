//! Actual shared SDK with an independent record/offset oracle across process death.

use bytes::Bytes;
use ozzy_broker::CheckedConfig;
use ozzy_proto::{
    EnvelopeLimits, MessageId, NodeId, Offset, ProducerId, append::Policy, data::DataLimits,
    handshake,
};
use ozzy_runtime::replicated::{
    AppendLinkLimits, BrokerAddress, BrokerLinks, BrokerLinksConfig, ReaderLinkLimits, RecordInput,
    RetryPolicy, SdkClock, SharedTopicPendingRecord, SharedTopicWriter, SharedTopicWriterConfig,
    TopicCheckpoint, TopicReader, TopicReaderConfig, WriterRuntime,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

mod consumers;
mod progress;
mod workload;
pub use progress::{ProgressTimeout, progress_timeout};

/// Production SDK client with an independent bounded record and offset oracle.
pub struct Client {
    /// Shared authenticated SDK links for controlled disconnect and reconnect schedules.
    pub links: BrokerLinks,
    runtime: WriterRuntime,
    writer: SharedTopicWriter,
    links_config: BrokerLinksConfig,
    keys: Vec<[u8; 4]>,
    history: Vec<Vec<(MessageId, Vec<Bytes>)>>,
    bases: Vec<usize>,
    next_sequences: Vec<u64>,
    submitted: Vec<Submitted>,
    policy: Policy,
    verified: Arc<AtomicU64>,
}

/// Admitted observation paired with independently retained record evidence.
#[derive(Debug)]
pub struct Pending {
    /// Independent confirmation observation; dropping its future cannot retract admission.
    pub record: SharedTopicPendingRecord,
    producer: ProducerId,
    id: MessageId,
    /// Independently submitted payload parts for later consumer comparison.
    pub parts: Vec<Bytes>,
}

struct Submitted {
    producer: ProducerId,
    partition: u32,
    sequence: u64,
    id: MessageId,
    parts: Vec<Bytes>,
}

impl Client {
    /// Open producer and consumer access on a new real SDK runtime.
    pub async fn open(checked: &CheckedConfig) -> Self {
        let runtime = WriterRuntime::new().unwrap();
        Self::open_with_runtime(checked, &runtime).await
    }

    /// Open access using the same OMQ context as the memory brokers.
    pub async fn open_with_runtime(checked: &CheckedConfig, runtime: &WriterRuntime) -> Self {
        Self::open_with_clock(checked, runtime, SdkClock::default()).await
    }

    /// Share independently stepped protocol time with the memory brokers.
    pub async fn open_with_clock(
        checked: &CheckedConfig,
        runtime: &WriterRuntime,
        clock: SdkClock,
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
        let links_config = BrokerLinksConfig {
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
            clock,
        };
        let links = BrokerLinks::connect(runtime, links_config.clone())
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
            runtime: runtime.clone(),
            links_config,
            writer,
            keys,
            history: vec![Vec::new(); partitions],
            bases: vec![0; partitions],
            next_sequences: vec![0; partitions],
            submitted: Vec::new(),
            policy,
            verified: Arc::default(),
        }
    }

    /// Admit four distinct records per partition without waiting for confirmation.
    pub async fn queue(&mut self, wave: usize) -> Vec<Pending> {
        self.queue_selected(wave, None).await
    }

    /// Resume or take over the saved identity, preserving oracle history.
    pub async fn reopen_producer(mut self, takeover: bool) -> Self {
        let trace = std::env::var("OZZY_SOAK_TRACE").is_ok_and(|value| value == "1");
        if trace {
            println!("producer close started, takeover={takeover}");
        }
        let identity = self.writer.identity();
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
            let writer = SharedTopicWriter::takeover(
                &self.links,
                "orders",
                identity,
                config,
                RetryPolicy::default(),
            )
            .await
            .unwrap();
            self.assert_old_producer_fenced().await;
            drop(self.writer);
            writer
        } else {
            self.writer.close().await.unwrap();
            SharedTopicWriter::resume(
                &self.links,
                "orders",
                identity,
                config,
                RetryPolicy::default(),
            )
            .await
            .unwrap()
        };
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
            runtime: self.runtime,
            links_config: self.links_config,
            keys: self.keys,
            history: self.history,
            bases: self.bases,
            policy: self.policy,
            submitted: self.submitted,
            verified: self.verified,
        }
    }

    /// Replace the actual OMQ connections and resume the producer's saved identity.
    /// Callers must close readers and settle observations before replacing links.
    pub async fn reconnect(mut self, runtime: &WriterRuntime, fresh_node: bool) -> Self {
        let identity = self.writer.identity();
        self.writer.close().await.unwrap();
        self.links.shutdown().await.unwrap();
        if fresh_node {
            self.links_config.local = NodeId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        }
        self.links = BrokerLinks::connect(runtime, self.links_config.clone())
            .await
            .unwrap();
        self.runtime = runtime.clone();
        self.writer = SharedTopicWriter::resume(
            &self.links,
            "orders",
            identity,
            SharedTopicWriterConfig::new(self.links_config.parameters.receive),
            RetryPolicy::default(),
        )
        .await
        .unwrap();
        self
    }

    /// Admit a wave excluding one unavailable partition.
    pub async fn queue_except(&mut self, wave: usize, partition: usize) -> Vec<Pending> {
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

    /// Admit bounded random 1 KiB payloads to partition zero.
    pub async fn queue_large(&mut self, wave: u32, records: u32) -> Vec<Pending> {
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

    /// Verify confirmation identity, policy and offset before retaining evidence.
    pub async fn confirm(&mut self, pending: Vec<Pending>) {
        let mut confirmed = Vec::with_capacity(pending.len());
        for pending in pending {
            let receipt = self.confirm_record(&pending.record).await;
            assert_eq!(receipt.record.policy, self.policy);
            assert_eq!(receipt.topic, pending.record.topic());
            assert_eq!(receipt.partition, pending.record.partition());
            assert_eq!(receipt.record.message_id, pending.id);
            assert_eq!(receipt.record.key.producer_id, pending.producer);
            assert_eq!(receipt.record.key.first_sequence, pending.record.sequence());
            self.submitted.retain(|submitted| {
                submitted.producer != pending.producer
                    || submitted.partition != receipt.partition
                    || submitted.sequence != pending.record.sequence()
            });
            confirmed.push((receipt.partition, receipt.record.offset, pending));
        }
        // Independent producers can confirm in a different order from admission.
        // The broker assigns one dense offset order for their shared partition.
        confirmed.sort_unstable_by_key(|(partition, offset, _)| (*partition, *offset));
        for (partition, offset, pending) in confirmed {
            let history = &mut self.history[partition as usize];
            assert_eq!(
                offset,
                (self.bases[partition as usize] + history.len()) as u64
            );
            history.push((pending.id, pending.parts));
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

    /// Open at the retained oracle base or its observed end.
    pub async fn reader(&self, at_end: bool) -> TopicReader {
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

    /// Exclusive independently confirmed offsets by partition.
    pub fn positions(&self) -> Vec<usize> {
        self.history
            .iter()
            .zip(&self.bases)
            .map(|(history, base)| base + history.len())
            .collect()
    }

    /// Observe current routing for a controlled broker fault.
    pub fn leader(&self, partition: u32) -> NodeId {
        self.links
            .routes(self.writer.metadata().clone())
            .unwrap()
            .route(partition)
            .unwrap()
            .unwrap()
            .leader
            .unwrap()
    }

    /// Resolve the first retained offset through the real reader protocol.
    pub async fn retained_floor(&self) -> Offset {
        let mut reader = TopicReader::open(
            self.links.clone(),
            "orders",
            TopicReaderConfig {
                partitions: Some(vec![0]),
                start: ozzy_runtime::replicated::ReaderStart::Checkpoint(TopicCheckpoint {
                    topic: self.writer.metadata().id(),
                    positions: vec![(0, Offset::ZERO)],
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        // Offset zero reports a gap even when every record has retired; an
        // Earliest subscription would park forever at the empty retained end.
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

    /// Check every received offset, identity, payload part and checkpoint.
    pub async fn read(&self, reader: &mut TopicReader, mut positions: Vec<usize>) {
        let wanted = self
            .history
            .iter()
            .zip(&positions)
            .zip(&self.bases)
            .map(|((history, &next), &base)| base + history.len() - next)
            .sum::<usize>();
        for _ in 0..wanted {
            let record = reader.next().await.unwrap();
            self.verify_record(&record, &mut positions);
        }
        assert_eq!(positions, self.positions());
        for (number, next) in reader.checkpoint().positions {
            assert_eq!(next.get(), positions[number as usize] as u64);
        }
    }

    fn verify_record(
        &self,
        record: &ozzy_runtime::replicated::TopicRecord,
        positions: &mut [usize],
    ) {
        let partition = record.partition as usize;
        let next = positions[partition];
        assert_eq!(record.offset, Offset::new(next as u64));
        let (id, body) = &self.history[partition][next - self.bases[partition]];
        assert_eq!(&record.message_id, id);
        assert_eq!(record.payload.as_slice(), body.as_slice());
        positions[partition] += 1;
        self.verified.fetch_add(1, Ordering::Relaxed);
    }

    /// Independent delivery verification count, preserved across SDK churn.
    pub fn verified_progress(&self) -> Arc<AtomicU64> {
        self.verified.clone()
    }

    /// Close producer admission and join the real SDK links.
    pub async fn close(self) {
        self.writer.close().await.unwrap();
        self.links.shutdown().await.unwrap();
    }

    /// Verify retained oracle evidence through a fresh reader.
    pub async fn replay(&self) {
        let mut reader = self.reader(false).await;
        self.read(&mut reader, self.bases.clone()).await;
        reader.close().await.unwrap();
    }

    /// Release verified payload evidence while retaining absolute offsets.
    pub fn discard_verified(&mut self) {
        for (history, base) in self.history.iter_mut().zip(&mut self.bases) {
            *base += history.len();
            history.clear();
        }
    }

    /// Independently submitted and confirmed identities and opaque payload parts.
    /// Outstanding submissions do not constitute confirmation evidence.
    pub fn evidence(&self) -> serde_json::Value {
        let routes = self.links.routes(self.writer.metadata().clone());
        let records = |id: &MessageId, parts: &[Bytes]| {
            serde_json::json!({
                "id": id.as_bytes(), "parts": parts.iter().map(Bytes::as_ref).collect::<Vec<_>>()
            })
        };
        serde_json::json!({
            "producer": format!("{:?}", self.writer.identity()),
            "bases": self.bases, "next_sequences": self.next_sequences,
            "sdk": (0..self.keys.len()).map(|partition| serde_json::json!({
                "partition": partition,
                "route": format!("{:?}", routes.as_ref().map(|routes| routes.route(partition as u32))),
                "stats": format!("{:?}", self.writer.partition_stats(partition as u32)),
            })).collect::<Vec<_>>(),
            "confirmed": self.history.iter().map(|history| history.iter()
                .map(|(id, parts)| records(id, parts)).collect::<Vec<_>>()).collect::<Vec<_>>(),
            "submitted": self.submitted.iter().map(|submitted|
                serde_json::json!({"producer":submitted.producer.as_bytes(),
                    "partition":submitted.partition,"sequence":submitted.sequence,
                    "record":records(&submitted.id, &submitted.parts)})).collect::<Vec<_>>(),
            "verified_deliveries": self.verified.load(Ordering::Relaxed)
        })
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("writer", &self.writer)
            .field("positions", &self.positions())
            .finish_non_exhaustive()
    }
}
