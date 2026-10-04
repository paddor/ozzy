//! Bounded churn shares the process suite's SDK and independent payload oracle.

use super::{Bytes, Client, MessageId, Pending, RecordInput, Submitted};
use ozzy_runtime::replicated::{RetryPolicy, SharedTopicWriter, SharedTopicWriterConfig};

impl Client {
    pub(super) async fn admit(
        &mut self,
        partition: usize,
        id: MessageId,
        parts: Vec<Bytes>,
    ) -> Pending {
        let admitted = self
            .writer
            .send(
                RecordInput::multipart(id, parts.clone()),
                Some(&self.keys[partition]),
            )
            .await
            .unwrap();
        assert_eq!(admitted.partition(), partition as u32);
        assert_eq!(admitted.sequence(), self.next_sequences[partition]);
        self.next_sequences[partition] += 1;
        self.submitted.push(Submitted {
            producer: self.writer.producer(),
            partition: partition as u32,
            sequence: admitted.sequence(),
            id,
            parts: parts.clone(),
        });
        Pending {
            record: admitted,
            producer: self.writer.producer(),
            id,
            parts,
        }
    }

    /// A fresh producer shares every partition with the live producer. Both
    /// submit before confirmation; receipts establish their global offset order.
    pub async fn shared_producers(&mut self, wave: usize) -> usize {
        let mut other = SharedTopicWriter::open(
            &self.links,
            "orders",
            SharedTopicWriterConfig::new(self.links_config.parameters.receive),
            RetryPolicy::default(),
        )
        .await
        .unwrap();
        let producer = other.producer();
        assert_ne!(producer, self.writer.producer());
        let keys = self.keys.clone();
        let auxiliary = async {
            let mut pending = Vec::new();
            for (partition, key) in keys.iter().enumerate() {
                for sequence in 0..2 {
                    let value = (2_u128 << 120)
                        | ((wave as u128) << 16)
                        | ((partition as u128) << 8)
                        | sequence;
                    let id = MessageId::from_bytes(value.to_be_bytes());
                    let parts = vec![Bytes::from(format!("shared-{wave}-{partition}-{sequence}"))];
                    let record = other
                        .send(RecordInput::multipart(id, parts.clone()), Some(key))
                        .await
                        .unwrap();
                    assert_eq!(record.partition(), partition as u32);
                    assert_eq!(record.sequence(), sequence as u64);
                    pending.push(Pending {
                        record,
                        producer,
                        id,
                        parts,
                    });
                }
            }
            pending
        };
        let (mut pending, auxiliary) = futures::join!(self.queue(wave), auxiliary);
        for item in &auxiliary {
            self.submitted.push(Submitted {
                producer,
                partition: item.record.partition(),
                sequence: item.record.sequence(),
                id: item.id,
                parts: item.parts.clone(),
            });
        }
        pending.extend(auxiliary);
        let count = pending.len();
        self.confirm(pending).await;
        other.close().await.unwrap();
        count
    }

    pub(super) async fn assert_old_producer_fenced(&mut self) {
        for key in &self.keys {
            let record = RecordInput::single(
                MessageId::from_bytes([254; 16]),
                Bytes::from_static(b"superseded producer must be fenced"),
            );
            let pending = self.writer.send(record, Some(key)).await.unwrap();
            assert!(
                pending.confirmed().await.is_err(),
                "superseded epoch confirmed a record"
            );
        }
    }

    /// Vary sparse, burst, hot-partition, mixed-size and multipart traffic.
    pub async fn queue_varied(&mut self, wave: usize) -> Vec<Pending> {
        let pattern = wave % 6;
        let records = [4, 1, 32, 64, 4, 1][pattern];
        let mut pending = Vec::new();
        for partition in 0..self.keys.len() {
            if pattern == 3 && partition != (wave / 6) % self.keys.len() {
                continue;
            }
            for record in 0..records {
                let value = (1_u128 << 120)
                    | ((wave as u128) << 16)
                    | ((partition as u128) << 8)
                    | record as u128;
                let id = MessageId::from_bytes(value.to_be_bytes());
                let length = if pattern == 1 {
                    1024
                } else {
                    [0, 1, 62, 128, 257, 1024][(wave + partition + record) % 6]
                };
                let mut state = (value as u64).wrapping_add(1);
                let body: Bytes = (0..length)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        state as u8
                    })
                    .collect();
                let parts = if pattern == 4 {
                    vec![
                        Bytes::new(),
                        body.slice(..body.len() / 2),
                        body.slice(body.len() / 2..),
                        Bytes::new(),
                    ]
                } else {
                    vec![body]
                };
                pending.push(self.admit(partition, id, parts).await);
            }
        }
        pending
    }
}
