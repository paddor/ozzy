//! Bounded churn shares the process suite's SDK and independent payload oracle.

use super::{Bytes, Client, MessageId, Pending, RecordInput};

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
        self.submitted
            .push((partition as u32, admitted.sequence(), id, parts.clone()));
        (admitted, id, parts)
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
