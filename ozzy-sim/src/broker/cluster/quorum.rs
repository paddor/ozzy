//! Explicit quorum loss preserves unconfirmed identities until exact restoration.
use super::{Client, Cluster, Duration, TopicReader};

impl Cluster {
    /// Stop two copies, cancel every confirmation observation while quorum is
    /// absent, then restore their clean images and verify the same admitted records.
    pub async fn quorum_loss(
        &mut self,
        client: &mut Client,
        reader: &mut TopicReader,
        wave: usize,
    ) -> usize {
        let positions = client.positions();
        let third = self.stop(2).await;
        let second = self.stop(1).await;
        let pending = client.queue(wave).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        for record in &pending {
            let mut observation = std::pin::pin!(record.record.confirmed());
            assert!(
                futures::poll!(observation.as_mut()).is_pending(),
                "a three-voter group confirmed with one eligible copy"
            );
        }
        let count = pending.len();
        self.restore_clean(1, second).await;
        self.restore_clean(2, third).await;
        client.confirm(pending).await;
        client.read(reader, positions).await;
        count
    }
}
