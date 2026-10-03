use super::*;

#[tokio::test]
async fn retired_shard_lanes_cannot_hide_queued_work_from_sleeping_workers() {
    for class in [Class::Data, Class::Progress] {
        let temporary = tempfile::tempdir().unwrap();
        let mut config = config();
        config.threads = 1;
        config.max_inflight = 1;
        config.handles = 32;
        config.limits.shards = 32;
        config.limits.data.operations = 32;
        config.limits.progress.operations = 32;
        let (pool, mut clients) = Pool::new(config).unwrap();
        let gate = Gate::new(|_| true);
        *pool.shared.gate.lock().unwrap() = Some(gate.clone());
        let first = clients[0]
            .submit(
                class,
                Operation::CreateDirectory {
                    path: temporary.path().join("first"),
                },
            )
            .unwrap();
        gate.wait().await;
        let mut last = clients.pop().unwrap();
        // Retiring empty sender lanes requires bounded receive maintenance.
        // The final shard has real work beyond those lanes before the only
        // eligible worker resumes. No later submission supplies another wake.
        drop(clients);
        let pending = last
            .submit(
                class,
                Operation::CreateDirectory {
                    path: temporary.path().join("last"),
                },
            )
            .unwrap();
        gate.release();
        first.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), pending)
            .await
            .expect("worker slept with an admitted job behind retired lanes")
            .unwrap();
        assert!(temporary.path().join("last").is_dir());
        pool.shutdown().await;
    }
}
