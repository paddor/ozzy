use super::*;

#[tokio::test(flavor = "current_thread")]
async fn two_fresh_memory_voters_recover_while_an_original_donor_keeps_serving() {
    recover(12, [3, 7]).await;
}

mod stress {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn two_fresh_memory_voters_recover_while_an_original_donor_keeps_serving() {
        recover(180, [50, 110]).await;
    }
}

async fn recover(waves: usize, restarts: [usize; 2]) {
    let mut cluster = MemoryCluster::new(Confirmation::ReplicatedPersisting).await;
    let mut client = SdkClient::open_with_runtime(&cluster.configs[0].0, &cluster.runtime).await;
    let mut reader = live_many(
        &cluster.brokers,
        "open fresh restart reader",
        client.reader(false),
    )
    .await;
    for wave in 0..waves {
        if let Some(index) = restarts.iter().position(|&restart| restart == wave) {
            cluster.restart_fresh(index).await;
            cluster.wait_recovered(index).await;
        }
        let positions = client.positions();
        let pending = live_many(
            &cluster.brokers,
            "admit original donor churn",
            client.queue_varied(wave),
        )
        .await;
        live_many(
            &cluster.brokers,
            "confirm original donor churn",
            client.confirm(pending),
        )
        .await;
        live_many(
            &cluster.brokers,
            "verify original donor churn",
            client.read(&mut reader, positions),
        )
        .await;
        client.discard_verified();
        if wave.is_multiple_of(23) {
            client = live_many(
                &cluster.brokers,
                "resume fresh restart producer",
                client.reopen_producer(wave.is_multiple_of(46)),
            )
            .await;
        }
    }
    live_many(
        &cluster.brokers,
        "close fresh restart reader",
        reader.close(),
    )
    .await
    .unwrap();
    live_many(&cluster.brokers, "close fresh restart SDK", client.close()).await;
    cluster.shutdown().await;
}
