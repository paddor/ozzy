use super::*;

mod fresh;

use ozzy_sim::broker::Cluster as MemoryCluster;

#[tokio::test(flavor = "current_thread")]
async fn repeated_recovery_reclaims_old_segments_and_preserves_selected_payloads() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let mut cluster = MemoryCluster::new(policy).await;
        let mut client =
            SdkClient::open_with_runtime(&cluster.configs[0].0, &cluster.runtime).await;
        for wave in 0..4 {
            let pending = live_many(
                &cluster.brokers,
                "seed repeated recovery",
                client.queue_varied(wave),
            )
            .await;
            live_many(
                &cluster.brokers,
                "confirm repeated recovery",
                client.confirm(pending),
            )
            .await;
            let image = cluster.controls[2].image().await;
            let old = cluster.configs[2]
                .0
                .plan
                .partitions
                .iter()
                .flat_map(|partition| {
                    super::super::retention::selected(&image, &partition.directory)
                        .segments
                        .into_iter()
                        .map(move |segment| {
                            let name = if segment.file_generation == 0 {
                                format!("{}.log", segment.segment_id)
                            } else {
                                format!("{}.{}.log", segment.segment_id, segment.file_generation)
                            };
                            partition.directory.join("segments").join(name)
                        })
                })
                .collect::<Vec<_>>();
            assert_ne!(old.len(), 0);
            drop(image);
            cluster.restart(2).await;
            cluster.wait_recovered(2).await;
            live_many(
                &cluster.brokers,
                "reclaim unselected recovery segments",
                async {
                    loop {
                        let image = cluster.controls[2].image().await;
                        if old.iter().all(|path| !image.exists(path, false)) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                },
            )
            .await;
            live_many(
                &cluster.brokers,
                "verify selected recovery payloads",
                client.replay(),
            )
            .await;
        }
        live_many(
            &cluster.brokers,
            "close repeated recovery SDK",
            client.close(),
        )
        .await;
        cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_multipartition_attachments_reuse_sdk_capacity_with_a_live_reader() {
    let cluster = MemoryCluster::new(Confirmation::ReplicatedPersisting).await;
    let mut client = SdkClient::open_with_runtime(&cluster.configs[0].0, &cluster.runtime).await;
    let mut reader = live_many(
        &cluster.brokers,
        "open attachment reader",
        client.reader(false),
    )
    .await;
    for wave in 0..40 {
        client = live_many(
            &cluster.brokers,
            "repeat multipartition attachment",
            client.reopen_producer(wave % 2 == 0),
        )
        .await;
        let positions = client.positions();
        let pending = live_many(
            &cluster.brokers,
            "admit attachment records",
            client.queue_varied(wave),
        )
        .await;
        live_many(
            &cluster.brokers,
            "confirm attachment records",
            client.confirm(pending),
        )
        .await;
        live_many(
            &cluster.brokers,
            "verify attachment reader",
            client.read(&mut reader, positions),
        )
        .await;
    }
    live_many(
        &cluster.brokers,
        "replay attachment records",
        client.replay(),
    )
    .await;
    live_many(&cluster.brokers, "close attachment reader", reader.close())
        .await
        .unwrap();
    live_many(&cluster.brokers, "close attachment SDK", client.close()).await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn restart_during_unfinished_recovery_resumes_the_nonvoting_store() {
    let mut cluster = MemoryCluster::new(Confirmation::ReplicatedPersisting).await;
    let mut client = SdkClient::open_with_runtime(&cluster.configs[0].0, &cluster.runtime).await;
    let pending = live_many(
        &cluster.brokers,
        "seed interrupted recovery",
        client.queue_varied(0),
    )
    .await;
    live_many(&cluster.brokers, "confirm seed", client.confirm(pending)).await;
    for control in &cluster.controls[..2] {
        control.hold_record_writes(true);
    }
    cluster.restart(2).await;
    let image = cluster.controls[2].image().await;
    assert!(
        cluster.configs[2]
            .0
            .plan
            .partitions
            .iter()
            .all(|partition| {
                image
                    .bytes(&partition.directory.join("CONFIGURATION"), false)
                    .unwrap()
                    .starts_with(b"OZYRECOV")
            }),
        "both donor barriers must keep all target partitions nonvoting"
    );
    cluster.restart(2).await;
    for control in &cluster.controls[..2] {
        control.hold_record_writes(false);
    }
    let pending = live_many(
        &cluster.brokers,
        "admit after interrupted recovery",
        client.queue_varied(1),
    )
    .await;
    live_many(
        &cluster.brokers,
        "confirm after interrupted recovery",
        client.confirm(pending),
    )
    .await;
    live_many(
        &cluster.brokers,
        "verify interrupted recovery payloads",
        client.replay(),
    )
    .await;
    live_many(
        &cluster.brokers,
        "close interrupted recovery SDK",
        client.close(),
    )
    .await;
    cluster.shutdown().await;
}
