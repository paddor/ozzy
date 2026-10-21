//! Idle producer sessions retire, and their identities resume fresh.

use super::*;
use crate::admin_client::AdminClient;
use ozzy_proto::admin::{Reply, Request};

#[tokio::test(flavor = "current_thread")]
async fn idle_producer_sessions_retire_and_resume_fresh_in_all_modes() {
    for mode in [DeploymentMode::Single, DeploymentMode::Three] {
        idle(mode).await;
    }
}

async fn idle(mode: DeploymentMode) {
    let runtime = WriterRuntime::new().unwrap();
    let root = std::path::PathBuf::from(format!("/ozzy-idle-producers-{}", Uuid::now_v7()));
    let admin = format!("inproc://idle-producers-admin-{}", Uuid::now_v7());
    let deployment = deployment_with(&root, mode, 1, |deployment| {
        deployment
            .topics
            .get_mut("orders")
            .unwrap()
            .producer_idle_secs = 1;
        let first = deployment.brokers.values_mut().next().unwrap();
        first.endpoints.admin = Some(admin.clone());
    });
    let checked = deployment[0].0.clone();
    let (brokers, _, images) = start_images(runtime.context().clone(), deployment, true).await;
    let sdk = links(&runtime, &checked).await;
    let config = SharedTopicWriterConfig::new(limits());
    let retry = RetryPolicy::default();
    let mut writer = live_many(
        &brokers,
        "open idle producer",
        SharedTopicWriter::open(&sdk, "orders", config.clone(), retry),
    )
    .await
    .unwrap();
    let identity = writer.identity();
    let pending = writer.send_to_partition(record(1), 0).await.unwrap();
    live_many(&brokers, "confirm idle record", pending.confirmed())
        .await
        .unwrap();
    live_many(&brokers, "close idle producer", writer.close())
        .await
        .unwrap();
    let mut client = AdminClient::connect(runtime.context(), &admin).await;
    live_many(&brokers, "retire the idle session", async {
        loop {
            let Reply::Partitions(partitions) = client.ask(Request::Partitions).await else {
                panic!("partitions reply expected");
            };
            if partitions
                .rows
                .iter()
                .all(|row| row.log.as_ref().is_some_and(|log| log.producers == 0))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    client.close().await;
    // The identity resumes as a new session with fresh sequences.
    let mut resumed = live_many(
        &brokers,
        "resume retired identity",
        SharedTopicWriter::resume(&sdk, "orders", identity, config, retry),
    )
    .await
    .unwrap();
    let pending = resumed.send_to_partition(record(2), 0).await.unwrap();
    assert_eq!(pending.sequence(), 0);
    let receipt = live_many(&brokers, "confirm resumed record", pending.confirmed())
        .await
        .unwrap();
    assert_eq!(receipt.record.offset, 1);
    live_many(&brokers, "close resumed producer", resumed.close())
        .await
        .unwrap();
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    for image in images {
        image.await.unwrap();
    }
}

fn record(id: u8) -> RecordInput {
    RecordInput::single(
        MessageId::from_bytes([id; 16]),
        bytes::Bytes::from_static(b"idle producer"),
    )
}
