//! Shared endpoint negotiation for SDKs that request only reader capabilities.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn production_reader_only_sdk_negotiates_and_replays_every_policy() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            reader_only(policy).await;
        }
    })
    .await
    .unwrap();
}

async fn reader_only(policy: Confirmation) {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let mode = if policy == Confirmation::LocalDurable {
        DeploymentMode::Single
    } else {
        DeploymentMode::Three
    };
    let deployment = deployment(root.path(), mode, policy, 2);
    let checked = &deployment[0].0;
    let sdk = links(&runtime, checked).await;
    let reading = role_links(&runtime, checked, handshake::CONSUMER).await;
    let brokers = start_brokers(&runtime, deployment).await;
    let mut writer = live_many(
        &brokers,
        "open writer",
        SharedTopicWriter::open(
            &sdk,
            "orders",
            SharedTopicWriterConfig::new(limits()),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    let mut expected = vec![Vec::new(); 2];
    let inputs = live::inputs(writer.metadata(), &mut expected, 0, 2);
    live::write(&brokers, &mut writer, inputs).await;
    read_topic(&brokers, &reading, &expected).await;
    reading.shutdown().await.unwrap();
    live_many(&brokers, "close writer", writer.close())
        .await
        .unwrap();
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}
