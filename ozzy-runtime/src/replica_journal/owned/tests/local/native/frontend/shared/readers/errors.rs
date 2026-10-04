//! A blocked reader failure preserves every later subscription's error reply.

use super::*;
use crate::replicated::{TopicCheckpoint, TopicReaderError};

#[tokio::test(flavor = "current_thread")]
async fn blocked_failure_reply_preserves_other_readers() {
    tokio::time::timeout(Duration::from_secs(10), scenario())
        .await
        .unwrap();
}

async fn scenario() {
    let (mut harness, links, _, _) = setup_shared_profile(&[], 1, Some((4, 8192, 2))).await;
    let metadata = harness.drive(links.topic("orders"), true).await.unwrap();
    harness.readers.block_failures = true;
    let mut readers = Vec::new();
    for _ in 0..2 {
        readers.push(
            harness
                .drive(
                    TopicReader::open(
                        links.clone(),
                        "orders",
                        TopicReaderConfig {
                            start: crate::replicated::ReaderStart::Checkpoint(TopicCheckpoint {
                                topic: metadata.id(),
                                positions: vec![(0, Offset::new(1))],
                            }),
                            ..TopicReaderConfig::default()
                        },
                    ),
                    true,
                )
                .await
                .unwrap(),
        );
    }
    for _ in 0..1000 {
        harness.pump(true);
        for reader in &mut readers {
            let mut next = pin!(reader.next());
            assert!(futures::poll!(next.as_mut()).is_pending());
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(harness.readers.failures.len(), 1);
    harness.readers.block_failures = false;
    let errors = harness
        .drive(
            futures::future::join_all(readers.iter_mut().map(TopicReader::next)),
            true,
        )
        .await;
    for error in errors {
        assert!(matches!(
            error,
            Err(TopicReaderError::Broker(BrokerLinkError::Rejected {
                code: 16,
                ..
            }))
        ));
    }
    assert_eq!(harness.readers.failures.len(), 2);
    for reader in &mut readers {
        harness.drive(reader.close(), true).await.unwrap();
    }
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}
