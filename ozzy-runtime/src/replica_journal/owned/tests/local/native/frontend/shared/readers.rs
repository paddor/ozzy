//! Actual SDK cursors with explicit socket delivery and controlled storage turns.

use super::*;
use crate::replicated::{
    RecordInput, SharedTopicWriter, SharedTopicWriterConfig, TopicReader, TopicReaderConfig,
};
use ozzy_proto::{MessageId, Offset, reader};

mod errors;
mod pressure;
mod source;
mod wake;

#[tokio::test(flavor = "current_thread")]
async fn reader_close_cancels_an_unanswered_open_after_its_observer_is_dropped() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut harness, links, clock, authority) =
            setup_shared_profile(&[], 1, Some((4, 8192, 4))).await;
        harness.readers.drop_subscribed = true;
        let mut reader = harness
            .drive(
                TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
                true,
            )
            .await
            .unwrap();
        {
            let mut next = pin!(reader.next());
            for turn in 0..10000 {
                clock.advance(Duration::from_micros(100 * turn)).unwrap();
                harness.pump(true);
                assert!(futures::poll!(next.as_mut()).is_pending());
                if !harness.readers.requests.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        let subscription = harness.readers.requests[0].1;
        assert_eq!(harness.readers.requests[0].0, Opcode::Subscribe);
        // Cleanup must survive cancellation of the close observer while the
        // shared PEER connection stays alive for other users.
        drop(reader.close());
        harness.drive(reader.close(), true).await.unwrap();
        assert_eq!(
            harness.readers.requests,
            vec![
                (Opcode::Subscribe, subscription),
                (Opcode::Unsubscribe, subscription),
            ]
        );
        assert!(links.session(authority.primary).is_some());
        harness.drive(links.shutdown(), true).await.unwrap();
        harness.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn shared_reader_repairs_compressed_batches_larger_than_its_actual_window() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for window in [(4, 8192, 2), (32, 8192, 8)] {
            compressed(window).await;
        }
    })
    .await
    .unwrap();
}

async fn compressed(window: (u64, u64, usize)) {
    let (mut harness, links, _, _) = setup_shared_profile(&[], 1, Some(window)).await;
    harness.publish = false;
    let mut writer = harness
        .drive(
            SharedTopicWriter::open_with_producer(
                &links,
                "orders",
                ProducerId::from_bytes([40; 16]),
                SharedTopicWriterConfig::new(harness.wire),
                appends::retry(),
            ),
            true,
        )
        .await
        .unwrap();
    let mut pending = Vec::new();
    // Broker opening cannot finish until the explicit pump runs. All four
    // records are ready before the SDK selects its first APPEND.
    for n in 0..4 {
        pending.push(
            writer
                .send(
                    RecordInput::copy_from_slice(MessageId::from_bytes([50 + n; 16]), &[7; 768]),
                    None,
                )
                .await
                .unwrap(),
        );
    }
    for pending in pending {
        harness.drive(pending.confirmed(), true).await.unwrap();
    }
    assert!(
        harness.compressed_appends > 0,
        "fixture must use actual SDK compression"
    );
    assert_eq!(harness.appends.last().unwrap().2.len(), 4);
    let mut reader = harness
        .drive(
            TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
            true,
        )
        .await
        .unwrap();
    for n in 0..4 {
        let record = harness.drive(reader.next(), true).await.unwrap();
        assert_eq!(record.offset, Offset::new(u64::from(n)));
        assert_eq!(record.message_id, MessageId::from_bytes([50 + n; 16]));
        assert_eq!(record.payload[0].as_ref(), &[7; 768]);
    }
    assert_eq!(
        reader.stats().live_records,
        0,
        "PUB cannot hide a replay stall"
    );
    harness.drive(reader.close(), true).await.unwrap();
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn shared_topic_reader_fences_sources_repairs_overlaps_and_keeps_checkpoints() {
    tokio::time::timeout(Duration::from_secs(10), overlap())
        .await
        .unwrap();
}

#[expect(
    clippy::too_many_lines,
    reason = "one explicit delivery schedule checks source fences, overlap, and individual cancellation"
)]
async fn overlap() {
    let (mut harness, links, _, authority) = setup_shared_profile(&[], 2, Some((8, 8192, 2))).await;
    harness.publish = false;
    let mut writer = harness
        .drive(
            SharedTopicWriter::open_with_producer(
                &links,
                "orders",
                ProducerId::from_bytes([40; 16]),
                SharedTopicWriterConfig::new(harness.wire),
                appends::retry(),
            ),
            true,
        )
        .await
        .unwrap();
    let keys = (0..2)
        .map(|number| {
            (0_u32..100)
                .map(u32::to_be_bytes)
                .find(|key| writer.metadata().keyed_partition(key).number == number)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut pending = Vec::new();
    for n in 0..5 {
        pending.push(
            writer
                .send(
                    RecordInput::copy_from_slice(MessageId::from_bytes([50 + n; 16]), &[n; 8]),
                    Some(&keys[0]),
                )
                .await
                .unwrap(),
        );
    }
    pending.push(
        writer
            .send(
                RecordInput::copy_from_slice(MessageId::from_bytes([90; 16]), &[9; 8]),
                Some(&keys[1]),
            )
            .await
            .unwrap(),
    );
    for pending in pending {
        harness.drive(pending.confirmed(), true).await.unwrap();
    }
    harness.readers.hold_records = true;
    let mut reader = harness
        .drive(
            TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
            true,
        )
        .await
        .unwrap();
    let checkpoint = reader.checkpoint();
    {
        let mut next = pin!(reader.next());
        for _ in 0..10000 {
            harness.pump(true);
            assert!(futures::poll!(next.as_mut()).is_pending());
            if harness.readers.records.len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(harness.readers.records.len(), 2);
    assert_eq!(reader.checkpoint(), checkpoint);
    let source = |number| reader::Source::Group {
        authority: Authority {
            group_id: harness.groups[number],
            ..authority.authority
        },
        partition: multiple::incarnation(number),
        owner_epoch: 1,
    };
    let actual = source(0);
    let session = links.session(authority.primary);
    let reader::Source::Group {
        authority: group,
        partition,
        owner_epoch,
    } = actual
    else {
        unreachable!()
    };
    for wrong in [
        reader::Source::Group {
            authority: Authority {
                view: group.view + 1,
                ..group
            },
            partition,
            owner_epoch,
        },
        reader::Source::Group {
            authority: Authority {
                config_epoch: group.config_epoch + 1,
                ..group
            },
            partition,
            owner_epoch,
        },
        reader::Source::Group {
            authority: group,
            partition,
            owner_epoch: owner_epoch + 1,
        },
    ] {
        links.inject_reader_publication(
            authority.primary,
            &publication(authority.primary, wrong, 0, &[(50, 0)], harness.wire),
        );
        let mut next = pin!(reader.next());
        for _ in 0..4 {
            assert!(futures::poll!(next.as_mut()).is_pending());
        }
    }
    assert_eq!(
        reader.checkpoint(),
        checkpoint,
        "foreign source cannot advance progress"
    );
    assert_eq!(
        links.session(authority.primary),
        session,
        "foreign publication cannot replace a shared PEER session"
    );
    links.inject_reader_publication(
        authority.primary,
        &publication(
            authority.primary,
            actual,
            2,
            &[(52, 2), (53, 3), (54, 4)],
            harness.wire,
        ),
    );
    {
        let mut next = pin!(reader.next());
        for _ in 0..4 {
            assert!(futures::poll!(next.as_mut()).is_pending());
        }
    }
    links.inject_reader_publication(
        authority.primary,
        &publication(authority.primary, source(1), 0, &[(90, 9)], harness.wire),
    );
    let healthy = harness.drive(reader.next(), true).await.unwrap();
    assert_eq!(healthy.partition, 1);
    assert_eq!(healthy.offset, Offset::ZERO);
    assert_eq!(healthy.message_id, MessageId::from_bytes([90; 16]));
    for message in harness.readers.records.drain(..) {
        harness.server.try_send(message).unwrap();
    }
    harness.readers.hold_records = false;
    for n in 0..5 {
        let record = harness.drive(reader.next(), true).await.unwrap();
        assert_eq!(record.partition, 0);
        assert_eq!(record.offset, Offset::new(u64::from(n)));
        assert_eq!(record.message_id, MessageId::from_bytes([50 + n; 16]));
        assert_eq!(record.payload[0].as_ref(), &[n; 8]);
        assert_eq!(reader.checkpoint().positions[0].1.get(), u64::from(n) + 1);
        let checkpoint = reader.checkpoint();
        drop(reader.next());
        assert_eq!(
            reader.checkpoint(),
            checkpoint,
            "unpolled cancellation keeps individual progress"
        );
    }
    assert_eq!(
        reader.stats().live_records,
        2,
        "healthy record and held suffix use PUB"
    );
    harness.drive(reader.close(), true).await.unwrap();
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

fn publication(
    local: NodeId,
    source: reader::Source,
    first: u64,
    records: &[(u8, u8)],
    limits: DataLimits,
) -> Message {
    let mut metadata = Vec::with_capacity(limits.envelope.max_metadata_bytes);
    let mut payload = Vec::with_capacity(limits.envelope.max_payload_bytes);
    let mut output = reader::RecordsEncoder::publication(
        Envelope {
            opcode: Opcode::RecordsPub,
            response: false,
            request_id: None,
            sender: local,
            session: None,
        },
        reader::PublicationHeader {
            source,
            first_offset: first,
        },
        &mut metadata,
        &mut payload,
        limits,
    )
    .unwrap();
    for &(id, byte) in records {
        output
            .push_raw(MessageId::from_bytes([id; 16]), &[byte; 8])
            .unwrap();
    }
    let header = output.finish().unwrap();
    crate::native_frames::message(
        &reader::publication_topic(source).unwrap(),
        header,
        &metadata,
        Bytes::from(payload),
    )
}
