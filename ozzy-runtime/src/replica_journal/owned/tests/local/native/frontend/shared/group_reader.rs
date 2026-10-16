use super::*;
use crate::replicated::ProgressError;
use crate::replicated::{GroupReader, GroupReaderConfig};
use ozzy_proto::ConsumerMemberId;

fn config(member: u8) -> GroupReaderConfig {
    let mut config = GroupReaderConfig::new(ConsumerMemberId::from_bytes([member; 16]));
    config.heartbeat = Duration::from_millis(1);
    config.drain = Duration::from_millis(50);
    config.window = 4;
    config
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_group_activation_retries_before_the_next_heartbeat() {
    let (mut harness, links, clock, _) = Box::pin(setup_shared_directory_profile(
        &[],
        1,
        Some((2, 4096, 2)),
        false,
        Some(ProgressHistory::Available),
    ))
    .await;
    harness.publish = false;
    harness.refuse_assign = 1;
    let mut config = config(1);
    config.heartbeat = Duration::from_secs(1);
    config.retry.initial_backoff = Duration::from_millis(5);
    config.retry.max_backoff = Duration::from_millis(20);
    let mut reader = harness
        .drive(
            GroupReader::open(links.clone(), "orders", "workers", config.clone()),
            true,
        )
        .await
        .unwrap();
    for _ in 0..10000 {
        harness.pump(true);
        if harness.assignment_attempts > 0 {
            break;
        }
        clock
            .advance(clock.now() + Duration::from_millis(1))
            .unwrap();
        tokio::task::yield_now().await;
    }
    assert_eq!(harness.assignment_attempts, 1);
    let refused = clock.now();
    let record = harness
        .drive_advancing(&clock, reader.next(), true)
        .await
        .unwrap();
    assert_eq!(record.record.offset, Offset::ZERO);
    assert_eq!(harness.assignment_attempts, 2);
    assert!(clock.now().checked_sub(refused).unwrap() < config.heartbeat / 2);
    drop(record);
    harness
        .drive_advancing(&clock, reader.close(), true)
        .await
        .unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_group_reader_rejoins_fenced_membership_and_discards_unconfirmed_tokens() {
    let (mut harness, links, clock, hint) = Box::pin(setup_shared_directory_profile(
        &[],
        1,
        Some((2, 4096, 2)),
        false,
        Some(ProgressHistory::Available),
    ))
    .await;
    harness.publish = false;
    let mut reader = harness
        .drive(
            GroupReader::open(links.clone(), "orders", "workers", config(1)),
            true,
        )
        .await
        .unwrap();
    let first = harness
        .drive_advancing(&clock, reader.next(), true)
        .await
        .unwrap();
    let group = first.token.identity().group;
    let pending = reader.acknowledge(first.token).unwrap();
    harness
        .drive_advancing(&clock, pending.confirmed(), true)
        .await
        .unwrap();
    let old = harness
        .drive_advancing(&clock, reader.next(), true)
        .await
        .unwrap();
    assert_eq!(old.record.offset, Offset::new(1));
    let old_epoch = old.token.identity().assignment_epoch;
    // Replace this stable member's join instance through the real protocol.
    let request = super::groups::join(hint, 1, 99, ozzy_proto::group::Mode::Join, Vec::new());
    harness
        .drive(links.group_join(hint.primary, request), true)
        .await
        .unwrap();
    // Wait for protocol evidence that the SDK replaced the intervening join;
    // an already queued live record can arrive before its next heartbeat.
    harness
        .drive_advancing(
            &clock,
            async {
                loop {
                    let request = super::groups::join(
                        hint,
                        1,
                        99,
                        ozzy_proto::group::Mode::Heartbeat,
                        Vec::new(),
                    );
                    if matches!(
                        links.group_join(hint.primary, request).await,
                        Err(BrokerLinkError::Progress(ProgressError::Fenced))
                    ) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            },
            true,
        )
        .await;
    let repeated = harness
        .drive_advancing(&clock, reader.next(), true)
        .await
        .unwrap();
    assert_eq!(repeated.record.offset, Offset::new(1));
    assert_eq!(repeated.token.identity().group, group);
    assert!(repeated.token.identity().assignment_epoch > old_epoch);
    assert!(matches!(
        reader.acknowledge(old.token),
        Err(ProgressError::Fenced)
    ));
    let pending = reader.acknowledge(repeated.token).unwrap();
    assert_eq!(
        harness
            .drive_advancing(&clock, pending.confirmed(), true)
            .await
            .unwrap()
            .cursor,
        Offset::new(2)
    );
    harness
        .drive_advancing(&clock, reader.close(), true)
        .await
        .unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_group_reader_delivers_confirms_and_hands_off_at_the_confirmed_cursor() {
    let (mut harness, links, clock, _) = Box::pin(setup_shared_directory_profile(
        &[],
        1,
        Some((2, 4096, 2)),
        false,
        Some(ProgressHistory::Available),
    ))
    .await;
    harness.publish = false;
    let mut first = harness
        .drive(
            GroupReader::open(links.clone(), "orders", "workers", config(1)),
            true,
        )
        .await
        .unwrap();
    let delivered = harness
        .drive_advancing(&clock, first.next(), true)
        .await
        .unwrap();
    assert_eq!(delivered.record.offset, Offset::ZERO);
    assert_eq!(delivered.token.identity().assignment_epoch, 1);
    assert_eq!(first.partitions(), [0]);
    let pending = first.acknowledge(delivered.token).unwrap();
    assert_eq!(
        harness
            .drive_advancing(&clock, pending.confirmed(), true)
            .await
            .unwrap()
            .cursor,
        Offset::new(1)
    );
    // Delivered but never acknowledged: the next owner repeats it.
    let unacknowledged = harness
        .drive_advancing(&clock, first.next(), true)
        .await
        .unwrap();
    assert_eq!(unacknowledged.record.offset, Offset::new(1));

    // A second member joins; the only source stays with its incumbent until
    // the incumbent drains and leaves.
    let mut second = harness
        .drive(
            GroupReader::open(links.clone(), "orders", "workers", config(2)),
            true,
        )
        .await
        .unwrap();
    harness
        .drive_advancing(&clock, first.close(), true)
        .await
        .unwrap();
    let mut offsets = Vec::new();
    let mut last = None;
    for _ in 0..2 {
        let delivered = harness
            .drive_advancing(&clock, second.next(), true)
            .await
            .unwrap();
        assert_eq!(delivered.token.identity().assignment_epoch, 2);
        offsets.push(delivered.record.offset);
        last = Some(second.acknowledge(delivered.token).unwrap());
    }
    assert_eq!(offsets, [Offset::new(1), Offset::new(2)]);
    assert_eq!(
        harness
            .drive_advancing(&clock, last.unwrap().confirmed(), true)
            .await
            .unwrap()
            .cursor,
        Offset::new(3)
    );
    drop(unacknowledged);
    harness
        .drive_advancing(&clock, second.close(), true)
        .await
        .unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}
