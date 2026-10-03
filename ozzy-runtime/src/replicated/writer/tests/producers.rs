use super::*;
use futures::{FutureExt, executor::block_on};

fn config(records: usize, bytes: usize) -> WriterConfig {
    WriterConfig {
        max_producers: 4,
        ..super::config(records, bytes)
    }
}

#[test]
fn delayed_ticket_bounds_reorder_even_when_other_lanes_keep_draining() {
    let (writer, mut driver) = channel(config(2, 128));
    let mut second = writer.try_clone().unwrap();
    // Reserve the first ticket without publishing it, as a descheduled producer
    // could. Draining later tickets must not free aggregate unbatched space.
    let reservation = writer.shared.inbox.reserve(0).unwrap();
    writer
        .shared
        .next
        .store(1, std::sync::atomic::Ordering::Release);
    let mut admitted = 1;
    while second.admit(&mut input(2, b"later"), 5).is_some() {
        admitted += 1;
        driver.settle();
    }
    assert_eq!(admitted, driver.inbox.capacity);
    assert!(driver.admission.records.is_empty());
    assert_eq!(driver.inbox.used(), driver.inbox.capacity);
    drop(reservation);
}

#[test]
fn delayed_publication_restores_global_order_across_window_wraps() {
    let (mut writer, mut driver) = channel(config(3, 128));
    let mut publish = |sequence: u64| {
        let bytes = sequence.to_le_bytes();
        let mut input = RecordInput::copy_from_slice(MessageId::new(), &bytes);
        let reservation = writer.shared.inbox.reserve(bytes.len()).unwrap();
        writer
            .sender
            .try_send(state::Queued {
                encoding: ozzy_proto::data::Encoding::Raw,
                admitted_at: None,
                sequence,
                completion: Arc::new(state::RecordCompletion {
                    message_id: input.message_id,
                    offset: std::sync::OnceLock::new(),
                }),
                lengths: None,
                body: super::super::payload::Body::take(&mut input, bytes.len()),
            })
            .unwrap();
        reservation.publish();
    };
    for first in (0..24).step_by(3) {
        // A producer can be descheduled after assigning a ticket. A later
        // ticket must wait in the driver until the missing prefix arrives.
        driver
            .next
            .store(first + 3, std::sync::atomic::Ordering::Release);
        publish(first + 2);
        driver.settle();
        assert!(driver.admission.records.is_empty());
        publish(first);
        driver.settle();
        assert_eq!(driver.admission.records.len(), 1);
        assert_eq!(driver.admission.records[0].sequence, first);
        publish(first + 1);
        driver.settle();
        assert_eq!(driver.admission.records.len(), 3);
        for (index, record) in driver.admission.records.iter().enumerate() {
            let expected = first + index as u64;
            assert_eq!(record.sequence, expected);
            assert_eq!(record.body.bytes(), expected.to_le_bytes());
        }
        confirm(&mut driver, first + 3, 0).unwrap();
    }
}

#[test]
fn producers_admit_independently_while_driver_is_parked() {
    let (writer, mut driver) = channel(config(64, 8192));
    let lanes: Vec<_> = (0..3).map(|_| writer.try_clone().unwrap()).collect();
    assert!(matches!(
        writer.try_clone(),
        Err(WriterError::ProducerLimit)
    ));
    let (done, results) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for (lane, mut writer) in lanes.into_iter().enumerate() {
            let done = done.clone();
            scope.spawn(move || {
                let mut admitted = Vec::new();
                for value in 0..16 {
                    let input =
                        RecordInput::copy_from_slice(MessageId::new(), &[lane as u8, value]);
                    admitted.push(block_on(writer.send(input)).unwrap().sequence());
                }
                done.send(admitted).unwrap();
            });
        }
        // Driver owns its batching and retry state throughout. No drain, network
        // progress, or confirmation is needed for admission inside the window.
        let mut tickets = Vec::new();
        for _ in 0..3 {
            tickets.extend(
                results
                    .recv_timeout(Duration::from_secs(3))
                    .expect("producer waited on parked driver"),
            );
        }
        tickets.sort_unstable();
        assert_eq!(tickets, (0..48).collect::<Vec<_>>());
    });
    driver.settle();
    assert_eq!(driver.admission.records.len(), 48);
    assert!(
        driver
            .admission
            .records
            .iter()
            .map(|record| record.sequence)
            .eq(0..48)
    );
    confirm(&mut driver, 48, 100).unwrap();
}

#[test]
fn concurrent_producers_wrap_the_window_without_sequence_gaps_or_reordering() {
    const PER_LANE: usize = 1024;
    let (writer, mut driver) = channel(config(67, 8192));
    let lanes: Vec<_> = (0..3).map(|_| writer.try_clone().unwrap()).collect();
    let mut expected = [0_u64; 3];
    std::thread::scope(|scope| {
        let mut joins = Vec::new();
        for (lane, mut writer) in lanes.into_iter().enumerate() {
            joins.push(scope.spawn(move || {
                let mut pending = Vec::new();
                for index in 0..PER_LANE as u64 {
                    let mut bytes = [0; 9];
                    bytes[0] = lane as u8;
                    bytes[1..].copy_from_slice(&index.to_le_bytes());
                    pending.push(
                        block_on(
                            writer.send(RecordInput::copy_from_slice(MessageId::new(), &bytes)),
                        )
                        .unwrap(),
                    );
                }
                for record in pending {
                    assert_eq!(
                        block_on(record.confirmed()).unwrap().offset,
                        100 + record.sequence()
                    );
                }
            }));
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut next = 0;
        while next != 3 * PER_LANE as u64 {
            driver.settle();
            for record in &driver.admission.records {
                assert_eq!(record.sequence, next);
                let bytes = record.body.bytes();
                let lane = usize::from(bytes[0]);
                assert_eq!(
                    u64::from_le_bytes(bytes[1..].try_into().unwrap()),
                    expected[lane]
                );
                expected[lane] += 1;
                next += 1;
            }
            if next > driver.progress.confirmed() {
                confirm(&mut driver, next, 100).unwrap();
            }
            assert!(
                std::time::Instant::now() < deadline,
                "producer/credit progress stalled"
            );
            std::thread::yield_now();
        }
        for join in joins {
            join.join().unwrap();
        }
    });
    assert_eq!(expected, [PER_LANE as u64; 3]);
}

#[tokio::test]
async fn canceled_full_lane_does_not_consume_a_ticket_or_block_another_producer() {
    let (mut first, mut driver) = channel(WriterConfig {
        max_producers: 2,
        ..config(1, 128)
    });
    let mut second = first.try_clone().unwrap();
    let admitted = first.send(input(1, b"a")).await.unwrap();
    second.send(input(2, b"b")).await.unwrap();
    assert!(second.send(input(9, b"canceled")).now_or_never().is_none());
    confirm(&mut driver, 2, 0).unwrap();
    assert_eq!(admitted.confirmed().await.unwrap().offset, 0);
    let next = second.send(input(3, b"c")).await.unwrap();
    assert_eq!(next.sequence(), 2);
    drop(first);
    assert!(!driver.stop.is_closed());
    confirm(&mut driver, 3, 0).unwrap();
    assert_eq!(next.confirmed().await.unwrap().offset, 2);
    drop(second);
    assert!(driver.stop.is_closed());
}

#[tokio::test]
async fn canceled_close_seals_every_lane_and_flush_captures_shared_prefix() {
    let (mut first, mut driver) = channel(config(4, 128));
    let mut second = first.try_clone().unwrap();
    first.send(input(1, b"a")).await.unwrap();
    second.send(input(2, b"b")).await.unwrap();
    let prefix = first.flush();
    second.send(input(3, b"c")).await.unwrap();
    confirm(&mut driver, 2, 0).unwrap();
    prefix.await.unwrap();
    assert!(first.close().now_or_never().is_none());
    assert!(matches!(
        second.send(input(4, b"d")).await,
        Err(WriterError::Closed)
    ));
    assert!(matches!(second.try_clone(), Err(WriterError::Closed)));
    confirm(&mut driver, 3, 0).unwrap();
    second.flush().await.unwrap();
}

#[test]
fn dropping_a_handle_releases_its_bound_without_losing_queued_records() {
    let mut settings = config(4, 128);
    settings.max_producers = 2;
    let (writer, mut driver) = channel(settings);
    let mut second = writer.try_clone().unwrap();
    second.admit(&mut input(1, b"a"), 1).unwrap().unwrap();
    drop(second);
    let replacement = writer.try_clone().unwrap();
    driver.settle();
    drop(replacement);
    confirm(&mut driver, 1, 0).unwrap();
}

#[tokio::test]
async fn driver_teardown_reclaims_unread_lanes_with_producer_handles_still_alive() {
    let (mut first, driver) = channel(config(4, 128));
    let mut second = first.try_clone().unwrap();
    let a = first.send(input(1, b"a")).await.unwrap();
    let b = second.send(input(2, b"b")).await.unwrap();
    drop(driver);
    assert!(matches!(a.confirmed().await, Err(WriterError::Closed)));
    assert!(matches!(b.confirmed().await, Err(WriterError::Closed)));
    assert!(matches!(
        first.send(input(3, b"c")).await,
        Err(WriterError::Closed)
    ));
    assert!(first.shared.finished.is_closed());
}

#[tokio::test]
async fn full_producer_lanes_do_not_make_blocked_producers_wake_each_other() {
    let (mut first, driver) = channel(WriterConfig {
        max_producers: 3,
        ..config(1, 128)
    });
    let mut second = first.try_clone().unwrap();
    let mut third = first.try_clone().unwrap();
    first.send(input(1, b"a")).await.unwrap();
    second.send(input(2, b"b")).await.unwrap();
    third.send(input(3, b"c")).await.unwrap();
    let generation = driver.capacity.generation();
    for _ in 0..8 {
        assert!(second.send(input(2, b"b")).now_or_never().is_none());
        assert!(third.send(input(3, b"c")).now_or_never().is_none());
        assert_eq!(
            driver.capacity.generation(),
            generation,
            "a rejected reservation cannot advertise capacity that remains full"
        );
    }
}

#[test]
fn batching_inbox_holds_one_ready_request_of_bytes() {
    let mut settings = super::config(2, 64);
    settings.batch_target_bytes = 64;
    settings.limits.max_records = 64;
    let (mut writer, mut driver) = Shared::channel_with_capacity(settings, 1024);
    // One ready request: eight 8-byte records, far below the record bound.
    let mut admitted = 0;
    while writer.admit(&mut input(1, b"12345678"), 8).is_some() {
        admitted += 1;
    }
    assert_eq!(admitted, 8);
    assert_eq!(driver.inbox.used_bytes(), 64);
    driver.settle();
    driver.stage(4);
    assert_eq!(driver.inbox.used_bytes(), 32);
    assert!(writer.admit(&mut input(1, b"12345678"), 8).is_some());
    // Staging everything returns all bytes; a full-slot record fits again.
    // Each settle round accepts at most one request's bytes.
    while driver.admission.records.len() < 9 {
        driver.settle();
    }
    driver.stage(9);
    assert_eq!(driver.inbox.used_bytes(), 0);
    assert!(writer.admit(&mut input(1, &[7; 64]), 64).is_some());
}
