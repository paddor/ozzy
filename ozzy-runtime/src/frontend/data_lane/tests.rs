use super::*;
use crate::dispatch::Class;
use crate::frontend::{Kind, test_support};
use ozzy_proto::ProducerId;

fn input() -> DataInput {
    let binding = test_support::binding(Kind::Client);
    let placement = test_support::placement(0, 0);
    DataInput {
        binding,
        route: Routed {
            placement,
            class: Class::Data,
            writer: Some(ProducerId::from_bytes([4; 16])),
        },
        message: test_support::append(placement, binding),
    }
}

#[tokio::test]
async fn received_input_observes_sender_close_before_its_dequeue_receipt() {
    let (mut sender, receiver) = data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        Class::Data,
        DataLaneLimits {
            slots: 1,
            maximum_retained_bytes: 4096,
            retained_budget_bytes: 8192,
        },
    )
    .unwrap();
    sender.try_send(input(), 4096).unwrap();
    let message = receiver.socket.recv().await.unwrap();
    // Drop signals closure before destroying the sender's socket. Hold that
    // exact interval so receipt routing cannot make this schedule flaky.
    sender.closed.close();
    assert!(matches!(
        decode(&receiver.socket, &receiver.closed, message),
        Err(DataLaneError::Closed)
    ));
}

#[tokio::test]
async fn full_shard_lane_returns_frame_and_wakes_on_dequeue() {
    let (mut sender, mut receiver) = data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        crate::dispatch::Class::Data,
        DataLaneLimits {
            slots: 1,
            maximum_retained_bytes: 4096,
            retained_budget_bytes: 8192,
        },
    )
    .unwrap();
    sender.try_send(input(), 4096).unwrap();
    let held = match sender.try_send(input(), 4096).unwrap_err() {
        DataSendError::Full(input) => input,
        other => panic!("unexpected send result: {other:?}"),
    };
    let generation = sender.space_generation();
    let changed = sender.space_changed_after(generation);
    tokio::pin!(changed);
    assert!(futures::poll!(changed.as_mut()).is_pending());
    assert!(receiver.try_recv().unwrap().is_some());
    changed.await;
    sender.try_send(held, 4096).unwrap();
    assert!(receiver.try_recv().unwrap().is_some());
    assert!(receiver.try_recv().unwrap().is_none());
}

#[test]
fn slot_count_and_held_frame_fit_fixed_retained_budget() {
    assert!(matches!(
        data_channel(
            &omq_tokio::Context::new(),
            0,
            Kind::Client,
            crate::dispatch::Class::Data,
            DataLaneLimits {
                slots: 2,
                maximum_retained_bytes: 4096,
                retained_budget_bytes: 8191
            }
        ),
        Err(DataLaneError::Capacity)
    ));
    assert!(matches!(
        data_channel(
            &omq_tokio::Context::new(),
            0,
            Kind::Client,
            crate::dispatch::Class::Data,
            DataLaneLimits {
                slots: 65537,
                maximum_retained_bytes: 4096,
                retained_budget_bytes: 16384
            }
        ),
        Err(DataLaneError::Capacity)
    ));
}

#[test]
fn byte_pressure_returns_capacity_without_waiting_for_ring_full() {
    let (mut sender, mut receiver) = data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        crate::dispatch::Class::Data,
        DataLaneLimits {
            slots: 4,
            maximum_retained_bytes: 4096,
            retained_budget_bytes: 12288,
        },
    )
    .unwrap();
    sender.try_send(input(), 4096).unwrap();
    sender.try_send(input(), 4096).unwrap();
    let held = match sender.try_send(input(), 4096).unwrap_err() {
        DataSendError::Full(input) => input,
        other => panic!("unexpected byte pressure: {other:?}"),
    };
    assert_eq!(sender.queued_bytes, 8192);
    receiver.try_recv().unwrap().unwrap();
    sender.try_send(held, 4096).unwrap();
    assert_eq!(sender.queued_bytes, 8192);
}

#[test]
fn small_frames_share_the_byte_budget_without_reserving_maximum_per_slot() {
    let (mut sender, mut receiver) = data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        crate::dispatch::Class::Data,
        DataLaneLimits {
            slots: 4,
            maximum_retained_bytes: 16384,
            retained_budget_bytes: 32768,
        },
    )
    .unwrap();
    for _ in 0..4 {
        sender.try_send(input(), 4096).unwrap();
    }
    assert_eq!(sender.queued_bytes, 16384);
    assert!(matches!(
        sender.try_send(input(), 4096),
        Err(DataSendError::Full(_))
    ));
    for _ in 0..4 {
        receiver.try_recv().unwrap().unwrap();
    }
    sender.try_send(input(), 16384).unwrap();
    assert_eq!(sender.queued_bytes, 16384);
    assert!(matches!(
        sender.try_send(input(), 4096),
        Err(DataSendError::Full(_))
    ));
}

#[test]
fn concurrent_dequeues_and_refills_keep_byte_returns_bounded() {
    let (mut sender, mut receiver) = data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        crate::dispatch::Class::Data,
        DataLaneLimits {
            slots: 4,
            maximum_retained_bytes: 4096,
            retained_budget_bytes: 24576,
        },
    )
    .unwrap();
    let reader = std::thread::spawn(move || {
        for _ in 0..5000 {
            while receiver.try_recv().unwrap().is_none() {
                std::thread::yield_now();
            }
        }
    });
    for _ in 0..5000 {
        let mut frame = input();
        loop {
            match sender.try_send(frame, 4096) {
                Ok(()) => break,
                Err(DataSendError::Full(returned)) => frame = returned,
                error => panic!("unexpected queue outcome: {error:?}"),
            }
            std::thread::yield_now();
        }
        assert!(sender.queued_bytes <= sender.queue_bytes);
    }
    reader.join().unwrap();
}

#[test]
fn wrong_shard_cannot_consume_a_producer_slot() {
    let (mut sender, mut receiver) = data_channel(
        &omq_tokio::Context::new(),
        1,
        Kind::Client,
        crate::dispatch::Class::Data,
        DataLaneLimits {
            slots: 1,
            maximum_retained_bytes: 4096,
            retained_budget_bytes: 8192,
        },
    )
    .unwrap();
    assert!(matches!(
        sender.try_send(input(), 4096),
        Err(DataSendError::Invalid(_))
    ));
    assert!(receiver.try_recv().unwrap().is_none());
}
