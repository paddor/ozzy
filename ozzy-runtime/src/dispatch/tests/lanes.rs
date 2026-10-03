use super::*;

#[test]
fn full_data_lane_preserves_control_capacity_and_healthy_destinations() {
    let limits = limits();
    let (mut stalled_tx, mut stalled) = channel::<usize>(limits).unwrap();
    let (mut healthy_tx, mut healthy) = channel::<usize>(limits).unwrap();
    let stalled_client = stalled
        .credits()
        .client(session(1), limits.capacity)
        .unwrap();
    let healthy_client = healthy
        .credits()
        .client(session(2), limits.capacity)
        .unwrap();
    let mut full = stalled
        .credits()
        .grant(&stalled_client, Class::Data, quota(4, 100))
        .unwrap();
    for value in 0..4 {
        stalled_tx
            .try_send(&mut full, session(1), 25, value)
            .unwrap();
    }
    let rejected = stalled_tx
        .try_send(&mut full, session(1), 1, 99)
        .unwrap_err();
    assert_eq!(rejected.reason, SendFailure::Admission(Error::Full));
    assert_eq!(rejected.value, 99);
    let mut control = stalled
        .credits()
        .grant(&stalled_client, Class::Control, quota(2, 20))
        .unwrap();
    for value in 4..6 {
        stalled_tx
            .try_send(&mut control, session(1), 10, value)
            .unwrap();
    }
    // The dispatcher keeps serving another destination without draining this one.
    for value in 0..100 {
        let mut grant = healthy
            .credits()
            .grant(&healthy_client, Class::Data, quota(1, 10))
            .unwrap();
        healthy_tx
            .try_send(&mut grant, session(2), 10, value)
            .unwrap();
        let received = healthy.try_recv().unwrap().unwrap();
        assert_eq!(received.value, value);
        assert_eq!(received.class, Class::Data);
        assert_eq!(received.session, session(2));
    }
    assert_eq!(stalled.credits().usage(Class::Data), budget(4, 4, 100));
    assert_eq!(stalled.credits().usage(Class::Control), budget(2, 2, 20));
    for value in 0..6 {
        assert_eq!(stalled.try_recv().unwrap().unwrap().value, value);
    }
    assert!(stalled.try_recv().unwrap().is_none());
    assert_eq!(stalled.credits().usage(Class::Data), Budget::default());
    assert_eq!(stalled.credits().usage(Class::Control), Budget::default());
}

#[test]
fn repeated_queue_release_keeps_grants_backed_by_physical_slots() {
    let limits = limits();
    let (mut sender, mut receiver) = channel::<usize>(limits).unwrap();
    let client = receiver
        .credits()
        .client(session(1), limits.capacity)
        .unwrap();
    for round in 0..100 {
        let mut data = receiver
            .credits()
            .grant(&client, Class::Data, quota(4, 100))
            .unwrap();
        let mut control = receiver
            .credits()
            .grant(&client, Class::Control, quota(2, 20))
            .unwrap();
        for value in 0..4 {
            sender
                .try_send(&mut data, session(1), 25, value + round)
                .unwrap();
        }
        for value in 4..6 {
            sender
                .try_send(&mut control, session(1), 10, value + round)
                .unwrap();
        }
        for value in 0..6 {
            assert_eq!(receiver.try_recv().unwrap().unwrap().value, value + round);
        }
    }
}

#[test]
fn wrong_destination_returns_value_without_spending_another_shards_grant() {
    let limits = limits();
    let (mut first, _receiver) = channel::<Bytes>(limits).unwrap();
    let (_second, mut receiver) = channel::<Bytes>(limits).unwrap();
    let client = receiver
        .credits()
        .client(session(1), limits.capacity)
        .unwrap();
    let mut grant = receiver
        .credits()
        .grant(&client, Class::Data, quota(1, 10))
        .unwrap();
    let bytes = Bytes::from_static(b"hello");
    let error = first
        .try_send(&mut grant, session(1), 5, bytes.clone())
        .unwrap_err();
    assert_eq!(error.reason, SendFailure::Admission(Error::Destination));
    assert_eq!(error.value.as_ptr(), bytes.as_ptr());
    assert_eq!(grant.remaining(), quota(1, 10));
    assert!(receiver.try_recv().unwrap().is_none());
}

#[test]
fn receiver_drop_drains_queued_values_but_not_downstream_aliases() {
    let limits = limits();
    let (mut sender, mut receiver) = channel::<Bytes>(limits).unwrap();
    let client = receiver
        .credits()
        .client(session(1), limits.capacity)
        .unwrap();
    let mut grant = receiver
        .credits()
        .grant(&client, Class::Data, quota(4, 100))
        .unwrap();
    for _ in 0..4 {
        sender
            .try_send(&mut grant, session(1), 25, Bytes::from(vec![7; 25]))
            .unwrap();
    }
    let message = receiver.try_recv().unwrap().unwrap();
    let held = message.retention.attach(message.value);
    assert_eq!(client.usage(Class::Data), budget(3, 4, 100));
    drop(receiver);
    assert_eq!(client.usage(Class::Data), budget(0, 1, 25));
    assert_eq!(
        sender
            .try_send(&mut grant, session(1), 1, Bytes::new())
            .unwrap_err()
            .reason,
        SendFailure::Admission(Error::Closed)
    );
    drop(held);
    assert_eq!(client.usage(Class::Data), Budget::default());
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_receive_wait_does_not_lose_queued_work_or_closure() {
    let limits = limits();
    let (mut sender, mut receiver) = channel::<u8>(limits).unwrap();
    let client = receiver
        .credits()
        .client(session(1), limits.capacity)
        .unwrap();
    let mut grant = receiver
        .credits()
        .grant(&client, Class::Data, quota(1, 1))
        .unwrap();
    assert!(receiver.try_recv().unwrap().is_none());
    let mut wait = Box::pin(receiver.ready());
    assert!(futures::poll!(wait.as_mut()).is_pending());
    drop(wait);
    sender.try_send(&mut grant, session(1), 1, 7).unwrap();
    assert!(futures::poll!(Box::pin(receiver.ready())).is_ready());
    assert_eq!(receiver.try_recv().unwrap().unwrap().value, 7);
    assert!(receiver.try_recv().unwrap().is_none());
    drop(sender);
    assert!(futures::poll!(Box::pin(receiver.ready())).is_ready());
    assert!(matches!(receiver.try_recv(), Err(Error::Closed)));
}
