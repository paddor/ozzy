use super::*;

#[tokio::test]
async fn interleaved_offsets_stay_exact_after_later_confirmation_and_driver_drop() {
    let (mut writer, mut driver) = channel(config(4, 128));
    let first = writer.send(input(1, b"first")).await.unwrap();
    let second = writer.send(input(2, b"second")).await.unwrap();
    let third = writer.send(input(3, b"third")).await.unwrap();
    driver.settle();
    driver.confirm(0, 1, 10).unwrap();
    driver.confirm(1, 3, 50).unwrap();
    drop(writer);
    drop(driver);
    for (pending, offset) in [(first, 10), (second, 50), (third, 51)] {
        assert_eq!(pending.try_confirmed().unwrap().unwrap().offset, offset);
        assert_eq!(pending.confirmed().await.unwrap().offset, offset);
    }
}

#[tokio::test]
async fn confirmation_uses_explicit_range_not_unsigned_offset_minus_sequence() {
    let mut settings = config(2, 128);
    settings.next_sequence = 100;
    let (mut writer, mut driver) = channel(settings);
    let pending = writer.send(input(1, b"record")).await.unwrap();
    driver.settle();
    driver.confirm(100, 101, 7).unwrap();
    assert_eq!(pending.confirmed().await.unwrap().offset, 7);
}

#[tokio::test]
async fn overlapping_confirmation_cannot_reassign_confirmed_boundary() {
    let (mut writer, mut driver) = channel(config(4, 128));
    let first = writer.send(input(1, b"first")).await.unwrap();
    let second = writer.send(input(2, b"second")).await.unwrap();
    driver.settle();
    driver.confirm(0, 1, 10).unwrap();
    assert!(driver.confirm(0, 2, 11).is_err());
    assert!(driver.confirm(1, 2, 10).is_err());
    assert_eq!(first.confirmed().await.unwrap().offset, 10);
    assert!(second.try_confirmed().is_none());
    driver.confirm(0, 2, 10).unwrap();
    assert_eq!(second.confirmed().await.unwrap().offset, 11);
}

#[tokio::test]
async fn nonblocking_observation_preserves_confirmed_prefix_after_failure() {
    let (mut writer, mut driver) = channel(config(2, 128));
    let first = writer.send(input(3, b"first")).await.unwrap();
    let second = writer.send(input(4, b"second")).await.unwrap();
    assert!(first.try_confirmed().is_none());
    assert!(second.try_confirmed().is_none());
    confirm(&mut driver, 1, 90).unwrap();
    let receipt = first.try_confirmed().unwrap().unwrap();
    assert_eq!(receipt.offset, 90);
    assert_eq!(receipt.message_id, first.completion.message_id());
    assert!(second.try_confirmed().is_none());
    driver.fail(WriterError::Configuration);
    assert_eq!(first.try_confirmed().unwrap().unwrap().offset, 90);
    assert!(matches!(
        second.try_confirmed(),
        Some(Err(WriterError::Configuration))
    ));
}

#[test]
fn confirmations_return_exact_new_prefix_bytes_independent_of_transport_owners() {
    let (mut writer, mut shared) = channel(config(4, 8192));
    for size in [0, 128, 4096, 128] {
        let mut record = RecordInput::copy_from_slice(MessageId::new(), &vec![7; size]);
        writer.admit(&mut record, size).unwrap().unwrap();
    }
    let transport = shared.record(1).unwrap().payload;
    assert_eq!(confirm(&mut shared, 1, 50).unwrap(), 0);
    assert_eq!(confirm(&mut shared, 3, 50).unwrap(), 4224);
    assert_eq!(confirm(&mut shared, 3, 50).unwrap(), 0);
    assert_eq!(confirm(&mut shared, 4, 51).unwrap(), 128);
    drop(transport);
}

#[test]
fn admitting_records_does_not_broadcast_available_capacity() {
    let (mut writer, mut shared) = channel(config(4, 128));
    let capacity = shared.capacity.generation();
    writer.admit(&mut input(3, b"first"), 5).unwrap().unwrap();
    writer.admit(&mut input(4, b"second"), 6).unwrap().unwrap();
    assert_eq!(shared.capacity.generation(), capacity);
    confirm(&mut shared, 2, 0).unwrap();
    assert!(shared.capacity.generation() > capacity);
}

#[tokio::test]
async fn fanring_arrivals_survive_canceled_waits_without_duplicate_control_wakes() {
    use futures::FutureExt;
    let (mut writer, mut shared) = channel(config(4, 128));
    assert!(shared.work.ready().now_or_never().is_none());
    writer.admit(&mut input(3, b"first"), 5).unwrap().unwrap();
    writer.admit(&mut input(4, b"second"), 6).unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(1), shared.work.ready())
        .await
        .unwrap();
    let work = shared.work.clone();
    work.drain(|| shared.drain());
    assert_eq!(shared.admission.records.len(), 2);
    let work = shared.work.clone();
    work.drain(|| {
        writer.admit(&mut input(5, b"third"), 5).unwrap().unwrap();
    });
    tokio::time::timeout(Duration::from_secs(1), shared.work.ready())
        .await
        .unwrap();
    let work = shared.work.clone();
    work.drain(|| shared.drain());
    assert_eq!(shared.admission.records.len(), 3);
    assert!(shared.work.ready().now_or_never().is_none());
    shared.fail(WriterError::Closed);
    assert!(shared.work.ready().now_or_never().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_observers_keep_the_confirmed_prefix_when_failure_follows() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for round in 0..256 {
            let mut settings = config(2, 128);
            settings.next_sequence = round * 2;
            let first_sequence = settings.next_sequence;
            let (mut writer, mut shared) = channel(settings);
            let first = Arc::new(writer.send(input(3, b"first")).await.unwrap());
            let second = writer.send(input(4, b"second")).await.unwrap();
            let mut observers = Vec::new();
            for _ in 0..4 {
                let first = first.clone();
                observers.push(tokio::spawn(async move { first.confirmed().await }));
            }
            let failed = tokio::spawn(async move { second.confirmed().await });
            tokio::task::yield_now().await;
            let base = (1_u64 << 40) + round;
            confirm(&mut shared, first_sequence + 1, base).unwrap();
            if round % 2 == 0 {
                tokio::task::yield_now().await;
            }
            shared.fail(WriterError::Configuration);
            shared.fail(WriterError::Closed);
            assert!(matches!(
                writer.send(input(5, b"late")).await,
                Err(WriterError::Configuration)
            ));
            drop(writer);
            drop(shared);
            for observer in observers {
                assert_eq!(
                    observer.await.unwrap().unwrap().offset,
                    base + first_sequence
                );
            }
            assert!(matches!(
                failed.await.unwrap(),
                Err(WriterError::Configuration)
            ));
            assert_eq!(
                first.confirmed().await.unwrap().offset,
                base + first_sequence
            );
        }
    })
    .await
    .expect("confirmation/failure race stranded an observer");
}

#[test]
fn invalid_confirmation_cannot_publish_a_new_prefix_or_offset_base() {
    let (mut writer, mut shared) = channel(config(2, 128));
    writer.admit(&mut input(3, b"first"), 5).unwrap().unwrap();
    writer.admit(&mut input(4, b"second"), 6).unwrap().unwrap();
    confirm(&mut shared, 1, 90).unwrap();
    for (end, base) in [(0, 90), (3, 90), (2, 89), (2, u64::MAX)] {
        assert!(confirm(&mut shared, end, base).is_err());
        assert_eq!(shared.progress.confirmed(), 1);
    }
    confirm(&mut shared, 2, 90).unwrap();
    assert_eq!(shared.progress.confirmed(), 2);
}

#[tokio::test]
async fn terminal_failure_rejects_late_confirmation_and_keeps_results_stable() {
    let (mut writer, mut shared) = channel(config(2, 128));
    let first = writer.send(input(3, b"first")).await.unwrap();
    let second = writer.send(input(4, b"second")).await.unwrap();
    confirm(&mut shared, 1, 90).unwrap();
    shared.fail(WriterError::Configuration);

    assert_eq!(first.confirmed().await.unwrap().offset, 90);
    assert!(matches!(
        second.confirmed().await,
        Err(WriterError::Configuration)
    ));
    assert!(matches!(
        confirm(&mut shared, 2, 90),
        Err(WriterError::Configuration)
    ));
    assert_eq!(shared.progress.confirmed(), 1);
    assert!(matches!(
        second.try_confirmed(),
        Some(Err(WriterError::Configuration))
    ));
}
