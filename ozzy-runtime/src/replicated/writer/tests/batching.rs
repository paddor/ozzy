use super::super::batch::Batch;
use super::*;
use futures::FutureExt;
use tokio::time::Instant;

#[tokio::test]
async fn one_k_group_cap_drains_without_waiting_for_confirmation() {
    const RECORDS: usize = 1024;
    let mut settings = config(3, RECORDS * 16);
    settings.max_producers = 1;
    settings.limits.max_records = RECORDS;
    settings.limits.max_parts = RECORDS;
    settings.limits.envelope.max_metadata_bytes = 1024 * 1024;
    settings.parameters_with_local_group(false).unwrap();
    // Use the production constructor, not the small-lane test helper.
    let (mut writer, mut driver) =
        Shared::channel_with_capacity(settings.clone(), settings.lane_records());
    let mut receipts = Vec::new();
    for _ in 0..RECORDS {
        receipts.push(writer.send(input(1, b"0123456789abcdef")).await.unwrap());
    }
    assert_eq!(driver.inbox.used(), RECORDS);
    assert!(writer.send(input(2, b"next")).now_or_never().is_none());
    let payload = packed(&mut driver, 0, RECORDS * 2);
    assert_eq!(payload.len(), RECORDS * 16);
    assert_eq!(driver.progress.confirmed(), 0);
    driver.stage(RECORDS as u64);
    writer.send(input(2, b"next")).await.unwrap();
    let next = packed(&mut driver, RECORDS as u64, RECORDS * 2);
    assert_eq!(next.as_slice(), b"next"); // No full-batch wait.
    confirm(&mut driver, RECORDS as u64 / 2, 100).unwrap();
    assert_eq!(receipts[0].confirmed().await.unwrap().offset, 100);
    assert!(receipts[RECORDS - 1].confirmed().now_or_never().is_none());
    confirm(&mut driver, RECORDS as u64, 100).unwrap();
    assert_eq!(
        receipts[RECORDS - 1].confirmed().await.unwrap().offset,
        100 + RECORDS as u64 - 1
    );
}

#[tokio::test]
async fn renegotiated_record_ceiling_checks_every_record_in_a_request() {
    let mut settings = config(4, 64);
    settings.limits.max_records = 4;
    let (mut writer, mut shared) = channel(settings);
    writer.send(input(1, b"ok")).await.unwrap();
    writer.send(input(2, b"too large")).await.unwrap();
    let limits = DataLimits {
        max_record_bytes: 2,
        ..shared.config.limits
    };
    let mut batch = Batch::new(4);
    assert!(batch.select_ready(0, limits, 4, 64, &mut shared).unwrap());
    assert_eq!(batch.records.len(), 1);
    assert!(matches!(
        batch.select_ready(1, limits, 4, 64, &mut shared),
        Err(Error::Configuration)
    ));
}

#[tokio::test]
async fn four_mib_target_sends_ready_records_and_oversized_singleton_whole() {
    const MIB: usize = 1024 * 1024;
    let mut settings = config(8, 16 * MIB);
    settings.limits.max_records = 8;
    settings.limits.max_record_bytes = 16 * MIB;
    let (mut writer, mut shared) = channel(settings);
    for (id, bytes) in [(1, 3 * MIB), (2, 2 * MIB), (3, 5 * MIB), (4, 1)] {
        writer
            .send(RecordInput::single(
                MessageId::from_bytes([id; 16]),
                Bytes::from(vec![id; bytes]),
            ))
            .await
            .unwrap();
    }
    let mut batch = Batch::new(8);
    for (sequence, bytes) in [(0, 3 * MIB), (1, 2 * MIB), (2, 5 * MIB), (3, 1)] {
        assert!(
            batch
                .select_ready(sequence, shared.config.limits, 8, 16 * MIB, &mut shared)
                .unwrap()
        );
        assert_eq!((batch.records.len(), batch.bytes), (1, bytes));
        assert_eq!(batch.take_payload().unwrap().len(), bytes);
        assert!(batch.deadline.is_none());
    }
}

#[tokio::test]
async fn transport_views_and_retries_cannot_reuse_live_packing_slots() {
    let (mut shared, mut writer) = setup(4, 32);
    // Two retained buffers exercise pool exhaustion independently of ACKs.
    shared.batches = super::super::batch::PayloadPool::new(
        &WriterConfig {
            inflight_appends: 2,
            ..shared.config.clone()
        },
        &shared.work,
    );
    for id in 3..7 {
        writer.send(input(id, b"abc")).await.unwrap();
    }
    let first = packed(&mut shared, 0, 2);
    let pointer = first.as_slice().as_ptr();
    let view = first.as_bytes();
    let retry = packed(&mut shared, 0, 2);
    let mut batch = Batch::new(2);
    assert!(
        !batch
            .select_ready(2, shared.config.limits, 2, 32, &mut shared)
            .unwrap()
    );
    confirm(&mut shared, 4, 0).unwrap();
    drop(first);
    // Confirmed originals are gone; transport views still own their bytes.
    writer.send(input(7, b"xyz")).await.unwrap();
    writer.send(input(8, b"xyz")).await.unwrap();
    assert!(
        !batch
            .select_ready(4, shared.config.limits, 2, 32, &mut shared)
            .unwrap()
    );
    assert_eq!(view.as_ref(), b"abcabc");
    shared.work.drain(|| {});
    drop(view);
    assert!(shared.work.ready().now_or_never().is_some());
    let reused = packed(&mut shared, 4, 2);
    assert_eq!(reused.as_slice().as_ptr(), pointer);
    assert_eq!(reused.as_slice(), b"xyzxyz");
    drop(writer);
    drop(shared);
    assert_eq!(retry.as_slice(), b"abcabc");
    assert_eq!(reused.as_slice(), b"xyzxyz");
}

fn packed(shared: &mut state::Driver, next: u64, count: usize) -> omq_tokio::message::Payload {
    let mut batch = Batch::new(count);
    assert!(
        batch
            .select_ready(next, shared.config.limits, count, usize::MAX, shared)
            .unwrap()
    );
    batch.take_payload().unwrap()
}

#[tokio::test]
async fn broker_packing_slots_remain_bounded_across_session_refreshes() {
    let (mut shared, mut writer) = setup(4, 32);
    let settings = WriterConfig {
        inflight_appends: 1,
        ..shared.config.clone()
    };
    let work = shared.work.clone();
    shared.batches = super::super::batch::PayloadPool::new(&settings, &work);
    for id in 3..5 {
        writer.send(input(id, b"abc")).await.unwrap();
    }
    let mut held = Vec::new();
    for route in 0..3 {
        shared.batches.select_route(route, &settings, &work);
        held.push(Some(packed(&mut shared, 0, 2)));
    }
    confirm(&mut shared, 2, 0).unwrap();
    for id in 5..7 {
        writer.send(input(id, b"xyz")).await.unwrap();
    }
    let mut batch = Batch::new(2);
    for route in [0, 1, 2, 0, 1, 2] {
        shared.batches.select_route(route, &settings, &work);
        assert!(
            !batch
                .select_ready(2, shared.config.limits, 2, 32, &mut shared)
                .unwrap()
        );
        assert!(batch.waiting_for_payload);
        assert_eq!(held[route].as_ref().unwrap().as_slice(), b"abcabc");
    }
    let pointer = held[1].as_ref().unwrap().as_slice().as_ptr();
    held[1] = None;
    shared.batches.select_route(1, &settings, &work);
    let reused = packed(&mut shared, 2, 2);
    assert_eq!(reused.as_slice().as_ptr(), pointer);
    assert_eq!(reused.as_slice(), b"xyzxyz");
    assert_eq!(held[0].as_ref().unwrap().as_slice(), b"abcabc");
    assert_eq!(held[2].as_ref().unwrap().as_slice(), b"abcabc");
}

#[tokio::test]
async fn request_preparation_returns_inbox_space_without_confirmation_or_transport_release() {
    let (mut shared, mut writer) = setup(2, 32);
    for id in 3..5 {
        writer.send(input(id, b"abc")).await.unwrap();
    }
    let payload = packed(&mut shared, 0, 2);
    shared.stage(2);
    assert_eq!(shared.inbox.used(), 0);
    for id in 5..7 {
        writer.send(input(id, b"xyz")).await.unwrap();
    }
    assert_eq!(shared.inbox.used(), 2);
    assert_eq!(shared.progress.confirmed(), 0);
    shared.stage(1); // A smaller regrouped retry cannot return space twice.
    shared.stage(2);
    assert_eq!(shared.inbox.used(), 2);
    assert_eq!(payload.as_slice(), b"abcabc");
    confirm(&mut shared, 4, 0).unwrap();
}

fn setup(records: usize, bytes: usize) -> (state::Driver, Writer) {
    let mut settings = config(records, bytes);
    settings.limits.max_records = records.min(1024);
    settings.limits.max_parts = records * 4;
    settings.limits.envelope.max_metadata_bytes = 64 * 1024;
    let (writer, driver) = channel(settings);
    (driver, writer)
}

#[tokio::test(start_paused = true)]
async fn zero_linger_sends_sparse_records_and_caps_whole_records_by_every_limit() {
    let (mut shared, mut writer) = setup(8, 32);
    writer.send(input(3, b"abc")).await.unwrap();
    writer.send(input(4, b"def")).await.unwrap();
    assert!(shared.record(0).unwrap().linger_deadline.is_none());
    assert!(shared.record(1).unwrap().linger_deadline.is_none());
    let mut batch = Batch::new(8);
    let limits = shared.config.limits;
    assert!(batch.select_ready(0, limits, 8, 32, &mut shared).unwrap());
    assert_eq!((batch.records.len(), batch.bytes), (2, 6));
    assert!(batch.deadline.is_none());
    for bounded in [
        DataLimits {
            max_records: 1,
            ..limits
        },
        DataLimits {
            max_parts: 1,
            ..limits
        },
        DataLimits {
            envelope: EnvelopeLimits {
                max_payload_bytes: 5,
                ..limits.envelope
            },
            ..limits
        },
        DataLimits {
            envelope: EnvelopeLimits {
                max_metadata_bytes: 123,
                ..limits.envelope
            },
            ..limits
        },
    ] {
        assert!(batch.select_ready(0, bounded, 8, 32, &mut shared).unwrap());
        assert_eq!(batch.records.len(), 1);
    }
    assert!(!batch.select_ready(0, limits, 0, 32, &mut shared).unwrap());
    assert!(!batch.select_ready(0, limits, 8, 2, &mut shared).unwrap());
    assert!(batch.select_ready(1, limits, 8, 32, &mut shared).unwrap());
    assert_eq!(batch.records.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn linger_deadline_does_not_move_and_flush_captures_only_its_prefix() {
    let mut settings = config(8, 64);
    settings.limits.max_records = 8;
    settings.limits.max_parts = 16;
    settings.linger = Duration::from_secs(1);
    let (mut writer, mut shared) = channel(settings);
    writer.send(input(3, b"a")).await.unwrap();
    let mut batch = Batch::new(8);
    assert!(
        !batch
            .select_ready(0, shared.config.limits, 8, 64, &mut shared)
            .unwrap()
    );
    let deadline = batch.deadline.unwrap();
    assert_eq!(shared.record(0).unwrap().linger_deadline, Some(deadline));
    tokio::time::advance(Duration::from_millis(900)).await;
    writer.send(input(4, b"b")).await.unwrap();
    assert!(
        !batch
            .select_ready(0, shared.config.limits, 8, 64, &mut shared)
            .unwrap()
    );
    assert_eq!(batch.deadline, Some(deadline));
    let flush = writer.flush(); // Its effect does not require polling the future.
    writer.send(input(5, b"c")).await.unwrap();
    assert!(
        batch
            .select_ready(0, shared.config.limits, 8, 64, &mut shared)
            .unwrap()
    );
    assert_eq!(batch.records.len(), 2);
    assert!(
        !batch
            .select_ready(2, shared.config.limits, 8, 64, &mut shared)
            .unwrap()
    );
    assert_eq!(
        batch.deadline,
        Some(Instant::now() + Duration::from_secs(1))
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        batch
            .select_ready(2, shared.config.limits, 8, 64, &mut shared)
            .unwrap()
    );
    batch.records.clear();
    confirm(&mut shared, 2, 90).unwrap();
    flush.await.unwrap();
}

#[tokio::test]
async fn simultaneous_transport_releases_return_a_packing_slot_exactly_once() {
    let (mut shared, mut writer) = setup(2, 32);
    let mut batch = Batch::new(2);
    for sequence in (0..64).step_by(2) {
        writer.send(input(3, b"abc")).await.unwrap();
        writer.send(input(4, b"def")).await.unwrap();
        assert!(
            batch
                .select_ready(sequence, shared.config.limits, 2, 32, &mut shared)
                .unwrap()
        );
        let first = batch.take_payload().unwrap();
        let second = first.clone();
        batch.records.clear();
        confirm(&mut shared, sequence + 2, 0).unwrap();
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                drop(first);
            });
            scope.spawn(|| {
                barrier.wait();
                drop(second);
            });
        });
    }
}

#[tokio::test]
async fn large_records_obey_credit_and_hard_limit_without_truncation() {
    const TARGET: usize = 1024 * 1024;
    let mut settings = config(8, 4 * TARGET);
    settings.limits.max_records = 8;
    settings.limits.envelope.max_payload_bytes = 2 * TARGET;
    let (mut writer, mut shared) = channel(settings);
    writer.send(input(1, b"before")).await.unwrap();
    writer
        .send(RecordInput::multipart(
            MessageId::from_bytes([2; 16]),
            [
                Bytes::from(vec![7; TARGET]),
                Bytes::new(),
                Bytes::from(vec![8; TARGET]),
            ],
        ))
        .await
        .unwrap();
    writer.send(input(3, b"")).await.unwrap();
    writer.send(input(4, b"after")).await.unwrap();
    let mut batch = Batch::new(8);
    assert!(
        batch
            .select_ready(0, shared.config.limits, 8, 4 * TARGET, &mut shared)
            .unwrap()
    );
    assert_eq!((batch.records.len(), batch.bytes), (1, 6));
    assert!(
        !batch
            .select_ready(1, shared.config.limits, 8, 2 * TARGET - 1, &mut shared)
            .unwrap()
    );
    assert!(
        batch
            .select_ready(1, shared.config.limits, 8, 2 * TARGET, &mut shared)
            .unwrap()
    );
    assert_eq!((batch.records.len(), batch.bytes), (2, 2 * TARGET));
    assert!(
        batch
            .select_ready(2, shared.config.limits, 8, 2 * TARGET, &mut shared)
            .unwrap()
    );
    assert_eq!((batch.records.len(), batch.bytes), (2, 5));
    let too_big = RecordInput::multipart(
        MessageId::from_bytes([5; 16]),
        [Bytes::from(vec![0; 2 * TARGET + 1])],
    );
    assert!(matches!(
        writer.send(too_big).await,
        Err(WriterError::RecordLimits)
    ));
}

impl Batch {
    fn select_ready(
        &mut self,
        next: u64,
        limits: DataLimits,
        records: usize,
        bytes: usize,
        driver: &mut state::Driver,
    ) -> Result<bool, Error> {
        driver.settle();
        self.select(next, limits, records, bytes, driver)
    }
}
