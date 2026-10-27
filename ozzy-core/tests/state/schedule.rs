use super::*;
use ozzy_core::state::{DelayState, StateSnapshotLimits};
use ozzy_journal::operation::{Release, ReleaseKey, Schedule};
use ozzy_proto::append::{AppendDisposition, AppendOutcome};

const MAX_DELAY: u64 = 60_000;

fn create_delayed() -> OperationBody<'static> {
    let OperationBody::CreatePartition(mut value) = create() else {
        unreachable!()
    };
    value.delay_max_millis = NonZeroU64::new(MAX_DELAY);
    OperationBody::CreatePartition(value)
}

fn record(message: u8, payload: &'static [u8]) -> AppendRecord<'static> {
    AppendRecord {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([message; 16]),
        parts: vec![payload].into(),
    }
}

fn batch(
    first_sequence: u64,
    first_offset: u64,
    time: u64,
    messages: &[u8],
) -> AppendBatch<'static> {
    AppendBatch {
        partition: partition(),
        owner_epoch: OwnerEpoch::new(9),
        producer_id: producer(),
        producer_epoch: ProducerEpoch::new(3),
        first_sequence: ProducerSequence::new(first_sequence),
        first_offset: Offset::new(first_offset),
        append_timestamp_millis: time,
        records: messages
            .iter()
            .map(|&message| record(message, b"held"))
            .collect::<Vec<_>>()
            .into(),
    }
}

/// Records `(message, due)` accepted at `time`.
fn schedule(
    first_sequence: u64,
    first_schedule: u64,
    time: u64,
    records: &[(u8, u64)],
) -> OperationBody<'static> {
    let messages: Vec<u8> = records.iter().map(|(message, _)| *message).collect();
    OperationBody::Schedule(Schedule {
        batch: batch(first_sequence, 0, time, &messages),
        first_schedule,
        due_millis: records.iter().map(|(_, due)| *due).collect(),
    })
}

fn release_batch(first_offset: u64, time: u64, messages: &[u8]) -> AppendBatch<'static> {
    AppendBatch {
        producer_id: ProducerId::from_bytes([0; 16]),
        producer_epoch: ProducerEpoch::new(0),
        first_sequence: ProducerSequence::ZERO,
        ..batch(0, first_offset, time, messages)
    }
}

fn release(
    first_offset: u64,
    time: u64,
    through: ReleaseKey,
    messages: &[u8],
) -> OperationBody<'static> {
    OperationBody::Release(Release {
        batch: release_batch(first_offset, time, messages),
        through,
    })
}

const fn key(millis: u64, schedule: u64) -> ReleaseKey {
    ReleaseKey { millis, schedule }
}

fn delayed_with(limits: StateLimits) -> CanonicalImages {
    let mut images = CanonicalImages::new(limits, 64, 8);
    images.admit(1, &create_delayed()).unwrap();
    images.admit(2, &open()).unwrap();
    images.commit_through(2).unwrap();
    images
}

fn delayed() -> CanonicalImages {
    delayed_with(StateLimits::default())
}

fn held(images: &CanonicalImages) -> &DelayState {
    images
        .speculative()
        .partition(partition())
        .unwrap()
        .delay
        .as_ref()
        .unwrap()
}

/// Held message bytes in release order.
fn held_messages(images: &CanonicalImages) -> Vec<(ReleaseKey, u8)> {
    held(images)
        .pending()
        .map(|(key, value)| (key, value.message_id.as_bytes()[0]))
        .collect()
}

fn state_error(error: StateError) -> Result<(), CanonicalImagesError> {
    Err(CanonicalImagesError::State(error))
}

#[test]
fn schedule_holds_records_in_release_order_without_offsets() {
    let mut images = delayed();
    images
        .admit(
            3,
            &schedule(0, 0, 1_000, &[(1, 5_000), (2, 3_000), (3, 900)]),
        )
        .unwrap();
    assert_eq!(
        held_messages(&images),
        [(key(1_000, 2), 3), (key(3_000, 1), 2), (key(5_000, 0), 1)]
    );
    let state = images.speculative().partition(partition()).unwrap();
    assert_eq!(state.next_offset, Offset::ZERO);
    let delay = state.delay.as_ref().unwrap();
    assert_eq!((delay.next_schedule, delay.time), (3, 1_000));
    assert_eq!(delay.oldest_operation(), Some(3));
    assert_eq!(delay.next_release(), Some(key(1_000, 2)));
    assert_eq!(
        delay.due(2_999).map(|(key, _)| key).collect::<Vec<_>>(),
        [key(1_000, 2)]
    );
    let held = delay.pending().next().unwrap().1;
    // Encoded bytes: a 24-byte descriptor and the 4-byte payload.
    assert_eq!((held.operation, held.index, held.bytes), (3, 2, 28));
    assert_eq!(delay.held_bytes(), 3 * 28);
    let writer = state.producer(producer()).unwrap();
    assert_eq!(writer.next_producer_sequence, ProducerSequence::new(3));
    for sequence in 0..3 {
        assert_eq!(
            writer.result_outcome(ProducerSequence::new(sequence)),
            Some(AppendOutcome {
                disposition: AppendDisposition::Scheduled,
                offset: Offset::new(sequence),
            })
        );
    }
    assert_eq!(writer.result_operation_floor(), Some(3));
}

#[test]
fn release_takes_the_due_prefix_at_the_next_offsets() {
    let mut images = delayed();
    images
        .admit(
            3,
            &schedule(0, 0, 1_000, &[(1, 5_000), (2, 3_000), (3, 900)]),
        )
        .unwrap();
    images
        .admit(4, &release(0, 3_000, key(3_000, 1), &[3, 2]))
        .unwrap();
    let state = images.speculative().partition(partition()).unwrap();
    assert_eq!(state.next_offset, Offset::new(2));
    assert_eq!(held_messages(&images), [(key(5_000, 0), 1)]);
    assert_eq!(held(&images).time, 3_000);
    assert_eq!(held(&images).oldest_operation(), Some(3));
    images
        .admit(5, &release(2, 6_000, key(5_000, 0), &[1]))
        .unwrap();
    images.commit_through(5).unwrap();
    let state = images.committed().partition(partition()).unwrap();
    assert_eq!(state.next_offset, Offset::new(3));
    let delay = state.delay.as_ref().unwrap();
    assert_eq!((delay.pending_len(), delay.held_bytes()), (0, 0));
    assert_eq!(delay.oldest_operation(), None);
    assert_eq!(delay.time, 6_000);
}

#[test]
fn equal_due_times_keep_schedule_order() {
    let mut images = delayed();
    images
        .admit(3, &schedule(0, 0, 1_000, &[(1, 2_000), (2, 2_000)]))
        .unwrap();
    images
        .admit(4, &schedule(2, 2, 1_500, &[(3, 2_000)]))
        .unwrap();
    assert_eq!(
        held_messages(&images),
        [(key(2_000, 0), 1), (key(2_000, 1), 2), (key(2_000, 2), 3)]
    );
    let before = images.clone();
    assert_eq!(
        images.admit(5, &release(0, 2_000, key(2_000, 2), &[2, 1, 3])),
        state_error(StateError::ReleaseMismatch)
    );
    assert_eq!(images, before);
    images
        .admit(5, &release(0, 2_000, key(2_000, 2), &[1, 2, 3]))
        .unwrap();
    assert_eq!(held(&images).oldest_operation(), None);
}

#[test]
fn release_must_match_the_held_prefix_exactly() {
    let mut images = delayed();
    images
        .admit(3, &schedule(0, 0, 1_000, &[(1, 2_000), (2, 3_000)]))
        .unwrap();
    let before = images.clone();
    let named = OperationBody::Release(Release {
        batch: batch(0, 0, 2_000, &[1]),
        through: key(2_000, 0),
    });
    let OperationBody::Release(mut longer) = release(0, 2_000, key(2_000, 0), &[1]) else {
        unreachable!()
    };
    longer.batch.records = vec![record(1, b"held!")].into();
    let cases = [
        (
            release(0, 1_999, key(2_000, 0), &[1]),
            StateError::DelayTimeRegressed,
        ),
        (
            release(0, 2_000, key(2_000, 0), &[2]),
            StateError::ReleaseMismatch,
        ),
        (
            release(0, 3_000, key(3_000, 1), &[1]),
            StateError::ReleaseMismatch,
        ),
        (
            release(0, 3_000, key(2_000, 0), &[1, 2]),
            StateError::ReleaseMismatch,
        ),
        (
            release(0, 2_500, key(2_500, 0), &[1]),
            StateError::ReleaseMismatch,
        ),
        (OperationBody::Release(longer), StateError::ReleaseMismatch),
        (
            release(1, 2_000, key(2_000, 0), &[1]),
            StateError::OffsetMismatch,
        ),
        (
            named,
            StateError::MalformedBody("release batch must not name a producer"),
        ),
    ];
    for (body, error) in cases {
        assert_eq!(images.admit(4, &body), state_error(error));
        assert_eq!(images, before);
    }
    let first = release(0, 2_000, key(2_000, 0), &[1]);
    images.admit(4, &first).unwrap();
    // A repeated release finds its records gone.
    assert_eq!(
        images.admit(5, &release(1, 2_000, key(2_000, 0), &[1])),
        state_error(StateError::ReleaseMismatch)
    );
}

#[test]
fn schedule_checks_delay_sequences_time_and_capacity() {
    let mut images = delayed();
    images
        .admit(3, &schedule(0, 0, 1_000, &[(1, 1_000 + MAX_DELAY)]))
        .unwrap();
    let before = images.clone();
    let OperationBody::Schedule(mut offset) = schedule(1, 1, 1_000, &[(2, 1_000)]) else {
        unreachable!()
    };
    offset.batch.first_offset = Offset::new(1);
    let cases = [
        (
            schedule(1, 1, 1_000, &[(2, 1_001 + MAX_DELAY)]),
            StateError::DueBeyondMaximum,
        ),
        (
            schedule(1, 0, 1_000, &[(2, 1_000)]),
            StateError::ScheduleMismatch,
        ),
        (
            schedule(2, 1, 1_000, &[(2, 1_000)]),
            StateError::ProducerSequenceMismatch,
        ),
        (
            schedule(1, 1, 999, &[(2, 1_000)]),
            StateError::DelayTimeRegressed,
        ),
        (OperationBody::Schedule(offset), StateError::OffsetMismatch),
    ];
    for (body, error) in cases {
        assert_eq!(images.admit(4, &body), state_error(error));
        assert_eq!(images, before);
    }

    let mut images = delayed_with(StateLimits {
        max_pending_schedules: 2,
        ..StateLimits::default()
    });
    assert_eq!(
        images.admit(3, &schedule(0, 0, 1_000, &[(1, 0), (2, 0), (3, 0)])),
        state_error(StateError::LimitExceeded("pending schedules"))
    );
    images
        .admit(3, &schedule(0, 0, 1_000, &[(1, 0), (2, 0)]))
        .unwrap();

    let mut plain = CanonicalImages::new(StateLimits::default(), 64, 8);
    plain.admit(1, &create()).unwrap();
    plain.admit(2, &open()).unwrap();
    assert_eq!(
        plain.admit(3, &schedule(0, 0, 1_000, &[(1, 0)])),
        state_error(StateError::DelayDisabled)
    );
    assert_eq!(
        plain.admit(3, &release(0, 1_000, key(1_000, 0), &[1])),
        state_error(StateError::DelayDisabled)
    );
}

#[test]
fn delay_excludes_deduplication() {
    let OperationBody::CreatePartition(mut value) = create_delayed() else {
        unreachable!()
    };
    value.dedup_window_millis = NonZeroU64::new(1_000);
    let mut images = CanonicalImages::new(StateLimits::default(), 64, 8);
    assert_eq!(
        images.admit(1, &OperationBody::CreatePartition(value)),
        state_error(StateError::DelayWithDeduplication)
    );
}

#[test]
fn discarded_release_holds_its_records_again() {
    let mut images = delayed();
    images
        .admit(3, &schedule(0, 0, 1_000, &[(1, 2_000), (2, 3_000)]))
        .unwrap();
    images.commit_through(3).unwrap();
    let held_before = held_messages(&images);
    images
        .admit(4, &release(0, 3_000, key(3_000, 1), &[1, 2]))
        .unwrap();
    images.discard_suffix();
    assert_eq!(held_messages(&images), held_before);
    assert_eq!(images.speculative(), images.committed());
    images
        .admit(4, &release(0, 2_000, key(2_000, 0), &[1]))
        .unwrap();
    images.commit_through(4).unwrap();
    assert_eq!(held_messages(&images), [(key(3_000, 1), 2)]);
}

#[test]
fn snapshot_restores_held_records_and_scheduled_results() {
    let mut images = delayed();
    images
        .admit(3, &schedule(0, 0, 1_000, &[(1, 5_000), (2, 3_000)]))
        .unwrap();
    images
        .admit(4, &schedule(2, 2, 1_200, &[(3, 1_100)]))
        .unwrap();
    images
        .admit(5, &release(0, 1_200, key(1_200, 2), &[3]))
        .unwrap();
    images.commit_through(5).unwrap();
    let state = images.committed();
    let restored = CanonicalState::decode_snapshot(
        &state
            .encode_snapshot(StateSnapshotLimits::default())
            .unwrap(),
        StateLimits::default(),
        StateSnapshotLimits::default(),
    )
    .unwrap();
    assert_eq!(&restored, state);
    let delay = restored
        .partition(partition())
        .unwrap()
        .delay
        .as_ref()
        .unwrap();
    assert_eq!(delay.oldest_operation(), Some(3));
    assert_eq!(delay.held_bytes(), 2 * 28);
    assert_eq!((delay.next_schedule, delay.time), (3, 1_200));
    assert!(matches!(
        CanonicalState::decode_snapshot(
            &state
                .encode_snapshot(StateSnapshotLimits::default())
                .unwrap(),
            StateLimits {
                max_pending_schedules: 1,
                ..StateLimits::default()
            },
            StateSnapshotLimits::default(),
        ),
        Err(ozzy_core::state::StateSnapshotError::LimitExceeded { .. })
    ));
}

#[test]
fn scheduled_results_hold_their_operation_until_floored() {
    let mut images = delayed();
    images
        .admit(3, &schedule(0, 0, 1_000, &[(1, 2_000)]))
        .unwrap();
    images
        .admit(4, &schedule(1, 1, 1_000, &[(2, 9_000)]))
        .unwrap();
    images
        .admit(5, &release(0, 2_000, key(2_000, 0), &[1]))
        .unwrap();
    images.commit_through(5).unwrap();
    let state = images.committed().partition(partition()).unwrap();
    assert_eq!(state.delay.as_ref().unwrap().oldest_operation(), Some(4));
    let writer = state.producer(producer()).unwrap();
    assert_eq!(writer.result_operation_floor(), Some(3));
    // A plan that keeps operation 3 keeps its retry result.
    assert_eq!(
        writer.result_floor_for_retention(Offset::new(1), 3),
        ProducerSequence::ZERO
    );
    // Retiring operation 3 expires its span; operation 4 stays.
    let floor = writer.result_floor_for_retention(Offset::new(1), 4);
    assert_eq!(floor, ProducerSequence::new(1));
    images
        .admit(
            6,
            &OperationBody::ProducerResultFloor(ProducerResultFloor {
                partition: partition(),
                producer_id: producer(),
                producer_epoch: ProducerEpoch::new(3),
                expected_floor: ProducerSequence::ZERO,
                new_floor: floor,
                operation_id: operation(0x60),
            }),
        )
        .unwrap();
    images.commit_through(6).unwrap();
    let writer = images
        .committed()
        .partition(partition())
        .unwrap()
        .producer(producer())
        .unwrap();
    assert_eq!(writer.result_operation_floor(), Some(4));
    assert_eq!(
        writer.result_outcome(ProducerSequence::new(1)),
        Some(AppendOutcome {
            disposition: AppendDisposition::Scheduled,
            offset: Offset::new(1),
        })
    );
}

#[test]
fn checksum_valid_snapshots_reject_inconsistent_held_records() {
    use ozzy_core::state::StateSnapshotError;
    let mut images = delayed();
    images
        .admit(3, &schedule(0, 0, 1_000, &[(1, 5_000), (2, 3_000)]))
        .unwrap();
    images.commit_through(3).unwrap();
    let encoded = images
        .committed()
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    let partition = 256;
    let names = usize::from(u16::from_be_bytes(encoded[260..262].try_into().unwrap()))
        + usize::from(u16::from_be_bytes(encoded[262..264].try_into().unwrap()));
    let first = partition + 144 + names + 80 + 40;
    let second = first + 48;
    let reseal = |damaged: &mut Vec<u8>| {
        let mut hash =
            ozzy_journal::integrity::IntegrityHasher::new("ozzy canonical state snapshot v1");
        hash.update(&damaged[..56]);
        hash.update(&[0; 32]);
        hash.update(&damaged[88..]);
        damaged[56..88].copy_from_slice(hash.finish().as_bytes());
    };
    let cases: [(usize, &[u8], StateSnapshotError); 6] = [
        // Held sequences must be issued.
        (
            partition + 120,
            &1_u64.to_be_bytes(),
            StateSnapshotError::InvalidPartition,
        ),
        // Delay fields without a maximum delay.
        (
            partition + 112,
            &0_u64.to_be_bytes(),
            StateSnapshotError::InvalidPartition,
        ),
        (
            partition + 140,
            &1_u32.to_be_bytes(),
            StateSnapshotError::UnsupportedFields,
        ),
        // A held record names an operation.
        (
            first + 16,
            &0_u64.to_be_bytes(),
            StateSnapshotError::InvalidPartition,
        ),
        // Release order is strict.
        (
            first,
            &9_000_u64.to_be_bytes(),
            StateSnapshotError::UnsortedOrDuplicate,
        ),
        // One schedule sequence per held record.
        (
            second + 8,
            &1_u64.to_be_bytes(),
            StateSnapshotError::UnsortedOrDuplicate,
        ),
    ];
    for (field, bytes, error) in cases {
        let mut damaged = encoded.clone();
        damaged[field..field + bytes.len()].copy_from_slice(bytes);
        reseal(&mut damaged);
        assert_eq!(
            CanonicalState::decode_snapshot(
                &damaged,
                StateLimits::default(),
                StateSnapshotLimits::default(),
            ),
            Err(error),
            "field {field}"
        );
    }
}
