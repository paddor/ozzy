use super::*;
use crate::state::{CanonicalPartition, CanonicalProducer, PartitionAddress};
use ozzy_proto::{OwnerEpoch, PartitionId, ProducerEpoch, ProducerId, ProducerSequence};
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

fn state(writers: usize, spans: usize, metadata: usize) -> CanonicalState {
    let mut state = CanonicalState::new(StateLimits::default());
    let id = PartitionIncarnation::from_bytes([1; 16]);
    let address = PartitionAddress {
        stream: "cluster".into(),
        topic: "events".into(),
        partition_id: PartitionId::new(0),
    };
    let mut partition = CanonicalPartition {
        address: address.clone(),
        owner_epoch: OwnerEpoch::INITIAL,
        producers: ahash::AHashMap::new(),
        next_offset: Offset::new((writers * spans) as u64),
        retained_from: Offset::ZERO,
        policy_revision: 1,
        retention: RetentionPolicy::default(),
    };
    for writer in 0..writers {
        let mut producer = CanonicalProducer::new(ProducerEpoch::INITIAL);
        for sequence in 0..spans {
            producer
                .record_assignment(
                    ProducerSequence::new(sequence as u64),
                    Offset::new((sequence * writers + writer) as u64),
                    1,
                    spans,
                )
                .unwrap();
        }
        state.retry_span_count += producer.results.len();
        partition.producers.insert(
            ProducerId::from_bytes((writer as u128 + 1).to_be_bytes()),
            producer,
        );
    }
    state.producer_count = writers;
    for number in 1..=metadata {
        state.progress.insert(
            ProgressKey::Subscription(
                SubscriptionId::from_bytes((number as u128).to_be_bytes()),
                id,
            ),
            Offset::ZERO,
        );
        state.assignments.insert(
            AssignmentKey(
                ConsumerGroupId::from_bytes((number as u128).to_be_bytes()),
                id,
            ),
            AssignmentState {
                epoch: 1,
                member: None,
            },
        );
    }
    state.addresses.insert(address, id);
    state.partitions.insert(id, partition);
    state.revision = 42;
    state
}

async fn yield_step(bytes: usize) {
    assert!(bytes <= 64 * 1024, "codec step exceeds its byte unit");
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

fn run<T>(future: impl Future<Output = T>) -> (T, usize) {
    let mut future = pin!(future);
    for polls in 1..1_000_000 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return (value, polls);
        }
    }
    panic!("cooperative snapshot codec exceeded its work bound")
}

#[test]
fn canonical_snapshot_encoding_bytes_fixture() {
    // Golden digests for the Ozzy snapshot domain. These include
    // empty writers, interleaved retry ranges, progress, and assignments.
    for (writers, spans, metadata, length, digest) in [
        (
            130,
            0,
            0,
            6605,
            0x073e_3551_2a3b_0136_fb1b_7ddb_6bfd_68a3_u128,
        ),
        (
            2,
            130,
            130,
            21261,
            0x749a_1e5f_7a1d_b0b6_84b4_edc2_370c_36fb,
        ),
        (
            2,
            6000,
            130,
            303_021,
            0x67b6_ceae_7bf4_f7ec_8954_dedd_ebc8_3e96,
        ),
    ] {
        let state = state(writers, spans, metadata);
        let (bytes, turns) =
            run(state.encode_snapshot_cooperative(StateSnapshotLimits::default(), yield_step));
        let bytes = bytes.unwrap();
        assert!(turns > writers + spans + metadata);
        assert_eq!(bytes.len(), length);
        let mut expected = [0; 32];
        expected[..16].copy_from_slice(&digest.to_be_bytes());
        assert_eq!(snapshot_digest(&bytes).as_bytes(), &expected);
        assert_eq!(
            &bytes[SNAPSHOT_DIGEST_START..SNAPSHOT_DIGEST_END],
            &expected
        );
        assert_eq!(
            bytes,
            state
                .encode_snapshot(StateSnapshotLimits::default())
                .unwrap()
        );
        assert_eq!(
            CanonicalState::decode_snapshot(
                &bytes,
                StateLimits::default(),
                StateSnapshotLimits::default(),
            )
            .unwrap(),
            state
        );
    }
}

#[test]
fn cooperative_encoding_cancellation_keeps_state_and_exact_retry_results() {
    let state = state(2, 6000, 130);
    let original = state.clone();
    let limits = StateSnapshotLimits::default();
    let (bytes, turns) = run(state.encode_snapshot_cooperative(limits, yield_step));
    let bytes = bytes.unwrap();
    for cut in [1, 2, 20, 500, 20_000, turns - 2] {
        let mut future = Box::pin(state.encode_snapshot_cooperative(limits, yield_step));
        for _ in 0..cut {
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        drop(future);
        assert_eq!(state, original);
    }
    assert_eq!(
        run(state.encode_snapshot_cooperative(limits, yield_step))
            .0
            .unwrap(),
        bytes
    );
}

#[test]
fn cooperative_encoding_checks_limits_and_late_writer_errors() {
    let mut state = state(130, 0, 0);
    let limits = StateSnapshotLimits {
        max_snapshot_bytes: STATE_SNAPSHOT_HEADER_BYTES,
        ..StateSnapshotLimits::default()
    };
    let (result, turns) = run(state.encode_snapshot_cooperative(limits, yield_step));
    assert!(turns > 130);
    assert!(matches!(
        result,
        Err(StateSnapshotError::LimitExceeded {
            kind: "state snapshot bytes",
            ..
        })
    ));
    state
        .partitions
        .values_mut()
        .next()
        .unwrap()
        .producers
        .get_mut(&ProducerId::from_bytes(130_u128.to_be_bytes()))
        .unwrap()
        .producer_epoch = ProducerEpoch::new(0);
    let (result, turns) =
        run(state.encode_snapshot_cooperative(StateSnapshotLimits::default(), yield_step));
    assert!(turns > 130);
    assert_eq!(result.unwrap_err(), StateSnapshotError::InvalidPartition);
}

#[test]
fn cooperative_state_snapshot_keeps_independent_writers_retry_ranges_and_progress() {
    for (writers, spans, metadata) in [(130, 0, 0), (2, 130, 130), (2, 6000, 130)] {
        let expected = state(writers, spans, metadata);
        let bytes = expected
            .encode_snapshot(StateSnapshotLimits::default())
            .unwrap();
        let (decoded, polls) = run(CanonicalState::decode_snapshot_cooperative(
            &bytes,
            StateLimits::default(),
            StateSnapshotLimits::default(),
            yield_step,
        ));
        let decoded = decoded.unwrap();
        assert_eq!(decoded, expected);
        assert!(polls > writers + spans + metadata);
        for writer in 0..writers {
            let producer = decoded
                .partition(PartitionIncarnation::from_bytes([1; 16]))
                .unwrap()
                .producer(ProducerId::from_bytes((writer as u128 + 1).to_be_bytes()))
                .unwrap();
            for sequence in 0..spans {
                assert_eq!(
                    producer.result_offset(ProducerSequence::new(sequence as u64)),
                    Some(Offset::new((sequence * writers + writer) as u64))
                );
            }
        }
        assert_eq!(
            CanonicalState::decode_snapshot(
                &bytes,
                StateLimits::default(),
                StateSnapshotLimits::default()
            )
            .unwrap(),
            expected
        );
    }
}

#[test]
fn cooperative_state_snapshot_cancellation_never_exposes_a_partial_image() {
    let expected = state(2, 6000, 130);
    let bytes = expected
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    let original = bytes.clone();
    for cut in [1, 2, 20, 500, 20_000] {
        let mut future = Box::pin(CanonicalState::decode_snapshot_cooperative(
            &bytes,
            StateLimits::default(),
            StateSnapshotLimits::default(),
            yield_step,
        ));
        for _ in 0..cut {
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        drop(future);
        assert_eq!(bytes, original);
    }
    let (restored, _) = run(CanonicalState::decode_snapshot_cooperative(
        &bytes,
        StateLimits::default(),
        StateSnapshotLimits::default(),
        yield_step,
    ));
    assert_eq!(restored.unwrap(), expected);
}

#[test]
fn cooperative_state_snapshot_refuses_late_semantic_errors_and_resource_limits() {
    let state = state(2, 130, 130);
    let original = state
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    let partition = STATE_SNAPSHOT_HEADER_BYTES;
    let names = usize::from(read_u16(&original, partition + 4))
        + usize::from(read_u16(&original, partition + 6));
    let first = partition + 96 + names;
    let second = first + 48 + 130 * 24;
    let progress = partition + read_u32(&original, partition) as usize;
    let assignments = progress + 130 * PROGRESS_BYTES;
    for (at, value, error) in [
        (second + 48 + 8, 0, StateSnapshotError::InvalidPartition),
        (
            first + 48 + 129 * 24,
            999,
            StateSnapshotError::InvalidPartition,
        ),
        (
            progress + 129 * PROGRESS_BYTES + 40,
            260,
            StateSnapshotError::ProgressBeyondState,
        ),
        (
            assignments + 129 * ASSIGNMENT_BYTES + 16,
            99,
            StateSnapshotError::UnknownPartition,
        ),
        (88, 3, StateSnapshotError::LengthMismatch),
        (96, 261, StateSnapshotError::LengthMismatch),
    ] {
        let mut bytes = original.clone();
        put_u64(&mut bytes, at, value);
        let digest = snapshot_digest(&bytes);
        bytes[SNAPSHOT_DIGEST_START..SNAPSHOT_DIGEST_END].copy_from_slice(digest.as_bytes());
        let (result, polls) = run(CanonicalState::decode_snapshot_cooperative(
            &bytes,
            StateLimits::default(),
            StateSnapshotLimits::default(),
            yield_step,
        ));
        assert!(polls > 1);
        assert_eq!(result.unwrap_err(), error);
    }
    let (result, _) = run(CanonicalState::decode_snapshot_cooperative(
        &original,
        StateLimits {
            max_producers: 1,
            ..StateLimits::default()
        },
        StateSnapshotLimits::default(),
        yield_step,
    ));
    assert!(matches!(
        result,
        Err(StateSnapshotError::LimitExceeded {
            kind: "producers",
            actual: 2,
            limit: 1
        })
    ));
    let mut bytes = original;
    *bytes.last_mut().unwrap() ^= 1;
    let (result, _) = run(CanonicalState::decode_snapshot_cooperative(
        &bytes,
        StateLimits::default(),
        StateSnapshotLimits::default(),
        yield_step,
    ));
    assert_eq!(result.unwrap_err(), StateSnapshotError::DigestMismatch);
}
