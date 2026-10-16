use super::*;
use ozzy_core::state::StateSnapshotLimits;
use ozzy_journal::operation::DedupWatermark;

fn watermark(time: u64) -> OperationBody<'static> {
    OperationBody::DedupWatermark(DedupWatermark {
        partition: partition(),
        owner_epoch: OwnerEpoch::new(9),
        logical_time: time,
    })
}

fn initialized() -> CanonicalImages {
    let mut images = CanonicalImages::new(StateLimits::default(), 64, 8);
    images.admit(1, &create()).unwrap();
    images.admit(2, &open()).unwrap();
    images.admit(3, &append(0, 1)).unwrap();
    images.commit_through(3).unwrap();
    images
}

#[test]
fn idle_clock_is_speculative_until_commit_and_discard_preserves_the_old_clock() {
    let mut images = initialized();
    let before = images.committed().partition(partition()).unwrap().clone();
    images.admit(4, &watermark(10)).unwrap();
    assert_eq!(images.committed().partition(partition()).unwrap(), &before);
    let accepted = images.speculative().partition(partition()).unwrap();
    assert_eq!(accepted.dedup_time, 10);
    assert_eq!(accepted.next_offset, before.next_offset);
    assert_eq!(accepted.producer(producer()), before.producer(producer()));
    images.discard_suffix();
    assert_eq!(
        images.speculative().partition(partition()).unwrap(),
        &before
    );
    images.admit(4, &watermark(5)).unwrap();
    images.commit_through(4).unwrap();
    let state = images.committed();
    assert_eq!(state.partition(partition()).unwrap().dedup_time, 5);
    let restored = CanonicalState::decode_snapshot(
        &state
            .encode_snapshot(StateSnapshotLimits::default())
            .unwrap(),
        StateLimits::default(),
        StateSnapshotLimits::default(),
    )
    .unwrap();
    assert_eq!(&restored, state);
}

#[test]
fn watermark_rejects_stale_clock_owner_and_incarnation_atomically() {
    let mut images = initialized();
    images.admit(4, &watermark(10)).unwrap();
    images.commit_through(4).unwrap();
    let before = images.clone();
    for time in [0, 9, 10] {
        assert_eq!(
            images.admit(5, &watermark(time)),
            Err(CanonicalImagesError::State(
                StateError::DedupTimeNotAdvanced
            ))
        );
        assert_eq!(images, before);
    }
    let OperationBody::DedupWatermark(mut value) = watermark(11) else {
        unreachable!()
    };
    value.owner_epoch = OwnerEpoch::new(8);
    assert_eq!(
        images.admit(5, &OperationBody::DedupWatermark(value)),
        Err(CanonicalImagesError::State(StateError::OwnerEpochMismatch))
    );
    assert_eq!(images, before);
    value.owner_epoch = OwnerEpoch::new(9);
    value.partition = another_partition();
    assert_eq!(
        images.admit(5, &OperationBody::DedupWatermark(value)),
        Err(CanonicalImagesError::State(StateError::UnknownPartition))
    );
    assert_eq!(images, before);
    let identities = images.speculative_identities().len();
    images.admit(5, &watermark(11)).unwrap();
    assert_eq!(images.speculative_identities().len(), identities);
}

#[test]
fn resolved_append_cannot_restore_time_behind_a_committed_idle_watermark() {
    use ozzy_journal::operation::{AppendBatchSummary, AppendResults, OperationLimits};
    use ozzy_proto::append::{AppendDisposition, AppendOutcome};
    let mut images = initialized();
    images.admit(4, &watermark(10)).unwrap();
    images.commit_through(4).unwrap();
    let rows = [AppendOutcome {
        disposition: AppendDisposition::Stored,
        offset: Offset::new(1),
    }];
    let summary = AppendBatchSummary {
        partition: partition(),
        owner_epoch: OwnerEpoch::new(9),
        producer_id: producer(),
        producer_epoch: ProducerEpoch::new(3),
        first_sequence: ProducerSequence::new(1),
        first_offset: Offset::new(1),
        record_count: 1,
        nonzero_message_ids: true,
    };
    let before = images.clone();
    let stale = AppendResults::classified(9, &rows, &[], OperationLimits::default()).unwrap();
    assert!(
        images
            .prepare_resolved_append_group(&[(5, summary, stale)])
            .is_err()
    );
    assert_eq!(images, before);
    let current = AppendResults::classified(10, &rows, &[], OperationLimits::default()).unwrap();
    let prepared = images
        .prepare_resolved_append_group(&[(5, summary, current)])
        .unwrap();
    images.install_prepared_group(prepared).unwrap();
    images.commit_through(5).unwrap();
    assert_eq!(
        images
            .committed()
            .partition(partition())
            .unwrap()
            .dedup_time,
        10
    );
    assert_eq!(
        images
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::new(2)
    );
}
