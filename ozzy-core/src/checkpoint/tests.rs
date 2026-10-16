use super::*;
use crate::dedup::{Index, Limits, Match, RestoringIndex};
use ozzy_proto::{MessageId, Offset};

fn limits() -> Limits {
    Limits {
        window_millis: 10,
        entries: 4,
        batch_records: 2,
        pending_records: 4,
        pending_operations: 8,
        bytes: 32 * 1024,
    }
}

fn image() -> (Layout, Vec<u8>) {
    let mut index = Index::new(limits(), Offset::ZERO, 0).unwrap();
    index.advance_time(1, 0).unwrap();
    index.advance_time(2, 0).unwrap();
    index.admit(3, 0, Offset::ZERO, &[id(1), id(2)]).unwrap();
    index.commit_through(3).unwrap();
    let mut snapshot = index.begin_snapshot().unwrap();
    let layout = Layout::new(
        256,
        Some(Identities {
            partition: ozzy_proto::PartitionIncarnation::from_bytes([7; 16]),
            header: *snapshot.header(),
        }),
    )
    .unwrap();
    let mut bytes = layout.encode().to_vec();
    bytes.extend_from_slice(&[5; 256]);
    let mut rows = [0; 64];
    assert_eq!(
        index
            .write_snapshot_rows(&mut snapshot, &mut rows, 2)
            .unwrap(),
        64
    );
    bytes.extend_from_slice(&rows);
    (layout, bytes)
}

fn id(byte: u8) -> MessageId {
    MessageId::from_bytes([byte; 16])
}

#[test]
fn arbitrary_fragment_boundaries_restore_without_retaining_identity_bytes() {
    let (layout, bytes) = image();
    assert_eq!(layout.encoded_bytes().unwrap(), bytes.len() as u64);
    for fragment in [1, 7, 31, 32, 103, 104, 257, 512] {
        let mut decoder = Decoder::new(bytes.len() as u64, 256);
        let mut index = None;
        for input in bytes.chunks(fragment) {
            decoder
                .push(
                    input,
                    |_| Ok(()),
                    |image, row| {
                        if index.is_none() {
                            index = Some(RestoringIndex::new(limits(), image.header)?);
                        }
                        index.as_mut().unwrap().push_rows(row)
                    },
                )
                .unwrap();
            assert!(decoder.retained_bytes() <= size_of::<Decoder>() + 256);
        }
        let (restored, core) = decoder.finish().unwrap();
        assert_eq!(restored, layout);
        assert_eq!(core, [5; 256]);
        let index = index.unwrap().finish().unwrap();
        assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
        assert_eq!(index.lookup(id(2)), Some(Match::Committed(Offset::new(1))));
    }
}

#[test]
fn every_truncated_prefix_and_extra_row_fails_closed() {
    let (_, bytes) = image();
    for cut in 0..bytes.len() {
        let mut decoder = Decoder::new(bytes.len() as u64, 256);
        decoder
            .push(&bytes[..cut], |_| Ok(()), |_, _| Ok(()))
            .unwrap();
        assert!(decoder.finish().is_err(), "truncated at {cut}");
    }
    let mut decoder = Decoder::new(bytes.len() as u64, 256);
    decoder.push(&bytes, |_| Ok(()), |_, _| Ok(())).unwrap();
    assert!(decoder.push(&[9; 32], |_| Ok(()), |_, _| Ok(())).is_err());
    assert!(decoder.push(&[], |_| Ok(()), |_, _| Ok(())).is_err());
    assert!(decoder.finish().is_err());
}

#[test]
fn declared_size_and_core_budget_are_checked_before_allocation() {
    let (_, bytes) = image();
    for (size, bound) in [(bytes.len() as u64 + 1, 256), (bytes.len() as u64, 255)] {
        let mut decoder = Decoder::new(size, bound);
        assert!(
            decoder
                .push(&bytes[..HEADER_BYTES], |_| Ok(()), |_, _| Ok(()))
                .is_err()
        );
        assert_eq!(decoder.retained_bytes(), size_of::<Decoder>());
        assert!(decoder.finish().is_err());
    }
}

#[test]
fn row_restore_failure_poisons_the_complete_candidate() {
    let (_, mut bytes) = image();
    let second = HEADER_BYTES + 256 + 32;
    bytes[second..second + 16].copy_from_slice(id(1).as_bytes());
    let mut decoder = Decoder::new(bytes.len() as u64, 256);
    let mut index = None;
    assert!(
        decoder
            .push(
                &bytes,
                |_| Ok(()),
                |image, row| {
                    if index.is_none() {
                        index = Some(RestoringIndex::new(limits(), image.header)?);
                    }
                    index.as_mut().unwrap().push_rows(row)
                }
            )
            .is_err()
    );
    assert!(decoder.finish().is_err());
    assert!(index.unwrap().finish().is_err());
}

#[test]
fn rejected_header_reserves_no_metadata_and_cannot_be_retried() {
    let (layout, bytes) = image();
    let mut decoder = Decoder::new(bytes.len() as u64, 256);
    let mut calls = 0;
    decoder
        .push(
            &bytes[..HEADER_BYTES - 1],
            |_| panic!("incomplete header"),
            |_, _| panic!("no identity rows before header admission"),
        )
        .unwrap();
    assert_eq!(decoder.retained_bytes(), size_of::<Decoder>());
    assert!(matches!(
        decoder.push(
            &bytes[HEADER_BYTES - 1..],
            |received| {
                calls += 1;
                assert_eq!(received, layout);
                Err(Error::Allocation)
            },
            |_, _| panic!("rejected header cannot deliver rows"),
        ),
        Err(Error::Allocation)
    ));
    assert_eq!(calls, 1);
    assert_eq!(decoder.retained_bytes(), size_of::<Decoder>());
    assert!(
        decoder
            .push(&bytes, |_| panic!("poisoned"), |_, _| Ok(()))
            .is_err()
    );
    assert!(decoder.finish().is_err());
}

#[test]
fn only_the_supported_header_and_canonical_partition_binding_are_accepted() {
    use crate::state::{MemoryIdentityIndex, StateLimits};
    use ozzy_journal::operation::{CreatePartition, OperationBody, RetentionPolicy};
    use ozzy_proto::{OwnerEpoch, PartitionId, PartitionIncarnation};

    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(4);
    let partition = PartitionIncarnation::from_bytes([7; 16]);
    let create = OperationBody::CreatePartition(CreatePartition {
        partition,
        stream: "stream",
        topic: "orders",
        partition_id: PartitionId::ZERO,
        owner_epoch: OwnerEpoch::INITIAL,
        dedup_window_millis: std::num::NonZeroU64::new(10),
        retention: RetentionPolicy::default(),
    });
    let plan = state.prepare(1, &create, &identities).unwrap();
    state.apply(plan, &mut identities).unwrap();
    let mut index = Index::new(limits(), Offset::ZERO, 0).unwrap();
    index.advance_time(1, 0).unwrap();
    index.commit_through(1).unwrap();
    let snapshot = index.begin_snapshot().unwrap();
    let layout = Layout::new(
        512,
        Some(Identities {
            partition,
            header: *snapshot.header(),
        }),
    )
    .unwrap();
    layout.validate_state(&state).unwrap();
    let encoded = layout.encode();
    assert_eq!(Layout::decode(&encoded).unwrap(), layout);
    for position in [0, 8, 10, 15] {
        let mut corrupt = encoded;
        corrupt[position] ^= 255;
        assert!(Layout::decode(&corrupt).is_err());
    }
    let mut corrupt = encoded;
    corrupt[24..40].fill(0);
    assert!(Layout::decode(&corrupt).is_err());
    let mut foreign = encoded;
    foreign[24..40].fill(8);
    assert!(
        Layout::decode(&foreign)
            .unwrap()
            .validate_state(&state)
            .is_err()
    );
    assert!(
        layout
            .validate_state(&CanonicalState::new(StateLimits::default()))
            .is_err()
    );
    assert!(Layout::new(u64::MAX, None).is_err());
    assert!(Layout::decode(b"OZYSTA01").is_err());
}
