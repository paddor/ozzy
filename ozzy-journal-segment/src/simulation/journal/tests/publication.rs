//! Publication faults judged by an independent confirmation oracle.

use super::*;
use crate::OperationKind;
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, encode_operation_body,
};
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};

fn position(operation: &CanonicalOperation<'_>) -> LogPosition {
    LogPosition {
        op_number: operation.op_number,
        digest: logical_operation_digest(operation),
    }
}

fn barrier(previous: LogPosition, body: &'static [u8]) -> CanonicalOperation<'static> {
    CanonicalOperation {
        group_id: GroupId::from_bytes([1; 16]),
        configuration_epoch: 1,
        original_view: 0,
        op_number: previous.op_number + 1,
        previous_digest: previous.digest,
        kind: OperationKind::Barrier,
        body,
    }
}

/// Operation 1 is synchronized with evidence; operation 2 is only written.
fn written_second() -> (Journal, LogPosition, LogPosition, usize) {
    let mut journal = voter();
    let first = barrier(LogPosition::GENESIS, &[1; 16]);
    journal.append(&[first]).unwrap();
    journal.sync(1).unwrap();
    let first_end = journal.unsynced_active_range().start;
    let second = barrier(position(&first), &[2; 16]);
    journal.append(&[second]).unwrap();
    (journal, position(&first), position(&second), first_end)
}

/// Lose every byte after operation 1, as a crash tail or offline damage would.
fn erase_second(journal: &mut Journal, first_end: usize, zero: bool) {
    let name = journal.active_name();
    let damage = if zero {
        Damage::ZeroSuffix(first_end)
    } else {
        Damage::Truncate(first_end)
    };
    journal.damage(&name, damage);
}

#[test]
fn omitted_metadata_barriers_are_detected_by_the_confirmation_oracle() {
    for omit in [false, true] {
        let (mut journal, _, second, _) = written_second();
        journal.omit_metadata_barriers(omit);
        // With omitted barriers this success is deliberately invalid.
        journal.publish_view(0, 0, second).unwrap();
        journal.crash();
        journal.omit_metadata_barriers(false);
        let recovered = journal.recover(JournalGeneration(2)).unwrap();
        let expected = if omit { LogPosition::GENESIS } else { second };
        assert_eq!(recovered.manifest.committed, expected, "omit={omit}");
    }
}

#[test]
fn omitted_evidence_barrier_is_detected_by_the_confirmation_oracle() {
    for omit in [false, true] {
        let (mut journal, first, _, first_end) = written_second();
        journal.omit_evidence_barrier(omit);
        // With an omitted barrier this success is deliberately invalid.
        journal.sync(2).unwrap();
        journal.crash();
        erase_second(&mut journal, first_end, true);
        let recovered = journal.recover(JournalGeneration(2));
        if omit {
            assert_eq!(recovered.unwrap().manifest.accepted, first);
        } else {
            assert!(recovered.is_err(), "lost protected operation must refuse");
        }
    }
}

#[test]
fn view_publication_cuts_never_forget_a_reported_commit() {
    let (mut journal, _, second, _) = written_second();
    let before = journal.executed_operations();
    journal.publish_view(0, 0, second).unwrap();
    let actions = journal.executed_operations() - before;
    let (mut reopened, mut refused) = (0, 0);
    for cut in 1..=actions + 1 {
        let (mut journal, first, second, first_end) = written_second();
        journal.fail_after(cut);
        let success = journal.publish_view(0, 0, second).is_ok();
        journal.crash();
        erase_second(&mut journal, first_end, cut % 2 == 0);
        // Refusing a damaged protected tail is safe; reopening must not forget.
        if let Ok(recovered) = journal.recover(JournalGeneration(2)) {
            assert!(!success, "cut={cut} forgot a reported commit");
            assert_eq!(recovered.manifest.committed, LogPosition::GENESIS);
            assert_eq!(recovered.manifest.accepted, first, "cut={cut}");
            reopened += 1;
        } else {
            refused += 1;
        }
    }
    assert!(
        reopened > 0 && refused > 0,
        "publication cuts must straddle the selection boundary"
    );
}

fn append_body() -> Vec<u8> {
    let mut payload = [42; 6000];
    let mut random = 17_u64;
    for byte in &mut payload[..2048] {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        *byte = random as u8;
    }
    encode_operation_body(
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: PartitionIncarnation::from_bytes([1; 16]),
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([2; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(0),
                first_offset: Offset::ZERO,
                append_timestamp_millis: 0,
                records: vec![AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([3; 16]),
                    parts: vec![payload.as_slice()].into(),
                }]
                .into(),
            }],
        }),
        OperationLimits::default(),
    )
    .unwrap()
}

/// Recovery must not assume that unsynchronized writes persist in append
/// order, nor trust a protected operation whose bytes were later damaged.
fn check_reordered_images(encoding: BodyEncoding) {
    let body = append_body();
    for protected in [false, true] {
        for length_first in [false, true] {
            for mask in 0..16_u32 {
                let mut journal = voter();
                journal.set_body_encoding(encoding).unwrap();
                let first = barrier(LogPosition::GENESIS, &[1; 16]);
                journal.append(&[first]).unwrap();
                journal.sync(1).unwrap();
                let latest = CanonicalOperation {
                    kind: OperationKind::Append,
                    body: &body,
                    ..barrier(position(&first), &[])
                };
                journal.append(&[latest]).unwrap();
                let range = journal.unsynced_active_range();
                let expected = if encoding == BodyEncoding::Raw {
                    8192
                } else {
                    4096
                };
                assert_eq!(range.len(), expected);
                if protected {
                    journal.sync(2).unwrap();
                }
                let path = root().join(journal.active_name());
                let complete = journal.device.image().bytes(&path, false).unwrap().to_vec();
                journal.crash();
                // Four sector-aligned regions: header, two body regions, and seal.
                let split = range.start + range.len() / 2;
                let regions = [
                    range.start..range.start + 512,
                    range.start + 512..split,
                    split..range.end - 512,
                    range.end - 512..range.end,
                ];
                let mut image = complete[..range.start].to_vec();
                if length_first {
                    image.resize(range.end, 0);
                }
                for (bit, region) in regions.iter().enumerate() {
                    if mask & (1 << bit) != 0 {
                        image.resize(image.len().max(region.end), 0);
                        image[region.clone()].copy_from_slice(&complete[region.clone()]);
                    }
                }
                let access = Access::new(journal.device.io.clone(), None);
                journal
                    .device
                    .run(overwrite(&access, path.clone(), &image))
                    .unwrap();
                drop(access);
                journal.device.crash();
                let result = journal.recover(JournalGeneration(2));
                let case = format!("protected={protected} length_first={length_first} mask={mask}");
                match result {
                    Ok(recovered) => {
                        let accepted = recovered.manifest.accepted;
                        assert!(
                            accepted == position(&latest)
                                || (!protected && accepted == position(&first)),
                            "{case}"
                        );
                        if accepted == position(&latest) {
                            // Never accept a partially decoded or changed record.
                            assert_eq!(image, complete[..range.end], "{case}");
                        }
                        assert!(
                            mask != 0 || !protected,
                            "forgot protected operation: {case}"
                        );
                        assert!(mask != 15 || accepted == position(&latest), "{case}");
                    }
                    Err(error) => {
                        assert_ne!(mask, 15, "intact image rejected: {case}: {error}");
                        assert!(
                            protected || mask != 0,
                            "empty tail rejected: {case}: {error}"
                        );
                        assert!(
                            matches!(error, DirectoryError::Writer(_)),
                            "{case}: {error}"
                        );
                        let after = journal.device.image().bytes(&path, false).unwrap();
                        assert_eq!(after, image, "refusal changed bytes: {case}");
                    }
                }
            }
        }
    }
}

#[test]
fn reordered_raw_writes_preserve_protected_history_or_refuse_without_mutation() {
    check_reordered_images(BodyEncoding::Raw);
}

#[cfg(feature = "lz4")]
#[test]
fn reordered_lz4_writes_preserve_protected_history_or_refuse_without_mutation() {
    check_reordered_images(BodyEncoding::Lz4 {
        min_savings_bytes: 1,
    });
}
