use super::*;
use crate::directory::progress_tests::journal_mode;
use crate::{Digest, OperationKind, OperationLimits};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, encode_operation_body,
    logical_operation_digest,
};
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};

#[cfg(target_os = "linux")]
mod direct;
mod pipeline;
mod raw;
mod shared;

fn body(number: u64) -> Vec<u8> {
    let payload = vec![number as u8; 16 * 1024];
    body_with_payload(number, &payload)
}

fn body_with_payload(number: u64, payload: &[u8]) -> Vec<u8> {
    encode_operation_body(
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: PartitionIncarnation::from_bytes([5; 16]),
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([6; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(number - 1),
                first_offset: Offset::new(number - 1),
                append_timestamp_millis: 1,
                records: vec![AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes(u128::from(number).to_be_bytes()),
                    parts: vec![payload].into(),
                }]
                .into(),
            }],
        }),
        OperationLimits::default(),
    )
    .unwrap()
}

fn operation<'a>(
    journal: &OpenGroupJournal,
    number: u64,
    previous: Digest,
    body: &'a [u8],
) -> CanonicalOperation<'a> {
    CanonicalOperation {
        group_id: journal.directory().identity().group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: number,
        previous_digest: previous,
        kind: OperationKind::Append,
        body,
    }
}

#[test]
fn successor_compression_overlaps_write_without_advancing_physical_history() {
    for encoding in [
        BodyEncoding::Raw,
        BodyEncoding::Lz4 {
            min_savings_bytes: 64,
        },
    ]
    .into_iter()
    .filter(|encoding| encoding.is_supported())
    {
        let (_temporary, mut journal) = journal_mode(true);
        let first_body = body(1);
        let second_body = body(2);
        let first = operation(&journal, 1, Digest::ZERO, &first_body);
        let first_digest = logical_operation_digest(&first);
        let second = operation(&journal, 2, first_digest, &second_body);
        let second_digest = logical_operation_digest(&second);
        let encoded = journal.preencode_journal_group(&[first], encoding).unwrap();
        let (mut pending, work) = journal.begin_preencoded_journal_write(encoded).unwrap();
        let next = pending
            .preencode_journal_group(&[second], encoding)
            .unwrap();
        assert_eq!(pending.journal().written_position().unwrap().op_number, 0);
        let completed = work.write();
        assert_eq!(pending.journal().accepted_position().unwrap().op_number, 0);
        let (journal, locations) = pending.complete(completed).unwrap();
        assert_eq!(journal.accepted_position().unwrap().op_number, 1);
        assert_eq!(locations[0].operation_digest, first_digest);
        let (pending, work) = journal.begin_preencoded_journal_write(next).unwrap();
        let (journal, locations) = pending.complete(work.write()).unwrap();
        assert_eq!(journal.accepted_position().unwrap().op_number, 2);
        assert_eq!(locations[0].operation_digest, second_digest);
        let root = journal.directory.root.clone();
        let identity = journal.directory.identity;
        let position = journal.accepted_position().unwrap();
        drop(journal);
        let recovered =
            crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default())
                .unwrap()
                .recover(
                    ozzy_journal::progress::JournalGeneration(2),
                    crate::DecodeLimits::default(),
                    OperationLimits::default(),
                )
                .unwrap();
        assert_eq!(recovered.accepted_position().unwrap(), position);
    }
}

#[test]
fn failed_or_foreign_completions_cannot_publish_a_persisted_prefix() {
    let (_temporary, mut first) = journal_mode(true);
    let bytes = body(1);
    let op = operation(&first, 1, Digest::ZERO, &bytes);
    let encoded = first
        .preencode_journal_group(&[op], BodyEncoding::Raw)
        .unwrap();
    let (pending, work) = first.begin_preencoded_journal_write(encoded).unwrap();
    assert!(pending.complete(work.fail()).is_err());

    let (_left_root, mut left) = journal_mode(true);
    let (_right_root, mut right) = journal_mode(true);
    let left_group = left
        .preencode_journal_group(&[op], BodyEncoding::Raw)
        .unwrap();
    let right_group = right
        .preencode_journal_group(&[op], BodyEncoding::Raw)
        .unwrap();
    let (left, left_work) = left.begin_preencoded_journal_write(left_group).unwrap();
    let (right, right_work) = right.begin_preencoded_journal_write(right_group).unwrap();
    assert!(left.complete(right_work.write()).is_err());
    assert!(right.complete(left_work.write()).is_err());
}

#[test]
fn detached_write_retains_store_lock_after_owner_is_dropped() {
    let (_temporary, mut journal) = journal_mode(true);
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    let bytes = body(1);
    let op = operation(&journal, 1, Digest::ZERO, &bytes);
    let encoded = journal
        .preencode_journal_group(&[op], BodyEncoding::Raw)
        .unwrap();
    let (pending, work) = journal.begin_preencoded_journal_write(encoded).unwrap();
    drop(pending);
    assert!(
        crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default()).is_err()
    );
    drop(work.write());
    assert!(crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default()).is_ok());
}
