use super::*;
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, decode_append_view, encode_operation_body,
};
use ozzy_proto::{OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId};

pub(super) fn body(offset: u64, sequence: u64) -> Bytes {
    Bytes::from(
        encode_operation_body(
            &OperationBody::Append(Append {
                batches: vec![AppendBatch {
                    partition: PartitionIncarnation::new(),
                    owner_epoch: OwnerEpoch::INITIAL,
                    producer_id: ProducerId::new(),
                    producer_epoch: ProducerEpoch::INITIAL,
                    first_sequence: ProducerSequence::new(sequence),
                    first_offset: Offset::new(offset),
                    append_timestamp_millis: 77,
                    records: (0..2)
                        .map(|_| AppendRecord {
                            encoding: ozzy_proto::data::Encoding::Raw,
                            message_id: MessageId::new(),
                            parts: [b"".as_slice(), b"payload"].into_iter().collect(),
                        })
                        .collect(),
                }],
            }),
            OperationLimits::default(),
        )
        .unwrap(),
    )
}

#[test]
fn cached_descriptors_do_not_create_per_record_payload_owners() {
    let body = body(20, 30);
    let append = decode_append_view(&body, OperationLimits::default()).unwrap();
    let batch = append.batches().next().unwrap();
    let cached = cache_batch(&body, &batch).unwrap();
    assert!(
        body.is_unique(),
        "descriptors must borrow ranges, not clone Bytes"
    );
    assert!(std::mem::size_of::<records::RecordStart>() < std::mem::size_of::<IndexedRecord>());
    let view = RecordView {
        body: &body,
        shared_backing_bytes: None,
        batch: &cached,
        decoded: &DecodedCell::default(),
        record: cached.records.get(1, &body).unwrap(),
        index: 1,
    };
    let materialized = view.materialize();
    assert_eq!(
        materialized
            .parts
            .iter()
            .map(Bytes::as_ref)
            .collect::<Vec<_>>(),
        [b"".as_slice(), b"payload"]
    );
    assert_eq!(materialized.offset.get(), 21);
    assert_eq!(materialized.producer_sequence.get(), 31);
    assert_eq!(materialized.append_timestamp_millis, 77);
}

#[test]
fn wide_materialization_reuses_descriptor_capacity_without_pinning_payloads() {
    let parts = [b"".as_slice(), b"a", b"", b"b"];
    let body = Bytes::from(
        encode_operation_body(
            &OperationBody::Append(Append {
                batches: vec![AppendBatch {
                    partition: PartitionIncarnation::from_bytes([3; 16]),
                    owner_epoch: OwnerEpoch::INITIAL,
                    producer_id: ProducerId::new(),
                    producer_epoch: ProducerEpoch::INITIAL,
                    first_sequence: ProducerSequence::new(30),
                    first_offset: Offset::new(20),
                    append_timestamp_millis: 77,
                    records: vec![AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::new(),
                        parts: parts.into_iter().collect(),
                    }]
                    .into(),
                }],
            }),
            OperationLimits::default(),
        )
        .unwrap(),
    );
    let append = decode_append_view(&body, OperationLimits::default()).unwrap();
    let cached = cache_batch(&body, &append.batches().next().unwrap()).unwrap();
    let view = RecordView {
        body: &body,
        shared_backing_bytes: None,
        batch: &cached,
        decoded: &DecodedCell::default(),
        record: cached.records.get(0, &body).unwrap(),
        index: 0,
    };
    let mut scratch = SmallVec::with_capacity(4);
    let pointer = scratch.as_ptr();
    for _ in 0..8 {
        let mut record = view.materialize_reusing(&mut scratch);
        assert_eq!(record.parts.as_ptr(), pointer);
        assert_eq!(
            record.parts.iter().map(Bytes::len).collect::<Vec<_>>(),
            [0, 1, 0, 1]
        );
        scratch = std::mem::take(&mut record.parts);
        scratch.clear();
        assert!(
            body.is_unique(),
            "idle scratch must not own any operation bytes"
        );
    }
}

#[test]
fn cache_rejects_position_overflow_before_any_unchecked_selection() {
    for (offset, sequence) in [(u64::MAX, 0), (0, u64::MAX)] {
        let body = body(0, 0);
        let append = decode_append_view(&body, OperationLimits::default()).unwrap();
        // The wire codec also rejects this. Exercise the cache's own
        // checked boundary using the view's public metadata fields.
        let mut batch = append.batches().next().unwrap();
        batch.summary.first_offset = Offset::new(offset);
        batch.summary.first_sequence = ProducerSequence::new(sequence);
        assert!(matches!(
            cache_batch(&body, &batch),
            Err(IndexedReadError::InvalidSelector)
        ));
    }
}
