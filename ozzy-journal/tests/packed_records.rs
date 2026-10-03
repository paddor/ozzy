use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecordList, OperationBody, OperationKind, OperationLimits,
    decode_append_batches, decode_operation_body, decode_packed_append_records,
    encode_operation_body,
};
use ozzy_proto::data::{RecordBatch, TinyRecords};
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};

fn body(records: &RecordBatch) -> OperationBody<'_> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: PartitionIncarnation::from_bytes([1; 16]),
            owner_epoch: OwnerEpoch::new(1),
            producer_id: ProducerId::from_bytes([2; 16]),
            producer_epoch: ProducerEpoch::new(1),
            first_sequence: ProducerSequence::new(11),
            first_offset: Offset::new(31),
            append_timestamp_millis: 77,
            records: AppendRecordList::batch(records, 0..records.len()),
        }],
    })
}

#[test]
fn compact_canonical_bytes_preserve_empty_parts_and_indexed_fragments() {
    let mut tiny = TinyRecords::default();
    for (index, n) in [0, 16, 128, 255, 1].into_iter().enumerate() {
        assert!(tiny.push(
            MessageId::from_bytes([index as u8 + 1; 16]),
            &vec![index as u8; n]
        ));
    }
    let records = RecordBatch::Tiny(tiny);
    let limits = OperationLimits::default();
    let encoded = encode_operation_body(&body(&records), limits).unwrap();
    assert_eq!(encoded.len(), 80 + 17 * 5 + 400);
    assert_eq!(
        u32::from_be_bytes(encoded[76..80].try_into().unwrap()),
        (1 << 31) | 5
    );
    assert_eq!(&encoded[160..165], &[0, 16, 128, 255, 1]);
    let general = RecordBatch::General(records.clone().into_owned());
    assert_eq!(
        encoded,
        encode_operation_body(&body(&general), limits).unwrap()
    );
    let mut split = body(&records);
    let OperationBody::Append(append) = &mut split else {
        unreachable!()
    };
    let slices = &mut append.batches[0].records;
    *slices = AppendRecordList::batch(&records, 0..1);
    slices.extend_batch(&records, 1..4).unwrap();
    slices.extend_batch(&records, 4..5).unwrap();
    assert_eq!(encode_operation_body(&split, limits).unwrap(), encoded);
    let decoded = decode_operation_body(OperationKind::Append, &encoded, limits).unwrap();
    assert_eq!(encode_operation_body(&decoded, limits).unwrap(), encoded);
    let batches = decode_append_batches(&encoded, limits).unwrap();
    let mut cursor = batches[0].records();
    for expected in &records {
        let (ids, payload) = cursor.remaining_bytes();
        let lengths = cursor.tiny_lengths().unwrap();
        let actual = cursor.next().unwrap();
        assert_eq!(actual.message_id, expected.message_id);
        assert!(actual.parts.clone().eq(expected.payload.iter()));
        let n = lengths[0] as usize;
        let fragment =
            decode_packed_append_records(&ids[..16], Some(&lengths[..1]), &payload[..n], 1, limits)
                .unwrap()
                .next()
                .unwrap();
        assert_eq!(fragment.message_id, expected.message_id);
        assert!(fragment.parts.eq(expected.payload.iter()));
    }
    assert_eq!(cursor.len(), 0);
    for end in 0..encoded.len() {
        assert!(
            decode_append_batches(&encoded[..end], limits).is_err(),
            "truncation at {end}"
        );
    }
    let mut bad = encoded.clone();
    bad[76..80].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(decode_append_batches(&bad, limits).is_err());
    bad = encoded.clone();
    bad[160] = 255;
    assert!(decode_append_batches(&bad, limits).is_err());
    let mut limits = limits;
    limits.max_payload_bytes = 399;
    assert!(decode_append_batches(&encoded, limits).is_err());
}
