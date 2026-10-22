use super::*;
use ozzy_journal::operation::Digest;
use ozzy_proto::GroupId;

fn header() -> OperationHeader {
    OperationHeader {
        group_id: GroupId::from_bytes([1; 16]),
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Append,
    }
}

#[test]
fn prepared_reads_share_bytes_and_tables_through_binding_and_source_drop() {
    let body = super::super::tests::body(40, 80);
    let records =
        PreparedOperationRecords::new(header(), body.clone(), OperationLimits::default()).unwrap();
    let partition = records.batches[0].summary.partition;
    let decoded = records.decoded_batches();
    for start in 39..44 {
        for end in start..44 {
            let actual = records
                .records(partition, Offset::new(start), Offset::new(end), &decoded)
                .map(|record| record.offset().get())
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                (start..end)
                    .filter(|offset| (40..42).contains(offset))
                    .collect::<Vec<_>>()
            );
        }
    }
    let location = OperationLocation {
        segment_id: 1,
        entry_offset: 4096,
        entry_bytes: 4096,
        op_number: 1,
        operation_digest: Digest::ZERO,
    };
    let cached = records.bind(location).unwrap();
    assert_eq!(cached.body.as_ptr(), body.as_ptr());
    assert!(Arc::ptr_eq(&cached.batches, &records.batches));
    let delivered = records
        .record(partition, Offset::new(41), &decoded)
        .unwrap()
        .materialize();
    assert_eq!(delivered.producer_sequence.get(), 81);
    assert_eq!(delivered.parts[0].len(), 0);
    let borrowed = records
        .record(partition, Offset::new(41), &decoded)
        .unwrap();
    assert_eq!(
        delivered.parts[1].as_ptr(),
        borrowed.parts().nth(1).unwrap().as_ptr()
    );
    drop((decoded, records, body));
    assert_eq!(delivered.parts[1], b"payload".as_slice());
    assert_eq!(cached.batches[0].records.len(), 2);
}

#[test]
fn shared_readers_account_for_full_backing_instead_of_body_slice() {
    let body = super::super::tests::body(40, 80);
    let mut arena = Vec::with_capacity(body.len() + 8192);
    arena.extend_from_slice(&body);
    let capacity = arena.capacity();
    let body = Bytes::from(arena).slice(..body.len());
    let records =
        PreparedOperationRecords::new(header(), body, OperationLimits::default()).unwrap();
    let partition = records.batches[0].summary.partition;
    let decoded = records.decoded_batches();
    assert!(
        records
            .record(partition, Offset::new(40), &decoded)
            .unwrap()
            .payload_backing(usize::MAX)
            .is_none()
    );
    let records = records.with_shared_backing_bytes(capacity);
    let cloned = records.clone();
    drop(records);
    for record in cloned.records(partition, Offset::new(40), Offset::new(42), &decoded) {
        assert!(record.payload_backing(capacity - 1).is_none());
        assert!(record.payload_backing(capacity).is_some());
    }
    let cached = cloned
        .bind(OperationLocation {
            segment_id: 1,
            entry_offset: 4096,
            entry_bytes: 4096,
            op_number: 1,
            operation_digest: Digest::ZERO,
        })
        .unwrap();
    assert_eq!(cached.shared_backing_bytes, Some(capacity));
    assert!(cached.body.len() < capacity);
}

#[test]
fn preparation_rejects_invalid_bounds_and_trailing_bytes() {
    let body = super::super::tests::body(0, 0);
    let limits = OperationLimits {
        max_records: 1,
        ..OperationLimits::default()
    };
    assert!(PreparedOperationRecords::new(header(), body.clone(), limits).is_err());
    let mut extra = body.to_vec();
    extra.push(0);
    assert!(
        PreparedOperationRecords::new(header(), extra.into(), OperationLimits::default()).is_err()
    );
    assert!(
        PreparedOperationRecords::new(
            header(),
            body.slice(..body.len() - 1),
            OperationLimits::default()
        )
        .is_err()
    );
}
