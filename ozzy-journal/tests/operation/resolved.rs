use super::*;
use ozzy_journal::operation::{
    AppendHeader, AppendResults, ResolvedAppend, RetryFingerprint,
    append_resolved_wire_record_batch, append_wire_record_batch, decode_resolved_append,
    decode_resolved_append_indexed, validate_operation_body,
};
use ozzy_proto::append::{AppendDisposition, AppendOutcome, PayloadEncoding};
use ozzy_proto::data::{DataLimits, decode_record_descriptors};

fn outcome(disposition: AppendDisposition, offset: u64) -> AppendOutcome {
    AppendOutcome {
        disposition,
        offset: Offset::new(offset),
    }
}

fn batch<'a>() -> AppendBatch<'a> {
    let OperationBody::Append(mut append) = bodies().remove(2) else {
        unreachable!()
    };
    let original = append.batches.remove(0);
    let records = vec![
        AppendRecord {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([0x41; 16]),
            parts: vec![b"ab".as_slice(), b"".as_slice(), b"c".as_slice()].into(),
        },
        AppendRecord {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([0x42; 16]),
            parts: vec![b"xyz".as_slice()].into(),
        },
    ]
    .into();
    AppendBatch {
        partition: original.partition,
        owner_epoch: original.owner_epoch,
        producer_id: original.producer_id,
        producer_epoch: original.producer_epoch,
        first_sequence: original.first_sequence,
        first_offset: original.first_offset,
        append_timestamp_millis: original.append_timestamp_millis,
        records,
    }
}

#[test]
fn mixed_operation_keeps_independent_record_and_attempt_coordinates_in_fixed_bytes() {
    let rows = [
        outcome(AppendDisposition::Stored, 40),
        outcome(AppendDisposition::Duplicate, 8),
        outcome(AppendDisposition::Stored, 41),
        outcome(AppendDisposition::Duplicate, 40),
    ];
    let proofs = [
        RetryFingerprint::from_bytes([0xa1; 32]),
        RetryFingerprint::from_bytes([0xb2; 32]),
    ];
    let limits = OperationLimits::default();
    let results = AppendResults::classified(123, &rows, &proofs, limits).unwrap();
    let body = OperationBody::ResolvedAppend(ResolvedAppend {
        batch: batch(),
        results,
    });
    let mut expected = encode_operation_body(&bodies().remove(2), limits).unwrap();
    expected.extend_from_slice(&123u64.to_be_bytes());
    expected.extend_from_slice(&4u32.to_be_bytes());
    expected.extend_from_slice(&4u32.to_be_bytes());
    for (tag, offset) in [(0u8, 40u64), (1, 8), (0, 41), (1, 40)] {
        expected.push(tag);
        expected.extend_from_slice(&offset.to_be_bytes());
        expected.extend_from_slice(&1u32.to_be_bytes());
    }
    expected.extend_from_slice(&[0xa1; 32]);
    expected.extend_from_slice(&[0xb2; 32]);
    let encoded = encode_operation_body(&body, limits).unwrap();
    assert_eq!(encoded, expected);
    assert_eq!(OperationKind::try_from(10).unwrap(), body.kind());
    assert_eq!(body.operation_id(), None);
    assert!(!body.kind().has_operation_id());
    let decoded = decode_operation_body(body.kind(), &encoded, limits).unwrap();
    assert_eq!(encode_operation_body(&decoded, limits).unwrap(), encoded);
    let view = decode_resolved_append(&encoded, limits).unwrap();
    assert_eq!(view.batch.summary.record_count, 2);
    assert_eq!(view.results.attempts(), 4);
    assert_eq!(
        view.results.duplicate_fingerprints().collect::<Vec<_>>(),
        proofs
    );
}

#[test]
fn all_duplicates_store_no_user_records_and_cannot_be_decoded_as_ordinary_append() {
    let rows = [outcome(AppendDisposition::Duplicate, 8); 3];
    let proofs = [RetryFingerprint::from_bytes([0x33; 32]); 3];
    let limits = OperationLimits::default();
    let results = AppendResults::classified(99, &rows, &proofs, limits).unwrap();
    let mut batch = batch();
    batch.records = Vec::new().into();
    let body = OperationBody::ResolvedAppend(ResolvedAppend { batch, results });
    let encoded = encode_operation_body(&body, limits).unwrap();
    assert_eq!(encoded.len(), 80 + 16 + 3 * 13 + 3 * 32);
    assert_eq!(&encoded[76..80], &[0; 4]);
    let decoded = decode_operation_body(body.kind(), &encoded, limits).unwrap();
    assert_eq!(encode_operation_body(&decoded, limits).unwrap(), encoded);
    let view = decode_resolved_append(&encoded, limits).unwrap();
    assert_eq!(view.batch.records().count(), 0);
    assert_eq!(view.results.attempts(), 3);
    assert!(validate_operation_body(OperationKind::Append, &encoded, limits).is_err());
    for flag in [1u32 << 30, 1 << 31] {
        let mut malformed = encoded.clone();
        malformed[76..80].copy_from_slice(&flag.to_be_bytes());
        assert!(decode_resolved_append(&malformed, limits).is_err());
    }
}

#[test]
fn truncated_or_inconsistent_results_never_expose_records_or_partial_selectors() {
    let rows = [
        outcome(AppendDisposition::Stored, 40),
        outcome(AppendDisposition::Duplicate, 40),
        outcome(AppendDisposition::Stored, 41),
    ];
    let proofs = [RetryFingerprint::from_bytes([0x44; 32])];
    let limits = OperationLimits::default();
    let body = OperationBody::ResolvedAppend(ResolvedAppend {
        batch: batch(),
        results: AppendResults::classified(1, &rows, &proofs, limits).unwrap(),
    });
    let encoded = encode_operation_body(&body, limits).unwrap();
    let prefix = encode_operation_body(&bodies().remove(2), limits)
        .unwrap()
        .len();
    for end in 0..encoded.len() {
        let mut selectors = Vec::new();
        assert!(
            decode_resolved_append_indexed(&encoded[..end], limits, &mut selectors).is_err(),
            "end {end}"
        );
        assert!(selectors.is_empty(), "end {end}");
        assert!(validate_operation_body(body.kind(), &encoded[..end], limits).is_err());
    }
    for (position, value) in [
        (3, 2),
        (prefix + 11, 4),
        (prefix + 16, 2),
        (prefix + 16 + 13 + 8, 41),
        (59, 255),
    ] {
        let mut malformed = encoded.clone();
        malformed[position] = value;
        if position == 59 {
            malformed[52..60].copy_from_slice(&u64::MAX.to_be_bytes());
        }
        assert!(
            decode_resolved_append(&malformed, limits).is_err(),
            "position {position}"
        );
        assert!(decode_operation_body(body.kind(), &malformed, limits).is_err());
    }
    let mut selectors = Vec::new();
    decode_resolved_append_indexed(&encoded, limits, &mut selectors).unwrap();
    assert_eq!(selectors.len(), 3);
    let previous = selectors.clone();
    assert!(
        decode_resolved_append_indexed(&encoded[..encoded.len() - 1], limits, &mut selectors)
            .is_err()
    );
    assert_eq!(selectors, previous);
}

#[test]
fn wire_append_preserves_raw_and_lz4_blocks_and_rolls_back_combined_limits() {
    let mut table = 2u32.to_be_bytes().to_vec();
    for id in [0x11, 0x22] {
        table.extend_from_slice(&[id; 16]);
        table.extend_from_slice(&1u32.to_be_bytes());
        table.extend_from_slice(&4u32.to_be_bytes());
    }
    let payload = b"aaaabbbb";
    let descriptors =
        decode_record_descriptors(&table, payload.len(), 0, DataLimits::default()).unwrap();
    let header = AppendHeader::from(&batch());
    let rows = [
        outcome(AppendDisposition::Stored, 40),
        outcome(AppendDisposition::Stored, 41),
    ];
    let limits = OperationLimits::default();
    let results = AppendResults::classified(3, &rows, &[], limits).unwrap();
    for encoding in [PayloadEncoding::Raw, PayloadEncoding::Lz4] {
        let block = match encoding {
            PayloadEncoding::Raw => payload.to_vec(),
            PayloadEncoding::Lz4 => lz4rip::block::compress(payload),
        };
        let mut ordinary = Vec::new();
        append_wire_record_batch(&mut ordinary, header, descriptors, encoding, &block, limits)
            .unwrap();
        let mut resolved = vec![0xaa, 0xbb];
        let range = append_resolved_wire_record_batch(
            &mut resolved,
            header,
            descriptors,
            encoding,
            &block,
            results,
            limits,
        )
        .unwrap();
        assert_eq!(
            &resolved[range.start..range.start + ordinary.len()],
            ordinary
        );
        let view = decode_resolved_append(&resolved[range.clone()], limits).unwrap();
        if encoding == PayloadEncoding::Lz4 {
            assert_eq!(view.batch.prepared_payload.unwrap().encoded, block);
        }
        let tight = OperationLimits {
            max_body_bytes: range.len() - 1,
            ..limits
        };
        let mut output = vec![0x55];
        assert!(
            append_resolved_wire_record_batch(
                &mut output,
                header,
                descriptors,
                encoding,
                &block,
                results,
                tight
            )
            .is_err()
        );
        assert_eq!(output, [0x55]);
    }
}
