#[cfg(feature = "lz4")]
use ozzy_journal_segment::{BodyEncoding, encode_group_with_body_encoding};
use ozzy_journal_segment::{
    CanonicalOperation, ChainPosition, CodecError, DecodeLimits, Digest, OperationKind,
    SEGMENT_HEADER_BYTES, SegmentHeader, TailState, decode_group, decode_segment_header,
    encode_group, encode_segment_header, logical_operation_digest, scan_segment,
};
use ozzy_proto::GroupId;

const MIB: u64 = 1024 * 1024;

fn group_id() -> GroupId {
    GroupId::from_bytes([
        0x01, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x70, 0x00, 0x80, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55,
        0x66,
    ])
}

fn segment() -> SegmentHeader {
    SegmentHeader::new(group_id(), 7, None, Digest::ZERO, 64 * MIB).unwrap()
}

#[test]
fn empty_segment_rejects_inconsistent_decode_limits_before_accepting_a_writer() {
    let bytes = encode_segment_header(&segment());
    for limits in [
        DecodeLimits {
            max_groups: 0,
            ..DecodeLimits::default()
        },
        DecodeLimits {
            max_entries: 0,
            ..DecodeLimits::default()
        },
        DecodeLimits {
            max_group_decoded_body_bytes: 1,
            ..DecodeLimits::default()
        },
        DecodeLimits {
            max_segment_decoded_body_bytes: 1,
            ..DecodeLimits::default()
        },
    ] {
        assert!(matches!(
            scan_segment(&bytes, 1, ChainPosition::GENESIS, limits),
            Err(CodecError::LimitExceeded {
                kind: "decoder configuration",
                ..
            }),
        ));
    }
}

fn operation(
    number: u64,
    previous_digest: Digest,
    kind: OperationKind,
    body: &[u8],
) -> CanonicalOperation<'_> {
    CanonicalOperation {
        group_id: group_id(),
        configuration_epoch: 3,
        original_view: 9,
        op_number: number,
        previous_digest,
        kind,
        body,
    }
}

fn two_operations() -> [CanonicalOperation<'static>; 2] {
    let first = operation(1, Digest::ZERO, OperationKind::CreatePartition, b"alpha");
    let second = operation(
        2,
        logical_operation_digest(&first),
        OperationKind::Append,
        b"beta payload",
    );
    [first, second]
}

#[test]
fn segment_header_matches_frozen_layout_and_round_trips() {
    let header = segment();
    let encoded = encode_segment_header(&header);

    assert_eq!(encoded.len(), SEGMENT_HEADER_BYTES);
    assert_eq!(&encoded[0..8], b"OZYSEG\0\0");
    assert_eq!(&encoded[8..10], &[0, 3]);
    assert_eq!(&encoded[10..12], &[0x10, 0]);
    assert_eq!(&encoded[16..32], group_id().as_bytes());
    assert_eq!(&encoded[32..40], &7_u64.to_be_bytes());
    assert_eq!(&encoded[80..88], &(64 * MIB).to_be_bytes());
    assert_eq!(decode_segment_header(&encoded).unwrap(), header);

    assert_eq!(
        ozzy_journal::integrity::hash("ozzy segment golden fixture v1", &encoded).as_bytes(),
        &[
            111, 14, 189, 72, 160, 127, 117, 220, 69, 74, 181, 254, 62, 115, 164, 252, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    );
}

#[test]
fn earlier_integrity_and_experimental_segment_versions_are_not_reinterpreted() {
    for version in [1_u16, 2, 0xff01] {
        let mut bytes = encode_segment_header(&segment());
        bytes[8..10].copy_from_slice(&version.to_be_bytes());
        assert_eq!(
            decode_segment_header(&bytes),
            Err(CodecError::UnsupportedVersion(version))
        );
    }
}

#[test]
fn physical_group_matches_frozen_layout_and_round_trips() {
    let segment = segment();
    let operations = two_operations();
    let encoded = encode_group(
        &segment,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
    )
    .unwrap();

    assert_eq!(encoded.as_bytes().len(), 4096);
    assert_eq!(&encoded.as_bytes()[0..4], b"OZJE");
    assert_eq!(&encoded.as_bytes()[200..204], b"OZJE");
    assert_eq!(&encoded.as_bytes()[4000..4008], b"OZYSEAL\0");
    assert_eq!(encoded.end_offset(), 8192);
    assert_eq!(encoded.entry_count(), 2);

    let decoded = decode_group(
        &segment,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        encoded.as_bytes(),
        DecodeLimits::default(),
    )
    .unwrap();
    assert_eq!(decoded.consumed_bytes(), 4096);
    assert_eq!(decoded.operations.len(), 2);
    assert_eq!(decoded.operations[0].body.as_ref(), b"alpha");
    assert_eq!(decoded.operations[1].body.as_ref(), b"beta payload");
    assert_eq!(decoded.next_chain, encoded.next_chain());

    assert_eq!(
        logical_operation_digest(&operations[0]).as_bytes(),
        &[
            84, 248, 115, 220, 74, 85, 126, 156, 231, 81, 62, 249, 63, 163, 57, 251, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    );
    assert_eq!(
        ozzy_journal::integrity::hash("ozzy segment golden fixture v1", encoded.as_bytes())
            .as_bytes(),
        &[
            44, 236, 122, 7, 216, 5, 114, 52, 93, 136, 242, 182, 183, 155, 191, 201, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    );
}

#[test]
fn every_truncated_group_prefix_is_rejected() {
    let segment = segment();
    let operations = two_operations();
    let encoded = encode_group(
        &segment,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
    )
    .unwrap();

    for length in 0..encoded.as_bytes().len() {
        assert!(
            decode_group(
                &segment,
                1,
                SEGMENT_HEADER_BYTES as u64,
                ChainPosition::GENESIS,
                &encoded.as_bytes()[..length],
                DecodeLimits::default(),
            )
            .is_err(),
            "accepted truncated group at {length} bytes"
        );
    }
}

#[test]
fn corruption_and_wrong_chain_are_rejected() {
    let segment = segment();
    let operations = two_operations();
    let encoded = encode_group(
        &segment,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
    )
    .unwrap();

    for offset in [20, 192, 205, 3999, 4040] {
        let mut corrupt = encoded.as_bytes().to_vec();
        corrupt[offset] ^= 0x01;
        assert!(
            decode_group(
                &segment,
                1,
                SEGMENT_HEADER_BYTES as u64,
                ChainPosition::GENESIS,
                &corrupt,
                DecodeLimits::default(),
            )
            .is_err(),
            "accepted corruption at byte {offset}"
        );
    }

    assert!(matches!(
        decode_group(
            &segment,
            1,
            SEGMENT_HEADER_BYTES as u64,
            ChainPosition::new(2, Digest::ZERO),
            encoded.as_bytes(),
            DecodeLimits::default(),
        ),
        Err(CodecError::ChainMismatch { expected_op: 2 })
    ));
}

#[test]
fn decoder_enforces_body_and_entry_count_limits() {
    let segment = segment();
    let operations = two_operations();
    let encoded = encode_group(
        &segment,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
    )
    .unwrap();

    assert!(matches!(
        decode_group(
            &segment,
            1,
            SEGMENT_HEADER_BYTES as u64,
            ChainPosition::GENESIS,
            encoded.as_bytes(),
            DecodeLimits {
                max_entries: 1,
                ..DecodeLimits::default()
            },
        ),
        Err(CodecError::LimitExceeded {
            kind: "entry count",
            ..
        })
    ));
    assert!(matches!(
        decode_group(
            &segment,
            1,
            SEGMENT_HEADER_BYTES as u64,
            ChainPosition::GENESIS,
            encoded.as_bytes(),
            DecodeLimits {
                max_decoded_body_bytes: 4,
                ..DecodeLimits::default()
            },
        ),
        Err(CodecError::LimitExceeded {
            kind: "decoded body bytes",
            ..
        })
    ));
}

#[test]
fn segment_header_rejects_changed_and_nonzero_reserved_bytes() {
    let encoded = encode_segment_header(&segment());
    for offset in [0, 16, 88, 120, 4095] {
        let mut corrupt = encoded;
        corrupt[offset] ^= 0x01;
        assert!(decode_segment_header(&corrupt).is_err());
    }
}

#[test]
fn scan_keeps_complete_groups_and_classifies_every_truncated_tail() {
    let segment = segment();
    let first = operation(1, Digest::ZERO, OperationKind::Barrier, b"first");
    let first_group = encode_group(
        &segment,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &[first],
    )
    .unwrap();
    let second = operation(
        2,
        first_group.next_chain().previous_digest(),
        OperationKind::Barrier,
        b"second",
    );
    let second_group = encode_group(
        &segment,
        2,
        first_group.end_offset(),
        first_group.next_chain(),
        &[second],
    )
    .unwrap();
    let third = operation(
        3,
        second_group.next_chain().previous_digest(),
        OperationKind::Barrier,
        b"third",
    );
    let third_group = encode_group(
        &segment,
        3,
        second_group.end_offset(),
        second_group.next_chain(),
        &[third],
    )
    .unwrap();

    let mut complete_prefix = encode_segment_header(&segment).to_vec();
    complete_prefix.extend_from_slice(first_group.as_bytes());
    complete_prefix.extend_from_slice(second_group.as_bytes());
    let valid_bytes = complete_prefix.len() as u64;
    let complete_scan = scan_segment(
        &complete_prefix,
        1,
        ChainPosition::GENESIS,
        DecodeLimits::default(),
    )
    .unwrap();
    SegmentHeader::new(group_id(), 8, Some(7), complete_scan.digest, 64 * MIB).unwrap();
    for tail_bytes in 1..third_group.as_bytes().len() {
        let mut file = complete_prefix.clone();
        file.extend_from_slice(&third_group.as_bytes()[..tail_bytes]);
        let scan = scan_segment(&file, 1, ChainPosition::GENESIS, DecodeLimits::default()).unwrap();
        assert_eq!(scan.groups.len(), 2);
        assert_eq!(scan.valid_bytes, valid_bytes);
        assert_eq!(scan.next_group_number, 3);
        assert_eq!(scan.next_chain, second_group.next_chain());
        assert_eq!(scan.digest, complete_scan.digest);
        assert!(matches!(
            scan.tail,
            TailState::Truncated { bytes, .. } if bytes == tail_bytes
        ));
    }
}

#[test]
fn scan_distinguishes_unused_zeros_from_corruption() {
    let segment = segment();
    let operations = two_operations();
    let group = encode_group(
        &segment,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
    )
    .unwrap();
    let mut file = encode_segment_header(&segment).to_vec();
    file.extend_from_slice(group.as_bytes());
    let valid_bytes = file.len() as u64;
    file.resize(file.len() + 4096, 0);

    let scan = scan_segment(&file, 1, ChainPosition::GENESIS, DecodeLimits::default()).unwrap();
    assert_eq!(scan.valid_bytes, valid_bytes);
    assert_eq!(scan.groups.len(), 1);
    assert_eq!(scan.tail, TailState::ZeroFilled { bytes: 4096 });

    file[SEGMENT_HEADER_BYTES + 192] ^= 1;
    assert!(matches!(
        scan_segment(&file, 1, ChainPosition::GENESIS, DecodeLimits::default(),),
        Err(CodecError::DigestMismatch("canonical body"))
    ));
}

#[test]
fn successor_segment_requires_a_nonzero_predecessor_digest() {
    assert!(matches!(
        SegmentHeader::new(group_id(), 8, Some(7), Digest::ZERO, 64 * MIB),
        Err(CodecError::InvalidPredecessor)
    ));
}

#[cfg(feature = "lz4")]
#[test]
fn lz4_body_round_trips_and_raw_fallback_remains_mixed_format_compatible() {
    let encoding = BodyEncoding::Lz4 {
        min_savings_bytes: 32,
    };
    compressed_body_roundtrip(encoding);
    compression_requires_possible_physical_savings(encoding);
}

#[cfg(feature = "lz4")]
fn compression_requires_possible_physical_savings(encoding: BodyEncoding) {
    let one_write_body = vec![0x5a; 1024];
    let one_write = operation(1, Digest::ZERO, OperationKind::Barrier, &one_write_body);
    let encoded = encode_group_with_body_encoding(
        &segment(),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &[one_write],
        encoding,
    )
    .unwrap();
    assert_eq!(&encoded.as_bytes()[176..178], &[0, 0]);
    let decoded = decode_group(
        &segment(),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        encoded.as_bytes(),
        DecodeLimits::default(),
    )
    .unwrap();
    assert_eq!(decoded.operations[0].body.as_ref(), one_write_body);

    // Each body fits in one write unit alone, but together they span two.
    let body = vec![0x5a; 2048];
    let first = operation(1, Digest::ZERO, OperationKind::Barrier, &body);
    let second = operation(
        2,
        logical_operation_digest(&first),
        OperationKind::Barrier,
        &body,
    );
    let operations = [first, second];
    let raw = encode_group(
        &segment(),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
    )
    .unwrap();
    let compressed = encode_group_with_body_encoding(
        &segment(),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
        encoding,
    )
    .unwrap();
    assert!(compressed.as_bytes().len() < raw.as_bytes().len());
    let decoded = decode_group(
        &segment(),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        compressed.as_bytes(),
        DecodeLimits::default(),
    )
    .unwrap();
    assert_eq!(decoded.operations.len(), 2);
    for operation in decoded.operations {
        assert_eq!(operation.body.as_ref(), body);
    }
}

#[cfg(feature = "lz4")]
fn compressed_body_roundtrip(encoding: BodyEncoding) {
    let body = vec![0x5a; 16 * 1024];
    let operation = operation(1, Digest::ZERO, OperationKind::Append, &body);
    let raw = encode_group(
        &segment(),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &[operation],
    )
    .unwrap();
    let encoded = encode_group_with_body_encoding(
        &segment(),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &[operation],
        encoding,
    )
    .unwrap();
    assert!(encoded.as_bytes().len() < raw.as_bytes().len());
    assert_eq!(
        u16::from_be_bytes(encoded.as_bytes()[176..178].try_into().unwrap()),
        match encoding {
            BodyEncoding::Raw => 0,
            BodyEncoding::Lz4 { .. } => 1,
        }
    );
    let decoded = decode_group(
        &segment(),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        encoded.as_bytes(),
        DecodeLimits::default(),
    )
    .unwrap();
    assert_eq!(decoded.operations[0].body.as_ref(), body);
    assert_eq!(
        decoded.operations[0].digest,
        logical_operation_digest(&operation)
    );
}
