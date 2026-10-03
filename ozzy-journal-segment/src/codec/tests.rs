use super::*;
use ozzy_journal::operation::logical_operation_digest;

#[test]
fn physical_incarnation_changes_source_digests_without_changing_logical_history() {
    let header = SegmentHeader::new(GroupId::new(), 1, None, Digest::ZERO, 64 * 1024).unwrap();
    let replacement = header.clone().with_file_generation(1);
    assert_eq!(
        decode_segment_header(&encode_segment_header(&replacement)).unwrap(),
        replacement
    );
    let operation = CanonicalOperation {
        group_id: header.group_id(),
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Barrier,
        body: &[5; 16],
    };
    let encode = |header| {
        encode_group(
            header,
            1,
            SEGMENT_HEADER_BYTES as u64,
            ChainPosition::GENESIS,
            &[operation],
        )
        .unwrap()
    };
    let original = encode(&header);
    let repaired = encode(&replacement);
    assert_eq!(original.end_offset(), repaired.end_offset());
    assert_eq!(original.next_chain(), repaired.next_chain());
    assert_ne!(original.digest(), repaired.digest());
}

#[test]
fn segment_header_rejects_reserved_bytes_even_with_valid_checksum() {
    let header = SegmentHeader::new(GroupId::new(), 1, None, Digest::ZERO, 64 * 1024).unwrap();
    for offset in [
        128,
        129,
        130,
        131,
        132,
        135,
        136,
        2175,
        SEGMENT_HEADER_BYTES - 1,
    ] {
        let mut bytes = encode_segment_header(&header);
        bytes[offset] = 1;
        let digest =
            hash_with_zeroed_range(SEGMENT_HEADER_HASH_CONTEXT, &bytes, SEGMENT_DIGEST_RANGE);
        bytes[SEGMENT_DIGEST_RANGE].copy_from_slice(digest.as_bytes());
        assert!(matches!(
            decode_segment_header(&bytes),
            Err(CodecError::NonZeroReserved("segment header"))
        ));
    }
}

#[test]
fn entry_rejects_unsupported_codecs_and_reserved_fields() {
    let header = SegmentHeader::new(GroupId::new(), 1, None, Digest::ZERO, 64 * 1024).unwrap();
    let operation = CanonicalOperation {
        group_id: header.group_id(),
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Append,
        body: b"payload",
    };
    let group = encode_group(
        &header,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &[operation],
    )
    .unwrap();
    for codec in [2, 3, u16::MAX] {
        let mut bytes = group.as_bytes().to_vec();
        put_u16(&mut bytes, 176, codec);
        assert!(matches!(
            parse_entry(&bytes, 0, DecodeLimits::default()),
            Err(CodecError::UnsupportedCodec(actual)) if actual == codec
        ));
    }
    for offset in 180..ENTRY_HEADER_BYTES {
        let mut bytes = group.as_bytes().to_vec();
        bytes[offset] = 1;
        assert!(matches!(
            parse_entry(&bytes, 0, DecodeLimits::default()),
            Err(CodecError::NonZeroReserved("entry"))
        ));
    }
}

#[test]
fn raw_extents_match_contiguous_encoding_without_copying_bodies() {
    let group_id = GroupId::from_bytes([1; 16]);
    let segment = SegmentHeader::new(group_id, 1, None, Digest::ZERO, 1024 * 1024).unwrap();
    let bodies: Vec<_> = [0, 1, 7, 8, 9, 128, 4096, 65537]
        .into_iter()
        .map(|len| vec![0x5a; len])
        .collect();
    let mut chain = ChainPosition::GENESIS;
    let operations: Vec<_> = bodies
        .iter()
        .map(|body| {
            let operation = CanonicalOperation {
                group_id,
                configuration_epoch: 1,
                original_view: 0,
                op_number: chain.next_op_number(),
                previous_digest: chain.previous_digest(),
                kind: OperationKind::Append,
                body,
            };
            chain = ChainPosition::new(
                operation.op_number + 1,
                logical_operation_digest(&operation),
            );
            operation
        })
        .collect();
    let digests: Vec<_> = bodies
        .iter()
        .map(|body| canonical_body_digest(body))
        .collect();
    let expected = encode_group(
        &segment,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
    )
    .unwrap();
    let mut buffer = Vec::with_capacity(16 * 1024);
    let pointer = buffer.as_ptr();
    for _ in 0..3 {
        let mut prepared =
            prepare_raw_group_extents(bodies.iter().map(Vec::as_slice), buffer).unwrap();
        let layouts = prepared.entry_layouts(SEGMENT_HEADER_BYTES as u64).unwrap();
        let finalized = finalize_group_bodies(
            &segment,
            1,
            SEGMENT_HEADER_BYTES as u64,
            ChainPosition::GENESIS,
            &operations,
            &digests,
            &mut prepared,
        )
        .unwrap();
        assert_eq!(finalized.end_offset, expected.end_offset());
        assert_eq!(finalized.next_chain, expected.next_chain());
        let chunks: Vec<_> = prepared.extents(&operations).collect();
        for body in bodies.iter().filter(|body| !body.is_empty()) {
            assert!(chunks.iter().any(|chunk| chunk.as_ptr() == body.as_ptr()));
        }
        assert_eq!(chunks.concat(), expected.as_bytes());
        let decoded = decode_group(
            &segment,
            1,
            SEGMENT_HEADER_BYTES as u64,
            ChainPosition::GENESIS,
            expected.as_bytes(),
            DecodeLimits::default(),
        )
        .unwrap();
        for (layout, operation) in layouts.iter().zip(&decoded.operations) {
            assert_eq!(layout.entry_offset, operation.entry_offset);
            assert_eq!(layout.entry_bytes, operation.entry_bytes);
        }
        buffer = prepared.into_bytes();
        assert_eq!(
            buffer.as_ptr(),
            pointer,
            "metadata allocation must be reused"
        );
        assert!(buffer.len() < 16 * 1024, "payload must not occupy scratch");
    }
}

#[test]
#[cfg(feature = "lz4")]
fn lz4_requires_matching_canonical_body_digest() {
    let group_id = GroupId::from_bytes([1; 16]);
    let segment = SegmentHeader::new(group_id, 1, None, Digest::ZERO, 64 * 1024).unwrap();
    let body = vec![0x5a; 16 * 1024];
    let altered = vec![0x5b; body.len()];
    let operation = CanonicalOperation {
        group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Append,
        body: &body,
    };
    let body_digest = canonical_body_digest(&body);
    for (stored_body, valid) in [(&body, true), (&altered, false)] {
        let mut scratch = BodyEncodeScratch::default();
        let mut prepared = prepare_group_bodies(
            std::iter::once(stored_body.as_slice()),
            BodyEncoding::Lz4 {
                min_savings_bytes: 32,
            },
            Vec::new(),
            &mut scratch,
        )
        .unwrap();
        assert_eq!(prepared.entries[0].codec, 1);
        // The alternate frame decodes successfully, but its bytes must not be
        // accepted under the original canonical body/operation/structural seal.
        let finalized = finalize_group_bodies(
            &segment,
            1,
            SEGMENT_HEADER_BYTES as u64,
            ChainPosition::GENESIS,
            &[operation],
            &[body_digest],
            &mut prepared,
        )
        .unwrap();
        let encoded = prepared.finish(finalized);
        let decoded = decode_group(
            &segment,
            1,
            SEGMENT_HEADER_BYTES as u64,
            ChainPosition::GENESIS,
            encoded.as_bytes(),
            DecodeLimits::default(),
        );
        if valid {
            let decoded = decoded.unwrap();
            assert_eq!(decoded.operations[0].body.as_ref(), body);
            assert_eq!(decoded.next_chain, encoded.next_chain());
        } else {
            assert!(matches!(
                decoded,
                Err(CodecError::DigestMismatch("canonical body"))
            ));
        }
    }
}
