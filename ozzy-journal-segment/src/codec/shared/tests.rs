use super::*;
use crate::codec::*;
use ozzy_journal::operation::logical_operation_digest;

fn fixture() -> (SegmentHeader, EncodedGroup, Vec<Vec<u8>>) {
    let header = SegmentHeader::new(GroupId::new(), 1, None, Digest::ZERO, 1024 * 1024).unwrap();
    let bodies: Vec<_> = (0..32)
        .map(|i| {
            let mut body = vec![b'a'; 256];
            body[0] = i;
            body
        })
        .collect();
    let mut previous = Digest::ZERO;
    let operations: Vec<_> = bodies
        .iter()
        .enumerate()
        .map(|(i, body)| {
            let operation = CanonicalOperation {
                group_id: header.group_id(),
                configuration_epoch: 1,
                original_view: 0,
                op_number: i as u64 + 1,
                previous_digest: previous,
                kind: OperationKind::Append,
                body,
            };
            previous = logical_operation_digest(&operation);
            operation
        })
        .collect();
    let mut prepared = prepare_shared_group_bodies(
        bodies.iter().map(Vec::as_slice),
        BodyEncoding::Lz4 {
            min_savings_bytes: 64,
        },
        Vec::new(),
        &mut BodyEncodeScratch::default(),
    )
    .unwrap();
    assert!(is_shared(&prepared.bytes));
    let layouts = prepared.entry_layouts(SEGMENT_HEADER_BYTES as u64).unwrap();
    assert!(layouts.windows(2).all(|pair| pair[0] == pair[1]));
    let finalized = finalize_group_bodies(
        &header,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
        &operations
            .iter()
            .map(|op| canonical_body_digest(op.body))
            .collect::<Vec<_>>(),
        &mut prepared,
    )
    .unwrap();
    (header, prepared.finish(finalized), bodies)
}

#[test]
fn one_block_roundtrips_independent_operations_and_indexed_selections() {
    let (header, encoded, bodies) = fixture();
    let limits = DecodeLimits::default();
    let (entries, shared) = parse(encoded.as_bytes(), limits).unwrap();
    assert_eq!(entries.len(), bodies.len());
    assert_eq!(
        decode_lz4_body(
            &encoded.as_bytes()[shared.encoded.clone()],
            bodies.len() * 256
        )
        .unwrap(),
        bodies.concat()
    );
    let decoded = decode_group(
        &header,
        1,
        encoded.start_offset(),
        ChainPosition::GENESIS,
        encoded.as_bytes(),
        limits,
    )
    .unwrap();
    assert_eq!(decoded.next_chain, encoded.next_chain());
    for (operation, body) in decoded.operations.iter().zip(&bodies) {
        assert_eq!(operation.body.as_ref(), body);
        assert_eq!(operation.entry_offset, encoded.start_offset());
        assert_eq!(operation.entry_bytes, shared.entry_bytes as u64);
        let selected = decode_indexed_operation(
            &header,
            operation.entry_offset,
            &encoded.as_bytes()[..shared.entry_bytes],
            operation.op_number,
            limits,
        )
        .unwrap();
        assert_eq!(&selected, operation);
    }
    assert!(
        decode_indexed_operation(
            &header,
            encoded.start_offset(),
            &encoded.as_bytes()[..shared.entry_bytes],
            33,
            limits
        )
        .is_err()
    );
}

#[test]
fn torn_headers_bodies_and_seals_fail_and_intact_shared_bodies_can_be_salvaged() {
    let (header, encoded, bodies) = fixture();
    let limits = DecodeLimits::default();
    let (_, shared) = parse(encoded.as_bytes(), limits).unwrap();
    let decode = |bytes: &[u8]| {
        decode_group(
            &header,
            1,
            encoded.start_offset(),
            ChainPosition::GENESIS,
            bytes,
            limits,
        )
        .map(|_| ())
    };
    for length in [
        0,
        1,
        7,
        63,
        HEADER_BYTES,
        shared.encoded.start,
        shared.encoded.end - 1,
        encoded.as_bytes().len() - 1,
    ] {
        assert!(
            decode(&encoded.as_bytes()[..length]).is_err(),
            "length {length}"
        );
    }
    for offset in [
        0,
        8,
        16,
        24,
        32,
        HEADER_BYTES + 104,
        shared.encoded.start,
        shared.encoded.end - 1,
        encoded.as_bytes().len() - 32,
    ] {
        let mut damaged = encoded.as_bytes().to_vec();
        damaged[offset] ^= 1;
        assert!(decode(&damaged).is_err(), "offset {offset}");
    }
    let mut segment = encode_segment_header(&header).to_vec();
    segment.extend_from_slice(encoded.as_bytes());
    *segment.last_mut().unwrap() ^= 1;
    assert!(scan_segment(&segment, 1, ChainPosition::GENESIS, limits).is_err());
    let recovered = salvage_entries(&header, &segment, 1, 32, limits).unwrap();
    assert_eq!(
        recovered
            .iter()
            .map(|op| op.body.as_ref())
            .collect::<Vec<_>>(),
        bodies.iter().map(Vec::as_slice).collect::<Vec<_>>()
    );
}

#[test]
fn declared_lengths_and_per_operation_limits_are_checked_before_decompression() {
    let (_, encoded, _) = fixture();
    let limits = DecodeLimits::default();
    for (field, value) in [(8, u64::MAX), (16, u64::MAX), (24, u64::MAX), (24, 8191)] {
        let mut bytes = encoded.as_bytes().to_vec();
        put_u64(&mut bytes, field, value);
        let digest = hash_with_zeroed_range(HASH_CONTEXT, &bytes[..HEADER_BYTES], HASH);
        bytes[HASH].copy_from_slice(digest.as_bytes());
        assert!(parse(&bytes, limits).is_err());
    }
    for limits in [
        DecodeLimits {
            max_entries: 31,
            ..limits
        },
        DecodeLimits {
            max_decoded_body_bytes: 255,
            ..limits
        },
        DecodeLimits {
            max_group_decoded_body_bytes: 8191,
            ..limits
        },
    ] {
        assert!(parse(encoded.as_bytes(), limits).is_err());
    }
}

#[test]
fn tiny_or_incompressible_groups_stay_raw() {
    let mut random = 13u64;
    let large: Vec<u8> = (0..8192)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random as u8
        })
        .collect();
    for body in [&b"tiny"[..], large.as_slice()] {
        let group = prepare_shared_group_bodies(
            std::iter::once(body),
            BodyEncoding::Lz4 {
                min_savings_bytes: 64,
            },
            Vec::new(),
            &mut BodyEncodeScratch::default(),
        )
        .unwrap();
        assert_eq!(group.entries[0].codec, 0);
    }
}
