use ozzy_proto::{Envelope, EnvelopeError, EnvelopeLimits, Opcode, decode_packet};
use ozzy_proto::{LinkSessionId, NodeId, RequestId};

fn notification() -> Envelope {
    Envelope {
        opcode: Opcode::Commit,
        response: false,
        request_id: None,
        sender: NodeId::from_bytes([0x11; 16]),
        session: Some(LinkSessionId::from_bytes([0x22; 16])),
    }
}

#[test]
fn validation_without_encoding_preserves_wire_limits_and_identity_errors() {
    let limits = EnvelopeLimits {
        max_metadata_bytes: usize::MAX,
        max_payload_bytes: usize::MAX,
    };
    for envelope in [
        notification(),
        Envelope {
            session: None,
            ..notification()
        },
        Envelope {
            response: true,
            ..notification()
        },
        Envelope {
            sender: NodeId::from_bytes([0; 16]),
            ..notification()
        },
    ] {
        for (metadata, payload) in [
            (0, 0),
            (64, 128),
            (u32::MAX as usize, 0),
            (usize::MAX, 0),
            (0, usize::MAX),
        ] {
            assert_eq!(
                envelope.validate_frames(metadata, payload, limits),
                envelope
                    .encode_header(metadata, payload, limits)
                    .map(|_| ())
            );
        }
    }
    #[cfg(target_pointer_width = "64")]
    assert_eq!(
        notification().validate_frames(u32::MAX as usize + 1, 0, limits),
        Err(EnvelopeError::Length)
    );
    assert_eq!(
        notification().validate_frames(
            1,
            0,
            EnvelopeLimits {
                max_metadata_bytes: 0,
                ..limits
            }
        ),
        Err(EnvelopeError::Limit)
    );
}

#[test]
fn commit_envelope_matches_fixed_bytes_and_borrows_both_frames() {
    let envelope = notification();
    let golden = [
        0x4f, 0x5a, 0x59, 0, 1, 0x34, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x11,
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
        0x22, 0, 0, 0, 3, 0, 0, 0, 2,
    ];
    let metadata = [1, 2, 3];
    let payload = [4, 5];
    assert_eq!(
        envelope
            .encode_header(3, 2, EnvelopeLimits::default())
            .unwrap(),
        golden
    );
    let packet = decode_packet(&[&golden, &metadata, &payload], EnvelopeLimits::default()).unwrap();
    assert_eq!(packet.envelope, envelope);
    assert_eq!(packet.metadata.as_ptr(), metadata.as_ptr());
    assert_eq!(packet.payload.as_ptr(), payload.as_ptr());
}

#[test]
fn frame_shape_truncation_lengths_and_directional_limits_reject_before_command_decode() {
    let limits = EnvelopeLimits::default();
    let header = notification().encode_header(3, 2, limits).unwrap();
    let metadata = [1, 2, 3];
    let payload = [4, 5];
    let frames: [&[u8]; 3] = [&header, &metadata, &payload];
    for count in 0..3 {
        assert_eq!(
            decode_packet(&frames[..count], limits),
            Err(EnvelopeError::FrameCount)
        );
    }
    assert_eq!(
        decode_packet(&[&header, &metadata, &payload, &[]], limits),
        Err(EnvelopeError::FrameCount)
    );
    for length in 0..64 {
        assert_eq!(
            decode_packet(&[&header[..length], &metadata, &payload], limits),
            Err(EnvelopeError::HeaderLength)
        );
    }
    assert_eq!(
        decode_packet(&[&[0; 65], &metadata, &payload], limits),
        Err(EnvelopeError::HeaderLength)
    );
    for field in [56, 60] {
        let mut bad = header;
        bad[field..field + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(
            decode_packet(&[&bad, &metadata, &payload], limits),
            Err(EnvelopeError::Length)
        );
    }
    for restricted in [
        EnvelopeLimits {
            max_metadata_bytes: 2,
            max_payload_bytes: 2,
        },
        EnvelopeLimits {
            max_metadata_bytes: 3,
            max_payload_bytes: 1,
        },
    ] {
        assert_eq!(
            decode_packet(&frames, restricted),
            Err(EnvelopeError::Limit)
        );
        assert_eq!(
            notification().encode_header(3, 2, restricted),
            Err(EnvelopeError::Limit)
        );
    }
    let empty = notification().encode_header(0, 0, limits).unwrap();
    assert!(decode_packet(&[&empty, &[], &[]], limits).is_ok());
    assert_eq!(
        decode_packet(&[&header, &metadata[..2], &payload], limits),
        Err(EnvelopeError::Length)
    );
    if let Ok(oversized) = usize::try_from(u64::from(u32::MAX) + 1) {
        let unbounded = EnvelopeLimits {
            max_metadata_bytes: usize::MAX,
            max_payload_bytes: usize::MAX,
        };
        assert_eq!(
            notification().encode_header(oversized, 0, unbounded),
            Err(EnvelopeError::Length)
        );
        assert_eq!(
            notification().encode_header(0, oversized, unbounded),
            Err(EnvelopeError::Length)
        );
    }
}

#[test]
fn bad_magic_unsupported_versions_reserved_flags_and_zero_sender_are_rejected() {
    let limits = EnvelopeLimits::default();
    let valid = notification().encode_header(0, 0, limits).unwrap();
    for (field, value, expected) in [
        (0, 0, EnvelopeError::Magic),
        (3, 1, EnvelopeError::Magic),
        (4, 2, EnvelopeError::Major(2)),
        (4, 3, EnvelopeError::Major(3)),
        (6, 1, EnvelopeError::Flags),
        (7, 2, EnvelopeError::Flags),
        (7, 1, EnvelopeError::Correlation),
    ] {
        let mut bad = valid;
        bad[field] = value;
        assert_eq!(decode_packet(&[&bad, &[], &[]], limits), Err(expected));
    }
    let mut bad = valid;
    bad[24..40].fill(0);
    assert_eq!(
        decode_packet(&[&bad, &[], &[]], limits),
        Err(EnvelopeError::ZeroIdentity)
    );
    bad = valid;
    bad[40..56].fill(0);
    assert_eq!(
        decode_packet(&[&bad, &[], &[]], limits),
        Err(EnvelopeError::Session)
    );
}

#[test]
fn every_allocated_opcode_is_distinct_and_unallocated_values_reject() {
    let limits = EnvelopeLimits::default();
    for byte in 0..=u8::MAX {
        let mut header = notification().encode_header(0, 0, limits).unwrap();
        header[5] = byte;
        if byte == 1 {
            header[8] = 1;
            header[40..56].fill(0);
        } else if matches!(byte, 0x52 | 0x53) {
            header[40..56].fill(0);
        } else if byte == 2 {
            header[8] = 1;
            header[7] = 1;
        }
        let packet = decode_packet(&[&header, &[], &[]], limits);
        if matches!(byte, 0x01..=0x03 | 0x10..=0x17 | 0x20..=0x25 | 0x27..=0x29 | 0x30..=0x43 | 0x50..=0x54 | 0x7f)
        {
            let packet = packet.unwrap();
            assert_eq!(packet.envelope.opcode as u8, byte);
            assert_eq!(packet.envelope.encode_header(0, 0, limits).unwrap(), header);
        } else {
            assert_eq!(packet, Err(EnvelopeError::Opcode(byte)));
        }
    }
}

#[test]
fn handshake_and_response_envelopes_preserve_correlation_and_session_rules() {
    let limits = EnvelopeLimits::default();
    let hello = Envelope {
        opcode: Opcode::Hello,
        request_id: Some(RequestId::from_bytes([0x33; 16])),
        session: None,
        ..notification()
    };
    let welcome = Envelope {
        opcode: Opcode::Welcome,
        response: true,
        request_id: hello.request_id,
        ..notification()
    };
    for valid in [hello, welcome] {
        let header = valid.encode_header(0, 0, limits).unwrap();
        assert_eq!(
            decode_packet(&[&header, &[], &[]], limits)
                .unwrap()
                .envelope,
            valid
        );
    }
    for bad in [
        Envelope {
            response: true,
            ..hello
        },
        Envelope {
            request_id: None,
            ..hello
        },
        Envelope {
            session: notification().session,
            ..hello
        },
        Envelope {
            response: false,
            ..welcome
        },
    ] {
        assert_eq!(
            bad.encode_header(0, 0, limits),
            Err(EnvelopeError::Handshake)
        );
    }
    assert_eq!(
        hello.encode_header(0, 1, limits),
        Err(EnvelopeError::Handshake)
    );
    for bad in [
        Envelope {
            sender: NodeId::from_bytes([0; 16]),
            ..notification()
        },
        Envelope {
            request_id: Some(RequestId::from_bytes([0; 16])),
            ..notification()
        },
        Envelope {
            session: Some(LinkSessionId::from_bytes([0; 16])),
            ..notification()
        },
    ] {
        assert_eq!(
            bad.encode_header(0, 0, limits),
            Err(EnvelopeError::ZeroIdentity)
        );
    }
}

#[test]
fn publication_is_sessionless_without_weakening_peer_sessions() {
    let mut envelope = notification();
    envelope.opcode = Opcode::PreparePub;
    let limits = EnvelopeLimits::default();
    assert_eq!(
        envelope.encode_header(0, 8, limits),
        Err(EnvelopeError::Session)
    );
    envelope.session = None;
    let header = envelope.encode_header(0, 8, limits).unwrap();
    assert_eq!(
        decode_packet(&[&header, &[], &[0; 8]], limits)
            .unwrap()
            .envelope,
        envelope
    );
    envelope.request_id = Some(RequestId::new());
    assert_eq!(
        envelope.encode_header(0, 8, limits),
        Err(EnvelopeError::Session)
    );
    envelope.request_id = None;
    envelope.opcode = Opcode::Prepare;
    assert_eq!(
        envelope.encode_header(0, 8, limits),
        Err(EnvelopeError::Session)
    );
}
