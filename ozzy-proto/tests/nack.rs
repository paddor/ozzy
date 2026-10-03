use ozzy_proto::nack::{self, Nack, RetryClass};
use ozzy_proto::{Envelope, EnvelopeLimits, Opcode, Packet};
use ozzy_proto::{LinkSessionId, NodeId, RequestId};

#[test]
fn authority_hint_has_fixed_bytes_and_rejects_invalid_identity_and_lengths() {
    use ozzy_proto::GroupId;
    use ozzy_proto::append::Authority;
    use ozzy_proto::nack::AuthorityHint;
    let hint = AuthorityHint {
        authority: Authority {
            group_id: GroupId::from_bytes([7; 16]),
            config_epoch: 8,
            view: 9,
        },
        primary: NodeId::from_bytes([10; 16]),
    };
    let bytes = hint.encode().unwrap();
    assert_eq!(&bytes[..16], &[7; 16]);
    assert_eq!(&bytes[16..24], &8_u64.to_be_bytes());
    assert_eq!(&bytes[24..32], &9_u64.to_be_bytes());
    assert_eq!(&bytes[32..], &[10; 16]);
    assert_eq!(AuthorityHint::decode(&bytes).unwrap(), hint);
    for end in 0..48 {
        assert!(AuthorityHint::decode(&bytes[..end]).is_err());
    }
    assert!(AuthorityHint::decode(&[0; 49]).is_err());
    for range in [0..16, 16..24, 32..48] {
        let mut zero = bytes;
        zero[range].fill(0);
        assert!(AuthorityHint::decode(&zero).is_err());
    }
    let genesis_view = AuthorityHint {
        authority: Authority {
            view: 0,
            ..hint.authority
        },
        ..hint
    };
    assert_eq!(
        AuthorityHint::decode(&genesis_view.encode().unwrap()).unwrap(),
        genesis_view
    );
}

#[test]
fn unknown_nack_codes_preserve_detail_without_parsing_diagnostics() {
    let envelope = Envelope {
        opcode: Opcode::Nack,
        response: true,
        request_id: Some(RequestId::from_bytes([1; 16])),
        sender: NodeId::from_bytes([2; 16]),
        session: Some(LinkSessionId::from_bytes([3; 16])),
    };
    let nack = Nack {
        code: 999,
        retry: RetryClass::UnknownOutcome,
        detail: b"\0\xfe",
        diagnostic: "unknown",
    };
    let mut metadata = Vec::with_capacity(100);
    nack::encode(envelope, nack, &mut metadata, EnvelopeLimits::default()).unwrap();
    assert_eq!(metadata, b"\x03\xe7\x05\0\0\0\x02\0\xfe\0\0\0\x07unknown");
    let packet = Packet {
        envelope,
        metadata: &metadata,
        payload: &[],
    };
    assert_eq!(
        nack::decode(packet, EnvelopeLimits::default()).unwrap(),
        nack
    );
    for end in 0..metadata.len() {
        assert!(
            nack::decode(
                Packet {
                    metadata: &metadata[..end],
                    ..packet
                },
                EnvelopeLimits::default()
            )
            .is_err()
        );
    }
    assert!(
        nack::decode(
            Packet {
                payload: b"extra",
                ..packet
            },
            EnvelopeLimits::default()
        )
        .is_err()
    );
    let mut trailing = metadata.clone();
    trailing.push(0);
    assert!(
        nack::decode(
            Packet {
                metadata: &trailing,
                ..packet
            },
            EnvelopeLimits::default()
        )
        .is_err()
    );
    let mut invalid = metadata.clone();
    invalid[2] = 0;
    assert!(
        nack::decode(
            Packet {
                metadata: &invalid,
                ..packet
            },
            EnvelopeLimits::default()
        )
        .is_err()
    );
    let mut small = vec![7];
    assert!(nack::encode(envelope, nack, &mut small, EnvelopeLimits::default()).is_err());
    assert_eq!(small, [7]);
}
