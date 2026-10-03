use ozzy_proto::append::DataLimits;
use ozzy_proto::handshake::{self, Handshake, HandshakeError, Parameters};
use ozzy_proto::{Envelope, EnvelopeLimits, Opcode, Packet, decode_packet};
use ozzy_proto::{LinkSessionId, NodeId, RequestId};

fn envelope() -> Envelope {
    Envelope {
        opcode: Opcode::Hello,
        response: false,
        request_id: Some(RequestId::from_bytes([1; 16])),
        sender: NodeId::from_bytes([2; 16]),
        session: None,
    }
}

fn hello() -> Handshake {
    Handshake {
        superseded_hello: None,
        instance_id: 3,
        hello_nonce: 4,
        parameters: Parameters::append(DataLimits::default(), handshake::PRODUCER).unwrap(),
    }
}

fn bytes() -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1024);
    handshake::encode(envelope(), hello(), &mut bytes, EnvelopeLimits::default()).unwrap();
    bytes
}

fn decode(bytes: &[u8]) -> Result<Handshake, HandshakeError> {
    handshake::decode(
        Packet {
            envelope: envelope(),
            metadata: bytes,
            payload: &[],
        },
        EnvelopeLimits::default(),
    )
}

#[test]
fn hello_fixture_and_welcome_preserve_directional_receive_limits() {
    let mut bytes = bytes();
    assert_eq!(&bytes[..16], &3_u128.to_be_bytes());
    assert_eq!(&bytes[16..32], &4_u128.to_be_bytes());
    assert_eq!(
        &bytes[32..50],
        b"\x08versions\x00\x00\x00\x05\x00\x00\x00\x01\x01"
    );
    assert_eq!(decode(&bytes).unwrap(), hello());
    let mut owner = Parameters::append(
        DataLimits {
            max_records: 7,
            ..DataLimits::default()
        },
        handshake::OWNER,
    )
    .unwrap();
    owner.capabilities |= 1 << 8;
    let selected = owner.select(hello().parameters).unwrap();
    assert_eq!(selected.capabilities, handshake::OWNER_APPEND);
    assert_eq!(selected.receive.max_records, 7);
    assert_eq!(hello().parameters.receive.max_records, 1000);
    let welcome = Handshake {
        superseded_hello: None,
        instance_id: 8,
        hello_nonce: hello().hello_nonce,
        parameters: selected,
    };
    let header = handshake::encode(
        Envelope {
            opcode: Opcode::Welcome,
            response: true,
            session: Some(LinkSessionId::from_bytes([9; 16])),
            ..envelope()
        },
        welcome,
        &mut bytes,
        EnvelopeLimits::default(),
    )
    .unwrap();
    let packet = decode_packet(&[&header, &bytes, &[]], EnvelopeLimits::default()).unwrap();
    assert_eq!(
        handshake::decode(packet, EnvelopeLimits::default()).unwrap(),
        welcome
    );
}

#[test]
fn truncations_duplicate_unknown_names_and_forged_counts_are_rejected() {
    let bytes = bytes();
    for end in 0..bytes.len() {
        assert!(decode(&bytes[..end]).is_err(), "truncation {end}");
    }
    let mut duplicate = bytes.clone();
    duplicate.extend_from_slice(b"\x05ROLES\0\0\0\x04\0\0\0\x01");
    assert_eq!(decode(&duplicate), Err(HandshakeError::Properties));
    let mut extra = bytes.clone();
    extra.extend_from_slice(b"\x01x\0\0\0\x01z");
    assert_eq!(decode(&extra).unwrap(), hello());
    extra.extend_from_slice(b"\x01X\0\0\0\0");
    assert_eq!(decode(&extra), Err(HandshakeError::Properties));
    let mut forged = bytes.clone();
    forged[41..45].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(decode(&forged), Err(HandshakeError::Length));
    for range in [0..16, 16..32] {
        let mut zero = bytes.clone();
        zero[range].fill(0);
        assert_eq!(decode(&zero), Err(HandshakeError::Command));
    }
}

#[test]
fn requirements_cannot_downgrade_and_encode_never_grows_or_clobbers_on_failure() {
    let mut other = hello().parameters;
    other.capabilities = 2;
    other.required_capabilities = 2;
    assert_eq!(
        other.select(hello().parameters),
        Err(HandshakeError::Capabilities)
    );
    let mut bytes = bytes();
    let original = bytes.clone();
    let capacity = bytes.capacity();
    let pointer = bytes.as_ptr();
    handshake::encode(envelope(), hello(), &mut bytes, EnvelopeLimits::default()).unwrap();
    assert_eq!(bytes.as_ptr(), pointer);
    assert_eq!(bytes.capacity(), capacity);
    let invalid = Handshake {
        superseded_hello: None,
        instance_id: 0,
        ..hello()
    };
    assert!(handshake::encode(envelope(), invalid, &mut bytes, EnvelopeLimits::default()).is_err());
    assert_eq!(bytes, original);
    let mut small = vec![77];
    assert_eq!(
        handshake::encode(envelope(), hello(), &mut small, EnvelopeLimits::default()),
        Err(HandshakeError::Capacity)
    );
    assert_eq!(small, [77]);
}

#[test]
fn routing_capability_has_fixed_id_and_cannot_be_silently_downgraded() {
    let mut hello = hello();
    hello.parameters.capabilities |= handshake::OWNER_ROUTING;
    hello.parameters.required_capabilities |= handshake::OWNER_ROUTING;
    let mut bytes = Vec::with_capacity(1024);
    handshake::encode(envelope(), hello, &mut bytes, EnvelopeLimits::default()).unwrap();
    assert_eq!(decode(&bytes).unwrap(), hello);
    let name = b"capabilities";
    let index = bytes
        .windows(name.len())
        .position(|part| part == name)
        .unwrap();
    assert_eq!(
        &bytes[index + name.len()..index + name.len() + 12],
        b"\0\0\0\x08\0\0\0\x02\0\x01\0\x0a"
    );
    let mut owner = Parameters::append(DataLimits::default(), handshake::OWNER).unwrap();
    assert_eq!(
        owner.select(hello.parameters),
        Err(HandshakeError::Capabilities)
    );
    owner.capabilities |= handshake::OWNER_ROUTING;
    assert_eq!(
        owner.select(hello.parameters).unwrap().capabilities,
        handshake::OWNER_APPEND | handshake::OWNER_ROUTING
    );
}

#[test]
fn capability_lists_require_sorted_unique_ids_and_reject_unknown_required_ids() {
    let bytes = bytes();
    // Locate the value after each exact property name; leave the wire count intact.
    for name in [
        b"capabilities".as_slice(),
        b"required-capabilities".as_slice(),
    ] {
        let name_at = bytes
            .windows(name.len())
            .position(|window| window == name)
            .unwrap();
        let id_at = name_at + name.len() + 8;
        let mut unknown = bytes.clone();
        unknown[id_at..id_at + 2].copy_from_slice(&100_u16.to_be_bytes());
        assert!(decode(&unknown).is_err()); // required append is no longer supported.
        let mut zero = bytes.clone();
        zero[id_at..id_at + 2].fill(0);
        assert_eq!(decode(&zero), Err(HandshakeError::Capabilities));
    }
}

#[test]
fn streaming_requires_explicit_negotiation_and_preserves_directional_windows() {
    let limits = DataLimits::default();
    let parameters = Parameters::streaming(limits, handshake::PRODUCER, 4000, 32 << 20).unwrap();
    assert_eq!(handshake::OWNER_STREAM, 1 << 12);
    assert_eq!(
        parameters.capabilities,
        handshake::OWNER_APPEND | handshake::OWNER_STREAM
    );
    assert_eq!(parameters.required_capabilities, parameters.capabilities);
    let old_owner = Parameters::append(limits, handshake::OWNER).unwrap();
    assert_eq!(
        parameters.select(old_owner),
        Err(HandshakeError::Capabilities)
    );
    assert_eq!(
        old_owner.select(parameters),
        Err(HandshakeError::Capabilities)
    );
    let owner = Parameters::streaming(limits, handshake::OWNER, 8000, 64 << 20).unwrap();
    let selected = owner.select(parameters).unwrap();
    assert_eq!(selected.inflight_records, 8000);
    assert_eq!(selected.inflight_bytes, 64 << 20);
    let hello = Handshake {
        parameters,
        ..hello()
    };
    let mut bytes = Vec::with_capacity(1024);
    handshake::encode(envelope(), hello, &mut bytes, EnvelopeLimits::default()).unwrap();
    assert_eq!(decode(&bytes).unwrap(), hello);
    for name in [
        b"capabilities".as_slice(),
        b"required-capabilities".as_slice(),
    ] {
        let index = bytes
            .windows(name.len())
            .position(|field| field == name)
            .unwrap();
        assert_eq!(
            &bytes[index + name.len()..index + name.len() + 12],
            b"\0\0\0\x08\0\0\0\x02\0\x01\0\x0d"
        );
    }
    for (records, bytes) in [
        (0, 32 << 20),
        (limits.max_records as u64 - 1, 32 << 20),
        (4000, 0),
        (4000, limits.envelope.max_payload_bytes as u64 - 1),
    ] {
        assert_eq!(
            Parameters::streaming(limits, handshake::PRODUCER, records, bytes),
            Err(HandshakeError::Parameters)
        );
    }
}

#[test]
fn streaming_capability_requires_append_and_all_known_ids_round_trip() {
    let mut parameters =
        Parameters::streaming(DataLimits::default(), handshake::PRODUCER, 4000, 32 << 20).unwrap();
    parameters.capabilities = handshake::OWNER_STREAM;
    parameters.required_capabilities = handshake::OWNER_STREAM;
    assert_eq!(parameters.validate(), Err(HandshakeError::Parameters));
    for unassigned in [1 << 11, 1 << 13] {
        parameters.capabilities = handshake::OWNER_APPEND | handshake::OWNER_STREAM | unassigned;
        parameters.required_capabilities = parameters.capabilities;
        assert_eq!(parameters.validate(), Err(HandshakeError::Parameters));
    }
    // Former FETCH capability 12 is unassigned. Streaming remains capability 13.
    parameters.capabilities = 0x17ff;
    parameters.required_capabilities = 0x17ff;
    let hello = Handshake {
        parameters,
        ..hello()
    };
    let mut bytes = Vec::with_capacity(1024);
    handshake::encode(envelope(), hello, &mut bytes, EnvelopeLimits::default()).unwrap();
    assert_eq!(decode(&bytes).unwrap(), hello);
}
