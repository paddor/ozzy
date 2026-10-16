use super::*;
use std::num::NonZeroU64;

fn profile(roles: u32) -> Parameters {
    let mut profile = Parameters::streaming(DataLimits::default(), roles).unwrap();
    profile.capabilities |= handshake::OWNER_READ | handshake::OWNER_ROUTING;
    profile.required_capabilities = 0;
    profile
}

fn configured(id: u8, parameters: Parameters, required: u32, maximum: usize) -> Sessions {
    Sessions::with_ids(
        node(id),
        parameters,
        required,
        maximum,
        LinkIds::deterministic(NonZeroU64::new(u64::from(id)).unwrap()),
    )
    .unwrap()
}

fn transcript() -> Vec<Vec<Bytes>> {
    let mut a = configured(1, profile(handshake::OWNER), handshake::OWNER, 2);
    let mut b = configured(2, profile(handshake::OWNER), handshake::OWNER, 2);
    let mut transcript = Vec::new();
    for _ in 0..5 {
        let left = a.start(node(2)).unwrap();
        let right = b.start(node(1)).unwrap();
        assert_eq!(left, a.start(node(2)).unwrap());
        let welcome = receive(&mut b, node(1), &left).reply.unwrap();
        assert_eq!(welcome, receive(&mut b, node(1), &left).reply.unwrap());
        receive(&mut a, node(2), &welcome);
        assert!(receive(&mut a, node(2), &right).reply.is_none());
        assert_eq!(a.session(node(2)), b.session(node(1)));
        transcript.extend([left, right, welcome]);
    }
    transcript
}

#[test]
fn shared_link_negotiation_replays_identical_crossed_reconnects() {
    assert_eq!(transcript(), transcript());
}

#[test]
fn mixed_clients_preserve_directional_profiles_without_partition_attachment() {
    let mut local = profile(handshake::OWNER);
    local.receive.max_parts = 64;
    local.receive.envelope.max_metadata_bytes = 1024;
    let mut broker = configured(1, local, 0, 3);
    for (id, role) in [(2, handshake::PRODUCER), (3, handshake::CONSUMER)] {
        let mut parameters = profile(role);
        parameters.receive.max_records = 32;
        parameters.receive.max_parts = 128;
        parameters.receive.envelope.max_payload_bytes = 8192;
        parameters.receive.max_record_bytes = 8192;
        if role == handshake::CONSUMER {
            parameters.capabilities = handshake::OWNER_READ;
        }
        let mut client = configured(id, parameters, handshake::OWNER, 1);
        let hello = client.start(node(1)).unwrap();
        let reply = receive(&mut broker, node(id), &hello).reply.unwrap();
        receive(&mut client, node(1), &reply);
        assert_eq!(client.session(node(1)), broker.session(node(id)));
        let selected = broker.established(node(id)).unwrap().remote;
        assert_eq!(selected.roles, role);
        assert_eq!(selected.receive, parameters.receive);
        assert_eq!(selected.capabilities, parameters.capabilities);
        let expected = DataLimits {
            envelope: ozzy_proto::EnvelopeLimits {
                max_metadata_bytes: 1024,
                max_payload_bytes: 8192,
            },
            max_records: 32,
            max_parts: 64,
            max_record_bytes: 8192,
        };
        assert_eq!(broker.established(node(id)).unwrap().send, expected);
        assert_eq!(client.established(node(1)).unwrap().send, expected);
        assert_eq!(
            client.established(node(1)).unwrap().remote.receive,
            local.receive
        );
    }
    assert_ne!(broker.session(node(2)), broker.session(node(3)));
}

#[test]
fn configured_receive_profiles_survive_reconnect_without_widening_clients() {
    let mut native = profile(handshake::OWNER | 8);
    native.receive.envelope.max_payload_bytes = 1024;
    native.receive.max_record_bytes = 1024;
    let mut bulk = native.receive;
    bulk.envelope.max_payload_bytes = 65536;
    let mut local = Sessions::with_ids(
        node(1),
        native,
        0,
        2,
        LinkIds::deterministic(NonZeroU64::new(99).unwrap()),
    )
    .and_then(|sessions| sessions.with_receive_profiles(&[(node(2), bulk)]))
    .unwrap();
    let mut broker = configured(2, profile(handshake::OWNER | 8), handshake::OWNER, 1);
    let mut client = configured(3, profile(handshake::PRODUCER), handshake::OWNER, 1);
    let mut previous = None;
    for _ in 0..3 {
        for (id, remote, expected) in [(2, &mut broker, bulk), (3, &mut client, native.receive)] {
            let hello = remote.start(node(1)).unwrap();
            let welcome = receive(&mut local, node(id), &hello).reply.unwrap();
            receive(remote, node(1), &welcome);
            assert_eq!(
                remote.established(node(1)).unwrap().remote.receive,
                expected
            );
            assert_eq!(
                local.established(node(id)).unwrap().send.envelope,
                expected.envelope
            );
            assert_eq!(
                remote.established(node(1)).unwrap().send.envelope,
                expected.envelope
            );
        }
        let current = local.session(node(2)).unwrap();
        assert_ne!(Some(current), previous);
        previous = Some(current);
    }
    // A fixed profile is bounded configuration, never another peer's credit or
    // authorization. Invalid overrides cannot partially initialize a session.
    for profiles in [
        vec![(node(1), bulk)],
        vec![(node(0), bulk)],
        vec![(node(2), bulk), (node(2), bulk)],
        vec![(node(2), bulk), (node(3), bulk), (node(4), bulk)],
    ] {
        assert!(
            Sessions::with_ids(node(1), native, 0, 2, LinkIds::random())
                .and_then(|sessions| sessions.with_receive_profiles(&profiles))
                .is_err()
        );
    }
}

#[test]
fn disconnected_peer_keeps_its_tombstone_and_rejects_old_hello() {
    let mut a = configured(1, profile(handshake::OWNER), handshake::OWNER, 1);
    let mut b = configured(2, profile(handshake::OWNER), handshake::OWNER, 1);
    let hello = a.start(node(2)).unwrap();
    let welcome = receive(&mut b, node(1), &hello).reply.unwrap();
    receive(&mut a, node(2), &welcome);
    let old = b.session(node(1)).unwrap();
    b.disconnect(node(1));
    assert_eq!(b.session(node(1)), None);
    assert!(receive(&mut b, node(1), &hello).replaced.is_none());
    assert!(receive(&mut b, node(1), &hello).reply.is_none());
    assert_eq!(b.session(node(1)), None);
    assert!(matches!(
        b.start(node(3)),
        Err(Error::TooManyPendingRequests)
    ));
    let next = a.start(node(2)).unwrap();
    let welcome = receive(&mut b, node(1), &next).reply.unwrap();
    receive(&mut a, node(2), &welcome);
    assert_eq!(a.session(node(2)), b.session(node(1)));
    assert_ne!(b.session(node(1)), Some(old));
}

#[test]
fn rejected_role_or_unsolicited_welcome_consumes_no_peer_capacity() {
    let mut broker = configured(1, profile(handshake::OWNER), handshake::OWNER, 1);
    let mut client = configured(2, profile(handshake::PRODUCER), handshake::OWNER, 1);
    let hello = client.start(node(1)).unwrap();
    let frames: Vec<_> = hello.iter().map(Bytes::as_ref).collect();
    let packet = ozzy_proto::decode_packet(&frames, DataLimits::default().envelope).unwrap();
    assert!(broker.receive(node(2), packet).is_err());
    assert_eq!(broker.session(node(2)), None);

    let mut other = configured(3, profile(handshake::OWNER), handshake::OWNER, 1);
    let mut old = configured(1, profile(handshake::OWNER), handshake::OWNER, 1);
    let hello = old.start(node(3)).unwrap();
    let welcome = receive(&mut other, node(1), &hello).reply.unwrap();
    assert!(receive(&mut broker, node(3), &welcome).replaced.is_none());
    assert!(broker.peers.is_empty());
    assert!(broker.start(node(4)).is_ok());
}

#[test]
fn bounded_handshake_encoding_covers_every_known_capability() {
    let mut parameters = profile(31);
    parameters.capabilities = 0x1fff;
    parameters.required_capabilities = parameters.capabilities;
    let h = Handshake {
        superseded_hello: Some(1),
        instance_id: 2,
        hello_nonce: 3,
        parameters,
    };
    let envelope = Envelope {
        opcode: Opcode::Welcome,
        response: true,
        request_id: Some(RequestId::from_bytes([1; 16])),
        sender: node(1),
        session: Some(LinkSessionId::from_bytes([2; 16])),
    };
    let frames = encode(envelope, h, parameters.receive).unwrap();
    assert!(frames[1].len() <= 512);
    let refs: Vec<_> = frames.iter().map(Bytes::as_ref).collect();
    let packet = ozzy_proto::decode_packet(&refs, parameters.receive.envelope).unwrap();
    assert_eq!(
        handshake::decode(packet, parameters.receive.envelope).unwrap(),
        h
    );
}
