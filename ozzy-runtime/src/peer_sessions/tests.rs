use super::*;

mod shared;

fn node(id: u8) -> NodeId {
    NodeId::from_bytes([id; 16])
}
fn pair() -> (Sessions, Sessions) {
    (
        Sessions::new(node(1), DataLimits::default(), 4).unwrap(),
        Sessions::new(node(2), DataLimits::default(), 4).unwrap(),
    )
}
fn receive(to: &mut Sessions, source: NodeId, frames: &[Bytes]) -> Handled {
    let refs: Vec<_> = frames.iter().map(Bytes::as_ref).collect();
    to.receive(
        source,
        ozzy_proto::decode_packet(&refs, DataLimits::default().envelope).unwrap(),
    )
    .unwrap()
}

#[test]
fn crossed_hellos_converge_even_when_welcome_overtakes_abandoned_hello() {
    for welcome_first in [false, true] {
        for duplicate_welcome in [false, true] {
            let (mut a, mut b) = pair();
            let ha = a.start(node(2)).unwrap();
            let hb = b.start(node(1)).unwrap();
            let welcome = receive(&mut b, node(1), &ha).reply.unwrap();
            let duplicate = receive(&mut b, node(1), &ha).reply.unwrap();
            assert_eq!(welcome, duplicate);
            let reply = if duplicate_welcome {
                &duplicate
            } else {
                &welcome
            };
            if !welcome_first {
                assert!(receive(&mut a, node(2), &hb).reply.is_none());
            }
            receive(&mut a, node(2), reply);
            if welcome_first {
                assert!(receive(&mut a, node(2), &hb).reply.is_none());
            }
            assert!(a.session(node(2)).is_some());
            assert_eq!(a.session(node(2)), b.session(node(1)));
            // A later WELCOME without an abandoned attempt must not erase the
            // old receive fence. Delay the crossed HELLO across several links.
            for _ in 0..3 {
                let hello = a.start(node(2)).unwrap();
                let welcome = receive(&mut b, node(1), &hello).reply.unwrap();
                receive(&mut a, node(2), &welcome);
                assert!(receive(&mut a, node(2), &hb).reply.is_none());
                assert_eq!(a.session(node(2)), b.session(node(1)));
            }
            // A later, intentional higher-ID reconnect must not be suppressed.
            let old = a.session(node(2)).unwrap();
            let hello = b.start(node(1)).unwrap();
            let welcome = receive(&mut a, node(2), &hello).reply.unwrap();
            receive(&mut b, node(1), &welcome);
            assert_eq!(a.session(node(2)), b.session(node(1)));
            assert!(!a.is_current(node(2), old));
            assert!(receive(&mut a, node(2), &hb).reply.is_none());
        }
    }
}

#[test]
fn stale_welcome_never_completes_new_attempt() {
    let (mut a, mut b) = pair();
    let hello = a.start(node(2)).unwrap();
    let welcome = receive(&mut b, node(1), &hello).reply.unwrap();
    assert_eq!(a.session(node(2)), None);
    receive(&mut a, node(2), &welcome);
    assert!(a.session(node(2)).is_some());
    let hello = a.start(node(2)).unwrap();
    receive(&mut a, node(2), &welcome);
    assert_eq!(a.session(node(2)), None);
    let next = receive(&mut b, node(1), &hello).reply.unwrap();
    assert_ne!(next, welcome);
    receive(&mut a, node(2), &next);
    assert!(a.session(node(2)).is_some());
    assert_eq!(a.session(node(2)), b.session(node(1)));
}

#[test]
fn negotiation_bounds_peers_and_enforces_reader_capability() {
    let mut a = Sessions::new(node(1), DataLimits::default(), 1).unwrap();
    a.start(node(2)).unwrap();
    assert!(matches!(
        a.start(node(3)),
        Err(Error::TooManyPendingRequests)
    ));
    assert!(a.start(node(1)).is_err());
    let mut b = Sessions::new(node(2), DataLimits::default(), 1).unwrap();
    let hello = a.start(node(2)).unwrap();
    let mut refs: Vec<_> = hello.iter().map(Bytes::as_ref).collect();
    let packet = ozzy_proto::decode_packet(&refs, DataLimits::default().envelope).unwrap();
    let mut h = handshake::decode(packet, DataLimits::default().envelope).unwrap();
    h.parameters = Parameters::append(DataLimits::default(), handshake::PRODUCER).unwrap();
    let forged = encode(packet.envelope, h, DataLimits::default()).unwrap();
    refs = forged.iter().map(Bytes::as_ref).collect();
    assert!(
        b.receive(
            node(1),
            ozzy_proto::decode_packet(&refs, DataLimits::default().envelope).unwrap()
        )
        .is_err()
    );
    assert_eq!(b.session(node(1)), None);
}
