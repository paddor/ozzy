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
fn receive(to: &Sessions, source: NodeId, frames: &[Bytes]) -> Handled {
    let refs: Vec<_> = frames.iter().map(Bytes::as_ref).collect();
    to.receive(
        source,
        ozzy_proto::decode_packet(&refs, DataLimits::default().envelope).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn crossed_hellos_converge_even_when_welcome_overtakes_abandoned_hello() {
    for welcome_first in [false, true] {
        for duplicate_welcome in [false, true] {
            let (a, b) = pair();
            let ha = a.start(node(2)).unwrap();
            let hb = b.start(node(1)).unwrap();
            let welcome = receive(&b, node(1), &ha).reply.unwrap();
            let duplicate = receive(&b, node(1), &ha).reply.unwrap();
            assert_eq!(welcome, duplicate);
            let reply = if duplicate_welcome {
                &duplicate
            } else {
                &welcome
            };
            if !welcome_first {
                assert!(receive(&a, node(2), &hb).reply.is_none());
            }
            receive(&a, node(2), reply);
            if welcome_first {
                assert!(receive(&a, node(2), &hb).reply.is_none());
            }
            assert_eq!(a.session(node(2)), b.session(node(1)));
            a.ready(node(2)).await;
            b.ready(node(1)).await;
            // A later WELCOME without an abandoned attempt must not erase the
            // old receive fence. Delay the crossed HELLO across several links.
            for _ in 0..3 {
                let hello = a.start(node(2)).unwrap();
                let welcome = receive(&b, node(1), &hello).reply.unwrap();
                receive(&a, node(2), &welcome);
                assert!(receive(&a, node(2), &hb).reply.is_none());
                assert_eq!(a.session(node(2)), b.session(node(1)));
            }
            // A later, intentional higher-ID reconnect must not be suppressed.
            let old = a.session(node(2)).unwrap();
            let hello = b.start(node(1)).unwrap();
            let welcome = receive(&a, node(2), &hello).reply.unwrap();
            receive(&b, node(1), &welcome);
            assert_eq!(a.session(node(2)), b.session(node(1)));
            assert!(!a.is_current(node(2), old));
            assert!(receive(&a, node(2), &hb).reply.is_none());
        }
    }
}

#[tokio::test]
async fn stale_welcome_never_completes_new_attempt_and_readiness_is_cancel_safe() {
    let (a, b) = pair();
    let hello = a.start(node(2)).unwrap();
    let welcome = receive(&b, node(1), &hello).reply.unwrap();
    {
        let mut waiting = std::pin::pin!(a.ready(node(2)));
        assert!(futures::poll!(&mut waiting).is_pending());
    }
    receive(&a, node(2), &welcome);
    a.ready(node(2)).await;
    let hello = a.start(node(2)).unwrap();
    receive(&a, node(2), &welcome);
    assert_eq!(a.session(node(2)), None);
    let next = receive(&b, node(1), &hello).reply.unwrap();
    assert_ne!(next, welcome);
    receive(&a, node(2), &next);
    a.ready(node(2)).await;
    assert_eq!(a.session(node(2)), b.session(node(1)));
}

#[test]
fn negotiation_bounds_peers_and_enforces_reader_capability() {
    let a = Sessions::new(node(1), DataLimits::default(), 1).unwrap();
    a.start(node(2)).unwrap();
    assert!(matches!(
        a.start(node(3)),
        Err(Error::TooManyPendingRequests)
    ));
    assert!(a.start(node(1)).is_err());
    let b = Sessions::new(node(2), DataLimits::default(), 1).unwrap();
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
