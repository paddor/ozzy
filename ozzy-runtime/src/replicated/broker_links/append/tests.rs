use super::*;
use ozzy_proto::{Envelope, EnvelopeLimits};

fn limits() -> AppendLinkLimits {
    AppendLinkLimits {
        writers: 4,
        requests: 2,
        records: 4,
        bytes: 65536,
    }
}

fn cost() -> Cost {
    Cost {
        requests: 1,
        records: 2,
        bytes: 8192,
        peer_bytes: 16,
        ..Cost::default()
    }
}

fn peer(node: u8) -> PeerLimit {
    PeerLimit {
        node: NodeId::from_bytes([node; 16]),
        records: 4,
        bytes: 32,
    }
}

#[test]
fn aggregate_windows_do_not_multiply_with_brokers_or_release_on_confirmation() {
    let budget = Arc::new(Budget::new(Some(limits())));
    let first = budget.acquire(cost(), Some(peer(1))).unwrap();
    let second = budget.acquire(cost(), Some(peer(2))).unwrap();
    assert!(!budget.available(cost(), Some(peer(3))));
    let transport = first.clone();
    drop(first);
    assert!(!budget.available(cost(), Some(peer(3))));
    drop(transport);
    let third = budget.acquire(cost(), Some(peer(3))).unwrap();
    assert!(!budget.available(cost(), Some(peer(1))));
    drop((second, third));
    assert!(budget.available(cost(), Some(peer(1))));
}

#[test]
fn concurrent_alias_release_never_overbooks_peer_or_aggregate_credit() {
    let budget = Arc::new(Budget::new(Some(AppendLinkLimits {
        writers: 1,
        requests: 4,
        records: 4,
        bytes: 65536,
    })));
    let start = Arc::new(std::sync::Barrier::new(17));
    let finish = Arc::new(std::sync::Barrier::new(17));
    let mut workers = Vec::new();
    for _ in 0..16 {
        let budget = budget.clone();
        let start = start.clone();
        let finish = finish.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            let lease = budget.acquire(
                Cost {
                    requests: 1,
                    records: 1,
                    bytes: 1024,
                    peer_bytes: 1,
                    ..Cost::default()
                },
                Some(PeerLimit {
                    records: 4,
                    bytes: 4,
                    ..peer(1)
                }),
            );
            finish.wait();
            lease
        }));
    }
    start.wait();
    finish.wait();
    let leases: Vec<_> = workers
        .into_iter()
        .filter_map(|worker| worker.join().unwrap())
        .collect();
    assert!(!leases.is_empty());
    assert!(leases.len() <= 4);
    drop(leases);
    assert!(budget.available(
        Cost {
            requests: 4,
            records: 4,
            bytes: 4096,
            peer_bytes: 4,
            ..Cost::default()
        },
        Some(PeerLimit {
            records: 4,
            bytes: 4,
            ..peer(1)
        })
    ));
}

#[test]
fn shared_peer_window_is_directional_and_unused_writer_lanes_count() {
    let budget = Arc::new(Budget::new(Some(limits())));
    let held = budget.acquire(cost(), Some(peer(1))).unwrap();
    let small = PeerLimit {
        records: 2,
        ..peer(1)
    };
    assert!(!budget.available(cost(), Some(small)));
    assert!(budget.available(cost(), Some(peer(2))));
    let idle = budget
        .acquire(
            Cost {
                writers: 4,
                bytes: 57344,
                ..Cost::default()
            },
            None,
        )
        .unwrap();
    assert!(!budget.available(
        Cost {
            writers: 1,
            ..Cost::default()
        },
        None
    ));
    assert!(!budget.available(cost(), Some(peer(2))));
    drop((held, idle));
    assert!(budget.available(cost(), Some(peer(1))));
}

#[test]
fn unused_writer_capacity_cannot_consume_the_largest_progress_reservation() {
    let budget = Arc::new(Budget::new(Some(limits())));
    let owner = budget
        .acquire(
            Cost {
                writers: 1,
                bytes: 40960,
                progress_bytes: 24576,
                ..Cost::default()
            },
            None,
        )
        .unwrap();
    assert!(
        budget
            .acquire(
                Cost {
                    writers: 1,
                    bytes: 1,
                    ..Cost::default()
                },
                None
            )
            .is_none()
    );
    let request = budget.acquire(cost(), Some(peer(1))).unwrap();
    assert!(!budget.available(
        Cost {
            writers: 1,
            bytes: 1,
            ..Cost::default()
        },
        None
    ));
    drop(request);
    assert!(!budget.available(
        Cost {
            writers: 1,
            bytes: 1,
            ..Cost::default()
        },
        None
    ));
    drop(owner);
    assert!(budget.available(
        Cost {
            writers: 1,
            bytes: 1,
            ..Cost::default()
        },
        None
    ));
}

#[test]
fn encoded_payload_is_shared_and_independent_frame_aliases_hold_the_budget() {
    let budget = Arc::new(Budget::new(Some(AppendLinkLimits {
        requests: 1,
        ..limits()
    })));
    let lease = budget.acquire(cost(), Some(peer(1))).unwrap();
    let payload = Bytes::from(vec![7; 4096]);
    let pointer = payload.as_ptr();
    let original = Message::multipart([
        Bytes::from(vec![1; 16]),
        Bytes::from(vec![2; 64]),
        Bytes::from(vec![3; 106]),
        payload,
    ]);
    let tracked = track(&original, &lease, None, false);
    assert_eq!(tracked.part_slice(3).unwrap().as_ptr(), pointer);
    let metadata = tracked.part_bytes(2).unwrap();
    let body = tracked.part_bytes(3).unwrap();
    drop((original, tracked, lease));
    assert!(!budget.available(cost(), Some(peer(1))));
    drop(body);
    assert!(!budget.available(cost(), Some(peer(1))));
    assert_eq!(metadata.as_ref(), &[3; 106]);
    drop(metadata);
    assert!(budget.available(cost(), Some(peer(1))));
}

fn stream(registry: &Registry, writer: u8) -> (Arc<Stream>, mpsc::Receiver<Message>) {
    let memory = registry
        .budget
        .acquire(
            Cost {
                writers: 1,
                bytes: 128,
                ..Cost::default()
            },
            None,
        )
        .unwrap();
    let (sender, incoming) = mpsc::channel(8);
    (
        Arc::new(Stream {
            id: RequestId::from_bytes([writer; 16]),
            sender,
            ready: StateSignal::default(),
            failed: AtomicBool::new(false),
            memory,
        }),
        incoming,
    )
}

fn admit(registry: &Registry, stream: &Arc<Stream>, node: u8, id: u8, end: u64) {
    let lease = registry.budget.acquire(cost(), Some(peer(node))).unwrap();
    registry.requests.lock().unwrap().insert(
        RequestId::from_bytes([id; 16]),
        Request {
            stream: Arc::downgrade(stream),
            writer: stream.id,
            broker: peer(node).node,
            session: LinkSessionId::from_bytes([node; 16]),
            end,
            remaining_replies: 3,
            lease,
        },
    );
}

fn reply(node: u8, id: u8) -> (Message, Envelope) {
    let envelope = Envelope {
        opcode: Opcode::Appended,
        response: true,
        request_id: Some(RequestId::from_bytes([id; 16])),
        sender: peer(node).node,
        session: Some(LinkSessionId::from_bytes([node; 16])),
    };
    let header = envelope
        .encode_header(
            106,
            0,
            EnvelopeLimits {
                max_metadata_bytes: 1024,
                max_payload_bytes: 1024,
            },
        )
        .unwrap();
    (
        Message::multipart([
            Bytes::copy_from_slice(peer(node).node.as_bytes()),
            Bytes::copy_from_slice(&header),
            Bytes::from(vec![5; 106]),
            Bytes::new(),
        ]),
        envelope,
    )
}

fn deliver(registry: &Registry, node: u8, id: u8) -> bool {
    let (message, envelope) = reply(node, id);
    registry.receive(
        peer(node).node,
        &message,
        Packet {
            envelope,
            metadata: message.part_slice(2).unwrap(),
            payload: &[],
        },
    )
}

#[test]
fn partial_confirmation_and_local_retry_do_not_fence_other_writers() {
    let registry = Registry::new(Some(limits()));
    let (first, mut a) = stream(&registry, 10);
    let (second, mut b) = stream(&registry, 20);
    admit(&registry, &first, 1, 11, 4);
    admit(&registry, &second, 1, 21, 2);
    assert!(deliver(&registry, 1, 11));
    let frame = a.try_recv().unwrap();
    registry.forget(first.id, Some(2));
    assert!(
        registry
            .requests
            .lock()
            .unwrap()
            .contains_key(&RequestId::from_bytes([11; 16]))
    );
    registry.forget(first.id, None);
    assert!(!deliver(&registry, 1, 11));
    assert!(deliver(&registry, 1, 21));
    assert!(b.try_recv().is_ok());
    assert!(!second.failed.load(Ordering::Acquire));
    assert!(!registry.budget.available(cost(), Some(peer(2))));
    drop(frame);
    assert!(registry.budget.available(cost(), Some(peer(2))));
}

#[test]
fn excessive_replies_fence_one_writer_and_physical_disconnect_fences_one_broker() {
    let registry = Registry::new(Some(limits()));
    let (first, _a) = stream(&registry, 10);
    let (second, mut b) = stream(&registry, 20);
    admit(&registry, &first, 1, 11, 4);
    admit(&registry, &second, 2, 21, 2);
    for _ in 0..4 {
        assert!(deliver(&registry, 1, 11));
    }
    assert!(first.failed.load(Ordering::Acquire));
    assert!(!second.failed.load(Ordering::Acquire));
    registry.fence(peer(1).node);
    assert!(!deliver(&registry, 1, 11));
    assert!(deliver(&registry, 2, 21));
    assert!(b.try_recv().is_ok());
    assert!(!second.failed.load(Ordering::Acquire));
}
