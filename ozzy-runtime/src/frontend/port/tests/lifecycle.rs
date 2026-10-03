use super::*;
use crate::frontend::PublicationError;
use std::time::{Duration, Instant};

#[tokio::test]
async fn reordered_duplicate_and_foreign_completions_cannot_settle_another_request() {
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let mut budgets = capacity();
    budgets.data = Budget {
        queue_slots: 2,
        retained_messages: 2,
        bytes: 8192,
    };
    let mut port = service.port(&context, 0, budgets).unwrap();
    let mut first = port
        .try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
        .unwrap();
    let mut second = port
        .try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
        .unwrap();
    for _ in 0..2 {
        assert!(service.poll_command().unwrap());
    }
    let one = port.lanes[0].try_recv().unwrap();
    let two = port.lanes[0].try_recv().unwrap();
    let (key, _) = wire::decode_completion(&one).unwrap();
    let stale = wire::Key {
        generation: ozzy_proto::RequestId::new(),
        id: key.id,
    };
    port.observe(&wire::completion(stale, Status::Reply(Ok(()))))
        .unwrap();
    port.observe(&wire::completion(
        key,
        Status::Publication(Err(PublicationError::Full)),
    ))
    .unwrap();
    assert_eq!(port.requests.len(), 2);
    assert!(futures::poll!(Pin::new(&mut first)).is_pending());
    port.observe(&two).unwrap();
    Pin::new(&mut second).await.unwrap().unwrap();
    assert!(futures::poll!(Pin::new(&mut first)).is_pending());
    port.observe(&two).unwrap();
    assert_eq!(port.requests.len(), 1);
    port.observe(&one).unwrap();
    Pin::new(&mut first).await.unwrap().unwrap();
    assert!(port.requests.is_empty());
}

#[tokio::test]
async fn canceled_observation_keeps_accepted_work_and_releases_only_after_final_alias() {
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let mut port = service.port(&context, 0, capacity()).unwrap();
    drop(
        port.try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
            .unwrap(),
    );
    assert!(service.poll_command().unwrap());
    let mut alias = None;
    service
        .flush(|message| {
            alias = message.part_bytes(3);
            Ok(())
        })
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    port.poll_progress(&mut cx).unwrap();
    assert!(port.requests.is_empty());
    assert!(
        port.try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
            .is_err()
    );
    drop(alias);
    assert!(
        port.try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
            .is_ok()
    );
}

#[tokio::test]
async fn full_data_keeps_control_and_another_shard_runnable() {
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, placements) = setup();
    let route = RouteState {
        group: placements[0].group,
        config_epoch: 1,
        partition: placements[0].partition,
        members: [
            service.local(),
            NodeId::from_bytes([8; 16]),
            NodeId::from_bytes([7; 16]),
        ]
        .into(),
        view: 0,
        leader: None,
    };
    publication_watch(&mut service, route.clone());
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    service.flush(|_| Ok(())).unwrap();
    let mut busy = service.port(&context, 0, capacity()).unwrap();
    let mut healthy = service.port(&context, 17, capacity()).unwrap();
    let data = busy
        .try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
        .unwrap();
    assert!(
        busy.try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
            .is_err()
    );
    let control = busy.try_route(route.clone()).unwrap();
    let other = healthy.try_route(route).unwrap();
    for _ in 0..6 {
        service.poll_command().unwrap();
    }
    assert!(!observe(&mut busy, control).await.unwrap().unwrap());
    assert!(matches!(
        observe(&mut healthy, other).await.unwrap(),
        Err(RouteError::Destination)
    ));
    observe(&mut busy, data).await.unwrap().unwrap();
}

#[test]
fn cross_thread_saturation_keeps_bounded_requests_and_delivers_every_command() {
    const MESSAGES: usize = 2000;
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    service.flush(|_| Ok(())).unwrap();
    let mut budgets = capacity();
    budgets.data = Budget {
        queue_slots: 4,
        retained_messages: 4,
        bytes: 16 * 1024,
    };
    let port = service.port(&context, 0, budgets).unwrap();
    let writer = std::thread::spawn(move || saturate(port, binding, MESSAGES));
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut delivered = std::collections::BTreeSet::new();
    while delivered.len() < MESSAGES {
        assert!(
            Instant::now() < deadline,
            "dispatcher stalled at {}",
            delivered.len()
        );
        service.poll_command().unwrap();
        service
            .flush(|message| {
                assert_eq!(message.len(), 4);
                let id = u64::from_be_bytes(message.part_slice(3).unwrap().try_into().unwrap());
                assert!(id < MESSAGES as u64);
                assert!(delivered.insert(id), "duplicate command {id}");
                Ok(())
            })
            .unwrap();
        std::thread::yield_now();
    }
    writer.join().unwrap();
    assert_eq!(delivered.len(), MESSAGES);
}

fn numbered_reply(binding: crate::frontend::Binding, id: u64) -> Message {
    let payload = Bytes::copy_from_slice(&id.to_be_bytes());
    let header = ozzy_proto::Envelope {
        opcode: Opcode::Records,
        response: false,
        request_id: None,
        sender: NodeId::from_bytes([9; 16]),
        session: Some(binding.session),
    }
    .encode_header(0, payload.len(), ozzy_proto::EnvelopeLimits::default())
    .unwrap();
    Message::multipart([
        Bytes::copy_from_slice(binding.peer.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::new(),
        payload,
    ])
}

fn saturate(mut port: Port, binding: crate::frontend::Binding, maximum: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut pending: std::collections::VecDeque<(u64, Pending<ReplyResult>)> =
        std::collections::VecDeque::new();
    let mut retries = std::collections::VecDeque::new();
    let mut next = 0;
    let mut admitted = 0;
    while admitted < maximum {
        assert!(Instant::now() < deadline, "shard command owner stalled");
        port.poll_progress(&mut cx).unwrap();
        while let Some((id, mut result)) = pending.pop_front() {
            match Pin::new(&mut result).poll(&mut cx) {
                Poll::Ready(Ok(Ok(()))) => admitted += 1,
                Poll::Ready(Ok(Err((ReplyError::Full, message)))) => {
                    retries.push_back((id, message));
                }
                Poll::Pending => {
                    pending.push_front((id, result));
                    break;
                }
                other @ Poll::Ready(_) => panic!("unexpected command completion: {other:?}"),
            }
        }
        loop {
            let input = retries.pop_front().or_else(|| {
                if next == maximum {
                    return None;
                }
                let id = next as u64;
                next += 1;
                Some((id, numbered_reply(binding, id)))
            });
            let Some((id, message)) = input else {
                break;
            };
            match port.try_reply(Class::Data, message, 2048) {
                Ok(result) => pending.push_back((id, result)),
                Err((
                    PortError::Admission(SendFailure::Admission(dispatch::Error::Full)),
                    message,
                )) => {
                    retries.push_front((id, message));
                    break;
                }
                other => panic!("unexpected command submission: {other:?}"),
            }
        }
        assert!(port.requests.len() <= 4);
        assert!(pending.len() <= 4);
        assert!(retries.len() <= 5);
        std::thread::yield_now();
    }
}
