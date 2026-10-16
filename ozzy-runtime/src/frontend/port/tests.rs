mod lifecycle;
use super::*;
use crate::dispatch::{Budget, Budgets, SendFailure};
use crate::frontend::Service;
use crate::frontend::service::tests::{begin, remote, reply, setup};
use crate::frontend::{WatchLimits, WatchRegistry};
use bytes::Bytes;
use ozzy_proto::{NodeId, Opcode, directory, handshake};

#[tokio::test]
async fn progress_retries_network_send_after_registering_capacity_wait() {
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    service
        .try_reply(Class::Data, reply(binding, Opcode::Records))
        .unwrap();
    let available = std::rc::Rc::new(std::cell::Cell::new(false));
    let for_send = available.clone();
    let mut progress = Box::pin(service.progress_with(
        move |message| {
            if for_send.get() {
                Ok(())
            } else {
                Err(omq_tokio::TrySendError::Full(message))
            }
        },
        |_| {
            let available = available.clone();
            Box::pin(std::future::poll_fn(move |_| {
                // Space became available before this waiter captured its baseline.
                available.set(true);
                Poll::Pending
            }))
        },
    ));
    assert!(futures::poll!(progress.as_mut()).is_ready());
}

#[tokio::test]
async fn refused_destinations_keep_one_wait_and_retry_only_after_send_progress() {
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    while service.flush(|_| Ok(())).unwrap() {}
    service
        .try_reply(Class::Data, reply(binding, Opcode::Records))
        .unwrap();
    let space = std::rc::Rc::new(std::cell::Cell::new(false));
    let (attempts, waits) = (std::cell::Cell::new(0), std::cell::Cell::new(0));
    let send = |message| {
        attempts.set(attempts.get() + 1);
        if space.get() {
            Ok(())
        } else {
            Err(omq_tokio::TrySendError::Full(message))
        }
    };
    let wait = |_| -> Pin<Box<dyn Future<Output = ()>>> {
        waits.set(waits.get() + 1);
        let space = space.clone();
        Box::pin(std::future::poll_fn(move |_| {
            if space.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }))
    };
    // Each serving turn builds a new progress future.
    for _ in 0..4 {
        let mut progress = Box::pin(service.progress_with(send, wait));
        assert!(futures::poll!(progress.as_mut()).is_pending());
    }
    // The first turn's attempt, then one retry once the wait was registered.
    assert_eq!((attempts.get(), waits.get()), (2, 1));
    space.set(true);
    let mut progress = Box::pin(service.progress_with(send, wait));
    assert!(futures::poll!(progress.as_mut()).is_ready());
    drop(progress);
    assert_eq!((attempts.get(), waits.get()), (3, 1));
    assert!(!service.has_pending());
}

#[tokio::test(start_paused = true)]
async fn send_waits_that_complete_without_usable_space_back_off() {
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    while service.flush(|_| Ok(())).unwrap() {}
    service
        .try_reply(Class::Data, reply(binding, Opcode::Records))
        .unwrap();
    let space = std::cell::Cell::new(false);
    let (attempts, waits) = (std::cell::Cell::new(0), std::cell::Cell::new(0));
    let send = |message| {
        attempts.set(attempts.get() + 1);
        if space.get() {
            Ok(())
        } else {
            Err(omq_tokio::TrySendError::Full(message))
        }
    };
    // OMQ can report send progress that a retry still cannot use.
    let wait = |_| -> Pin<Box<dyn Future<Output = ()>>> {
        waits.set(waits.get() + 1);
        Box::pin(std::future::ready(()))
    };
    let turn = async |service: &mut Service| {
        let mut progress = Box::pin(service.progress_with(send, wait));
        futures::poll!(progress.as_mut()).is_ready()
    };
    // The rotation's attempt, then one retry for the wasted wake. The next
    // wait is delayed instead of retrying again at once.
    assert!(!turn(&mut service).await);
    assert_eq!((attempts.get(), waits.get()), (2, 2));
    for _ in 0..3 {
        assert!(!turn(&mut service).await);
    }
    assert_eq!(attempts.get(), 2);
    // Each further wasted wake doubles the delay.
    for (advance, expected) in [(1, 3), (1, 3), (1, 4), (3, 4), (1, 5)] {
        tokio::time::advance(std::time::Duration::from_millis(advance)).await;
        assert!(!turn(&mut service).await);
        assert_eq!(attempts.get(), expected);
    }
    space.set(true);
    tokio::time::advance(std::time::Duration::from_millis(8)).await;
    assert!(turn(&mut service).await);
    assert_eq!(attempts.get(), 6);
    assert!(!service.has_pending());
}

#[tokio::test]
async fn completed_send_waits_are_observed_while_commands_keep_arriving() {
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    while service.flush(|_| Ok(())).unwrap() {}
    service
        .try_reply(Class::Data, reply(binding, Opcode::Records))
        .unwrap();
    let space = std::rc::Rc::new(std::cell::Cell::new(false));
    let records = std::cell::Cell::new(0);
    let send = |message: Message| {
        if !space.get() {
            return Err(omq_tokio::TrySendError::Full(message));
        }
        if message.part_slice(1).unwrap()[5] == Opcode::Records as u8 {
            records.set(records.get() + 1);
        }
        Ok(())
    };
    let wait = |_| -> Pin<Box<dyn Future<Output = ()>>> {
        let space = space.clone();
        Box::pin(std::future::poll_fn(move |_| {
            if space.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }))
    };
    let mut progress = Box::pin(service.progress_with(send, wait));
    assert!(futures::poll!(progress.as_mut()).is_pending());
    drop(progress);
    space.set(true);
    let mut port = service.port(&context, 0, capacity()).unwrap();
    let pending = port
        .try_reply(Class::Control, reply(binding, Opcode::Appended), 2048, 0)
        .unwrap();
    let mut progress = Box::pin(service.progress_with(send, wait));
    assert!(futures::poll!(progress.as_mut()).is_ready());
    drop(progress);
    // The refused reply went out in the same turn that admitted the command.
    assert_eq!(records.get(), 1);
    observe(&mut port, pending).await.unwrap().unwrap();
}

#[tokio::test]
async fn reply_capacity_follows_transport_aliases_and_unobserved_completions() {
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let mut port = service.port(&context, 0, capacity()).unwrap();
    let packet = reply(binding, Opcode::Records);
    let pending = port
        .try_reply(Class::Data, packet.clone(), 2048, 0)
        .unwrap();
    assert!(service.poll_command().unwrap());
    let mut transmitted = Vec::new();
    service
        .flush(|message| {
            transmitted.push(message);
            Ok(())
        })
        .unwrap();
    service.poll_command().unwrap();
    assert!(matches!(
        port.try_reply(Class::Data, packet.clone(), 2048, 0),
        Err((
            PortError::Admission(SendFailure::Admission(dispatch::Error::Full)),
            _
        ))
    ));
    drop(transmitted);
    service.poll_command().unwrap();
    // The reply observer itself is bounded until it consumes the outcome.
    assert!(
        port.try_reply(Class::Data, packet.clone(), 2048, 0)
            .is_err()
    );
    let generation = port.generation();
    observe(&mut port, pending).await.unwrap().unwrap();
    let ready = port.changed_after(generation);
    tokio::time::timeout(std::time::Duration::from_secs(1), ready)
        .await
        .unwrap();
    let pending = port.try_reply(Class::Data, packet, 2048, 0).unwrap();
    assert!(service.poll_command().unwrap());
    observe(&mut port, pending).await.unwrap().unwrap();
    let mut backing = None;
    service
        .flush(|message| {
            backing = message.part_bytes(1);
            Ok(())
        })
        .unwrap();
    service.poll_command().unwrap();
    assert!(
        port.try_reply(Class::Data, reply(binding, Opcode::Records), 2048, 0)
            .is_err()
    );
    drop(backing);
    service.poll_command().unwrap();
    assert!(
        port.try_reply(Class::Data, reply(binding, Opcode::Records), 2048, 0)
            .is_ok()
    );
}

#[tokio::test]
async fn full_port_wakes_when_dispatcher_exits() {
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let mut port = service.port(&context, 0, capacity()).unwrap();
    let packet = reply(binding, Opcode::Records);
    let pending = port
        .try_reply(Class::Data, packet.clone(), 2048, 0)
        .unwrap();
    let generation = port.generation();
    drop(service);
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        port.changed_after(generation),
    )
    .await
    .unwrap();
    assert!(matches!(
        port.try_reply(Class::Data, packet, 2048, 0),
        Err((PortError::Admission(SendFailure::Closed), _))
    ));
    assert_eq!(observe(&mut port, pending).await, Err(PortError::Closed));
}

#[tokio::test]
async fn queued_replies_transmit_before_more_shard_commands_are_admitted() {
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    while service.flush(|_| Ok(())).unwrap() {}
    for _ in 0..2 {
        service
            .try_reply(Class::Data, reply(binding, Opcode::Records))
            .unwrap();
    }
    let mut port = service.port(&context, 0, capacity()).unwrap();
    let pending = port
        .try_reply(Class::Data, reply(binding, Opcode::Records), 2048, 0)
        .unwrap();
    let sent = std::cell::Cell::new(0);
    let mut progress = Box::pin(service.progress_with(
        |_| {
            sent.set(sent.get() + 1);
            Ok(())
        },
        |_| Box::pin(std::future::pending()),
    ));
    assert!(futures::poll!(progress.as_mut()).is_ready());
    drop(progress);
    // A full queue would refuse the command and the shard would retry it
    // while the dispatcher kept admitting commands instead of sending.
    assert_eq!(sent.get(), 1);
    assert!(observe(&mut port, pending).await.unwrap().is_ok());
}

#[tokio::test]
async fn full_peer_returns_original_message_and_cannot_deadlock_its_own_retry_credit() {
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    for _ in 0..2 {
        service
            .try_reply(Class::Data, reply(binding, Opcode::Records))
            .unwrap();
    }
    let mut port = service.port(&context, 0, capacity()).unwrap();
    let pending = port
        .try_reply(Class::Data, reply(binding, Opcode::Records), 2048, 0)
        .unwrap();
    service.poll_command().unwrap();
    let (error, message) = observe(&mut port, pending).await.unwrap().unwrap_err();
    assert_eq!(error, ReplyError::Full);
    service.poll_command().unwrap();
    // Keep returned bytes alive while reusing the released queue reservation.
    let again = port.try_reply(Class::Data, message, 2048, 0).unwrap();
    service.flush(|_| Ok(())).unwrap();
    service.poll_command().unwrap();
    observe(&mut port, again).await.unwrap().unwrap();
}

#[tokio::test]
async fn route_publication_is_bounded_to_its_partition_owner_shard() {
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, placements) = setup();
    let mut route = directory::RouteState {
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
    service
        .install_watches(
            WatchRegistry::new(
                WatchLimits {
                    partitions: 1,
                    peers: 1,
                    registrations: 1,
                    interests_per_registration: 1,
                    pending_per_registration: 1,
                },
                [route.clone()],
            )
            .unwrap(),
        )
        .unwrap();
    let mut right = service.port(&context, 0, capacity()).unwrap();
    let mut wrong = service.port(&context, 17, capacity()).unwrap();
    let mut invalid = route.clone();
    invalid.members = [
        service.local(),
        service.local(),
        NodeId::from_bytes([7; 16]),
    ]
    .into();
    assert_eq!(right.try_route(invalid).unwrap_err().0, PortError::Route);
    route.view = 1;
    route.leader = Some(service.local());
    let rejected = wrong.try_route(route.clone()).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(matches!(
        observe(&mut wrong, rejected).await.unwrap(),
        Err(RouteError::Destination)
    ));
    let published = right.try_route(route.clone()).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(observe(&mut right, published).await.unwrap().unwrap());
    let repeated = right.try_route(route).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(!observe(&mut right, repeated).await.unwrap().unwrap());
}

fn publication(route: &RouteState, local: NodeId) -> Message {
    use ozzy_proto::{Envelope, MessageId, data::DataLimits, reader};
    let source = reader::Source::Group {
        authority: ozzy_proto::data::Authority {
            group_id: route.group,
            config_epoch: route.config_epoch,
            view: route.view,
        },
        partition: route.partition,
        owner_epoch: 1,
    };
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let mut output = reader::RecordsEncoder::publication(
        Envelope {
            opcode: Opcode::RecordsPub,
            response: false,
            request_id: None,
            sender: local,
            session: None,
        },
        reader::PublicationHeader {
            source,
            first_offset: 7,
        },
        &mut metadata,
        &mut payload,
        DataLimits::default(),
    )
    .unwrap();
    output
        .push_raw(MessageId::from_bytes([6; 16]), b"confirmed payload")
        .unwrap();
    let header = output.finish().unwrap();
    crate::native_frames::message(
        &reader::publication_topic(source).unwrap(),
        header,
        &metadata,
        bytes::Bytes::from(payload),
    )
}

#[tokio::test]
async fn reader_publications_fence_sources_and_charge_transport_aliases() {
    use crate::frontend::PublicationError;
    let context = omq_tokio::Context::new();
    let (mut service, _lanes, placements) = setup();
    let local = service.local();
    let mut route = directory::RouteState {
        group: placements[0].group,
        config_epoch: 1,
        partition: placements[0].partition,
        members: [
            local,
            NodeId::from_bytes([8; 16]),
            NodeId::from_bytes([7; 16]),
        ]
        .into(),
        view: 2,
        leader: Some(local),
    };
    publication_watch(&mut service, route.clone());
    let outgoing = publication_capacity();
    let allowance = Budget {
        queue_slots: 1,
        retained_messages: 1,
        bytes: 4096,
    };
    let mut right = service
        .port_with_publications(&context, 0, outgoing, allowance)
        .unwrap();
    let mut wrong = service
        .port_with_publications(&context, 17, outgoing, allowance)
        .unwrap();
    let rejected = wrong.try_publish(publication(&route, local), 2048).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(matches!(
        observe(&mut wrong, rejected).await.unwrap(),
        Err((PublicationError::Destination, _))
    ));

    let pending = right.try_publish(publication(&route, local), 2048).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    observe(&mut right, pending).await.unwrap().unwrap();
    let transmitted = service
        .take_publication()
        .expect("current source publication");
    let alias = transmitted.part_bytes(3).unwrap();
    drop(transmitted);
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(right.try_publish(publication(&route, local), 2048).is_err());
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let reply = right
        .try_reply(Class::Data, reply(binding, Opcode::Records), 2048, 0)
        .unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    observe(&mut right, reply).await.unwrap().unwrap();
    service.flush(|_| Ok(())).unwrap();
    // Control progress stays available while the data alias retains its charge.
    route.view = 3;
    route.leader = Some(NodeId::from_bytes([8; 16]));
    let changed = right.try_route(route.clone()).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(observe(&mut right, changed).await.unwrap().unwrap());
    drop(alias);
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    let stale = right.try_publish(publication(&route, local), 2048).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(matches!(
        observe(&mut right, stale).await.unwrap(),
        Err((PublicationError::Stale, _))
    ));

    route.view = 4;
    route.leader = Some(local);
    service.publish_route(&route).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    let queued = right.try_publish(publication(&route, local), 2048).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    observe(&mut right, queued).await.unwrap().unwrap();
    route.view = 5;
    route.leader = None;
    service.publish_route(&route).unwrap();
    assert!(
        service.take_publication().is_none(),
        "queued old source is fenced before PUB"
    );
}

fn publication_watch(service: &mut Service, route: directory::RouteState) {
    service
        .install_watches(
            WatchRegistry::new(
                WatchLimits {
                    partitions: 1,
                    peers: 1,
                    registrations: 1,
                    interests_per_registration: 1,
                    pending_per_registration: 1,
                },
                [route],
            )
            .unwrap(),
        )
        .unwrap();
}

#[test]
fn follower_and_reader_publications_have_independent_fair_slots() {
    let (mut service, _lanes, placements) = setup();
    let local = service.local();
    let placement = placements[0];
    let route = directory::RouteState {
        group: placement.group,
        config_epoch: 1,
        partition: placement.partition,
        members: [
            local,
            NodeId::from_bytes([8; 16]),
            NodeId::from_bytes([7; 16]),
        ]
        .into(),
        view: 2,
        leader: Some(local),
    };
    publication_watch(&mut service, route.clone());
    let original = crate::frontend::test_support::control(
        placement,
        crate::frontend::test_support::binding(crate::frontend::Kind::Broker),
    );
    let mut metadata = original.part_bytes(2).unwrap().to_vec();
    metadata[24..32].copy_from_slice(&route.view.to_be_bytes());
    metadata[32..48].copy_from_slice(local.as_bytes());
    let header = ozzy_proto::Envelope {
        opcode: Opcode::PreparePub,
        response: false,
        request_id: None,
        sender: local,
        session: None,
    }
    .encode_header(metadata.len(), 0, service.envelope_limits())
    .unwrap();
    let follower = Message::multipart([
        Bytes::copy_from_slice(placement.group.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::new(),
    ]);
    let reader = publication(&route, local);
    service.publish(0, follower.clone()).unwrap();
    assert!(matches!(
        service.publish(0, follower.clone()),
        Err((super::super::PublicationError::Full, _))
    ));
    service.publish(0, reader).unwrap();
    assert_eq!(
        service.take_publication().unwrap().part_slice(0),
        Some(placement.group.as_bytes().as_slice())
    );
    service.publish(0, follower).unwrap();
    assert_eq!(
        service
            .take_publication()
            .unwrap()
            .part_slice(0)
            .unwrap()
            .len(),
        32
    );
    assert_eq!(
        service.take_publication().unwrap().part_slice(0),
        Some(placement.group.as_bytes().as_slice())
    );
}

fn publication_capacity() -> Budgets {
    let mut outgoing = capacity();
    outgoing.data.queue_slots += 1;
    outgoing.data.retained_messages += 1;
    outgoing.data.bytes += 4096;
    outgoing
}

fn capacity() -> Budgets {
    Budgets {
        data: Budget {
            queue_slots: 1,
            retained_messages: 1,
            bytes: 4096,
        },
        control: Budget {
            queue_slots: 2,
            retained_messages: 2,
            bytes: 8192,
        },
    }
}

async fn observe<T>(port: &mut Port, mut pending: Pending<T>) -> Result<T, PortError> {
    std::future::poll_fn(|cx| {
        port.poll_progress(cx)?;
        Pin::new(&mut pending).poll(cx)
    })
    .await
}
