use super::*;

#[derive(Default)]
pub(super) struct Traffic {
    pub(super) hold: bool,
    pub(super) held: Vec<Message>,
    pub(super) peer: Option<Box<Peer>>,
}

/// Force multiple real pages without constructing hundreds of actors.
pub(super) fn paginate(catalog: &TopicCatalog, count: usize) -> TopicCatalog {
    TopicCatalog::new(
        (0..count).map(|first| {
            catalog
                .page(
                    &directory::TopicRequest {
                        name: "orders".to_owned(),
                        first: first as u32,
                        maximum: 1,
                    },
                    limits().envelope.max_metadata_bytes,
                )
                .unwrap()
        }),
        1,
        count,
    )
    .unwrap()
}

/// A second production directory/session owner on the same OMQ context. It
/// serves the identical catalog, without pretending to own the partition.
pub(super) struct Peer {
    service: Service,
    _input: TestInput,
    pub(super) server: Socket,
    pages: usize,
    hold_pages: bool,
}

impl Peer {
    pub(super) fn new(service: Service, input: TestInput, server: Socket) -> Self {
        Self {
            service,
            _input: input,
            server,
            pages: 0,
            hold_pages: false,
        }
    }

    pub(super) fn pump(&mut self) {
        for _ in 0..16 {
            match self.server.try_recv() {
                Ok(message) => {
                    self.service.receive(message, 4096).unwrap();
                }
                Err(omq_tokio::Error::WouldBlock) => break,
                Err(error) => panic!("alternate directory receive: {error:?}"),
            }
        }
        self.service
            .flush(|message| {
                let decoded = packet(&message);
                if decoded.envelope.opcode == Opcode::StateSnapshot
                    && decoded.metadata.first() == Some(&0)
                {
                    self.pages += 1;
                    if self.hold_pages {
                        return Ok(());
                    }
                }
                self.server.try_send(message)
            })
            .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn topic_discovery_uses_another_broker_when_the_first_response_is_lost() {
    let (mut harness, links, clock, authority) =
        setup_shared_directory_profile(&[], 2, None, true).await;
    harness
        .until(false, "both directory sessions established", |_| {
            links.session(authority.primary).is_some()
                && links.session(NodeId::from_bytes([2; 16])).is_some()
        })
        .await;
    harness.metadata.hold = true;
    let mut lookup = Box::pin(links.topic("orders"));
    assert!(futures::poll!(lookup.as_mut()).is_pending());
    harness
        .until(false, "first directory response intercepted", |harness| {
            !harness.metadata.held.is_empty()
        })
        .await;
    // Keep negotiation alive without allowing the five-second request budget
    // to expire. Discovery must use the available second directory now.
    clock.advance(Duration::from_millis(100)).unwrap();
    let discovered = harness.drive(lookup, false).await.unwrap();
    assert_eq!(discovered.partition_count(), 2);
    assert_eq!(discovered.partition(0).unwrap().group, harness.groups[0]);
    assert_eq!(discovered.partition(1).unwrap().group, harness.groups[1]);
    assert_eq!(harness.metadata.peer.as_ref().unwrap().pages, 2);
    assert_eq!(clock.now(), Duration::from_millis(100));
    // The canceled request's late and duplicate frames must not satisfy a
    // later lookup or leak its reserved control slot.
    let late = harness.metadata.held.pop().unwrap();
    harness.server.try_send(late.clone()).unwrap();
    harness.server.try_send(late).unwrap();
    for _ in 0..24 {
        assert_eq!(
            harness.drive(links.topic("orders"), false).await.unwrap(),
            discovered
        );
    }
    // Force the formerly stalled broker to supply every subsequent answer.
    // A leak there cannot hide behind the still healthy second directory.
    harness.metadata.hold = false;
    harness.metadata.peer.as_mut().unwrap().hold_pages = true;
    for _ in 0..24 {
        assert_eq!(
            harness.drive(links.topic("orders"), false).await.unwrap(),
            discovered
        );
    }
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn lost_topic_responses_share_one_deadline_and_release_admission_after_timeout() {
    let (mut harness, links, clock, authority) =
        setup_shared_directory_profile(&[], 2, None, true).await;
    harness
        .until(false, "both directory sessions established", |_| {
            links.session(authority.primary).is_some()
                && links.session(NodeId::from_bytes([2; 16])).is_some()
        })
        .await;
    harness.metadata.hold = true;
    harness.metadata.peer.as_mut().unwrap().hold_pages = true;
    let mut lookup = Box::pin(links.topic("orders"));
    assert!(futures::poll!(lookup.as_mut()).is_pending());
    harness
        .until(false, "both topic responses intercepted", |harness| {
            !harness.metadata.held.is_empty() && harness.metadata.peer.as_ref().unwrap().pages > 0
        })
        .await;
    clock.advance(Duration::from_secs(5)).unwrap();
    assert!(matches!(lookup.await, Err(BrokerLinkError::Timeout)));
    harness.metadata.hold = false;
    harness.metadata.peer.as_mut().unwrap().hold_pages = false;
    for _ in 0..24 {
        let topic = harness.drive(links.topic("orders"), false).await.unwrap();
        assert_eq!(topic.partition_count(), 2);
    }
    assert_eq!(clock.now(), Duration::from_secs(5));
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn topic_discovery_retries_a_replaced_session_without_extending_its_deadline() {
    let (mut harness, links, clock, authority) = setup_shared_partitions(&[], 2).await;
    let expected = harness.drive(links.topic("orders"), false).await.unwrap();
    let old_session = links.session(authority.primary).unwrap();
    harness.metadata.hold = true;
    let mut lookup = Box::pin(links.topic("orders"));
    assert!(futures::poll!(lookup.as_mut()).is_pending());
    harness
        .until(false, "old-session topic response intercepted", |harness| {
            !harness.metadata.held.is_empty()
        })
        .await;
    harness.session = None;
    harness.service.start(link(70, 80).binding.peer).unwrap();
    harness
        .until(false, "replacement directory session established", |_| {
            links
                .session(authority.primary)
                .is_some_and(|session| session != old_session)
        })
        .await;
    let late = harness.metadata.held.pop().unwrap();
    let stale = packet(&late).envelope;
    harness.server.try_send(late.clone()).unwrap();
    harness.server.try_send(late).unwrap();
    harness
        .until(
            false,
            "fresh-session topic response intercepted",
            |harness| {
                assert!(
                    lookup
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending(),
                    "an old-session reply completed discovery"
                );
                !harness.metadata.held.is_empty()
            },
        )
        .await;
    let fresh = harness.metadata.held.pop().unwrap();
    let current = packet(&fresh).envelope;
    assert_ne!(current.session, stale.session);
    assert_ne!(current.request_id, stale.request_id);
    harness.server.try_send(fresh).unwrap();
    assert_eq!(harness.drive(lookup, false).await.unwrap(), expected);
    assert_eq!(clock.now(), Duration::ZERO);
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}
