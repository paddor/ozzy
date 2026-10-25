use super::*;
use ozzy_proto::{Envelope, GroupId, MessageId, PartitionIncarnation, RequestId, data::DataLimits};

fn config() -> BrokerLinksConfig {
    let data = DataLimits {
        envelope: EnvelopeLimits {
            max_metadata_bytes: 16 * 1024,
            max_payload_bytes: 1024,
        },
        max_records: 4,
        max_parts: 4,
        max_record_bytes: 1024,
    };
    let mut parameters = handshake::Parameters::streaming(data, handshake::CONSUMER).unwrap();
    parameters.capabilities |= handshake::OWNER_READ;
    BrokerLinksConfig {
        local: NodeId::from_bytes([1; 16]),
        brokers: vec![super::super::BrokerAddress {
            data_endpoint: "inproc://reader-unit-data".parse().unwrap(),
            node: NodeId::from_bytes([2; 16]),
            endpoint: "inproc://reader-inbox-unit".parse().unwrap(),
        }],
        parameters,
        requests: 4,
        control_bytes: 1024 * 1024,
        routing_bytes: 1024 * 1024,
        append: None,
        reader: Some(ReaderLinkLimits {
            subscriptions: 2,
            bytes: 1024 * 1024,
            queue_messages: 1,
        }),
        maximum_partitions: 1,
        request_timeout: std::time::Duration::from_secs(1),
        retry_interval: std::time::Duration::from_millis(10),
        clock: super::super::SdkClock::default(),
    }
}

/// Caller-side admission and the driver's registry, without the driver.
struct Readers {
    admission: Admission,
    registry: Registry,
}

impl Readers {
    fn new(config: &BrokerLinksConfig) -> Self {
        let admission = Admission::new(config).unwrap();
        let registry = Registry::new(config, &admission);
        Self {
            admission,
            registry,
        }
    }

    /// Take a slot, then install the inbox as the driver does.
    fn register(
        &self,
        context: &omq_tokio::Context,
        id: SubscriptionId,
        prefix: Bytes,
    ) -> Result<Arc<Inbox>, BrokerLinkError> {
        let inbox = self.admission.inbox(context, id)?;
        Ok(self.registry.register(Registration { inbox, prefix }))
    }
}

fn readers() -> Readers {
    Readers::new(&config())
}

#[tokio::test(flavor = "current_thread")]
async fn declared_reader_reservation_admits_all_subscriptions_and_one_byte_less_does_not() {
    let context = omq_tokio::Context::new();
    for brokers in [1, 3] {
        let mut config = config();
        config.brokers = (0..brokers)
            .map(|index| super::super::BrokerAddress {
                data_endpoint: "inproc://reader-unit-data".parse().unwrap(),
                node: NodeId::from_bytes([index as u8 + 2; 16]),
                endpoint: format!("inproc://reader-budget-{index}").parse().unwrap(),
            })
            .collect();
        let mut reader = config.reader.unwrap();
        reader.subscriptions = 4;
        reader.bytes = reader
            .reservation_bytes(config.parameters.receive, brokers)
            .unwrap();
        config.reader = Some(reader);
        let readers = Readers::new(&config);
        let entries = (0..4)
            .map(|index| {
                readers
                    .register(
                        &context,
                        SubscriptionId::from_bytes([index + 10; 16]),
                        Bytes::from_static(b"prefix"),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(
            readers
                .register(
                    &context,
                    SubscriptionId::from_bytes([20; 16]),
                    Bytes::from_static(b"prefix")
                )
                .is_err()
        );
        assert_eq!(entries.len(), 4);
        config.reader.as_mut().unwrap().bytes -= 1;
        assert_eq!(Admission::capacity(&config).unwrap().0, 3);
        config.control_bytes = config.control_reservation_bytes().unwrap();
        config.validate().unwrap();
        config.reader.as_mut().unwrap().queue_messages = 0;
        assert!(config.validate().is_err());
    }
}

fn source() -> reader::Source {
    reader::Source::Group {
        authority: ozzy_proto::data::Authority {
            group_id: GroupId::from_bytes([3; 16]),
            config_epoch: 1,
            view: 2,
        },
        partition: PartitionIncarnation::from_bytes([4; 16]),
        owner_epoch: 1,
    }
}

fn publication(offset: u64) -> Message {
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let mut output = reader::RecordsEncoder::publication(
        Envelope {
            opcode: Opcode::RecordsPub,
            response: false,
            request_id: None,
            sender: NodeId::from_bytes([2; 16]),
            session: None,
        },
        reader::PublicationHeader {
            source: source(),
            first_offset: offset,
        },
        &mut metadata,
        &mut payload,
        DataLimits::default(),
    )
    .unwrap();
    output
        .push_raw(MessageId::from_bytes([5; 16]), b"body")
        .unwrap();
    let header = output.finish().unwrap();
    crate::native_frames::message(
        &reader::publication_topic(source()).unwrap(),
        header,
        &metadata,
        Bytes::from(payload),
    )
}

fn broker() -> NodeId {
    NodeId::from_bytes([2; 16])
}

fn subscription(inbox: &Inbox, generation: u128) -> reader::Subscription {
    reader::Subscription {
        id: inbox.id,
        generation,
    }
}

/// Select `generation` as the driver does when it sends its SUBSCRIBE.
fn select(inbox: &Inbox, generation: u128) {
    inbox.select(
        Sent {
            broker: broker(),
            session: LinkSessionId::from_bytes([6; 16]),
            request: RequestId::from_bytes(generation.to_be_bytes()),
        },
        reader::Subscribed {
            subscription: subscription(inbox, generation),
            source: source(),
            resolved_offset: 0,
        },
    );
}

/// One replay frame of `generation`.
fn records(inbox: &Inbox, generation: u128, offset: u64) -> Message {
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let mut output = reader::RecordsEncoder::new(
        Envelope {
            opcode: Opcode::Records,
            response: false,
            request_id: None,
            sender: broker(),
            session: Some(LinkSessionId::from_bytes([6; 16])),
        },
        reader::RecordHeader {
            subscription: subscription(inbox, generation),
            source: source(),
            first_offset: offset,
        },
        &mut metadata,
        &mut payload,
        DataLimits::default(),
    )
    .unwrap();
    output
        .push_raw(MessageId::from_bytes([5; 16]), b"body")
        .unwrap();
    let header = output.finish().unwrap();
    crate::native_frames::message(broker().as_bytes(), header, &metadata, Bytes::from(payload))
}

async fn inbox(readers: &Readers, context: &omq_tokio::Context) -> Arc<Inbox> {
    let inbox = readers
        .register(
            context,
            SubscriptionId::new(),
            Bytes::from_static(b"prefix"),
        )
        .unwrap();
    inbox.initialize().await.unwrap();
    inbox
}

#[tokio::test(flavor = "current_thread")]
async fn replay_live_and_rejection_capacity_stay_separate_from_retained_aliases() {
    let context = omq_tokio::Context::new();
    let readers = readers();
    let inbox = readers
        .register(
            &context,
            SubscriptionId::new(),
            Bytes::from_static(b"prefix"),
        )
        .unwrap();
    assert!(
        !inbox.can_subscribe(),
        "bootstrap must finish before replay admission"
    );
    inbox.initialize().await.unwrap();
    select(&inbox, 1);
    let first = Some(subscription(&inbox, 1));
    let replay = records(&inbox, 1, 0);
    let live = publication(0);
    assert!(inbox.push(&replay, false));
    assert!(!inbox.push(&replay, false), "one queued replay frame");
    assert!(inbox.push(&live, true), "live has its own queue");
    assert!(!inbox.push(&live, true), "one queued live frame");
    inbox.reject(&live);
    let popped = inbox.pop(first).unwrap();
    let alias = popped.part_bytes(3).unwrap();
    drop(popped);
    assert!(
        inbox.can_subscribe(),
        "a retained alias still leaves capacity for a subscription"
    );
    assert!(
        inbox.pop(first).is_some(),
        "rejection is independent of a full live lane"
    );
    assert!(inbox.push(&replay, false));
    inbox.clear();
    assert!(inbox.pop(first).is_none());
    assert!(inbox.push(&replay, false), "late replay after a clear");
    assert!(
        inbox.can_subscribe(),
        "a full old replay lane must allow a replacement subscription"
    );
    select(&inbox, 2);
    assert!(
        inbox.pop(Some(subscription(&inbox, 2))).is_none(),
        "the replacement drops old replay"
    );
    assert!(
        inbox.publication().is_some(),
        "dropping replay preserves live repair evidence"
    );
    assert_eq!(alias.as_ref(), replay.part_slice(3).unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn replacement_reclaims_stale_replay_backing_but_waits_for_application_aliases() {
    let context = omq_tokio::Context::new();
    let readers = readers();
    let inbox = inbox(&readers, &context).await;
    select(&inbox, 1);
    let live = publication(0);
    let mut aliases = Vec::new();
    for _ in 1..frame_slots(readers.admission.queue) {
        assert!(inbox.push(&live, true));
        aliases.push(inbox.publication().unwrap().part_bytes(3).unwrap());
    }
    assert!(inbox.push(&records(&inbox, 1, 0), false));
    assert!(
        inbox.can_subscribe(),
        "replacement can reclaim queued replay even with no free frame backing"
    );
    select(&inbox, 2);
    let current = Some(subscription(&inbox, 2));
    let fresh = records(&inbox, 2, 0);
    assert!(!inbox.push(&fresh, false), "stale replay holds the backing");
    assert!(
        inbox.pop(current).is_none(),
        "the opening cursor drops stale replay"
    );
    assert!(inbox.push(&fresh, false), "old backing was reclaimed");
    aliases.push(inbox.pop(current).unwrap().part_bytes(3).unwrap());
    assert!(
        !inbox.can_subscribe(),
        "application aliases still retain every frame permit"
    );
    drop(aliases);
    assert!(inbox.can_subscribe());
}

#[tokio::test(flavor = "current_thread")]
async fn stale_replay_ahead_of_an_opening_generation_is_dropped_not_delivered() {
    let context = omq_tokio::Context::new();
    let mut config = config();
    config.reader.as_mut().unwrap().queue_messages = 2;
    let readers = Readers::new(&config);
    let inbox = inbox(&readers, &context).await;
    select(&inbox, 1);
    assert!(inbox.push(&records(&inbox, 1, 0), false));
    // The cursor replaced the subscription before it read the old frame.
    select(&inbox, 2);
    let fresh = records(&inbox, 2, 7);
    assert!(inbox.push(&fresh, false));
    let early = inbox.pop(Some(subscription(&inbox, 2))).unwrap();
    assert_eq!(early.part_slice(1), fresh.part_slice(1));
    assert_eq!(early.part_slice(2), fresh.part_slice(2));
    assert!(inbox.pop(Some(subscription(&inbox, 2))).is_none());
    assert!(inbox.push(&records(&inbox, 2, 8), false));
    assert!(
        inbox.pop(None).is_none(),
        "with no subscription, all replay is stale"
    );
    assert!(inbox.pop(Some(subscription(&inbox, 2))).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn a_rejection_answers_only_the_generation_it_ended() {
    let context = omq_tokio::Context::new();
    let readers = readers();
    let inbox = inbox(&readers, &context).await;
    let refusal = publication(0);
    select(&inbox, 1);
    inbox.reject(&refusal);
    select(&inbox, 2);
    let current = Some(subscription(&inbox, 2));
    assert!(
        inbox.pop(current).is_none(),
        "a refusal of the replaced generation is stale"
    );
    inbox.reject(&refusal);
    assert!(inbox.pop(current).is_some());
    assert!(
        !inbox.unselect(subscription(&inbox, 1)),
        "an old UNSUBSCRIBE keeps the replacement"
    );
    assert!(inbox.unselect(subscription(&inbox, 2)));
    assert!(inbox.selected().is_none());
    inbox.reject(&refusal);
    assert!(
        inbox.pop(current).is_none(),
        "nothing selected, nothing refused"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn publication_fanout_bounds_each_reader_and_charges_retained_aliases() {
    let context = omq_tokio::Context::new();
    let readers = readers();
    let prefix = Bytes::copy_from_slice(&reader::publication_topic(source()).unwrap());
    let slow = readers
        .register(
            &context,
            SubscriptionId::from_bytes([10; 16]),
            prefix.clone(),
        )
        .unwrap();
    let healthy = readers
        .register(
            &context,
            SubscriptionId::from_bytes([11; 16]),
            prefix.clone(),
        )
        .unwrap();
    slow.initialize().await.unwrap();
    healthy.initialize().await.unwrap();
    assert_eq!(
        readers.registry.prefixes().len(),
        1,
        "shared subscription prefix"
    );
    let broker = NodeId::from_bytes([2; 16]);
    let original = publication(0);
    let mut last = None;
    for offset in 0..4 {
        readers
            .registry
            .publication(broker, &publication(offset), EnvelopeLimits::default());
        last = healthy.publication();
        assert!(last.is_some(), "slow inbox cannot gate another reader");
    }
    let held = slow.publication().unwrap();
    assert_eq!(
        held.part_slice(2),
        original.part_slice(2),
        "full inbox drops newer PUB frames"
    );
    assert!(slow.publication().is_none());
    let last = last.unwrap();
    let alias = last.part_bytes(3).unwrap();
    drop((last, healthy));
    assert!(
        readers
            .register(
                &context,
                SubscriptionId::from_bytes([12; 16]),
                prefix.clone()
            )
            .is_err()
    );
    drop(alias);
    let next = readers
        .register(&context, SubscriptionId::from_bytes([12; 16]), prefix)
        .unwrap();
    drop((held, slow, next));
    assert!(readers.registry.prefixes().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn publications_validate_broker_and_prefix_before_copying_opaque_payloads() {
    let context = omq_tokio::Context::new();
    let readers = readers();
    let prefix = Bytes::copy_from_slice(&reader::publication_topic(source()).unwrap());
    let inbox = readers
        .register(&context, SubscriptionId::from_bytes([10; 16]), prefix)
        .unwrap();
    inbox.initialize().await.unwrap();
    let message = publication(0);
    readers.registry.publication(
        NodeId::from_bytes([9; 16]),
        &message,
        EnvelopeLimits::default(),
    );
    assert!(inbox.publication().is_none());
    let foreign = Message::multipart([
        Bytes::from_static(b"other-prefix"),
        message.part_bytes(1).unwrap(),
        message.part_bytes(2).unwrap(),
        message.part_bytes(3).unwrap(),
    ]);
    readers.registry.publication(
        NodeId::from_bytes([2; 16]),
        &foreign,
        EnvelopeLimits::default(),
    );
    assert!(inbox.publication().is_none());
    readers.registry.publication(
        NodeId::from_bytes([2; 16]),
        &message,
        EnvelopeLimits::default(),
    );
    let copied = inbox.publication().unwrap();
    assert_eq!(copied.part_slice(3), message.part_slice(3));
    assert_ne!(
        copied.part_bytes(3).unwrap().as_ptr(),
        message.part_bytes(3).unwrap().as_ptr()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn retained_publication_frames_cannot_exceed_one_readers_reserved_storage() {
    let context = omq_tokio::Context::new();
    let readers = readers();
    let prefix = Bytes::copy_from_slice(&reader::publication_topic(source()).unwrap());
    let slow = readers
        .register(
            &context,
            SubscriptionId::from_bytes([10; 16]),
            prefix.clone(),
        )
        .unwrap();
    let healthy = readers
        .register(&context, SubscriptionId::from_bytes([11; 16]), prefix)
        .unwrap();
    let broker = NodeId::from_bytes([2; 16]);
    slow.initialize().await.unwrap();
    healthy.initialize().await.unwrap();
    let mut retained = Vec::new();
    for offset in 0..7 {
        readers
            .registry
            .publication(broker, &publication(offset), EnvelopeLimits::default());
        retained.push(slow.publication().unwrap().part_bytes(3).unwrap());
        drop(healthy.publication().unwrap());
    }
    readers
        .registry
        .publication(broker, &publication(7), EnvelopeLimits::default());
    assert!(
        slow.publication().is_none(),
        "retained aliases fill capacity"
    );
    drop(healthy.publication().unwrap());
    drop(retained.pop());
    readers
        .registry
        .publication(broker, &publication(8), EnvelopeLimits::default());
    assert!(
        slow.publication().is_some(),
        "released alias returns capacity"
    );
    assert!(healthy.publication().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn an_accepted_subscription_admits_its_live_topic_on_its_broker_until_unselected() {
    let context = omq_tokio::Context::new();
    let readers = readers();
    let inbox = inbox(&readers, &context).await;
    let subscribe = reader::Subscribe {
        subscription: subscription(&inbox, 1),
        target: reader::Target::Group {
            authority: ozzy_proto::data::Authority {
                group_id: GroupId::from_bytes([3; 16]),
                config_epoch: 1,
                view: 2,
            },
            partition: PartitionIncarnation::from_bytes([4; 16]),
            owner_epoch: 1,
        },
        start: reader::Start::Offset(0),
    };
    let sent = Sent {
        broker: broker(),
        session: LinkSessionId::from_bytes([6; 16]),
        request: RequestId::from_bytes([1; 16]),
    };
    readers
        .registry
        .select(&Arc::downgrade(&inbox), sent, &subscribe);
    assert!(
        readers.registry.admitted(broker()).is_empty(),
        "a sent SUBSCRIBE admits nothing yet"
    );
    let subscribed = reader::Subscribed {
        subscription: subscribe.subscription,
        source: source(),
        resolved_offset: 0,
    };
    readers.registry.accept(broker(), &subscribed);
    let topic = Bytes::copy_from_slice(&reader::publication_topic(source()).unwrap());
    let first = readers.registry.admitted(broker());
    assert_eq!(first.keys().collect::<Vec<_>>(), [&topic]);
    assert!(
        readers
            .registry
            .admitted(NodeId::from_bytes([9; 16]))
            .is_empty(),
        "another broker admits nothing"
    );
    readers.registry.accept(broker(), &subscribed);
    assert!(
        readers.registry.admitted(broker())[&topic] > first[&topic],
        "each acceptance renews the SUBSCRIBE"
    );
    readers.registry.unselect(&subscribed);
    assert!(readers.registry.admitted(broker()).is_empty());
}
