use super::*;
use ozzy_proto::{Envelope, GroupId, MessageId, PartitionIncarnation, data::DataLimits};

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

fn registry() -> Registry {
    Registry::new(&config()).unwrap()
}

#[test]
fn declared_reader_reservation_admits_all_subscriptions_and_one_byte_less_does_not() {
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
        let registry = Registry::new(&config).unwrap();
        let entries = (0..4)
            .map(|index| {
                registry
                    .register(
                        SubscriptionId::from_bytes([index + 10; 16]),
                        Bytes::from_static(b"prefix"),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(
            registry
                .register(
                    SubscriptionId::from_bytes([20; 16]),
                    Bytes::from_static(b"prefix")
                )
                .is_err()
        );
        assert_eq!(entries.len(), 4);
        config.reader.as_mut().unwrap().bytes -= 1;
        assert_eq!(Registry::capacity(&config).unwrap().0, 3);
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

#[test]
fn publication_fanout_bounds_each_reader_and_charges_retained_aliases() {
    let registry = registry();
    let prefix = Bytes::copy_from_slice(&reader::publication_topic(source()).unwrap());
    let slow = registry
        .register(SubscriptionId::from_bytes([10; 16]), prefix.clone())
        .unwrap();
    let healthy = registry
        .register(SubscriptionId::from_bytes([11; 16]), prefix.clone())
        .unwrap();
    assert_eq!(registry.prefixes().len(), 1, "shared subscription prefix");
    let broker = NodeId::from_bytes([2; 16]);
    let original = publication(0);
    let mut last = None;
    for offset in 0..4 {
        registry.publication(broker, &publication(offset), EnvelopeLimits::default());
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
        registry
            .register(SubscriptionId::from_bytes([12; 16]), prefix.clone())
            .is_err()
    );
    drop(alias);
    let next = registry
        .register(SubscriptionId::from_bytes([12; 16]), prefix)
        .unwrap();
    drop((held, slow, next));
    assert!(registry.prefixes().is_empty());
}

#[test]
fn publications_validate_broker_and_prefix_before_copying_opaque_payloads() {
    let registry = registry();
    let prefix = Bytes::copy_from_slice(&reader::publication_topic(source()).unwrap());
    let inbox = registry
        .register(SubscriptionId::from_bytes([10; 16]), prefix)
        .unwrap();
    let message = publication(0);
    registry.publication(
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
    registry.publication(
        NodeId::from_bytes([2; 16]),
        &foreign,
        EnvelopeLimits::default(),
    );
    assert!(inbox.publication().is_none());
    registry.publication(
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

#[test]
fn retained_publication_frames_cannot_exceed_one_readers_reserved_storage() {
    let registry = registry();
    let prefix = Bytes::copy_from_slice(&reader::publication_topic(source()).unwrap());
    let slow = registry
        .register(SubscriptionId::from_bytes([10; 16]), prefix.clone())
        .unwrap();
    let healthy = registry
        .register(SubscriptionId::from_bytes([11; 16]), prefix)
        .unwrap();
    let broker = NodeId::from_bytes([2; 16]);
    let mut retained = Vec::new();
    for offset in 0..7 {
        registry.publication(broker, &publication(offset), EnvelopeLimits::default());
        retained.push(slow.publication().unwrap().part_bytes(3).unwrap());
        drop(healthy.publication().unwrap());
    }
    registry.publication(broker, &publication(7), EnvelopeLimits::default());
    assert!(
        slow.publication().is_none(),
        "retained aliases fill capacity"
    );
    drop(healthy.publication().unwrap());
    drop(retained.pop());
    registry.publication(broker, &publication(8), EnvelopeLimits::default());
    assert!(
        slow.publication().is_some(),
        "released alias returns capacity"
    );
    assert!(healthy.publication().is_some());
}
