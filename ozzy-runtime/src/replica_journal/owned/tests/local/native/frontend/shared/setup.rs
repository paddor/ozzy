//! Harness construction: one or two partitions behind shared broker links,
//! with optional writers, reader windows, and consumer progress history.

use super::*;

pub(super) fn config(
    local: NodeId,
    addresses: Vec<BrokerAddress>,
    clock: SdkClock,
) -> BrokerLinksConfig {
    let mut parameters = link(70, 80).remote;
    parameters.capabilities |= handshake::OWNER_ROUTING;
    BrokerLinksConfig {
        reader: None,
        local,
        brokers: addresses,
        parameters,
        requests: 12,
        control_bytes: 1024 * 1024,
        routing_bytes: 1024 * 1024,
        append: Some(crate::replicated::AppendLinkLimits {
            writers: 4,
            requests: 8,
            records: 16,
            bytes: 16 * 1024 * 1024,
        }),
        maximum_partitions: 16,
        request_timeout: Duration::from_secs(5),
        retry_interval: Duration::from_millis(100),
        clock,
    }
}

pub(super) async fn setup_shared() -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    setup_shared_with_writers(&[12, 30]).await
}

/// A harness whose partitions keep confirmed history for consumer progress
/// and group tests, with no writers of its own.
pub(super) async fn setup_progress(
    count: usize,
    reader_window: Option<(u64, u64, usize)>,
) -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    Box::pin(setup_shared_directory_profile(
        &[],
        count,
        reader_window,
        false,
        Some(ProgressHistory::Available),
    ))
    .await
}

pub(super) async fn setup_shared_with_writers(
    writers: &[u8],
) -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    setup_shared_partitions(writers, 1).await
}

pub(super) async fn setup_shared_partitions(
    writers: &[u8],
    count: usize,
) -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    setup_shared_profile(writers, count, None).await
}

pub(super) async fn setup_shared_profile(
    writers: &[u8],
    count: usize,
    reader_window: Option<(u64, u64, usize)>,
) -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    Box::pin(setup_shared_directory_profile(
        writers,
        count,
        reader_window,
        false,
        None,
    ))
    .await
}

#[expect(clippy::too_many_lines, reason = "one linear scenario")]
pub(super) async fn setup_shared_directory_profile(
    writers: &[u8],
    count: usize,
    reader_window: Option<(u64, u64, usize)>,
    alternate_metadata: bool,
    progress_history: Option<ProgressHistory>,
) -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    assert!(matches!(count, 1 | 2));
    let runtime = WriterRuntime::new().unwrap();
    let mut wire = limits();
    if reader_window.is_some() {
        wire.envelope.max_payload_bytes = 4096;
    }
    let (mut controller, io) = setup();
    let capacity = multiple::budgets(count);
    let memory = |budget: crate::dispatch::Budget| {
        crate::memory::Domain::new(None, budget.bytes)
            .unwrap()
            .owner(crate::memory::Limits {
                bytes: budget.bytes,
                buffers: 64 * count,
                cache_bytes: budget.bytes,
            })
            .unwrap()
    };
    let data = memory(capacity.data);
    let control = memory(capacity.control);
    let mut native = Vec::new();
    let mut partitions = Vec::new();
    let mut groups = Vec::new();
    let mut assignment_service = None;
    let mut coordinators = Vec::new();
    let topology = std::sync::Arc::new(crate::replica_actor::TopicTopology {
        seed: 7,
        partitions: (0..count)
            .map(|number| (number as u32, multiple::incarnation(number)))
            .collect(),
    });
    let coordinate = |actor: &mut crate::replica_actor::LocalActor, number: usize| {
        let config = crate::replica_actor::GroupCoordinatorConfig {
            local: actor.authority_hint().primary,
            group: actor.group(),
            partition: multiple::incarnation(number),
            number: number as u32,
            topology: topology.clone(),
            limits: wire.envelope,
            requests: 4,
            plans: 1,
            max_members: 8,
            membership_timeout: Duration::from_secs(10),
            drain_deadline: Duration::from_secs(5),
        };
        let arena = actor
            .lease_proposal_buffer_with_limits(ozzy_replication::PipelineLimits {
                max_operations: 1,
                max_body_bytes: config.plan_bytes(),
            })
            .unwrap();
        crate::replica_actor::GroupCoordinator::new(
            config,
            actor.take_submitter().unwrap(),
            vec![arena],
        )
        .unwrap()
    };
    for number in 0..count {
        if let Some(history) = progress_history
            && number == 0
        {
            let (mut actor, intake) =
                super::super::group_progress::fixture(&mut controller, io.clone(), history);
            let buffer = actor
                .lease_proposal_buffer_with_limits(ozzy_replication::PipelineLimits {
                    max_operations: 1,
                    max_body_bytes: 98,
                })
                .unwrap();
            assignment_service = Some((actor.take_submitter().unwrap(), buffer));
            coordinators.push(coordinate(&mut actor, number));
            groups.push(actor.group());
            native.push(intake);
            partitions.push(PartitionActor::Local(Box::new(actor)));
            continue;
        }
        let incarnation = multiple::incarnation(number);
        let mut actor = actor_with_options(
            &mut controller,
            io.clone(),
            number as u8 + 1,
            writers,
            incarnation,
            PartitionId::new(number as u32),
            ActorOptions {
                memory: Some(&data),
                ..ActorOptions::default()
            },
        );
        groups.push(actor.group());
        native.push(super::super::intake_access_profile(
            &mut actor,
            NativeAccess::TrustedClients {
                clients: 2,
                writers: 2,
            },
            incarnation,
            None,
            wire,
        ));
        coordinators.push(coordinate(&mut actor, number));
        partitions.push(PartitionActor::Local(Box::new(actor)));
    }
    let authority = partitions[0].authority_hint();
    let group = groups[0];
    let (mut service, input) = multiple::service(
        authority.primary,
        &groups,
        &data,
        &control,
        wire,
        progress_history.is_some(),
    );
    let server = runtime.context().socket(
        SocketType::Peer,
        Options::default()
            .identity(Bytes::copy_from_slice(authority.primary.as_bytes()))
            .router_mandatory(true)
            .send_hwm(16)
            .recv_hwm(16)
            .max_message_size(8192),
    );
    let data_server = runtime.context().socket(
        SocketType::Peer,
        Options::default()
            .identity(Bytes::copy_from_slice(authority.primary.as_bytes()))
            .router_mandatory(true)
            .send_hwm(16)
            .recv_hwm(16)
            .max_message_size(8192),
    );
    let endpoint = server
        .bind(format!("inproc://shared-native-{}", RequestId::new()))
        .await
        .unwrap();
    let data_endpoint = data_server
        .bind(format!("inproc://shared-native-data-{}", RequestId::new()))
        .await
        .unwrap();
    let publisher = runtime
        .context()
        .socket(SocketType::Pub, Options::default().send_hwm(4));
    let publications = publisher
        .bind(format!("inproc://shared-readers-{}", RequestId::new()))
        .await
        .unwrap();
    let catalog = multiple::catalog(authority, &groups, &endpoint, &publications);
    service
        .install_catalog(if alternate_metadata {
            metadata::paginate(&catalog, count)
        } else {
            catalog
        })
        .unwrap();
    let mut actors = PartitionActors::new(partitions, count, 1).unwrap();
    for native in native {
        actors.install_native(native, service.links()).unwrap();
    }
    for coordinator in coordinators {
        actors
            .install_coordinator(coordinator, service.links())
            .unwrap();
    }
    if reader_window.is_some() {
        for (number, &group) in groups.iter().enumerate() {
            actors
                .install_readers(
                    group,
                    crate::replica_actor::SharedReaderConfig {
                        partition: multiple::incarnation(number),
                        limits: wire,
                        subscriptions: 2,
                    },
                    service.links(),
                    None,
                )
                .unwrap();
        }
    }
    multiple::watches(&mut service, &actors, &groups);
    // These addresses are never bound. Only the primary can complete a
    // transport or Ozzy handshake in this inproc harness.
    let mut addresses = vec![BrokerAddress {
        node: authority.primary,
        endpoint,
        data_endpoint,
    }];
    for index in 0..2 {
        addresses.push(BrokerAddress {
            node: NodeId::from_bytes([index as u8 + 2; 16]),
            data_endpoint: format!(
                "inproc://shared-unavailable-data-{}-{index}",
                RequestId::new()
            )
            .parse()
            .unwrap(),
            endpoint: format!("inproc://shared-unavailable-{}-{index}", RequestId::new())
                .parse()
                .unwrap(),
        });
    }
    let metadata_peer = if alternate_metadata {
        let address = &addresses[1];
        let (mut service, input) =
            multiple::service(address.node, &groups, &data, &control, wire, false);
        service
            .install_catalog(metadata::paginate(
                &multiple::catalog(authority, &groups, &addresses[0].endpoint, &publications),
                count,
            ))
            .unwrap();
        let server = runtime.context().socket(
            SocketType::Peer,
            Options::default()
                .identity(Bytes::copy_from_slice(address.node.as_bytes()))
                .router_mandatory(true)
                .send_hwm(16)
                .recv_hwm(16)
                .max_message_size(8192),
        );
        let peer = Box::new(metadata::Peer::new(service, input, server));
        peer.server.bind(address.endpoint.clone()).await.unwrap();
        Some(peer)
    } else {
        None
    };
    let clock = SdkClock::manual();
    let mut options = config(link(70, 80).binding.peer, addresses, clock.clone());
    // Four logical owners must fit even when each allows two caller handles.
    // Test the owner bound, independently of the queue representation's size.
    let writer = crate::replicated::SharedTopicWriterConfig {
        limits: limits(),
        compress_payloads: true,
        batch_target_bytes: 1024,
        max_producers: 2,
        inflight_appends: 1,
    };
    let reservation = writer
        .link_reservation(options.parameters.receive.envelope.max_metadata_bytes)
        .unwrap();
    let append = options.append.as_mut().unwrap();
    append.bytes =
        append.writers * reservation.idle_bytes + (append.requests + 1) * reservation.request_bytes;
    if let Some((_records, _bytes, subscriptions)) = reader_window {
        options.parameters =
            handshake::Parameters::streaming(wire, handshake::PRODUCER | handshake::CONSUMER)
                .unwrap();
        options.parameters.capabilities |= handshake::OWNER_ROUTING | handshake::OWNER_READ;
        options.reader = Some(crate::replicated::ReaderLinkLimits {
            subscriptions,
            bytes: 4 * 1024 * 1024,
            queue_messages: 1,
        });
    }
    if progress_history.is_some() {
        options.parameters = handshake::Parameters::group(wire, handshake::CONSUMER).unwrap();
        options.parameters.capabilities |= handshake::OWNER_ROUTING;
        options.append = None;
    }
    let links = BrokerLinks::connect_with_ids(
        &runtime,
        options,
        LinkIds::deterministic(NonZeroU64::new(70).unwrap()),
        LinkIds::deterministic(NonZeroU64::new(71).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(
        links.socket_count(),
        if reader_window.is_some() { 5 } else { 2 }
    );
    let harness = Harness {
        local: authority.primary,
        service,
        input,
        waiting: None,
        session: None,
        server,
        data_server,
        publisher,
        wire,
        readers: ReaderTraffic::default(),
        publish: true,
        publications: std::collections::VecDeque::new(),
        compressed_appends: 0,
        controller,
        actors,
        group,
        groups,
        held_io: BTreeSet::new(),
        watch: None,
        drop_snapshots: false,
        refuse_snapshots: false,
        metadata: metadata::Traffic {
            peer: metadata_peer,
            ..metadata::Traffic::default()
        },
        snapshots: 0,
        drop_opened: None,
        defer_openings: false,
        openings: Vec::new(),
        drop_confirmation: None,
        hold_confirmation: None,
        held_confirmation: None,
        drop_append: None,
        refuse_append: None,
        capacity_replies: BTreeSet::new(),
        appends: Vec::new(),
        drop_progress: false,
        drop_progress_request: false,
        refuse_assign: 0,
        assignment_attempts: 0,
        assignment_service,
        now: Duration::ZERO,
    };
    (harness, links, clock, authority)
}
