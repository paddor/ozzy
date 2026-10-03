use super::*;

pub(super) fn incarnation(number: usize) -> PartitionIncarnation {
    if number == 0 {
        partition()
    } else {
        PartitionIncarnation::from_bytes([number as u8 + 1; 16])
    }
}

pub(super) fn budgets(partitions: usize) -> Budgets {
    let mut capacity = super::super::budgets();
    for budget in [&mut capacity.data, &mut capacity.control] {
        budget.queue_slots *= partitions;
        budget.retained_messages *= partitions;
        budget.bytes *= partitions;
    }
    capacity
}

pub(super) fn service(
    local: NodeId,
    groups: &[GroupId],
    _data: &crate::memory::Owner,
    _control: &crate::memory::Owner,
    wire: DataLimits,
) -> (Service, TestInput) {
    let capacity = budgets(groups.len());
    let (data, data_rx) = data_channel(
        &omq_tokio::Context::new(),
        7,
        Kind::Client,
        Class::Data,
        16,
        8192,
        capacity.data.bytes,
    )
    .unwrap();
    let (control, control_rx) = data_channel(
        &omq_tokio::Context::new(),
        7,
        Kind::Client,
        Class::Control,
        16,
        8192,
        capacity.control.bytes,
    )
    .unwrap();
    let placements = groups
        .iter()
        .enumerate()
        .map(|(number, &group)| Placement {
            group,
            partition: incarnation(number),
            shard: 7,
        })
        .collect::<Vec<_>>();
    let routes =
        crate::frontend::RoutingTable::new(&[7], &placements, groups.len(), wire.envelope).unwrap();
    let queue = QueueLimits {
        messages: 8 * groups.len(),
        bytes: 16 * 1024 * groups.len(),
        message_bytes: 8192,
    };
    let dispatcher = Dispatcher::new(
        local,
        routes,
        vec![(7, data), (7, control)],
        DispatcherLimits {
            peers: 2,

            replies: ReplyLimits {
                data: queue,
                control: queue,
            },
        },
    )
    .unwrap();
    let parameters = handshake::Parameters::streaming(wire, handshake::OWNER).unwrap();
    (
        Service::new(
            dispatcher,
            parameters,
            &[],
            LinkIds::deterministic(NonZeroU64::new(99).unwrap()),
        )
        .unwrap()
        .with_trusted_clients(2)
        .unwrap(),
        TestInput([data_rx, control_rx]),
    )
}

pub(super) fn catalog(
    authority: ozzy_proto::nack::AuthorityHint,
    groups: &[GroupId],
    endpoint: &omq_tokio::Endpoint,
    publications: &omq_tokio::Endpoint,
) -> TopicCatalog {
    TopicCatalog::new(
        [directory::TopicPage {
            id: TopicId::from_bytes([5; 16]),
            name: "orders".to_owned(),
            partitioner_seed: 7,
            total: groups.len() as u32,
            policy: Policy::LocalDurable,
            brokers: vec![directory::BrokerEndpoint {
                node: authority.primary,
                peer: endpoint.to_string(),
                reader_pub: publications.to_string(),
                follower_pub: None,
            }],
            first: 0,
            partitions: groups
                .iter()
                .enumerate()
                .map(|(number, &group)| directory::TopicPartition {
                    number: number as u32,
                    group,
                    config_epoch: authority.authority.config_epoch,
                    incarnation: incarnation(number),
                    members: vec![authority.primary],
                })
                .collect(),
        }],
        1,
        groups.len(),
    )
    .unwrap()
}

pub(super) fn watches(service: &mut Service, actors: &PartitionActors, groups: &[GroupId]) {
    service
        .install_watches(
            crate::frontend::WatchRegistry::new(
                crate::frontend::WatchLimits {
                    partitions: groups.len(),
                    peers: 1,
                    registrations: groups.len(),
                    interests_per_registration: groups.len(),
                    pending_per_registration: groups.len(),
                },
                groups.iter().enumerate().map(|(number, &group)| {
                    actors.route_state(group, incarnation(number)).unwrap()
                }),
            )
            .unwrap(),
        )
        .unwrap();
}
