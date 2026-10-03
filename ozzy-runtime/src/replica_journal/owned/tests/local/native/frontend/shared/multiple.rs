use super::*;

pub(super) fn grant_writer(
    input: &mut dispatch::Receiver<Message>,
    client: &Client,
    service: &mut Service,
    keys: &mut BTreeMap<(GroupId, ProducerId), dispatch::GrantKey>,
    group: GroupId,
    writer: ProducerId,
) {
    if keys.contains_key(&(group, writer)) {
        return;
    }
    let grant = input
        .credits()
        .grant(
            client,
            Class::Data,
            Quota {
                messages: 2,
                bytes: 8 * 1024,
            },
        )
        .unwrap();
    let key = grant.key();
    service
        .install(
            link(70, 80).binding.peer,
            Subject {
                group,
                writer: Some(writer),
            },
            grant,
        )
        .unwrap();
    keys.insert((group, writer), key);
}

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
    data: &crate::memory::Owner,
    control: &crate::memory::Owner,
    wire: DataLimits,
) -> (Service, dispatch::Receiver<Message>) {
    let capacity = budgets(groups.len());
    let (sender, mut receiver) = dispatch::channel(dispatch::Limits {
        capacity,
        clients: 2,
        grants: 2 * groups.len() + 1,
    })
    .unwrap();
    receiver.credits().bind_memory(data, control).unwrap();
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
        vec![(7, sender)],
        DispatcherLimits {
            peers: 2,
            grants_per_class: 2 * groups.len(),
            replies: ReplyLimits {
                data: queue,
                control: queue,
            },
        },
    )
    .unwrap();
    let parameters = handshake::Parameters::streaming(wire, handshake::OWNER, 4, 4096).unwrap();
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
        receiver,
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
