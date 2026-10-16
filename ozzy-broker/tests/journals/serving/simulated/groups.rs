use super::*;
use ozzy_proto::{
    ConsumerMemberId, Offset, OperationId,
    append::Authority,
    group::{self, Mode, Owned, Start, Status},
    progress,
};
use ozzy_runtime::replicated::{BrokerLinkError, ProgressError, TopicRoutes};
use ozzy_sim::broker::Cluster;

mod churn;
mod deletion;
mod reader;

async fn authority(cluster: &Cluster, routes: &TopicRoutes, number: u32) -> (NodeId, Authority) {
    routes.interest(number).unwrap();
    live_many(&cluster.brokers, "current group authority", async {
        loop {
            if let Some(route) = routes.route(number).unwrap()
                && let Some(leader) = route.leader
            {
                return (
                    leader,
                    Authority {
                        group_id: route.group,
                        config_epoch: route.config_epoch,
                        view: route.view,
                    },
                );
            }
            tokio::task::yield_now().await;
        }
    })
    .await
}

/// Progress as the SDK processing driver sends it: a response timeout resends
/// the cumulative cursor, which is safe to repeat. A loaded suite can delay
/// one commit past the request timeout; any other outcome returns at once.
async fn progress(
    cluster: &Cluster,
    links: &BrokerLinks,
    leader: NodeId,
    request: progress::Request,
) -> Result<progress::Confirmed, BrokerLinkError> {
    let mut attempts = 3;
    loop {
        let result = tokio::select! {
            result = links.group_progress(leader, request) => result,
            (result, broker, _) = futures::future::select_all(
                cluster.brokers.iter().map(|broker| Box::pin(broker.closed())),
            ) => panic!("broker {broker} exited during group progress: {result:?}"),
        };
        attempts -= 1;
        if attempts == 0 || !matches!(result, Err(BrokerLinkError::Timeout)) {
            return result;
        }
    }
}

async fn join(
    cluster: &Cluster,
    links: &BrokerLinks,
    routes: &TopicRoutes,
    member: u8,
    mode: Mode,
    owned: Vec<Owned>,
    ready: bool,
) -> group::JoinReply {
    let coordinator = routes.metadata().coordinator("workers");
    live_many(
        &cluster.brokers,
        &format!("observe committed group plan {member}/{mode:?}"),
        async {
            loop {
                let (leader, authority) = authority(cluster, routes, coordinator.number).await;
                let result = links
                    .group_join(
                        leader,
                        group::JoinRequest {
                            authority,
                            coordinator: coordinator.incarnation,
                            name: "workers".into(),
                            member: ConsumerMemberId::from_bytes([member; 16]),
                            join: OperationId::from_bytes([member; 16]),
                            group: None,
                            mode,
                            initial: Start::Earliest,
                            owned: owned.clone(),
                        },
                    )
                    .await;
                match result {
                    Ok(reply) if !ready || reply.status == Status::Member => return reply,
                    Ok(_)
                    | Err(
                        BrokerLinkError::Session
                        | BrokerLinkError::Timeout
                        | BrokerLinkError::Transport(_),
                    ) => {}
                    Err(BrokerLinkError::Rejected { retry, .. })
                        if retry != ozzy_proto::nack::RetryClass::Permanent => {}
                    result => panic!("group join failed: {result:?}"),
                }
                routes.refresh(coordinator.number).unwrap();
                tokio::task::yield_now().await;
            }
        },
    )
    .await
}

async fn activate(
    cluster: &Cluster,
    links: &BrokerLinks,
    routes: &TopicRoutes,
    plan: &group::JoinReply,
    member: u8,
    target: group::Target,
) -> group::AssignReply {
    live_many(&cluster.brokers, "activate committed target", async {
        loop {
            let (leader, authority) = authority(cluster, routes, target.number).await;
            match links
                .group_assign(
                    leader,
                    group::AssignRequest {
                        authority,
                        partition: target.partition,
                        group: plan.group,
                        member: Some(ConsumerMemberId::from_bytes([member; 16])),
                        plan_revision: target.plan_revision,
                        initial: Start::Earliest,
                        policy: routes.metadata().policy(),
                    },
                )
                .await
            {
                Ok(reply) => return reply,
                Err(BrokerLinkError::Session | BrokerLinkError::Timeout) => {}
                Err(BrokerLinkError::Rejected { retry, .. })
                    if retry != ozzy_proto::nack::RetryClass::Permanent => {}
                result => panic!("activation failed: {result:?}"),
            }
            routes.refresh(target.number).unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn coordinator_failover_preserves_drains_epochs_and_confirmed_source_cursors() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        failover_during_drain(policy).await;
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "keep the broker failover schedule and independent cursor checks together"
)]
async fn failover_during_drain(policy: Confirmation) {
    let mut cluster = Cluster::new(policy).await;
    let links = role_links_with_config(
        &cluster.runtime,
        &cluster.configs[0].0,
        handshake::PRODUCER | handshake::CONSUMER,
        limits(),
        |config| {
            config.request_timeout = Duration::from_millis(200);
            config.parameters.capabilities |= handshake::OWNER_GROUPS | handshake::OWNER_PROGRESS;
            config.parameters.required_capabilities |=
                handshake::OWNER_GROUPS | handshake::OWNER_PROGRESS;
        },
    )
    .await;
    let mut writer = live_many(
        &cluster.brokers,
        "seed group sources",
        SharedTopicWriter::open(
            &links,
            "orders",
            SharedTopicWriterConfig::new(limits()),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    for number in 0..4 {
        let key = (0_u32..1000)
            .map(u32::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number == number)
            .unwrap();
        let pending = writer
            .send(
                RecordInput::copy_from_slice(
                    MessageId::from_bytes([number as u8 + 1; 16]),
                    b"confirmed source",
                ),
                Some(&key),
            )
            .await
            .unwrap();
        live_many(
            &cluster.brokers,
            "confirm source record",
            pending.confirmed(),
        )
        .await
        .unwrap();
    }
    let routes = links.routes(writer.metadata().clone()).await.unwrap();
    let first = join(&cluster, &links, &routes, 2, Mode::Join, Vec::new(), true).await;
    assert_eq!(first.targets.len(), 4);
    let mut owned = Vec::new();
    for target in &first.targets {
        let assigned = activate(&cluster, &links, &routes, &first, 2, *target).await;
        assert_eq!((assigned.epoch, assigned.cursor), (1, Offset::ZERO));
        let (leader, authority) = authority(&cluster, &routes, target.number).await;
        let reply = progress(
            &cluster,
            &links,
            leader,
            progress::Request {
                authority,
                assignment: progress::Assignment {
                    group: first.group,
                    partition: target.partition,
                    member: ConsumerMemberId::from_bytes([2; 16]),
                    epoch: assigned.epoch,
                },
                policy: routes.metadata().policy(),
                cursor: Some(Offset::new(1)),
            },
        )
        .await
        .unwrap();
        assert_eq!(reply.cursor, Offset::new(1));
        owned.push(Owned {
            partition: target.partition,
            plan_revision: target.plan_revision,
            epoch: assigned.epoch,
        });
    }
    join(
        &cluster,
        &links,
        &routes,
        2,
        Mode::Heartbeat,
        owned.clone(),
        true,
    )
    .await;
    let second = join(&cluster, &links, &routes, 1, Mode::Join, Vec::new(), true).await;
    assert_eq!(second.targets.len(), 2);
    assert!(second.targets.iter().all(|target| !target.activatable));
    let coordinator = routes.metadata().coordinator("workers");
    let (leader, _) = authority(&cluster, &routes, coordinator.number).await;
    let index = cluster
        .configs
        .iter()
        .position(|(_, identity)| NodeId::from_bytes(*identity.broker.as_bytes()) == leader)
        .unwrap();
    cluster.controls[index].crash(false);
    let _ = cluster.brokers.remove(index).shutdown().await;
    cluster.controls.remove(index);
    cluster.images.remove(index).await.unwrap();
    routes.refresh(coordinator.number).unwrap();
    let recovered = join(
        &cluster,
        &links,
        &routes,
        1,
        Mode::Heartbeat,
        Vec::new(),
        true,
    )
    .await;
    assert_eq!(
        (recovered.group, recovered.revision),
        (second.group, second.revision)
    );
    assert_eq!(recovered.targets, second.targets);
    join(&cluster, &links, &routes, 2, Mode::Heartbeat, owned, true).await;
    assert!(
        join(
            &cluster,
            &links,
            &routes,
            1,
            Mode::Heartbeat,
            Vec::new(),
            true
        )
        .await
        .targets
        .iter()
        .all(|target| !target.activatable)
    );
    join(
        &cluster,
        &links,
        &routes,
        2,
        Mode::Heartbeat,
        Vec::new(),
        true,
    )
    .await;
    let released = join(
        &cluster,
        &links,
        &routes,
        1,
        Mode::Heartbeat,
        Vec::new(),
        true,
    )
    .await;
    assert!(released.targets.iter().all(|target| target.activatable));
    for target in released.targets.iter().copied() {
        let assigned = activate(&cluster, &links, &routes, &released, 1, target).await;
        assert_eq!((assigned.epoch, assigned.cursor), (2, Offset::new(1)));
        assert_eq!(
            activate(&cluster, &links, &routes, &released, 1, target).await,
            assigned
        );
        let (leader, authority) = authority(&cluster, &routes, target.number).await;
        assert!(matches!(
            progress(
                &cluster,
                &links,
                leader,
                progress::Request {
                    authority,
                    assignment: progress::Assignment {
                        group: first.group,
                        partition: target.partition,
                        member: ConsumerMemberId::from_bytes([2; 16]),
                        epoch: 1
                    },
                    policy: routes.metadata().policy(),
                    cursor: Some(Offset::new(1)),
                }
            )
            .await,
            Err(BrokerLinkError::Progress(ProgressError::Fenced))
        ));
    }
    writer.close().await.unwrap();
    links.shutdown().await.unwrap();
    cluster.shutdown().await;
}
