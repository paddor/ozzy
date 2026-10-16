use super::*;
use ozzy_runtime::replicated::{GroupReader, GroupReaderConfig, GroupReaderError};

#[tokio::test(flavor = "current_thread")]
async fn deleted_groups_fence_members_and_free_the_name_for_a_new_incarnation() {
    for mode in [DeploymentMode::Single, DeploymentMode::Three] {
        deleted(mode).await;
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "keep deletion, fencing, and recreation evidence in one schedule"
)]
async fn deleted(mode: DeploymentMode) {
    let cluster = Cluster::new(mode).await;
    let links = role_links_with_config(
        &cluster.runtime,
        &cluster.configs[0].0,
        handshake::PRODUCER | handshake::CONSUMER,
        limits(),
        |config| {
            config.parameters.capabilities |= handshake::OWNER_GROUPS | handshake::OWNER_PROGRESS;
            config.parameters.required_capabilities |=
                handshake::OWNER_GROUPS | handshake::OWNER_PROGRESS;
        },
    )
    .await;
    let mut writer = live_many(
        &cluster.brokers,
        "seed deleted group sources",
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
        for sequence in 0..2_u128 {
            let id = MessageId::from_bytes((u128::from(number) * 2 + sequence + 1).to_be_bytes());
            let pending = writer
                .send(
                    RecordInput::copy_from_slice(id, b"deleted group"),
                    Some(&key),
                )
                .await
                .unwrap();
            live_many(
                &cluster.brokers,
                "confirm deleted group source",
                pending.confirmed(),
            )
            .await
            .unwrap();
        }
    }
    let routes = links.routes(writer.metadata().clone()).await.unwrap();
    live_many(
        &cluster.brokers,
        "stop deleted group writer",
        writer.close(),
    )
    .await
    .unwrap();
    let plan = join(&cluster, &links, &routes, 2, Mode::Join, Vec::new(), true).await;
    let mut activated = Vec::new();
    for target in &plan.targets {
        let assigned = activate(&cluster, &links, &routes, &plan, 2, *target).await;
        let (leader, authority) = authority(&cluster, &routes, target.number).await;
        let confirmed = live_many(
            &cluster.brokers,
            "confirm processing before deletion",
            links.group_progress(
                leader,
                progress::Request {
                    authority,
                    assignment: progress::Assignment {
                        group: plan.group,
                        partition: target.partition,
                        member: ConsumerMemberId::from_bytes([2; 16]),
                        epoch: assigned.epoch,
                    },
                    policy: routes.metadata().policy(),
                    cursor: Some(Offset::new(1)),
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(confirmed.cursor, Offset::new(1));
        activated.push((*target, assigned));
    }
    let inspection = live_many(
        &cluster.brokers,
        "inspect the committed group",
        links.inspect_group("orders", "workers"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(inspection.group, plan.group);
    assert_eq!(inspection.revision, plan.revision);
    assert_eq!(inspection.start, Start::Earliest);
    assert_eq!(inspection.members.len(), 1);
    assert_eq!(
        inspection.members[0].id,
        ConsumerMemberId::from_bytes([2; 16])
    );
    assert_eq!(inspection.targets.len(), 4);
    assert!(
        inspection
            .targets
            .iter()
            .all(|target| target.member == Some(ConsumerMemberId::from_bytes([2; 16])))
    );

    let deleted = live_many(
        &cluster.brokers,
        "delete the group",
        links.delete_group("orders", "workers"),
    )
    .await
    .unwrap();
    assert_eq!(deleted, Some(plan.group));
    assert_eq!(
        live_many(
            &cluster.brokers,
            "inspect the deleted group",
            links.inspect_group("orders", "workers"),
        )
        .await
        .unwrap(),
        None
    );
    assert_eq!(
        live_many(
            &cluster.brokers,
            "delete the group again",
            links.delete_group("orders", "workers"),
        )
        .await
        .unwrap(),
        None
    );
    // The old member can neither advance progress nor reactivate a source.
    for (target, assigned) in &activated {
        let (leader, authority) = authority(&cluster, &routes, target.number).await;
        let progressed = live_many(
            &cluster.brokers,
            "fence progress of the deleted group",
            links.group_progress(
                leader,
                progress::Request {
                    authority,
                    assignment: progress::Assignment {
                        group: plan.group,
                        partition: target.partition,
                        member: ConsumerMemberId::from_bytes([2; 16]),
                        epoch: assigned.epoch,
                    },
                    policy: routes.metadata().policy(),
                    cursor: Some(Offset::new(2)),
                },
            ),
        )
        .await;
        assert!(
            matches!(
                progressed,
                Err(BrokerLinkError::Progress(ProgressError::Fenced))
            ),
            "{progressed:?}"
        );
        let reactivated = live_many(
            &cluster.brokers,
            "fence activation of the deleted group",
            links.group_assign(
                leader,
                group::AssignRequest {
                    authority,
                    partition: target.partition,
                    group: plan.group,
                    member: Some(ConsumerMemberId::from_bytes([2; 16])),
                    plan_revision: target.plan_revision,
                    initial: Start::Earliest,
                    policy: routes.metadata().policy(),
                },
            ),
        )
        .await;
        assert!(
            matches!(
                reactivated,
                Err(BrokerLinkError::Progress(ProgressError::Fenced))
            ),
            "{reactivated:?}"
        );
    }
    // An instance of the retired incarnation is told so instead of
    // recreating the group.
    let coordinator = routes.metadata().coordinator("workers");
    let (leader, authority) = authority(&cluster, &routes, coordinator.number).await;
    let heartbeat = live_many(
        &cluster.brokers,
        "heartbeat of the deleted group",
        links.group_join(
            leader,
            group::JoinRequest {
                authority,
                coordinator: coordinator.incarnation,
                name: "workers".into(),
                member: ConsumerMemberId::from_bytes([2; 16]),
                join: OperationId::from_bytes([2; 16]),
                group: Some(plan.group),
                mode: Mode::Heartbeat,
                initial: Start::Earliest,
                owned: Vec::new(),
            },
        ),
    )
    .await
    .unwrap();
    assert_eq!(heartbeat.status, Status::Deleted);
    assert_eq!(heartbeat.targets, [] as [group::Target; 0]);

    // A new reader creates a new incarnation without the old cursors.
    let mut config = GroupReaderConfig::new(ConsumerMemberId::from_bytes([3; 16]));
    config.heartbeat = Duration::from_millis(10);
    config.window = 1;
    let mut reader = GroupReader::open(links.clone(), "orders", "workers", config)
        .await
        .unwrap();
    let record = live_many(&cluster.brokers, "read the recreated group", reader.next())
        .await
        .unwrap();
    assert_eq!(record.record.offset, Offset::ZERO);
    let recreated = live_many(
        &cluster.brokers,
        "inspect the recreated group",
        links.inspect_group("orders", "workers"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_ne!(recreated.group, plan.group);
    let retired = live_many(
        &cluster.brokers,
        "delete the recreated group",
        links.delete_group("orders", "workers"),
    )
    .await
    .unwrap();
    assert_eq!(retired, Some(recreated.group));
    // Its reader stops instead of rejoining under a new incarnation.
    let stopped = live_many(&cluster.brokers, "stop the deleted reader", async {
        loop {
            match reader.next().await {
                Ok(_) => {}
                Err(error) => return error,
            }
        }
    })
    .await;
    assert!(matches!(stopped, GroupReaderError::Deleted), "{stopped:?}");
    assert_eq!(
        live_many(
            &cluster.brokers,
            "inspect after the reader stopped",
            links.inspect_group("orders", "workers"),
        )
        .await
        .unwrap(),
        None
    );
    drop(reader);
    links.shutdown().await.unwrap();
    cluster.shutdown().await;
}
