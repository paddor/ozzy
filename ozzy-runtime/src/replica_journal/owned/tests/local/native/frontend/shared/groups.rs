use super::*;
use crate::replicated::ProgressError;
use ozzy_proto::{
    ConsumerMemberId,
    group::{self, JoinReply, JoinRequest, Mode, Owned, Start, Status, Target},
};

pub(super) fn join(
    hint: ozzy_proto::nack::AuthorityHint,
    member: u8,
    join: u8,
    mode: Mode,
    owned: Vec<Owned>,
) -> JoinRequest {
    JoinRequest {
        authority: hint.authority,
        coordinator: partition(),
        name: "workers".into(),
        member: ConsumerMemberId::from_bytes([member; 16]),
        join: OperationId::from_bytes([join; 16]),
        group: None,
        mode,
        initial: Start::Earliest,
        owned,
    }
}

/// Repeat heartbeats until the committed plan satisfies `done`.
async fn until_plan(
    harness: &mut Harness,
    links: &BrokerLinks,
    hint: ozzy_proto::nack::AuthorityHint,
    request: &JoinRequest,
    what: &str,
    mut done: impl FnMut(&JoinReply) -> bool,
) -> JoinReply {
    for _ in 0..200 {
        let reply = harness
            .drive(
                links.group_join(
                    hint.primary,
                    JoinRequest {
                        mode: Mode::Heartbeat,
                        ..request.clone()
                    },
                ),
                true,
            )
            .await
            .unwrap();
        if done(&reply) {
            return reply;
        }
        harness.now += Duration::from_millis(10);
    }
    panic!("{what}");
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
async fn sdk_group_membership_creates_plans_fences_instances_and_moves_sources() {
    let (mut harness, links, _, hint) = setup_progress(1, None).await;
    let first = join(hint, 1, 11, Mode::Join, Vec::new());
    let created = harness
        .drive(links.group_join(hint.primary, first.clone()), true)
        .await
        .unwrap();
    assert_eq!(
        (created.status, created.revision, created.group.as_bytes()),
        (Status::Joining, 0, &[0; 16])
    );
    assert_eq!(created.targets, [] as [Target; 0]);
    let member = until_plan(&mut harness, &links, hint, &first, "first plan", |reply| {
        reply.status == Status::Member
    })
    .await;
    assert_eq!(member.revision, 1);
    assert_ne!(member.group.as_bytes(), &[0; 16]);
    assert_eq!(
        member.targets,
        vec![Target {
            partition: partition(),
            number: 0,
            plan_revision: 1,
            activatable: true,
            prior_member: None,
        }]
    );

    // A second member joins; the only source stays with its incumbent.
    let second = join(hint, 2, 22, Mode::Join, Vec::new());
    assert_eq!(
        harness
            .drive(links.group_join(hint.primary, second.clone()), true)
            .await
            .unwrap()
            .status,
        Status::Joining
    );
    let joined = until_plan(
        &mut harness,
        &links,
        hint,
        &second,
        "second plan",
        |reply| reply.status == Status::Member,
    )
    .await;
    assert_eq!((joined.revision, joined.targets.len()), (2, 0));
    let incumbent = harness
        .drive(
            links.group_join(
                hint.primary,
                JoinRequest {
                    mode: Mode::Heartbeat,
                    owned: vec![Owned {
                        partition: partition(),
                        plan_revision: 1,
                        epoch: 1,
                    }],
                    ..first.clone()
                },
            ),
            true,
        )
        .await
        .unwrap();
    assert_eq!(incumbent.targets[0].plan_revision, 1);
    assert_eq!(incumbent.revision, 2);

    // An older instance of the same member is fenced once a newer join exists.
    let stale = join(hint, 1, 12, Mode::Heartbeat, Vec::new());
    assert!(matches!(
        harness
            .drive(links.group_join(hint.primary, stale), true)
            .await,
        Err(BrokerLinkError::Progress(ProgressError::Fenced))
    ));

    // Leaving moves the source to the remaining member without a drain wait.
    harness
        .drive(
            links.group_join(
                hint.primary,
                JoinRequest {
                    mode: Mode::Leave,
                    ..first.clone()
                },
            ),
            true,
        )
        .await
        .unwrap();
    let moved = until_plan(&mut harness, &links, hint, &second, "leave plan", |reply| {
        !reply.targets.is_empty()
    })
    .await;
    assert_eq!(moved.revision, 3);
    assert_eq!(
        moved.targets,
        vec![Target {
            partition: partition(),
            number: 0,
            plan_revision: 3,
            activatable: true,
            prior_member: Some(first.member),
        }]
    );
    assert!(matches!(
        harness
            .drive(
                links.group_join(
                    hint.primary,
                    JoinRequest {
                        mode: Mode::Heartbeat,
                        ..first.clone()
                    }
                ),
                true
            )
            .await,
        Err(BrokerLinkError::Progress(ProgressError::Fenced))
    ));

    // The new owner relays the committed revision to the source.
    let activated = harness
        .drive(
            links.group_assign(
                hint.primary,
                group::AssignRequest {
                    authority: hint.authority,
                    partition: partition(),
                    group: moved.group,
                    member: Some(second.member),
                    plan_revision: 3,
                    initial: Start::Earliest,
                    policy: Policy::LocalDurable,
                },
            ),
            true,
        )
        .await
        .unwrap();
    assert_eq!(
        (activated.epoch, activated.cursor, activated.end),
        (1, Offset::ZERO, Offset::new(3))
    );

    // A restarted instance rejoins with a fresh identity; the incumbent keeps
    // its source, and the restarted member owns nothing until churn.
    let restarted = join(hint, 1, 13, Mode::Join, Vec::new());
    assert_eq!(
        harness
            .drive(links.group_join(hint.primary, restarted.clone()), true)
            .await
            .unwrap()
            .status,
        Status::Joining
    );
    let rejoined = until_plan(
        &mut harness,
        &links,
        hint,
        &restarted,
        "restart plan",
        |reply| reply.status == Status::Member,
    )
    .await;
    assert_eq!((rejoined.revision, rejoined.targets.len()), (4, 0));

    // Silence beyond the membership timeout removes the incumbent and the
    // source moves without waiting for a drain. The survivor keeps heartbeating.
    let mut inherited = None;
    for _ in 0..30 {
        harness.now += Duration::from_secs(1);
        let reply = harness
            .drive(
                links.group_join(
                    hint.primary,
                    JoinRequest {
                        mode: Mode::Heartbeat,
                        ..restarted.clone()
                    },
                ),
                true,
            )
            .await
            .unwrap();
        if !reply.targets.is_empty() {
            inherited = Some(reply);
            break;
        }
    }
    let inherited = inherited.expect("timeout plan");
    assert_eq!(inherited.revision, 5);
    assert_eq!(
        inherited.targets,
        vec![Target {
            partition: partition(),
            number: 0,
            plan_revision: 5,
            activatable: true,
            prior_member: Some(second.member),
        }]
    );
    assert!(matches!(
        harness
            .drive(
                links.group_join(
                    hint.primary,
                    JoinRequest {
                        mode: Mode::Heartbeat,
                        ..second.clone()
                    }
                ),
                true
            )
            .await,
        Err(BrokerLinkError::Progress(ProgressError::Fenced))
    ));
    let reassigned = harness
        .drive(
            links.group_assign(
                hint.primary,
                group::AssignRequest {
                    authority: hint.authority,
                    partition: partition(),
                    group: inherited.group,
                    member: Some(restarted.member),
                    plan_revision: 5,
                    initial: Start::Earliest,
                    policy: Policy::LocalDurable,
                },
            ),
            true,
        )
        .await
        .unwrap();
    assert_eq!((reassigned.epoch, reassigned.cursor), (2, Offset::ZERO));
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
async fn sdk_group_handoff_waits_for_the_prior_owner_or_the_drain_deadline() {
    let (mut harness, links, _, hint) = setup_progress(2, None).await;
    let topic = harness.drive(links.topic("orders"), true).await.unwrap();
    let coordinator = topic.coordinator("workers");
    let other = topic
        .partition(1 - coordinator.number)
        .expect("two partitions");
    let coordinator_hint = ozzy_proto::nack::AuthorityHint {
        authority: ozzy_proto::append::Authority {
            group_id: coordinator.group,
            config_epoch: coordinator.config_epoch,
            view: 0,
        },
        primary: hint.primary,
    };
    let at = |member: u8, join: u8, mode: Mode, owned: Vec<Owned>| JoinRequest {
        coordinator: coordinator.incarnation,
        ..self::join(coordinator_hint, member, join, mode, owned)
    };

    // The other partition refuses groups whose name selects this coordinator.
    assert!(matches!(
        harness
            .drive(
                links.group_join(
                    hint.primary,
                    JoinRequest {
                        authority: ozzy_proto::append::Authority {
                            group_id: other.group,
                            config_epoch: other.config_epoch,
                            view: 0,
                        },
                        coordinator: other.incarnation,
                        ..at(1, 11, Mode::Join, Vec::new())
                    }
                ),
                true
            )
            .await,
        Err(BrokerLinkError::Progress(ProgressError::Fenced))
    ));

    let first = at(1, 11, Mode::Join, Vec::new());
    harness
        .drive(links.group_join(hint.primary, first.clone()), true)
        .await
        .unwrap();
    let incumbent_plan = until_plan(
        &mut harness,
        &links,
        coordinator_hint,
        &first,
        "first plan",
        |reply| reply.status == Status::Member,
    )
    .await;
    assert_eq!(incumbent_plan.targets.len(), 2);
    assert!(
        incumbent_plan
            .targets
            .iter()
            .all(|target| target.activatable)
    );
    let owned: Vec<Owned> = incumbent_plan
        .targets
        .iter()
        .map(|target| Owned {
            partition: target.partition,
            plan_revision: target.plan_revision,
            epoch: 1,
        })
        .collect();

    // A joining member receives one source, blocked while the incumbent
    // still reports owning it under the older revision.
    let second = at(2, 22, Mode::Join, Vec::new());
    harness
        .drive(links.group_join(hint.primary, second.clone()), true)
        .await
        .unwrap();
    let mut moved = None;
    for _ in 0..200 {
        harness
            .drive(
                links.group_join(
                    hint.primary,
                    JoinRequest {
                        mode: Mode::Heartbeat,
                        owned: owned.clone(),
                        ..first.clone()
                    },
                ),
                true,
            )
            .await
            .unwrap();
        let reply = harness
            .drive(
                links.group_join(
                    hint.primary,
                    JoinRequest {
                        mode: Mode::Heartbeat,
                        ..second.clone()
                    },
                ),
                true,
            )
            .await
            .unwrap();
        if reply.status == Status::Member {
            moved = Some(reply);
            break;
        }
        harness.now += Duration::from_millis(10);
    }
    let moved = moved.expect("second plan");
    assert_eq!(moved.revision, 2);
    let [target] = moved.targets.as_slice() else {
        panic!("one moved source: {:?}", moved.targets)
    };
    assert_eq!(
        (
            target.plan_revision,
            target.activatable,
            target.prior_member
        ),
        (2, false, Some(first.member))
    );
    let kept = owned
        .iter()
        .copied()
        .filter(|entry| entry.partition != target.partition)
        .collect::<Vec<_>>();

    // Releasing the source makes it activatable before any deadline.
    harness
        .drive(
            links.group_join(
                hint.primary,
                JoinRequest {
                    mode: Mode::Heartbeat,
                    owned: kept.clone(),
                    ..first.clone()
                },
            ),
            true,
        )
        .await
        .unwrap();
    let released = harness
        .drive(
            links.group_join(
                hint.primary,
                JoinRequest {
                    mode: Mode::Heartbeat,
                    ..second.clone()
                },
            ),
            true,
        )
        .await
        .unwrap();
    assert!(released.targets[0].activatable);

    // Reporting the source again does not retract activation once the drain
    // deadline has passed under this leader.
    harness.now += Duration::from_secs(6);
    harness
        .drive(
            links.group_join(
                hint.primary,
                JoinRequest {
                    mode: Mode::Heartbeat,
                    owned: owned.clone(),
                    ..first.clone()
                },
            ),
            true,
        )
        .await
        .unwrap();
    let expired = harness
        .drive(
            links.group_join(
                hint.primary,
                JoinRequest {
                    mode: Mode::Heartbeat,
                    ..second.clone()
                },
            ),
            true,
        )
        .await
        .unwrap();
    assert!(expired.targets[0].activatable);
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}
