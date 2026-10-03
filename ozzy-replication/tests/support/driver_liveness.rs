use super::*;

fn wish(view: u64) -> Control {
    Control::ExitView(Scope {
        view,
        ..configuration().scope()
    })
}

#[test]
fn one_exit_request_cannot_fence_a_healthy_primary() {
    let mut primary = driver(normal(0));
    let before = primary.begin_validation().unwrap();
    primary
        .receive(node(2), wire_control(2, wish(0)), Duration::ZERO)
        .unwrap();
    assert_eq!(primary.begin_validation().unwrap(), before);
    assert_eq!(primary.poll(Duration::ZERO).unwrap(), None);
}

#[test]
fn duplicate_requests_neither_form_quorum_nor_postpone_local_timeout() {
    let mut backup = driver(normal(1));
    for millis in 0..100 {
        let now = Duration::from_millis(millis);
        backup
            .receive(node(2), wire_control(2, wish(0)), now)
            .unwrap();
        assert_eq!(backup.poll(now).unwrap(), None);
        assert_eq!(backup.scope().view, 0);
    }
    let now = timing().primary_timeout;
    persist(&mut backup, now);
    assert_eq!(backup.scope().view, 1); // Remote exit plus local timeout, not duplicates.
    assert_eq!(start(&mut backup, now).scope().view, 1);
    assert_eq!(backup.poll(now).unwrap(), None); // EXIT_VIEW is not a durable start vote.
    assert!(matches!(
        backup.select(|_, _| None),
        Err(DriverError::ViewChange(
            ViewChangeError::ReportQuorumMissing
        ))
    ));
}

#[test]
fn exit_quorum_fences_but_does_not_fabricate_durable_start_votes() {
    for order in [[1, 2], [2, 1]] {
        let mut replica = driver(normal(0));
        let now = Duration::ZERO;
        let [first, second] = order;
        replica
            .receive(node(first), wire_control(first, wish(0)), now)
            .unwrap();
        assert_eq!(replica.scope().view, 0);
        replica
            .receive(node(second), wire_control(second, wish(0)), now)
            .unwrap();
        assert_eq!(replica.scope().view, 1);
        let Some(Action::PersistPromise(ticket)) = replica.poll(now).unwrap() else {
            panic!("majority request still needs local persistence");
        };
        assert_eq!(ticket.scope().view, 1);
        assert_eq!(replica.poll(now).unwrap(), None);
        replica.complete_promise(ticket).unwrap();
        let own_start = start(&mut replica, now);
        assert_eq!(own_start.scope().view, 1);
        assert_eq!(replica.poll(now).unwrap(), None);
        replica
            .receive(node(1), wire_control(1, own_start), now)
            .unwrap();
        let Some(Action::Send {
            to,
            message: Control::DoViewChange(report),
        }) = replica.poll(now).unwrap()
        else {
            panic!("actual durable start plus local promise forms start quorum");
        };
        assert_eq!(to, node(1));
        assert_eq!(report.scope.view, 1);
    }
}

#[test]
fn future_exit_requests_do_not_supply_authority_or_votes_for_current_view() {
    let mut replica = driver(normal(0));
    for view in [5, 2, 5] {
        replica
            .receive(node(1), wire_control(1, wish(view)), Duration::ZERO)
            .unwrap();
        assert_eq!(replica.scope().view, 0);
    }
    replica
        .receive(node(2), wire_control(2, wish(0)), Duration::ZERO)
        .unwrap();
    assert_eq!(replica.scope().view, 0);
    replica
        .receive(node(1), wire_control(1, wish(0)), Duration::ZERO)
        .unwrap();
    assert_eq!(replica.scope().view, 1);
}

#[test]
fn a_voter_past_a_view_supports_a_lagging_peer_leaving_it() {
    let now = timing().primary_timeout;
    let mut lagging = driver(normal(1));
    let mut ahead = driver(normal(2));
    // The lagging backup times out first and asks to leave view 0.
    assert_eq!(lagging.poll(now).unwrap(), Some(Action::Broadcast(wish(0))));
    // The other backup's own timeout expires as that request arrives: it
    // leaves view 0 before its own request goes out.
    ahead
        .receive(node(1), wire_control(1, wish(0)), now)
        .unwrap();
    let Some(Action::PersistPromise(_)) = ahead.poll(now).unwrap() else {
        panic!("exit quorum starts the durable promise");
    };
    assert_eq!(ahead.scope().view, 1);
    // Its promise is still pending, so no START_VIEW_CHANGE can go out. A
    // retransmitted old request is answered for the old view.
    ahead
        .receive(node(1), wire_control(1, wish(0)), now)
        .unwrap();
    assert_eq!(
        ahead.poll(now).unwrap(),
        Some(Action::Send {
            to: node(1),
            message: wish(0)
        })
    );
    assert_eq!(ahead.poll(now).unwrap(), None);
    lagging
        .receive(node(2), wire_control(2, wish(0)), now)
        .unwrap();
    assert_eq!(lagging.scope().view, 1);
}

#[test]
fn voters_past_a_view_stop_answering_each_other() {
    let now = timing().primary_timeout;
    // All three leave view 0. Their promises stay pending.
    let mut voters: Vec<_> = (0..3).map(|index| driver(normal(index))).collect();
    for (index, voter) in voters.iter_mut().enumerate() {
        for from in (0..3).filter(|from| usize::from(*from) != index) {
            voter
                .receive(node(from), wire_control(from, wish(0)), now)
                .unwrap();
        }
        assert_eq!(voter.scope().view, 1);
    }
    // One late request to leave view 0 arrives.
    let mut flying = vec![(0_u8, 1_usize)];
    let mut answers = 0;
    while let Some((from, to)) = flying.pop() {
        voters[to]
            .receive(node(from), wire_control(from, wish(0)), now)
            .unwrap();
        for _ in 0..4 {
            let targets: Vec<usize> = match voters[to].poll(now).unwrap() {
                Some(Action::Broadcast(Control::ExitView(scope))) if scope.view == 0 => {
                    (0..3).filter(|other| *other != to).collect()
                }
                Some(Action::Send {
                    to: peer,
                    message: Control::ExitView(scope),
                }) if scope.view == 0 => {
                    (0..3).filter(|other| node(*other as u8) == peer).collect()
                }
                Some(_) => continue,
                None => break,
            };
            for target in targets {
                answers += 1;
                flying.push((to as u8, target));
            }
        }
        assert!(answers <= 8, "answers for a view everyone left never stop");
    }
}

#[test]
fn a_durable_promise_answers_an_old_request_with_its_own_start() {
    let now = timing().primary_timeout;
    let mut ahead = driver(normal(2));
    peer_exit(&mut ahead, 1, now);
    persist(&mut ahead, now);
    assert_eq!(ahead.scope().view, 1);
    assert_eq!(start(&mut ahead, now).scope().view, 1);
    ahead
        .receive(node(1), wire_control(1, wish(0)), now)
        .unwrap();
    assert_eq!(ahead.poll(now).unwrap(), None);
}

#[test]
fn invalid_identity_or_configuration_cannot_poison_request_quorum() {
    let mut replica = driver(normal(0));
    for from in [node(0), node(99)] {
        assert!(replica.receive(from, wish(9), Duration::ZERO).is_err());
    }
    let invalid = Control::ExitView(Scope {
        view: 9,
        configuration_digest: Digest::ZERO,
        ..configuration().scope()
    });
    assert!(replica.receive(node(1), invalid, Duration::ZERO).is_err());
    replica
        .receive(node(2), wire_control(2, wish(0)), Duration::ZERO)
        .unwrap();
    assert_eq!(replica.scope().view, 0);
    assert!(replica.begin_validation().is_ok());
}

fn rejoining_fixture() -> ([ReplicaDriver; 3], [Prefix; 3], Prefix) {
    let recovering = ViewChange::recover_intact(
        configuration(),
        node(2),
        JournalGeneration(32),
        RecoveredState {
            scope: configuration().scope(),
            log: FrozenLog {
                last_normal_view: 0,
                accepted: Prefix::GENESIS,
                committed: Prefix::GENESIS,
            },
        },
        PipelineLimits {
            max_operations: 8,
            max_body_bytes: 8192,
        },
    )
    .unwrap();
    let mut primary = normal(0);
    let mut backup = normal(1);
    let acknowledged = operation().prefix();
    for core in [&mut primary, &mut backup] {
        let Admission::Write { ticket, .. } = core
            .prepare(node(0), configuration().scope(), &[operation()])
            .unwrap()
        else {
            panic!("new operation");
        };
        core.complete_durable_write(ticket).unwrap();
    }
    primary
        .receive_ack(node(1), backup.acknowledgment().unwrap())
        .unwrap();
    primary.apply_through(acknowledged).unwrap(); // Client success; no COMMIT sent yet.
    let stable = [acknowledged, acknowledged, Prefix::GENESIS];
    let replicas = [
        driver(primary),
        driver(backup),
        ReplicaDriver::from_view_change(recovering, Duration::ZERO, timing()).unwrap(),
    ];
    (replicas, stable, acknowledged)
}

#[test]
fn healthy_quorum_eventually_admits_an_intact_rejoining_voter() {
    let (mut replicas, mut stable, acknowledged) = rejoining_fixture();
    let now = admit_rejoin(&mut replicas, &mut stable, acknowledged);
    commit_after_rejoin(&mut replicas, &mut stable, acknowledged, now);
}

/// Drive timers and healthy links until every voter is `Normal` in view 1 at
/// the acknowledged prefix. Returns the convergence time.
fn admit_rejoin(
    replicas: &mut [ReplicaDriver; 3],
    stable: &mut [Prefix; 3],
    acknowledged: Prefix,
) -> Duration {
    let mut network = VecDeque::with_capacity(48);
    let mut starts = [None; 3];
    for millis in (0..5000).step_by(10) {
        let now = Duration::from_millis(millis);
        for (from, replica) in replicas.iter_mut().enumerate() {
            for _ in 0..4 {
                match replica.poll(now).unwrap() {
                    Some(Action::PersistPromise(ticket)) => {
                        assert_eq!(ticket.log().accepted, stable[from]);
                        replica.complete_promise(ticket).unwrap(); // Simulated stable metadata.
                    }
                    Some(Action::Broadcast(message)) => {
                        for to in 0..3 {
                            if to != from {
                                network.push_back((from, to, message));
                            }
                        }
                    }
                    Some(Action::Send { to, message }) => {
                        let to = configuration()
                            .voters()
                            .iter()
                            .position(|peer| *peer == to)
                            .unwrap();
                        network.push_back((from, to, message));
                    }
                    None => break,
                }
            }
            complete_modeled_installation(
                replica,
                from,
                starts[from].take(),
                &mut stable[from],
                acknowledged,
                now,
            );
            if let Some(normal) = replica.normal()
                && normal.snapshot().scope.view == 1
                && from != 1
            {
                network.push_back((
                    from,
                    1,
                    Control::PrepareOk {
                        ack: normal.acknowledgment().unwrap(),
                        grant: grant(),
                    },
                ));
            }
            if let Ok(activation) = replica.begin_activation() {
                replica.complete_activation(activation).unwrap();
            }
        }
        assert!(network.len() <= 48);
        for _ in 0..48 {
            let Some((from, to, message)) = network.pop_front() else {
                break;
            };
            if let Some(start) = replicas[to]
                .receive(node(from as u8), wire_control(from as u8, message), now)
                .unwrap()
            {
                starts[to] = Some((node(from as u8), start));
            }
        }
        if replicas.iter().all(|replica| {
            replica.normal().is_some_and(|normal| {
                let snapshot = normal.snapshot();
                snapshot.scope.view == 1
                    && snapshot.ready_for_appends
                    && snapshot.committed == acknowledged
            })
        }) {
            return now;
        }
    }
    panic!(
        "all links/storage healthy, but rejoin cannot activate: {:?}",
        replicas.each_ref().map(ReplicaDriver::scope)
    );
}

fn grant() -> Grant {
    Grant {
        revision: 1,
        record_limit: 8,
        byte_limit: 8192,
    }
}

fn complete_modeled_installation(
    replica: &mut ReplicaDriver,
    index: usize,
    start: Option<(NodeId, ozzy_replication::StartView)>,
    stable: &mut Prefix,
    acknowledged: Prefix,
    now: Duration,
) {
    let generation = JournalGeneration(40 + index as u128);
    let ticket = if let Some((sender, start)) = start {
        replica
            .begin_backup_install(sender, start, generation, |_, _| None)
            .unwrap()
    } else if replica.select(|_, _| None).is_ok() {
        replica.begin_primary_install(generation).unwrap()
    } else {
        return;
    };
    assert_eq!(ticket.accepted(), acknowledged);
    if ticket.committed() != acknowledged {
        replica.validate_install_suffix(&[operation()]).unwrap();
    }
    *stable = acknowledged; // Modeled verified transfer plus stable publication, not real filesystem I/O.
    replica
        .complete_installation(ticket, ticket.committed(), now)
        .unwrap();
}

fn commit_after_rejoin(
    replicas: &mut [ReplicaDriver; 3],
    stable: &mut [Prefix; 3],
    previous: Prefix,
    now: Duration,
) {
    let body = encode_operation_body(
        &OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes([20; 16]),
        }),
        OperationLimits::default(),
    )
    .unwrap();
    let scope = replicas[1].scope();
    let operation = PreparedOperation::from_verified(
        &CanonicalOperation {
            group_id: scope.group_id,
            configuration_epoch: scope.configuration_epoch,
            original_view: scope.view,
            op_number: previous.op.0 + 1,
            previous_digest: previous.digest,
            kind: ozzy_journal::operation::OperationKind::Barrier,
            body: &body,
        },
        canonical_body_digest(&body),
    );
    for (index, replica) in replicas.iter_mut().enumerate() {
        let Admission::Write { ticket, .. } =
            replica.prepare(node(1), scope, &[operation], now).unwrap()
        else {
            panic!("fresh operation after rejoin");
        };
        replica.complete_write(ticket).unwrap();
        let sync = replica.begin_sync().unwrap();
        stable[index] = operation.prefix();
        replica.complete_sync(sync, now).unwrap();
    }
    // Specifically require the rejoined voter's durable ACK, not just the old pair.
    let ack = replicas[2].normal().unwrap().acknowledgment().unwrap();
    replicas[1]
        .receive(
            node(2),
            wire_control(
                2,
                Control::PrepareOk {
                    ack,
                    grant: grant(),
                },
            ),
            now,
        )
        .unwrap();
    let commit = replicas[1].normal().unwrap().announcement().unwrap();
    assert_eq!(commit.committed, operation.prefix());
    assert_eq!(stable[1], stable[2]);
    for (index, replica) in replicas.iter_mut().enumerate() {
        if index != 1 {
            replica
                .receive(node(1), wire_control(1, Control::Commit(commit)), now)
                .unwrap();
        }
        replica.apply_through(commit.committed).unwrap();
    }
}

#[test]
fn healthy_contact_withdraws_local_suspicion_before_a_late_peer_exit() {
    let mut backup = driver(normal(1));
    let now = timing().primary_timeout;
    assert_eq!(backup.poll(now).unwrap(), Some(Action::Broadcast(wish(0))));
    backup
        .receive(
            node(0),
            wire_control(0, Control::Commit(normal(0).announcement().unwrap())),
            now,
        )
        .unwrap();
    peer_exit(&mut backup, 2, now);
    assert_eq!(backup.scope().view, 0);
    assert_eq!(backup.poll(now).unwrap(), None);
}

#[test]
fn unanswered_election_timeouts_cannot_advance_the_promised_view() {
    let mut replica = driver(normal(1));
    let now = timing().primary_timeout;
    peer_exit(&mut replica, 2, now);
    persist(&mut replica, now);
    start(&mut replica, now);
    for millis in (300..5000).step_by(10) {
        let now = Duration::from_millis(millis);
        for _ in 0..4 {
            match replica.poll(now).unwrap() {
                Some(Action::Broadcast(Control::ExitView(scope))) => assert_eq!(scope.view, 1),
                Some(Action::Broadcast(Control::StartViewChange(start))) => {
                    assert_eq!(start.scope.view, 1);
                }
                None => break,
                other => panic!("a minority must not manufacture another promise: {other:?}"),
            }
        }
        assert_eq!(replica.scope().view, 1);
    }
}

/// A replica already installed in view 1 treats every view-0 command as inert:
/// no state change, no election, and no admission of view-0 operations.
#[test]
fn older_view_traffic_at_an_installed_newer_view_replica_is_inert() {
    let (mut replicas, mut stable, acknowledged) = rejoining_fixture();
    let now = admit_rejoin(&mut replicas, &mut stable, acknowledged);
    let old_scope = configuration().scope();
    let ack = normal(2).acknowledgment().unwrap();
    for (index, replica) in replicas.iter_mut().enumerate() {
        let from = if index == 0 { 2 } else { 0 };
        let before = replica.normal().unwrap().snapshot();
        assert_eq!(before.scope.view, 1);
        let controls = [
            Control::Commit(ozzy_replication::Commit {
                scope: old_scope,
                committed: acknowledged,
            }),
            Control::StartViewChange(StartViewChange { scope: old_scope }),
            Control::PrepareOk {
                ack,
                grant: grant(),
            },
        ];
        for control in controls {
            // A view-zero election start has no wire form; deliver it directly.
            let delivered = if matches!(control, Control::StartViewChange(_)) {
                control
            } else {
                wire_control(from, control)
            };
            assert_eq!(
                replica.receive(node(from), delivered, now).unwrap(),
                None,
                "voter {index}: {control:?}"
            );
            assert_eq!(
                replica.normal().unwrap().snapshot(),
                before,
                "voter {index}"
            );
        }
        assert!(
            replica
                .prepare(node(from), old_scope, &[operation()], now)
                .is_err(),
            "voter {index}"
        );
        assert_eq!(
            replica.normal().unwrap().snapshot(),
            before,
            "voter {index}"
        );
        assert_eq!(replica.scope().view, 1);
    }
}
