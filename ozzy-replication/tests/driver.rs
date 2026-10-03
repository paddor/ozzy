use std::collections::VecDeque;
use std::time::Duration;

use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
    encode_operation_body,
};
use ozzy_proto::{GroupId, LinkSessionId, NodeId, OperationId};
use ozzy_replication::driver::{Action, DriverError, ReplicaDriver, Timing};
use ozzy_replication::wire::{
    Control, PeerBinding, ReplicaMessage, WireLimits, decode, encode_control,
};
use ozzy_replication::{
    Admission, Configuration, Digest, FrozenLog, JournalGeneration, NormalReplica, PipelineLimits,
    Prefix, PreparedOperation, RecoveredState, ReplicationError, Scope, StartViewChange,
    ViewChange, ViewChangeError,
};

#[path = "support/driver_installation.rs"]
mod installation;

#[path = "support/driver_validation.rs"]
mod validation;

#[path = "support/driver_liveness.rs"]
mod liveness;

fn wire_control(from: u8, control: Control) -> Control {
    let session = LinkSessionId::from_bytes([11; 16]);
    let mut metadata = [0; 184];
    let encoded = encode_control(node(from), session, control, &mut metadata).unwrap();
    let ReplicaMessage::Control(message) = decode(
        &[&encoded.header, &metadata[..encoded.metadata_bytes], &[]],
        PeerBinding::new(configuration(), node(from), session).unwrap(),
        WireLimits::default(),
    )
    .unwrap() else {
        panic!("control")
    };
    message
}

#[test]
fn partitioned_minority_cannot_select_but_healed_quorum_resumes_after_lost_votes() {
    let mut backup = normal(1);
    let Admission::Write { ticket, .. } = backup
        .prepare(node(0), configuration().scope(), &[operation()])
        .unwrap()
    else {
        panic!("new operation")
    };
    backup.complete_durable_write(ticket).unwrap();
    let mut replicas = [driver(backup), driver(normal(2))];
    let mut disk = [None, None];
    let mut network = VecDeque::with_capacity(32);
    let mut delayed = None;
    for millis in (0..2000).step_by(10) {
        let now = Duration::from_millis(millis);
        for (index, replica) in replicas.iter_mut().enumerate() {
            if let Some((due, ticket)) = disk[index]
                && millis >= due
            {
                disk[index] = None;
                replica.complete_promise(ticket).unwrap();
            }
            // At most four controls per voter, each <= 184 metadata + 64 envelope bytes.
            for _ in 0..4 {
                let action = match replica.poll(now).unwrap() {
                    Some(Action::PersistPromise(ticket)) => {
                        assert!(disk[index].is_none());
                        disk[index] = Some((millis + 20 + index as u64 * 10, ticket));
                        continue;
                    }
                    Some(Action::Broadcast(message)) => message,
                    Some(Action::Send { to, message }) if to == node(2 - index as u8) => message,
                    Some(Action::Send { .. }) => continue, // Crashed voter zero.
                    None => break,
                };
                if delayed.is_none() {
                    delayed = Some((index, action));
                }
                if millis >= 600 {
                    // Heal the link, never lower the configured quorum.
                    network.push_back((index, action));
                    network.push_back((index, action)); // Duplicated packets.
                }
                assert!(network.len() <= 32);
            }
        }
        if millis == 600 {
            network.push_back(delayed.take().unwrap()); // Delayed old-view packet.
        }
        // Eight controls also bound decoded bytes to 1,984 per tick.
        for _ in 0..8 {
            let Some((from, message)) = network.pop_front() else {
                break;
            };
            let sender = from as u8 + 1;
            replicas[1 - from]
                .receive(node(sender), wire_control(sender, message), now)
                .unwrap();
        }
        for replica in &mut replicas {
            match replica.select(|_, _| None) {
                Ok(selected) => {
                    assert!(millis >= 600);
                    assert_eq!(selected.source().accepted, operation().prefix());
                    assert_eq!(selected.voter_mask(), 0b110);
                    assert_eq!(selected.committed(), Prefix::GENESIS);
                    assert_eq!(selected.scope().view, 1); // No unilateral view ratchet while partitioned.
                    return;
                }
                Err(
                    DriverError::Replication(ReplicationError::WrongRole)
                    | DriverError::ViewChange(
                        ViewChangeError::PromiseRequired
                        | ViewChangeError::ReportQuorumMissing
                        | ViewChangeError::Replication(ReplicationError::WrongRole),
                    ),
                ) => {}
                Err(error) => panic!("unexpected selection failure: {error}"),
            }
        }
    }
    panic!("healthy quorum failed to select acknowledged history");
}

fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}

fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [node(0), node(1), node(2)],
    )
    .unwrap()
}

fn normal(index: u8) -> NormalReplica {
    NormalReplica::bootstrap(
        configuration(),
        node(index),
        JournalGeneration(u128::from(index) + 1),
        PipelineLimits {
            max_operations: 8,
            max_body_bytes: 1024,
        },
    )
    .unwrap()
}

fn operation() -> PreparedOperation {
    let body = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([10; 16]),
    });
    let bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
    PreparedOperation::from_verified(
        &CanonicalOperation {
            group_id: configuration().scope().group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind: body.kind(),
            body: &bytes,
        },
        canonical_body_digest(&bytes),
    )
}

fn timing() -> Timing {
    Timing {
        heartbeat: Duration::from_millis(10),
        primary_timeout: Duration::from_millis(100),
        retransmit: Duration::from_millis(10),
        election_timeout: Duration::from_millis(200),
        max_election_timeout: Duration::from_secs(2),
    }
}

fn driver(normal: NormalReplica) -> ReplicaDriver {
    ReplicaDriver::from_normal(normal, Duration::ZERO, timing()).unwrap()
}

#[test]
fn disk_failure_is_reported_even_between_election_retransmission_deadlines() {
    let mut replica = driver(normal(1));
    let now = timing().primary_timeout;
    peer_exit(&mut replica, 2, now);
    persist(&mut replica, now);
    let message = start(&mut replica, now);
    replica.receive(node(2), message, now).unwrap();
    assert_eq!(replica.poll(now).unwrap(), None); // Local report retained, nothing left to send.
    replica.fail_io(JournalGeneration(2)).unwrap();
    assert_eq!(
        replica.poll(now),
        Err(DriverError::ViewChange(ViewChangeError::Faulted))
    );
}

// Explicit peer timeout suspicion. It grants no durable start/report vote.
fn peer_exit(driver: &mut ReplicaDriver, from: u8, now: Duration) {
    driver
        .receive(
            node(from),
            wire_control(from, Control::ExitView(driver.scope())),
            now,
        )
        .unwrap();
}

fn persist(driver: &mut ReplicaDriver, now: Duration) {
    let Some(Action::PersistPromise(ticket)) = driver.poll(now).unwrap() else {
        panic!("durable promise must precede election messages");
    };
    assert_eq!(driver.poll(now).unwrap(), None);
    driver.complete_promise(ticket).unwrap(); // Simulated durable metadata completion.
}

fn start(driver: &mut ReplicaDriver, now: Duration) -> Control {
    let Some(Action::Broadcast(message @ Control::StartViewChange(_))) = driver.poll(now).unwrap()
    else {
        panic!("expected first election phase");
    };
    message
}

#[test]
fn primary_loss_after_quorum_ack_automatically_starts_durable_election() {
    let operation = operation();
    let mut primary = normal(0);
    let mut backup = normal(1);
    for replica in [&mut primary, &mut backup] {
        let Admission::Write { ticket, .. } = replica
            .prepare(node(0), configuration().scope(), &[operation])
            .unwrap()
        else {
            panic!("new operation");
        };
        replica.complete_durable_write(ticket).unwrap();
    }
    primary
        .receive_ack(node(1), backup.acknowledgment().unwrap())
        .unwrap();
    assert_eq!(primary.snapshot().committed, operation.prefix());
    drop(primary); // Producer success, then crash before COMMIT announcement.
    assert_eq!(backup.snapshot().committed, Prefix::GENESIS);
    let mut candidate = driver(backup);
    let mut other = driver(normal(2));
    let now = timing().primary_timeout;
    assert_eq!(
        candidate
            .poll(now.checked_sub(Duration::from_nanos(1)).unwrap())
            .unwrap(),
        None
    );
    peer_exit(&mut candidate, 2, now);
    peer_exit(&mut other, 1, now);
    persist(&mut candidate, now);
    persist(&mut other, now);
    let candidate_start = start(&mut candidate, now);
    let other_start = start(&mut other, now);
    candidate.receive(node(2), other_start, now).unwrap();
    other.receive(node(1), candidate_start, now).unwrap();
    assert_eq!(candidate.poll(now).unwrap(), None); // Candidate retains own report.
    let Some(Action::Send { to, message }) = other.poll(now).unwrap() else {
        panic!("backup sends second-phase report");
    };
    assert_eq!(to, node(1));
    candidate.receive(node(2), message, now).unwrap();
    candidate.receive(node(2), message, now).unwrap(); // Duplicate is not another voter.
    let selected = candidate.select(|_, _| None).unwrap();
    assert_eq!(selected.scope().view, 1);
    assert_eq!(selected.source().accepted, operation.prefix());
    assert_eq!(selected.committed(), Prefix::GENESIS);
    assert_eq!(selected.voter_mask(), 0b110);
    assert!(candidate.normal().is_none()); // Selection is not installed leadership.
}

#[test]
fn delayed_writes_and_promises_never_emit_stale_or_unpersisted_election_votes() {
    let mut old = normal(0);
    let Admission::Write { ticket, .. } = old
        .prepare(node(0), configuration().scope(), &[operation()])
        .unwrap()
    else {
        panic!("new operation");
    };
    let mut replica = driver(old);
    let now = timing().primary_timeout;
    peer_exit(&mut replica, 1, now);
    assert_eq!(replica.poll(now).unwrap(), None); // Accepted old write still outstanding.
    assert!(replica.normal().is_none());
    replica.complete_write(ticket).unwrap();
    assert_eq!(replica.poll(now).unwrap(), None); // Written is not synchronized.
    let sync = replica.begin_sync().unwrap();
    replica.complete_sync(sync, now).unwrap();
    let Some(Action::PersistPromise(first)) = replica.poll(now).unwrap() else {
        panic!("old tail settled");
    };
    assert_eq!(first.scope().view, 1);
    assert_eq!(first.log().accepted, operation().prefix());
    let later = Control::StartViewChange(StartViewChange {
        scope: Scope {
            view: 4,
            ..configuration().scope()
        },
    });
    replica.receive(node(1), later, now).unwrap();
    assert_eq!(replica.scope().view, 4); // Durable first phase, not timeout-only suspicion.
    replica.receive(node(2), later, now).unwrap();
    assert_eq!(replica.poll(now).unwrap(), None); // Keep first metadata action alive.
    replica.complete_promise(first).unwrap();
    let Some(Action::PersistPromise(second)) = replica.poll(now).unwrap() else {
        panic!("later view needs its own durable promise");
    };
    assert_eq!(second.scope().view, 4);
    assert_eq!(second.log().accepted, first.log().accepted);
    assert_eq!(second.log().committed, Prefix::GENESIS);
    assert_eq!(replica.poll(now).unwrap(), None);
    replica.complete_promise(second).unwrap();
    assert_eq!(start(&mut replica, now).scope().view, 4);
}

#[test]
fn idle_heartbeats_do_not_hide_stalled_outstanding_operations() {
    let mut primary = driver(normal(0));
    let mut backup = driver(normal(1));
    let now = Duration::from_millis(10);
    let Some(Action::Broadcast(heartbeat @ Control::Commit(_))) = primary.poll(now).unwrap() else {
        panic!("idle primary heartbeat");
    };
    backup.receive(node(0), heartbeat, now).unwrap();
    assert_eq!(backup.poll(now).unwrap(), None);
    let admitted_at = Duration::from_millis(20);
    backup
        .prepare(
            node(0),
            configuration().scope(),
            &[operation()],
            admitted_at,
        )
        .unwrap();
    for millis in [40, 80, 119] {
        let now = Duration::from_millis(millis);
        backup.receive(node(0), heartbeat, now).unwrap();
        assert_eq!(backup.poll(now).unwrap(), None);
        assert!(backup.normal().is_some());
    }
    let now = Duration::from_millis(120);
    assert_eq!(
        backup.poll(now).unwrap(),
        Some(Action::Broadcast(Control::ExitView(
            configuration().scope()
        )))
    );
    assert!(backup.normal().is_some()); // Suspicion alone cannot initiate a new view.
    peer_exit(&mut backup, 2, now);
    assert!(backup.normal().is_none()); // Still waiting for old disk work, no vote yet.
    assert_eq!(backup.scope().view, 1);
}

#[test]
fn dropped_election_packets_retry_at_bounded_intervals_then_advance_with_backoff() {
    let mut replica = driver(normal(1));
    let now = timing().primary_timeout;
    peer_exit(&mut replica, 2, now);
    persist(&mut replica, now);
    let first = start(&mut replica, now);
    for _ in 0..32 {
        assert_eq!(replica.poll(now).unwrap(), None);
    }
    assert_eq!(replica.poll(Duration::from_millis(109)).unwrap(), None);
    assert_eq!(start(&mut replica, Duration::from_millis(110)), first);
    assert_eq!(
        replica.poll(Duration::from_millis(299)).unwrap(),
        Some(Action::Broadcast(first))
    );
    let now = Duration::from_millis(300);
    assert!(matches!(
        replica.poll(now).unwrap(),
        Some(Action::Broadcast(Control::ExitView(_)))
    ));
    assert_eq!(replica.scope().view, 1);
    peer_exit(&mut replica, 2, now);
    let Some(Action::PersistPromise(second)) = replica.poll(now).unwrap() else {
        panic!("first election timed out");
    };
    assert_eq!(second.scope().view, 2);
    replica.complete_promise(second).unwrap();
    start(&mut replica, Duration::from_millis(300));
    // Delayed phase-one traffic cannot refresh the new election deadline.
    replica
        .receive(node(2), first, Duration::from_millis(699))
        .unwrap();
    assert_eq!(replica.scope().view, 2);
    let now = Duration::from_millis(700);
    assert!(matches!(
        replica.poll(now).unwrap(),
        Some(Action::Broadcast(Control::ExitView(_)))
    ));
    peer_exit(&mut replica, 2, now);
    let Some(Action::PersistPromise(third)) = replica.poll(now).unwrap() else {
        panic!("second election gets twice the interval, not an unlimited wait");
    };
    assert_eq!(third.scope().view, 3);
    assert!(replica.normal().is_none());
}

#[test]
fn invalid_peers_scopes_and_time_cannot_postpone_primary_failure() {
    let mut backup = driver(normal(1));
    let commit = Control::Commit(normal(0).announcement().unwrap());
    let now = Duration::from_millis(99);
    assert!(backup.receive(node(3), commit, now).is_err());
    assert!(backup.receive(node(2), commit, now).is_err());
    let Control::Commit(mut foreign) = commit else {
        unreachable!()
    };
    foreign.scope.configuration_digest = Digest::from_bytes([99; 32]);
    assert!(
        backup
            .receive(node(0), Control::Commit(foreign), now)
            .is_err()
    );
    assert_eq!(
        backup.poll(Duration::ZERO),
        Err(DriverError::ClockRegressed)
    );
    peer_exit(&mut backup, 2, timing().primary_timeout);
    persist(&mut backup, timing().primary_timeout);
    assert_eq!(backup.scope().view, 1);
    assert!(ReplicaDriver::from_normal(normal(0), Duration::MAX, timing()).is_err());
    assert!(
        ReplicaDriver::from_normal(
            normal(0),
            Duration::ZERO,
            Timing {
                heartbeat: Duration::ZERO,
                ..timing()
            }
        )
        .is_err()
    );
}

#[test]
fn intact_restart_drives_a_higher_promise_and_failed_publication_stops_votes() {
    let changing = ViewChange::recover_intact(
        configuration(),
        node(1),
        JournalGeneration(101),
        RecoveredState {
            scope: Scope {
                view: 4,
                ..configuration().scope()
            },
            log: FrozenLog {
                last_normal_view: 4,
                accepted: operation().prefix(),
                committed: Prefix::GENESIS,
            },
        },
        PipelineLimits {
            max_operations: 8,
            max_body_bytes: 1024,
        },
    )
    .unwrap();
    let mut replica = ReplicaDriver::from_view_change(changing, Duration::ZERO, timing()).unwrap();
    assert!(replica.normal().is_none());
    let Some(Action::PersistPromise(ticket)) = replica.poll(Duration::ZERO).unwrap() else {
        panic!("restart immediately schedules a higher durable promise");
    };
    assert_eq!(ticket.scope().view, 5);
    assert_eq!(ticket.log().accepted, operation().prefix());
    replica.fail_promise(ticket).unwrap();
    assert!(replica.complete_promise(ticket).is_err());
    assert!(replica.poll(Duration::ZERO).is_err());
    assert!(replica.poll(Duration::from_secs(1)).is_err());
}

#[test]
fn normal_driver_releases_committed_work_but_io_failure_fences_future_actions() {
    let mut primary = driver(normal(0));
    let mut backup = normal(1);
    let now = Duration::from_secs(1);
    assert!(matches!(
        primary.poll(now).unwrap(),
        Some(Action::Broadcast(Control::Commit(_)))
    ));
    let Admission::Write { ticket, .. } = primary
        .prepare(node(0), configuration().scope(), &[operation()], now)
        .unwrap()
    else {
        panic!("new operation")
    };
    primary.complete_write(ticket).unwrap();
    let sync = primary.begin_sync().unwrap();
    primary.complete_sync(sync, now).unwrap();
    let Admission::Write { ticket, .. } = backup
        .prepare(node(0), configuration().scope(), &[operation()])
        .unwrap()
    else {
        panic!("new operation")
    };
    backup.complete_durable_write(ticket).unwrap();
    primary
        .receive(
            node(1),
            Control::PrepareOk {
                ack: backup.acknowledgment().unwrap(),
            },
            now,
        )
        .unwrap();
    primary.apply_through(operation().prefix()).unwrap();
    assert_eq!(primary.normal().unwrap().snapshot().pending_operations, 0);
    assert!(matches!(
        primary.poll(Duration::from_secs(2)).unwrap(),
        Some(Action::Broadcast(Control::Commit(_)))
    ));
    assert!(primary.fail_io(JournalGeneration(999)).is_err());
    assert!(primary.normal().unwrap().acknowledgment().is_err()); // Primary cannot vote as backup.
    primary.fail_io(JournalGeneration(1)).unwrap();
    assert!(primary.poll(Duration::from_secs(2)).is_err());
    assert!(
        primary
            .receive(
                node(1),
                Control::StartViewChange(StartViewChange {
                    scope: Scope {
                        view: 1,
                        ..configuration().scope()
                    },
                }),
                Duration::from_secs(2)
            )
            .is_err()
    );
}
