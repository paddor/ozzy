//! A primary that stopped cleanly resumes its view after a restart.

use super::*;
use ozzy_replication::driver::CleanStop;
use ozzy_replication::{PrepareOk, StartView};

/// The disk of a copy that last installed view 4, which node 1 leads, with
/// one accepted, uncommitted operation.
fn recovered() -> RecoveredState {
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
    }
}

fn recover(local: u8, recovered: RecoveredState) -> ViewChange {
    ViewChange::recover_intact(
        configuration(),
        node(local),
        JournalGeneration(101),
        recovered,
        PipelineLimits {
            max_operations: 8,
            max_body_bytes: 1024,
        },
    )
    .unwrap()
}

fn restarted(changing: ViewChange) -> ReplicaDriver {
    ReplicaDriver::from_view_change(changing, Duration::ZERO, timing()).unwrap()
}

/// With evidence of a clean stop, the restarted primary installs its own
/// log in the view it led, announces it under its new writer incarnation,
/// and commits again once a backup acknowledges the whole log.
#[test]
fn a_primary_that_stopped_cleanly_resumes_its_view() {
    let now = Duration::from_millis(5);
    let mut changing = recover(1, recovered());
    assert!(changing.resume_primary(JournalGeneration(101), recovered()));
    let mut primary = restarted(changing);
    assert!(primary.rejoin_pending());
    assert_eq!(primary.poll(now).unwrap(), None, "no promise of view 5");
    let ticket = primary
        .begin_rejoin_install(JournalGeneration(102))
        .unwrap();
    assert_eq!(ticket.scope().view, 4);
    primary.validate_install_suffix(&[operation()]).unwrap();
    primary
        .complete_installation(ticket, Prefix::GENESIS, now)
        .unwrap();
    let normal = primary.normal().unwrap();
    let scope = normal.scope();
    assert_eq!(scope.view, 4);
    assert!(!normal.snapshot().ready_for_appends);
    // A backup may hold the end of this log unapplied and need it again.
    assert_eq!(normal.replay_floor().unwrap(), Prefix::GENESIS);
    let announced = StartView {
        scope,
        generation: JournalGeneration(102),
        accepted: operation().prefix(),
        committed: Prefix::GENESIS,
    };
    assert_eq!(
        primary.poll(now).unwrap(),
        Some(Action::Broadcast(Control::StartView(announced)))
    );
    let ack = PrepareOk {
        scope,
        durable: operation().prefix(),
    };
    primary
        .receive(node(2), Control::PrepareOk { ack }, now)
        .unwrap();
    assert_eq!(
        primary.normal().unwrap().snapshot().committed,
        operation().prefix()
    );
    assert_eq!(
        primary.clean_stop(),
        Some(CleanStop {
            view: 4,
            accepted: operation().prefix(),
        })
    );
}

/// Without the evidence the primary leaves its view. A backup, and a copy
/// that promised a later view, get no right from it.
#[test]
fn only_the_primary_of_the_installed_view_may_resume_it() {
    let mut primary = restarted(recover(1, recovered()));
    assert!(!primary.rejoin_pending());
    let Some(Action::PersistPromise(promise)) = primary.poll(Duration::ZERO).unwrap() else {
        panic!("the primary promises the next view");
    };
    assert_eq!(promise.scope().view, 5);

    let mut backup = recover(2, recovered());
    assert!(backup.rejoinable());
    assert!(!backup.resume_primary(JournalGeneration(101), recovered()));

    // View 7 is node 1's too, but it never installed it.
    let promised = RecoveredState {
        scope: Scope {
            view: 7,
            ..configuration().scope()
        },
        ..recovered()
    };
    let mut changing = recover(1, promised);
    assert!(!changing.resume_primary(JournalGeneration(101), promised));
    assert!(!changing.rejoinable());
}

/// Only a normal primary whose whole log is durable leaves evidence.
#[test]
fn clean_stop_evidence_needs_a_primary_with_a_durable_log() {
    let now = Duration::from_secs(1);
    assert_eq!(driver(normal(1)).clean_stop(), None, "a backup");
    let mut primary = driver(normal(0));
    let empty = CleanStop {
        view: 0,
        accepted: Prefix::GENESIS,
    };
    assert_eq!(primary.clean_stop(), Some(empty));
    let Admission::Write { ticket, .. } = primary
        .prepare(node(0), configuration().scope(), &[operation()], now)
        .unwrap()
    else {
        panic!("new operation")
    };
    assert_eq!(primary.clean_stop(), None, "admitted, not stored");
    primary.complete_write(ticket).unwrap();
    assert_eq!(primary.clean_stop(), None, "written, not synchronized");
    let sync = primary.begin_sync().unwrap();
    primary.complete_sync(sync, now).unwrap();
    assert_eq!(
        primary.clean_stop(),
        Some(CleanStop {
            view: 0,
            accepted: operation().prefix(),
        })
    );
}

/// The operation after [`operation`].
fn second() -> PreparedOperation {
    let body = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([11; 16]),
    });
    let bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
    PreparedOperation::from_verified(
        &CanonicalOperation {
            group_id: configuration().scope().group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: 2,
            previous_digest: operation().prefix().digest,
            kind: body.kind(),
            body: &bytes,
        },
        canonical_body_digest(&bytes),
    )
}

/// A backup that lacks the end of the resumed primary's log acknowledges
/// an operation inside it. The primary keeps that log on disk and has no
/// prefix to count the acknowledgment at: it commits nothing and keeps
/// leading. The acknowledgment of the whole log commits it.
#[test]
fn a_resumed_primary_waits_for_a_backup_that_lacks_the_end_of_its_log() {
    let now = Duration::from_millis(5);
    let found = RecoveredState {
        log: FrozenLog {
            accepted: second().prefix(),
            ..recovered().log
        },
        ..recovered()
    };
    let mut changing = recover(1, found);
    assert!(changing.resume_primary(JournalGeneration(101), found));
    let mut primary = restarted(changing);
    let ticket = primary
        .begin_rejoin_install(JournalGeneration(102))
        .unwrap();
    primary
        .validate_install_suffix(&[operation(), second()])
        .unwrap();
    primary
        .complete_installation(ticket, Prefix::GENESIS, now)
        .unwrap();
    let scope = primary.normal().unwrap().scope();
    let mut acknowledge = |durable: Prefix| {
        let ack = PrepareOk { scope, durable };
        primary
            .receive(node(2), Control::PrepareOk { ack }, now)
            .unwrap();
        primary.normal().unwrap().snapshot().committed
    };
    assert_eq!(acknowledge(operation().prefix()), Prefix::GENESIS);
    assert_eq!(acknowledge(second().prefix()), second().prefix());
}
