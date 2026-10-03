use super::*;
use ozzy_replication::driver::{Action, ReplicaDriver, Timing};
use ozzy_replication::wire::Control;
use std::time::Duration;

fn now(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn driver() -> ReplicaDriver {
    ReplicaDriver::from_normal(
        replica(record(QuorumPolicy::Replicated).configuration(), 0),
        now(0),
        Timing {
            heartbeat: now(10),
            primary_timeout: now(100),
            retransmit: now(10),
            election_timeout: now(200),
            max_election_timeout: now(800),
        },
    )
    .unwrap()
}

fn retain(
    driver: &mut ReplicaDriver,
    previous: Prefix,
    millis: u64,
) -> (PreparedOperation, WriteTicket) {
    let operation = operation(driver.scope(), previous);
    let Admission::Write { ticket, .. } = driver
        .prepare(node(0), driver.scope(), &[operation], now(millis))
        .unwrap()
    else {
        panic!("fresh operation");
    };
    (operation, ticket)
}

fn confirm(driver: &mut ReplicaDriver, operation: PreparedOperation, millis: u64) {
    driver
        .receive(
            node(1),
            Control::PrepareRetained {
                ack: ozzy_replication::RetainedPrepareOk {
                    scope: driver.scope(),
                    retained: operation.prefix(),
                },
            },
            now(millis),
        )
        .unwrap();
    driver.apply_through(operation.prefix()).unwrap();
}

#[test]
fn retained_vote_during_validation_does_not_require_revalidation_or_durability() {
    let mut driver = driver();
    let (first, _) = retain(&mut driver, Prefix::GENESIS, 1);
    let validation = driver.begin_validation().unwrap();
    driver
        .receive(
            node(1),
            Control::PrepareRetained {
                ack: ozzy_replication::RetainedPrepareOk {
                    scope: driver.scope(),
                    retained: first.prefix(),
                },
            },
            now(2),
        )
        .unwrap();
    let second = operation(driver.scope(), first.prefix());
    assert!(matches!(
        driver.prepare_validated(node(0), validation, &[second], now(2)),
        Ok(Admission::Write { .. })
    ));
    let snapshot = driver.normal().unwrap().snapshot();
    assert_eq!(snapshot.accepted, second.prefix());
    assert_eq!(snapshot.committed, first.prefix());
    assert_eq!(snapshot.applied, Prefix::GENESIS);
    assert_eq!(snapshot.journal.durable, OpNumber(0));
    assert_eq!(snapshot.pending_operations, 2);
    let before_apply = driver.begin_validation().unwrap();
    driver.apply_through(first.prefix()).unwrap();
    assert!(matches!(
        driver.prepare_validated(node(0), before_apply, &[second], now(2)),
        Err(ozzy_replication::driver::DriverError::StaleValidation)
    ));
    assert_eq!(
        driver.normal().unwrap().snapshot().accepted,
        second.prefix()
    );
}

#[test]
fn ram_confirmations_do_not_hide_stalled_background_writes() {
    let mut driver = driver();
    let (operation, _) = retain(&mut driver, Prefix::GENESIS, 20);
    confirm(&mut driver, operation, 110);
    assert!(!matches!(
        driver.poll(now(119)).unwrap(),
        Some(Action::Broadcast(Control::ExitView(_)))
    ));
    assert_eq!(
        driver.poll(now(120)).unwrap(),
        Some(Action::Broadcast(Control::ExitView(driver.scope())))
    );
    assert_eq!(
        driver.normal().unwrap().snapshot().committed,
        operation.prefix()
    );
    assert_eq!(
        driver.normal().unwrap().snapshot().journal.durable,
        OpNumber(0)
    );
}

#[test]
fn partial_buffered_progress_rearms_its_own_deadline() {
    for direct in [false, true] {
        let mut driver = driver();
        let (first, write) = retain(&mut driver, Prefix::GENESIS, 20);
        let (second, _) = retain(&mut driver, first.prefix(), 30);
        confirm(&mut driver, first, 100);
        if direct {
            driver.complete_buffered_write(write, now(119)).unwrap();
        } else {
            driver.complete_buffered_write(write, now(119)).unwrap();
            let sync = driver.begin_sync().unwrap();
            driver.complete_sync(sync, now(119)).unwrap();
        }
        confirm(&mut driver, second, 180);
        assert!(!matches!(
            driver.poll(now(218)).unwrap(),
            Some(Action::Broadcast(Control::ExitView(_)))
        ));
        assert_eq!(
            driver.poll(now(219)).unwrap(),
            Some(Action::Broadcast(Control::ExitView(driver.scope())))
        );
    }
}

#[test]
fn drained_idle_time_does_not_age_the_next_disk_write() {
    let mut driver = driver();
    let (first, write) = retain(&mut driver, Prefix::GENESIS, 20);
    confirm(&mut driver, first, 30);
    driver.complete_buffered_write(write, now(40)).unwrap();
    assert!(!matches!(
        driver.poll(now(1000)).unwrap(),
        Some(Action::Broadcast(Control::ExitView(_)))
    ));
    let (second, _) = retain(&mut driver, first.prefix(), 1001);
    confirm(&mut driver, second, 1002);
    assert!(!matches!(
        driver.poll(now(1100)).unwrap(),
        Some(Action::Broadcast(Control::ExitView(_)))
    ));
    assert_eq!(
        driver.poll(now(1101)).unwrap(),
        Some(Action::Broadcast(Control::ExitView(driver.scope())))
    );
}

#[test]
fn buffered_completion_keeps_durability_separate_until_election_barrier() {
    let mut driver = driver();
    let (operation, write) = retain(&mut driver, Prefix::GENESIS, 20);
    confirm(&mut driver, operation, 30);
    driver.complete_buffered_write(write, now(40)).unwrap();
    let snapshot = driver.normal().unwrap().snapshot();
    assert_eq!(snapshot.journal.written, operation.prefix().op);
    assert_eq!(snapshot.journal.durable, OpNumber(0));
    assert_eq!(snapshot.pending_operations, 0);
    assert!(!driver.election_needs_sync());
    assert!(!matches!(
        driver.poll(now(1000)).unwrap(),
        Some(Action::Broadcast(Control::ExitView(_)))
    ));
    let scope = driver.scope();
    driver
        .receive(
            node(1),
            Control::StartViewChange(ozzy_replication::StartViewChange {
                scope: Scope {
                    view: scope.view + 1,
                    ..scope
                },
            }),
            now(1001),
        )
        .unwrap();
    assert!(driver.normal().is_none());
    assert!(driver.election_needs_sync());
    assert!(!matches!(
        driver.poll(now(1001)).unwrap(),
        Some(Action::PersistPromise(_))
    ));
    let sync = driver.begin_sync().unwrap();
    driver.complete_sync(sync, now(1002)).unwrap();
    assert!(!driver.election_needs_sync());
    assert!(matches!(
        driver.poll(now(1002)).unwrap(),
        Some(Action::PersistPromise(_))
    ));
}
