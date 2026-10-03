use super::writeback::{admit, finish, prepare, replica, settle};
use super::*;

#[test]
fn owned_detached_sync_allows_later_writes_without_enlarging_its_evidence() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut replica = replica(&mut controller, io, 0, policy, 8192);
        let (first, _, receipt) = admit(&mut controller, &mut replica, 1);
        let work = prepare(&mut replica);
        finish(&mut controller, &mut replica, work);
        settle(&mut replica, receipt);
        let ticket = replica.driver.begin_sync().unwrap();
        let sync = replica.journal.begin_sync(ticket).unwrap();
        assert!(replica.journal.begin_sync(ticket).is_err());
        assert!(replica.journal.begin_roll(8).is_err());
        assert!(!replica.journal.is_faulted());
        let mut publishing = Box::pin(sync.publish());
        assert!(poll(publishing.as_mut()).is_pending());
        let held = controller.jobs()[0].0;
        let (second, _, receipt) = admit(&mut controller, &mut replica, 1);
        let write = prepare(&mut replica);
        let done = drive_except(&mut controller, write.write(), Some(held), |_| {
            Effect::Normal
        });
        replica.journal.complete_write(done).unwrap();
        settle(&mut replica, receipt);
        assert_eq!(
            replica.driver.normal().unwrap().snapshot().accepted.op.0,
            second.through().0
        );
        controller.execute(held, Effect::Normal).unwrap();
        controller.deliver(held).unwrap();
        let done = drive(&mut controller, publishing);
        let completed = replica.journal.complete_sync(done).unwrap();
        assert_eq!(completed, ticket);
        assert_eq!(completed.through().0, first.through().0);
        replica
            .driver
            .complete_sync(completed, Duration::ZERO)
            .unwrap();
        assert_eq!(
            replica
                .driver
                .normal()
                .unwrap()
                .snapshot()
                .journal
                .durable
                .0,
            first.through().0
        );
        let ticket = replica.driver.begin_sync().unwrap();
        let sync = replica.journal.begin_sync(ticket).unwrap();
        let done = drive(&mut controller, sync.publish());
        let completed = replica.journal.complete_sync(done).unwrap();
        replica
            .driver
            .complete_sync(completed, Duration::ZERO)
            .unwrap();
        assert_eq!(
            replica
                .driver
                .normal()
                .unwrap()
                .snapshot()
                .journal
                .durable
                .0,
            second.through().0
        );
        let roll = replica.journal.begin_roll(8).unwrap();
        let done = drive(&mut controller, roll.publish());
        replica.journal.complete_roll(done).unwrap();
        drive(&mut controller, replica.journal.shutdown()).unwrap();
    }
}

#[test]
fn owned_detached_sync_cancellation_and_publication_errors_fence_the_owner() {
    for failure in 0..4 {
        let (mut controller, io) = setup();
        let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
        let (_, _, receipt) = admit(&mut controller, &mut replica, 1);
        let work = prepare(&mut replica);
        finish(&mut controller, &mut replica, work);
        settle(&mut replica, receipt);
        let ticket = replica.driver.begin_sync().unwrap();
        let work = replica.journal.begin_sync(ticket).unwrap();
        match failure {
            0 => drop(work),
            1 => {
                let mut pending = Box::pin(work.publish());
                assert!(poll(pending.as_mut()).is_pending());
                drop(pending);
                for (id, _) in controller.jobs() {
                    controller.execute(id, Effect::Normal).unwrap();
                    controller.deliver(id).unwrap();
                }
            }
            2 => drop(drive(&mut controller, work.publish())),
            _ => {
                let mut failed = false;
                let done = drive_except(&mut controller, work.publish(), None, |operation| {
                    if matches!(operation, Operation::Write { .. }) && !failed {
                        failed = true;
                        Effect::FailAfter(std::io::ErrorKind::Other)
                    } else {
                        Effect::Normal
                    }
                });
                assert!(failed);
                assert!(replica.journal.complete_sync(done).is_err());
            }
        }
        assert!(replica.journal.is_faulted());
        assert_eq!(
            replica
                .driver
                .normal()
                .unwrap()
                .snapshot()
                .journal
                .durable
                .0,
            0
        );
        assert!(replica.journal.begin_roll(8).is_err());
    }
}

#[test]
fn owned_detached_sync_foreign_owner_never_supplies_a_core_completion() {
    let (mut controller, io) = setup();
    let mut left = replica(&mut controller, io.clone(), 0, QuorumPolicy::Durable, 8192);
    let mut right = replica(&mut controller, io, 2, QuorumPolicy::Durable, 8192);
    let (_, _, receipt) = admit(&mut controller, &mut left, 1);
    let work = prepare(&mut left);
    finish(&mut controller, &mut left, work);
    settle(&mut left, receipt);
    let ticket = left.driver.begin_sync().unwrap();
    let work = left.journal.begin_sync(ticket).unwrap();
    let done = drive(&mut controller, work.publish());
    assert!(right.journal.complete_sync(done).is_err());
    assert!(right.journal.is_faulted());
    assert!(left.journal.is_faulted(), "unobserved originating mutation");
}
