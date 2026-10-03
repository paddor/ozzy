use std::time::Duration;

use ozzy_proto::{GroupId, RequestId};
use ozzy_replication::flow::{ProbeError, ProbeScheduler, ProbeTiming};
use ozzy_replication::{Digest, OpNumber, Prefix, Scope};

fn scope() -> Scope {
    Scope {
        group_id: GroupId::from_bytes([1; 16]),
        configuration_epoch: 1,
        configuration_digest: Digest::from_bytes([2; 32]),
        view: 0,
    }
}
fn timing() -> ProbeTiming {
    ProbeTiming {
        initial: Duration::from_millis(10),
        maximum: Duration::from_millis(80),
    }
}
fn scheduler() -> ProbeScheduler {
    ProbeScheduler::new(
        RequestId::from_bytes(10u128.to_be_bytes()),
        timing(),
        Duration::ZERO,
    )
    .unwrap()
}

#[test]
fn lost_probes_retry_one_frozen_small_request_with_a_bounded_backoff() {
    let mut scheduler = scheduler();
    let first = scheduler
        .poll(scope(), Prefix::GENESIS, OpNumber(0), Duration::ZERO)
        .unwrap()
        .unwrap();
    for (due, next) in [(10, 30), (30, 70), (70, 150), (150, 230), (230, 310)] {
        assert_eq!(
            scheduler
                .poll(
                    scope(),
                    Prefix::GENESIS,
                    OpNumber(0),
                    Duration::from_millis(due - 1)
                )
                .unwrap(),
            None
        );
        let later_tail = Prefix {
            op: ozzy_replication::OpNumber(1),
            digest: Digest::from_bytes([3; 32]),
        };
        assert_eq!(
            scheduler
                .poll(
                    scope(),
                    later_tail,
                    later_tail.op,
                    Duration::from_millis(due)
                )
                .unwrap(),
            Some(first)
        );
        assert_eq!(scheduler.pending(), Some(first));
        assert_eq!(
            scheduler
                .poll(
                    scope(),
                    later_tail,
                    later_tail.op,
                    Duration::from_millis(next - 1)
                )
                .unwrap(),
            None
        );
    }
}

#[test]
fn stale_responses_do_not_postpone_retry_or_complete_a_replacement_probe() {
    let mut scheduler = scheduler();
    let first = scheduler
        .poll(scope(), Prefix::GENESIS, OpNumber(0), Duration::ZERO)
        .unwrap()
        .unwrap();
    let wrong = RequestId::from_bytes([99; 16]);
    assert!(
        !scheduler
            .complete(scope(), wrong, Duration::from_millis(9))
            .unwrap()
    );
    assert_eq!(
        scheduler
            .poll(
                scope(),
                Prefix::GENESIS,
                OpNumber(0),
                Duration::from_millis(10)
            )
            .unwrap(),
        Some(first)
    );
    assert!(
        scheduler
            .complete(scope(), first.request_id, Duration::from_millis(11))
            .unwrap()
    );
    assert_eq!(
        scheduler
            .poll(
                scope(),
                Prefix::GENESIS,
                OpNumber(0),
                Duration::from_millis(20)
            )
            .unwrap(),
        None
    );
    let next = scheduler
        .poll(
            scope(),
            Prefix::GENESIS,
            OpNumber(0),
            Duration::from_millis(21),
        )
        .unwrap()
        .unwrap();
    assert_ne!(next.request_id, first.request_id);
    assert!(
        !scheduler
            .complete(scope(), first.request_id, Duration::from_millis(22))
            .unwrap()
    );
    assert_eq!(scheduler.pending(), Some(next));
}

#[test]
fn scope_invalidation_discards_old_correlation_without_reusing_ids() {
    let mut scheduler = scheduler();
    let first = scheduler
        .poll(scope(), Prefix::GENESIS, OpNumber(0), Duration::ZERO)
        .unwrap()
        .unwrap();
    let later = Scope { view: 1, ..scope() };
    assert_eq!(
        scheduler.poll(
            later,
            Prefix::GENESIS,
            OpNumber(0),
            Duration::from_millis(10)
        ),
        Err(ProbeError::Scope)
    );
    scheduler.invalidate(Duration::from_millis(10)).unwrap();
    let next = scheduler
        .poll(
            later,
            Prefix::GENESIS,
            OpNumber(0),
            Duration::from_millis(10),
        )
        .unwrap()
        .unwrap();
    assert_ne!(first.request_id, next.request_id);
    assert!(
        !scheduler
            .complete(scope(), first.request_id, Duration::from_millis(11))
            .unwrap()
    );
    assert_eq!(scheduler.pending(), Some(next));
}

#[test]
fn clock_regression_and_overflow_leave_live_request_intact() {
    let mut scheduler = scheduler();
    let first = scheduler
        .poll(
            scope(),
            Prefix::GENESIS,
            OpNumber(0),
            Duration::from_millis(10),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduler.poll(scope(), Prefix::GENESIS, OpNumber(0), Duration::ZERO),
        Err(ProbeError::Clock)
    );
    assert_eq!(
        scheduler.complete(scope(), first.request_id, Duration::ZERO),
        Err(ProbeError::Clock)
    );
    assert_eq!(scheduler.invalidate(Duration::ZERO), Err(ProbeError::Clock));
    assert_eq!(
        scheduler.poll(scope(), Prefix::GENESIS, OpNumber(0), Duration::MAX),
        Err(ProbeError::TimeExhausted)
    );
    assert_eq!(
        scheduler.complete(scope(), first.request_id, Duration::MAX),
        Err(ProbeError::TimeExhausted)
    );
    assert_eq!(scheduler.pending(), Some(first));
    assert!(
        scheduler
            .complete(scope(), first.request_id, Duration::from_millis(11))
            .unwrap()
    );
}

#[test]
fn exhausted_request_ids_do_not_wrap_or_revive_retired_requests() {
    let mut scheduler = ProbeScheduler::new(
        RequestId::from_bytes((u128::MAX - 1).to_be_bytes()),
        timing(),
        Duration::ZERO,
    )
    .unwrap();
    let last = scheduler
        .poll(scope(), Prefix::GENESIS, OpNumber(0), Duration::ZERO)
        .unwrap()
        .unwrap();
    scheduler
        .complete(scope(), last.request_id, Duration::ZERO)
        .unwrap();
    assert_eq!(
        scheduler.poll(
            scope(),
            Prefix::GENESIS,
            OpNumber(0),
            Duration::from_millis(10)
        ),
        Err(ProbeError::IdsExhausted)
    );
    assert_eq!(scheduler.pending(), None);
}
