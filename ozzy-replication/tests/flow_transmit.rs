use std::time::Duration;

use ozzy_proto::{GroupId, RequestId};
use ozzy_replication::flow::{
    Channel, FlowError, Operation, ProbeTiming, ReceiveEpoch, Receiver, StatusOutcome,
    TransmitError, Transmitter,
};
use ozzy_replication::{Digest, OpNumber, PipelineLimits, Prefix, Scope};

fn channel(epoch: u128) -> Channel {
    Channel {
        scope: Scope {
            group_id: GroupId::from_bytes([1; 16]),
            configuration_epoch: 1,
            configuration_digest: Digest::from_bytes([2; 32]),
            view: 0,
        },
        epoch: ReceiveEpoch::new(epoch).unwrap(),
    }
}

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

fn pair() -> (Transmitter, Receiver) {
    pair_with_credit(false)
}

fn pair_with_credit(reserved: bool) -> (Transmitter, Receiver) {
    let limits = PipelineLimits {
        max_operations: 4,
        max_body_bytes: 100,
    };
    let receiver = if reserved {
        Receiver::new_reserved(channel(1), Prefix::GENESIS, limits)
    } else {
        Receiver::new(channel(1), Prefix::GENESIS, limits)
    }
    .unwrap();
    let mut sender = Transmitter::new(
        channel(1).scope,
        limits,
        RequestId::from_bytes([3; 16]),
        ProbeTiming {
            initial: ms(10),
            maximum: ms(80),
        },
        ms(0),
    )
    .unwrap();
    let probe = sender
        .poll_probe(Prefix::GENESIS, 1, false, ms(0))
        .unwrap()
        .unwrap();
    let StatusOutcome::Verify = sender
        .observe(receiver.report(), Some(probe.request_id), ms(0))
        .unwrap()
    else {
        panic!("new epoch needs independent history verification");
    };
    let request = sender.candidate().unwrap();
    assert!(
        sender
            .open_verified(request, Prefix::GENESIS, Prefix::GENESIS, ms(0))
            .unwrap()
    );
    (sender, receiver)
}

fn operations() -> [Operation; 4] {
    let mut previous = Prefix::GENESIS;
    std::array::from_fn(|index| {
        let operation = Operation {
            prefix: Prefix {
                op: OpNumber(index as u64 + 1),
                digest: Digest::from_bytes([index as u8 + 4; 32]),
            },
            previous_digest: previous.digest,
            body_bytes: 25,
        };
        previous = operation.prefix;
        operation
    })
}

#[test]
fn renewed_credit_starts_a_probe_without_waiting_or_trusting_the_hint() {
    let (mut sender, mut receiver) = pair_with_credit(true);
    let ops = operations();
    receiver.grant(channel(1), 4, 100).unwrap();
    sender.observe(receiver.report(), None, ms(1)).unwrap();
    sender.record_send(&ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    sender.observe(receiver.report(), None, ms(2)).unwrap();
    receiver.release(ops[3].prefix).unwrap();
    receiver.revoke_unused(channel(2).epoch).unwrap();
    receiver.grant(channel(2), 4, 100).unwrap();

    assert_eq!(
        sender.observe(receiver.report(), None, ms(3)).unwrap(),
        StatusOutcome::Ignored
    );
    assert_eq!(sender.sender().unwrap().channel(), channel(1));
    assert_eq!(sender.sender().unwrap().available().max_operations, 0);
    assert!(sender.candidate().is_none());
    assert_eq!(
        sender.poll_probe(ops[3].prefix, 1, true, ms(3)).unwrap(),
        None
    );
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(3))
        .unwrap()
        .expect("renewed credit must not wait for the periodic probe timer");

    sender.observe(receiver.report(), None, ms(4)).unwrap();
    assert_eq!(sender.pending_probe(), Some(probe));
    assert_eq!(
        sender.poll_probe(ops[3].prefix, 1, false, ms(4)).unwrap(),
        None
    );
    assert_eq!(
        sender
            .observe(receiver.report(), Some(probe.request_id), ms(4))
            .unwrap(),
        StatusOutcome::Verify
    );
    assert_eq!(sender.sender().unwrap().channel(), channel(1));
    assert!(
        sender
            .open_verified(
                sender.candidate().unwrap(),
                ops[3].prefix,
                ops[3].prefix,
                ms(4),
            )
            .unwrap()
    );
    assert_eq!(sender.sender().unwrap().channel(), channel(2));
    assert_eq!(sender.sender().unwrap().available().max_operations, 4);
}

#[test]
fn replacement_hint_retries_live_probe_once_and_preserves_history_check() {
    let (mut sender, mut receiver) = pair_with_credit(true);
    let probe = sender
        .poll_probe(Prefix::GENESIS, 1, false, ms(10))
        .unwrap()
        .unwrap();
    receiver.revoke_unused(channel(2).epoch).unwrap();
    sender.observe(receiver.report(), None, ms(11)).unwrap();
    assert_eq!(sender.pending_probe(), Some(probe));
    assert_eq!(
        sender
            .poll_probe(Prefix::GENESIS, 1, false, ms(11))
            .unwrap(),
        Some(probe)
    );
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(12))
        .unwrap();
    let candidate = sender.candidate().unwrap();
    sender.observe(receiver.report(), None, ms(13)).unwrap();
    assert_eq!(sender.candidate(), Some(candidate));
    assert_eq!(sender.pending_probe(), Some(probe));
    assert_eq!(
        sender
            .poll_probe(Prefix::GENESIS, 1, false, ms(13))
            .unwrap(),
        None
    );
    assert!(
        sender
            .open_verified(candidate, Prefix::GENESIS, Prefix::GENESIS, ms(13))
            .unwrap()
    );
}

#[test]
fn new_unsent_work_advertises_receive_demand_without_timer_delay_or_repair() {
    let (mut sender, mut receiver) = pair_with_credit(true);
    let ops = operations();
    let probe = sender
        .poll_probe(ops[0].prefix, 1, false, ms(1))
        .unwrap()
        .expect("new work must advertise demand before the idle probe deadline");
    assert_eq!(probe.tail, Prefix::GENESIS);
    assert_eq!(probe.available, ops[0].prefix.op);
    assert_eq!(receiver.report().operation_limit, 0);
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(1))
        .unwrap();
    assert_eq!(
        sender.poll_probe(ops[0].prefix, 1, false, ms(2)).unwrap(),
        None
    );
    assert!(sender.repair(false).is_none());
    assert_eq!(sender.record_send(&ops[..1]), Err(FlowError::Capacity));
    receiver.grant(channel(1), 1, 25).unwrap();
    sender.observe(receiver.report(), None, ms(2)).unwrap();
    sender.record_send(&ops[..1]).unwrap();
    receiver.retain(channel(1), &ops[..1]).unwrap();
    assert_eq!(receiver.report().received, ops[0].prefix);
}

#[test]
fn free_receive_capacity_preserves_retained_bodies_across_credit_revocation() {
    let (_, mut receiver) = pair_with_credit(true);
    let ops = operations();
    assert_eq!(receiver.available().max_operations, 4);
    assert_eq!(receiver.available().max_body_bytes, 100);
    receiver.grant(channel(1), 4, 100).unwrap();
    assert_eq!(receiver.available().max_operations, 0);
    receiver.retain(channel(1), &ops[..1]).unwrap();
    let report = receiver
        .revoke_unused(ReceiveEpoch::new(2).unwrap())
        .unwrap();
    let available = receiver.available();
    assert_eq!(available.max_operations, 3);
    assert_eq!(available.max_body_bytes, 75);
    receiver.grant(report.channel, 4, 100).unwrap_err();
    receiver
        .grant(
            report.channel,
            available.max_operations as u64,
            available.max_body_bytes as u64,
        )
        .unwrap();
    receiver.release(ops[0].prefix).unwrap();
    assert_eq!(receiver.available().max_operations, 1);
    assert_eq!(receiver.available().max_body_bytes, 25);
}

#[test]
fn availability_hint_cannot_extend_repair_into_work_sent_after_the_probe() {
    let (mut sender, receiver) = pair();
    let ops = operations();
    sender.record_send(&ops[..1]).unwrap();
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(10))
        .unwrap()
        .unwrap();
    assert_eq!(probe.tail, ops[0].prefix);
    assert_eq!(probe.available, ops[3].prefix.op);
    sender.record_send(&ops[1..3]).unwrap();
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(11))
        .unwrap();
    assert_eq!(sender.repair(false).unwrap().through, ops[0].prefix);
    assert_eq!(sender.sender().unwrap().sent(), ops[2].prefix);
}

#[test]
fn replacing_session_preserves_spent_credit_and_fences_pending_history_verification() {
    let (mut sender, receiver) = pair();
    let ops = operations();
    sender.record_send(&ops).unwrap();
    let old = sender
        .poll_probe(ops[3].prefix, 1, false, ms(10))
        .unwrap()
        .unwrap();
    let replacement = Receiver::new(
        channel(2),
        Prefix::GENESIS,
        PipelineLimits {
            max_operations: 4,
            max_body_bytes: 100,
        },
    )
    .unwrap();
    assert_eq!(
        sender
            .observe(replacement.report(), Some(old.request_id), ms(10))
            .unwrap(),
        StatusOutcome::Verify
    );
    let candidate = sender.candidate().unwrap();
    let before = sender.sender().unwrap().available();
    sender.replace_session(ms(11)).unwrap();
    assert_eq!(sender.sender().unwrap().available(), before);
    assert_eq!(sender.sender().unwrap().sent(), ops[3].prefix);
    assert_eq!(sender.sender().unwrap().outstanding().count(), 4);
    assert!(sender.candidate().is_none());
    assert!(
        !sender
            .open_verified(candidate, Prefix::GENESIS, Prefix::GENESIS, ms(11))
            .unwrap()
    );
    let current = sender
        .poll_probe(ops[3].prefix, 1, false, ms(11))
        .unwrap()
        .unwrap();
    assert_ne!(current.request_id, old.request_id);
    assert_eq!(
        sender
            .observe(replacement.report(), Some(old.request_id), ms(11))
            .unwrap(),
        StatusOutcome::Ignored
    );
    // The same receiver still owns the original ledger. Reconnection provides
    // no fresh credit; its correlated report requests repair of the lost tail.
    sender
        .observe(receiver.report(), Some(current.request_id), ms(11))
        .unwrap();
    assert_eq!(sender.repair(false).unwrap().through, ops[3].prefix);
    let repair = sender.repair(false);
    sender.replace_session(ms(12)).unwrap();
    assert_eq!(sender.repair(false), repair);
    assert_eq!(sender.sender().unwrap().available(), before);
}

#[test]
fn delayed_disk_after_receipt_never_schedules_payload_repair() {
    let (mut sender, mut receiver) = pair();
    let ops = operations();
    sender.record_send(&ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    sender.observe(receiver.report(), None, ms(0)).unwrap();
    for instant in (10..=1_000).step_by(10) {
        let probe = sender
            .poll_probe(ops[3].prefix, 1, false, ms(instant))
            .unwrap()
            .unwrap();
        sender
            .observe(receiver.report(), Some(probe.request_id), ms(instant))
            .unwrap();
        assert_eq!(sender.repair(false), None);
        assert_eq!(sender.sender().unwrap().outstanding().count(), 0);
        // No application release and no durable vote have occurred.
        assert_eq!(receiver.report().operation_limit, 4);
        assert_eq!(receiver.report().byte_limit, 100);
    }
}

#[test]
fn lost_tail_and_lost_repair_retry_only_after_correlated_gap_reports() {
    let (mut sender, mut receiver) = pair();
    let ops = operations();
    sender.record_send(&ops).unwrap();
    receiver.retain(channel(1), &ops[..1]).unwrap();
    sender.observe(receiver.report(), None, ms(1)).unwrap();
    assert_eq!(sender.repair(false), None); // Partial receipt alone is not loss evidence.
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(10))
        .unwrap()
        .unwrap();
    assert_eq!(sender.repair(false), None); // Timer emits no payload action.
    let retry = sender
        .poll_probe(ops[3].prefix, 1, false, ms(20))
        .unwrap()
        .unwrap();
    assert_eq!(retry, probe); // First response was lost.
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(21))
        .unwrap();
    let repair = sender.repair(false).unwrap();
    assert_eq!(repair.after, ops[0].prefix);
    assert_eq!(repair.through, ops[3].prefix);
    assert_eq!(sender.repair(true), None); // Locally queued data cannot be duplicated.
    assert_eq!(
        sender.poll_probe(ops[3].prefix, 1, false, ms(100)).unwrap(),
        None
    );
    sender.record_repair(repair, ops[1].prefix).unwrap();
    receiver.retain(channel(1), &ops[1..2]).unwrap();
    sender.observe(receiver.report(), None, ms(101)).unwrap();
    let remaining = sender.repair(false).unwrap();
    assert_eq!(remaining.after, ops[1].prefix);
    sender.record_repair(remaining, ops[3].prefix).unwrap(); // Lost retry packet.
    assert_eq!(sender.sender().unwrap().outstanding().count(), 2);
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(102))
        .unwrap()
        .unwrap();
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(103))
        .unwrap();
    let repair = sender.repair(false).unwrap();
    assert_eq!(repair.after, ops[1].prefix);
    receiver.retain(channel(1), &ops[2..]).unwrap();
    sender.record_repair(repair, ops[3].prefix).unwrap();
    sender.observe(receiver.report(), None, ms(104)).unwrap();
    assert_eq!(sender.sender().unwrap().received(), ops[3].prefix);
    assert_eq!(sender.sender().unwrap().outstanding().count(), 0);
    assert_eq!(receiver.report().received_bytes, 100);
}

#[test]
fn lost_receipt_and_lost_credit_updates_recover_without_payload_copies() {
    let (mut sender, mut receiver) = pair();
    let ops = operations();
    sender.record_send(&ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap(); // Lost unsolicited receipt.
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(10))
        .unwrap()
        .unwrap();
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(10))
        .unwrap();
    assert_eq!(sender.repair(false), None);
    let next = Operation {
        prefix: Prefix {
            op: OpNumber(5),
            digest: Digest::from_bytes([8; 32]),
        },
        previous_digest: ops[3].prefix.digest,
        body_bytes: 25,
    };
    assert_eq!(sender.record_send(&[next]), Err(FlowError::Capacity));
    receiver.release(ops[0].prefix).unwrap(); // Lost credit update.
    assert_eq!(sender.record_send(&[next]), Err(FlowError::Capacity));
    let probe = sender
        .poll_probe(next.prefix, 1, false, ms(20))
        .unwrap()
        .unwrap();
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(20))
        .unwrap();
    assert_eq!(sender.repair(false), None);
    sender.record_send(&[next]).unwrap();
    receiver.retain(channel(1), &[next]).unwrap();
}

#[test]
fn replacement_epoch_needs_current_correlation_and_both_history_boundaries() {
    let (mut sender, mut receiver) = pair();
    let ops = operations();
    sender.record_send(&ops[..2]).unwrap();
    receiver.retain(channel(1), &ops[..2]).unwrap();
    sender.observe(receiver.report(), None, ms(1)).unwrap();
    receiver.retract(channel(2).epoch, ops[0].prefix).unwrap();
    assert_eq!(
        sender.observe(receiver.report(), None, ms(2)).unwrap(),
        StatusOutcome::Ignored
    );
    assert_eq!(sender.sender().unwrap().channel(), channel(1));
    let probe = sender
        .poll_probe(ops[1].prefix, 1, false, ms(10))
        .unwrap()
        .unwrap();
    let StatusOutcome::Verify = sender
        .observe(receiver.report(), Some(probe.request_id), ms(11))
        .unwrap()
    else {
        panic!("new epoch");
    };
    receiver.retract(channel(3).epoch, Prefix::GENESIS).unwrap();
    let old = sender.candidate().unwrap();
    let StatusOutcome::Verify = sender
        .observe(receiver.report(), Some(probe.request_id), ms(12))
        .unwrap()
    else {
        panic!("new epoch");
    };
    let current = sender.candidate().unwrap();
    assert!(
        !sender
            .open_verified(old, Prefix::GENESIS, ops[0].prefix, ms(12))
            .unwrap()
    );
    for (base, received) in [
        (ops[0].prefix, Prefix::GENESIS),
        (Prefix::GENESIS, ops[0].prefix),
    ] {
        assert_eq!(
            sender.open_verified(current, base, received, ms(12)),
            Err(TransmitError::Flow(FlowError::History))
        );
        assert_eq!(sender.sender().unwrap().channel(), channel(1));
    }
    assert!(
        sender
            .open_verified(current, Prefix::GENESIS, Prefix::GENESIS, ms(12))
            .unwrap()
    );
    sender.record_send(&ops[..2]).unwrap();
    receiver.retain(channel(3), &ops[..2]).unwrap();
    assert_eq!(
        sender
            .observe(receiver.report(), Some(probe.request_id), ms(13))
            .unwrap(),
        StatusOutcome::Ignored
    );
    assert_eq!(sender.sender().unwrap().outstanding().count(), 2);
    sender.observe(receiver.report(), None, ms(13)).unwrap();
    assert_eq!(sender.sender().unwrap().outstanding().count(), 0);
}

#[test]
fn local_backpressure_and_new_views_fence_probe_and_repair_ownership() {
    let (mut sender, receiver) = pair();
    let ops = operations();
    sender.record_send(&ops).unwrap();
    assert_eq!(
        sender.poll_probe(ops[3].prefix, 1, true, ms(10)).unwrap(),
        None
    );
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(20))
        .unwrap()
        .unwrap();
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(21))
        .unwrap();
    let old_repair = sender.repair(false).unwrap();
    sender
        .change_scope(
            Scope {
                view: 1,
                ..channel(1).scope
            },
            ms(22),
        )
        .unwrap();
    assert!(sender.sender().is_none());
    assert_eq!(sender.repair(false), None);
    assert_eq!(
        sender.record_repair(old_repair, ops[0].prefix),
        Err(FlowError::Channel)
    );
    assert_eq!(
        sender
            .observe(receiver.report(), Some(probe.request_id), ms(23))
            .unwrap(),
        StatusOutcome::Ignored
    );
    assert_eq!(sender.record_send(&ops), Err(FlowError::Channel));
    let next = sender
        .poll_probe(Prefix::GENESIS, 1, false, ms(23))
        .unwrap()
        .unwrap();
    assert_ne!(next.request_id, probe.request_id);
    assert_eq!(next.scope.view, 1);
}

#[test]
fn broadcast_progress_requires_history_but_no_send_reservations() {
    let (mut sender, mut receiver) = pair();
    sender.enable_broadcast();
    let ops = operations();
    receiver.retain(channel(1), &ops[..2]).unwrap();
    assert_eq!(
        sender.observe(receiver.report(), None, ms(1)).unwrap(),
        StatusOutcome::Verify
    );
    assert_eq!(sender.sender().unwrap().received(), Prefix::GENESIS);
    let request = sender.candidate().unwrap();
    assert!(matches!(
        sender.open_verified(request, Prefix::GENESIS, ops[0].prefix, ms(1)),
        Err(TransmitError::Flow(FlowError::History))
    ));
    assert!(
        sender
            .open_verified(request, Prefix::GENESIS, ops[1].prefix, ms(1))
            .unwrap()
    );
    assert_eq!(sender.sender().unwrap().received(), ops[1].prefix);
    assert_eq!(sender.sender().unwrap().outstanding().count(), 0);
    assert_eq!(sender.sender().unwrap().available().max_body_bytes, 50);
    assert_eq!(
        sender.observe(receiver.report(), None, ms(2)).unwrap(),
        StatusOutcome::Ignored
    );
    assert!(!sender.needs_catch_up());
}

#[test]
fn lost_unreserved_publication_opens_bounded_peer_repair() {
    let (mut sender, mut receiver) = pair();
    sender.enable_broadcast();
    let ops = operations();
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(10))
        .unwrap()
        .unwrap();
    assert_eq!(probe.tail, ops[3].prefix);
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(10))
        .unwrap();
    assert!(sender.needs_catch_up());
    sender.record_send(&ops[..2]).unwrap();
    receiver.retain(channel(1), &ops[..2]).unwrap();
    sender.observe(receiver.report(), None, ms(11)).unwrap();
    assert_eq!(sender.sender().unwrap().available().max_operations, 2);
    sender.record_send(&ops[2..]).unwrap();
    assert!(!sender.needs_catch_up());
    // The PEER repair packet was lost too. Retry after receipt has been quiet.
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(91))
        .unwrap()
        .unwrap();
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(91))
        .unwrap();
    let repair = sender.repair(false).unwrap();
    assert_eq!(repair.after, ops[1].prefix);
    assert_eq!(repair.through, ops[3].prefix);
    receiver.retain(channel(1), &ops[2..]).unwrap();
    sender.observe(receiver.report(), None, ms(92)).unwrap();
    assert_eq!(sender.repair(false), None);
    assert_eq!(sender.sender().unwrap().available().max_body_bytes, 0);
}

#[test]
fn broadcast_overtaking_peer_repair_retires_only_verified_reservations() {
    let (mut sender, mut receiver) = pair();
    sender.enable_broadcast();
    let ops = operations();
    sender.record_send(&ops[..2]).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    assert_eq!(
        sender.observe(receiver.report(), None, ms(1)).unwrap(),
        StatusOutcome::Verify
    );
    assert_eq!(sender.sender().unwrap().outstanding().count(), 2);
    let request = sender.candidate().unwrap();
    sender
        .open_verified(request, Prefix::GENESIS, ops[3].prefix, ms(1))
        .unwrap();
    assert_eq!(sender.sender().unwrap().outstanding().count(), 0);
    assert_eq!(sender.sender().unwrap().available().max_operations, 0);
    let mut invalid = receiver.report();
    invalid.revision += 1;
    invalid.received_bytes -= 1;
    assert!(sender.observe(invalid, None, ms(2)).is_err());
    receiver.release(ops[3].prefix).unwrap();
    sender.observe(receiver.report(), None, ms(3)).unwrap();
    assert_eq!(sender.sender().unwrap().available().max_operations, 4);
}

#[test]
fn broadcast_receiver_epoch_and_scope_still_require_fences() {
    let (mut sender, mut receiver) = pair();
    sender.enable_broadcast();
    let ops = operations();
    receiver.retain(channel(1), &ops[..1]).unwrap();
    sender.observe(receiver.report(), None, ms(1)).unwrap();
    let stale = sender.candidate().unwrap();
    sender
        .change_scope(
            Scope {
                view: 1,
                ..channel(1).scope
            },
            ms(2),
        )
        .unwrap();
    assert!(
        !sender
            .open_verified(stale, Prefix::GENESIS, ops[0].prefix, ms(2))
            .unwrap()
    );
    assert_eq!(
        sender.observe(receiver.report(), None, ms(2)).unwrap(),
        StatusOutcome::Ignored
    );
    assert!(!sender.needs_catch_up());
    let (mut sender, mut receiver) = pair();
    sender.enable_broadcast();
    receiver.reinitialize(channel(2), Prefix::GENESIS).unwrap();
    receiver.retain(channel(2), &ops[..1]).unwrap();
    assert_eq!(
        sender.observe(receiver.report(), None, ms(1)).unwrap(),
        StatusOutcome::Ignored
    );
    let probe = sender
        .poll_probe(ops[0].prefix, 1, false, ms(10))
        .unwrap()
        .unwrap();
    assert_eq!(
        sender
            .observe(receiver.report(), Some(probe.request_id), ms(10))
            .unwrap(),
        StatusOutcome::Verify
    );
    let request = sender.candidate().unwrap();
    sender
        .open_verified(request, Prefix::GENESIS, ops[0].prefix, ms(10))
        .unwrap();
    assert_eq!(sender.sender().unwrap().channel(), channel(2));
}

#[test]
fn delayed_publication_verification_cannot_erase_newer_peer_reservations() {
    let (mut sender, mut receiver) = pair();
    sender.enable_broadcast();
    let ops = operations();
    receiver.retain(channel(1), &ops[..1]).unwrap();
    sender.observe(receiver.report(), None, ms(1)).unwrap();
    let delayed = sender.candidate().unwrap();
    sender.record_send(&ops).unwrap();
    receiver.retain(channel(1), &ops[1..2]).unwrap();
    sender.observe(receiver.report(), None, ms(2)).unwrap();
    assert!(
        !sender
            .open_verified(delayed, Prefix::GENESIS, ops[0].prefix, ms(3))
            .unwrap()
    );
    assert_eq!(sender.sender().unwrap().received(), ops[1].prefix);
    assert_eq!(sender.sender().unwrap().outstanding().count(), 2);
    assert_eq!(sender.sender().unwrap().available().max_body_bytes, 0);
}

#[test]
fn held_publication_bounds_correlated_repair_before_its_predecessor() {
    let (mut sender, receiver) = pair();
    sender.enable_broadcast();
    let ops = operations();
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(10))
        .unwrap()
        .unwrap();
    sender
        .observe_with_repair_limit(
            receiver.report(),
            Some(probe.request_id),
            Some(ops[0].prefix.op),
            ms(10),
        )
        .unwrap();
    assert_eq!(sender.catch_up_through(), Some(ops[0].prefix.op));
    assert_eq!(sender.record_send(&ops[..2]), Err(FlowError::Capacity));
    sender.record_send(&ops[..1]).unwrap();
    assert!(!sender.needs_catch_up());
    // PUB has operations 2..4 buffered. Repair must stop after operation 1.
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(80))
        .unwrap()
        .unwrap();
    sender
        .observe_with_repair_limit(
            receiver.report(),
            Some(probe.request_id),
            Some(ops[0].prefix.op),
            ms(80),
        )
        .unwrap();
    assert_eq!(sender.repair(false).unwrap().through, ops[0].prefix);
}

#[test]
fn capacity_blocked_publication_requests_no_missing_data() {
    let (mut sender, mut receiver) = pair();
    sender.enable_broadcast();
    let ops = operations();
    receiver.retain(channel(1), &ops[..1]).unwrap();
    sender.observe(receiver.report(), None, ms(1)).unwrap();
    let candidate = sender.candidate().unwrap();
    sender
        .open_verified(candidate, Prefix::GENESIS, ops[0].prefix, ms(1))
        .unwrap();
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(10))
        .unwrap()
        .unwrap();
    sender
        .observe_with_repair_limit(
            receiver.report(),
            Some(probe.request_id),
            Some(ops[0].prefix.op),
            ms(10),
        )
        .unwrap();
    assert!(!sender.needs_catch_up());
    assert_eq!(sender.repair(false), None);
}

#[test]
fn advancing_peer_catch_up_is_not_retransmitted_by_a_probe() {
    let (mut sender, mut receiver) = pair();
    sender.enable_broadcast();
    let ops = operations();
    sender.record_send(&ops).unwrap();
    for index in 0..3 {
        let at = 10 + index as u64 * 10;
        receiver.retain(channel(1), &ops[index..=index]).unwrap();
        sender.observe(receiver.report(), None, ms(at - 1)).unwrap();
        let probe = sender
            .poll_probe(ops[3].prefix, 1, false, ms(at))
            .unwrap()
            .unwrap();
        sender
            .observe(receiver.report(), Some(probe.request_id), ms(at))
            .unwrap();
        assert_eq!(sender.repair(false), None);
    }
    // A genuinely missing final PEER chunk still retries after receipt stops.
    let probe = sender
        .poll_probe(ops[3].prefix, 1, false, ms(110))
        .unwrap()
        .unwrap();
    sender
        .observe(receiver.report(), Some(probe.request_id), ms(110))
        .unwrap();
    let repair = sender.repair(false).unwrap();
    assert_eq!(repair.after, ops[2].prefix);
    assert_eq!(repair.through, ops[3].prefix);
}
