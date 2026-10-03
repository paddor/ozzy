use ozzy_proto::GroupId;
use ozzy_replication::flow::{Channel, FlowError, Operation, ReceiveEpoch, Receiver, Sender};
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

fn limits() -> PipelineLimits {
    PipelineLimits {
        max_operations: 4,
        max_body_bytes: 100,
    }
}

fn operations(predecessor: Prefix, sizes: &[u64]) -> Vec<Operation> {
    let mut previous = predecessor;
    sizes
        .iter()
        .map(|&body_bytes| {
            let op = previous.op.0 + 1;
            let mut bytes = [9; 32];
            bytes[..8].copy_from_slice(&op.to_be_bytes());
            let prefix = Prefix {
                op: OpNumber(op),
                digest: Digest::from_bytes(bytes),
            };
            let operation = Operation {
                prefix,
                previous_digest: previous.digest,
                body_bytes,
            };
            previous = prefix;
            operation
        })
        .collect()
}

fn pair() -> (Receiver, Sender) {
    let receiver = Receiver::new(channel(1), Prefix::GENESIS, limits()).unwrap();
    let sender = Sender::open(receiver.report(), limits(), Prefix::GENESIS).unwrap();
    (receiver, sender)
}

#[test]
fn larger_receiver_window_does_not_expand_sender_admission() {
    let local = PipelineLimits {
        max_operations: 1,
        max_body_bytes: 10,
    };
    let mut receiver = Receiver::new(channel(1), Prefix::GENESIS, limits()).unwrap();
    let mut sender = Sender::open(receiver.report(), local, Prefix::GENESIS).unwrap();
    assert_eq!(sender.available(), local);
    for _ in 0..4 {
        let ops = operations(sender.sent(), &[10, 1]);
        sender.record_send(channel(1), &ops[..1]).unwrap();
        assert_eq!(sender.available().max_operations, 0);
        assert_eq!(sender.available().max_body_bytes, 0);
        assert_eq!(sender.outstanding().count(), 1);
        assert_eq!(
            sender.record_send(channel(1), &ops[1..]),
            Err(FlowError::Capacity)
        );
        receiver.retain(channel(1), &ops[..1]).unwrap();
        sender.observe(receiver.report()).unwrap();
        assert_eq!(sender.outstanding().count(), 0);
    }
    // Local metadata is free, but the receiver has not released any operations.
    assert_eq!(sender.available().max_operations, 0);
    let next = operations(sender.sent(), &[1]);
    assert_eq!(
        sender.record_send(channel(1), &next),
        Err(FlowError::Capacity)
    );
    receiver.release(sender.sent()).unwrap();
    sender.observe(receiver.report()).unwrap();
    assert_eq!(sender.available(), local);
    // A fresh receiver epoch also preserves the original local capacity.
    receiver.retract(channel(2).epoch, sender.sent()).unwrap();
    sender.reopen(receiver.report(), sender.sent()).unwrap();
    assert_eq!(sender.available(), local);
    let too_large = operations(sender.sent(), &[11]);
    assert_eq!(
        sender.record_send(channel(2), &too_large),
        Err(FlowError::Capacity)
    );
    sender.record_send(channel(2), &next).unwrap();
    assert_eq!(sender.outstanding().count(), 1);
}

#[test]
fn maximal_remote_credit_is_clamped_before_conversion_and_reservation() {
    let receiver = Receiver::new(channel(1), Prefix::GENESIS, limits()).unwrap();
    let mut report = receiver.report();
    report.operation_limit = u64::MAX;
    report.byte_limit = u64::MAX;
    let mut sender = Sender::open(report, limits(), Prefix::GENESIS).unwrap();
    assert_eq!(sender.available(), limits());
    let ops = operations(Prefix::GENESIS, &[25; 4]);
    sender.record_send(channel(1), &ops).unwrap();
    assert_eq!(sender.available().max_operations, 0);
    assert_eq!(sender.available().max_body_bytes, 0);
    assert_eq!(sender.outstanding().count(), 4);
    assert_eq!(
        sender.record_send(channel(1), &operations(ops[3].prefix, &[1])),
        Err(FlowError::Capacity)
    );
    // A newer report without a receipt cannot release local metadata or bytes.
    report.revision += 1;
    sender.observe(report).unwrap();
    assert_eq!(sender.available().max_operations, 0);
    assert_eq!(sender.available().max_body_bytes, 0);
}

#[test]
fn non_genesis_epochs_count_only_the_suffix_after_the_applied_base() {
    let base = operations(Prefix::GENESIS, &[20; 10])[9].prefix;
    let mut receiver = Receiver::new(channel(1), base, limits()).unwrap();
    let mut sender = Sender::open(receiver.report(), limits(), base).unwrap();
    let ops = operations(base, &[25; 4]);
    sender.record_send(channel(1), &ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    sender.observe(receiver.report()).unwrap();
    assert_eq!(receiver.report().received.op.0, 14);
    assert_eq!(receiver.report().operation_limit, 4);
    assert_eq!(receiver.report().received_bytes, 100);
    receiver.release(ops[0].prefix).unwrap();
    sender.observe(receiver.report()).unwrap();
    sender
        .record_send(channel(1), &operations(ops[3].prefix, &[25]))
        .unwrap();
}

#[test]
fn operation_number_exhaustion_does_not_wrap_or_charge() {
    let base = Prefix {
        op: OpNumber(u64::MAX),
        digest: Digest::from_bytes([1; 32]),
    };
    let mut receiver = Receiver::new(channel(1), base, limits()).unwrap();
    let mut sender = Sender::open(receiver.report(), limits(), base).unwrap();
    let op = Operation {
        prefix: Prefix {
            op: OpNumber(1),
            digest: Digest::from_bytes([2; 32]),
        },
        previous_digest: base.digest,
        body_bytes: 1,
    };
    assert_eq!(
        sender.record_send(channel(1), &[op]),
        Err(FlowError::Exhausted)
    );
    assert_eq!(
        receiver.retain(channel(1), &[op]),
        Err(FlowError::Exhausted)
    );
    assert_eq!(sender.sent(), base);
    assert_eq!(receiver.report().received, base);
    assert_eq!(receiver.report().received_bytes, 0);
}

#[test]
fn receipt_does_not_return_credit_or_create_durability() {
    let (mut receiver, mut sender) = pair();
    let ops = operations(Prefix::GENESIS, &[25, 25, 25, 25]);
    sender.record_send(channel(1), &ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    assert!(sender.observe(receiver.report()).unwrap());
    assert_eq!(sender.outstanding().count(), 0);
    let next = operations(ops[3].prefix, &[1]);
    assert_eq!(
        sender.record_send(channel(1), &next),
        Err(FlowError::Capacity)
    );
    assert_eq!(receiver.report().operation_limit, 4);
    assert_eq!(receiver.report().byte_limit, 100);
    // Only an application-release event returns space. No durable/quorum API exists here.
    receiver.release(ops[1].prefix).unwrap();
    sender.observe(receiver.report()).unwrap();
    sender.record_send(channel(1), &next).unwrap();
    receiver.retain(channel(1), &next).unwrap();
}

#[test]
fn retraction_rebases_counters_but_keeps_accepted_unapplied_operations_charged() {
    let (mut receiver, mut sender) = pair();
    let ops = operations(Prefix::GENESIS, &[10, 20, 30, 40]);
    sender.record_send(channel(1), &ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    receiver.release(ops[0].prefix).unwrap();
    sender.observe(receiver.report()).unwrap();
    let retired = receiver.report();

    // Applied op 1 stays the floor. Accepted ops 2/3 remain retained; only the
    // unadmitted staged op 4 is discarded after its validation became stale.
    receiver
        .retract(ReceiveEpoch::new(2).unwrap(), ops[2].prefix)
        .unwrap();
    let report = receiver.report();
    assert_eq!(report.channel, channel(2));
    assert_eq!(report.revision, 1);
    assert_eq!(report.base, ops[0].prefix);
    assert_eq!(report.received, ops[2].prefix);
    assert_eq!(report.received_bytes, 50);
    assert_eq!(report.operation_limit, 4);
    assert_eq!(report.byte_limit, 100);
    assert_eq!(sender.observe(report), Err(FlowError::Channel));
    assert_eq!(
        receiver.retain(channel(1), &ops[3..]),
        Err(FlowError::Channel)
    );

    sender.reopen(report, ops[2].prefix).unwrap();
    assert_eq!(sender.observe(retired), Err(FlowError::Channel));
    let next = operations(ops[2].prefix, &[50, 1]);
    sender.record_send(channel(2), &next[..1]).unwrap();
    receiver.retain(channel(2), &next[..1]).unwrap();
    assert_eq!(
        sender.record_send(channel(2), &next[1..]),
        Err(FlowError::Capacity)
    );
    receiver.release(ops[2].prefix).unwrap();
    sender.observe(receiver.report()).unwrap();
    sender.record_send(channel(2), &next[1..]).unwrap();
    receiver.retain(channel(2), &next[1..]).unwrap();
}

#[test]
fn retraction_rejects_reused_epochs_and_unknown_boundaries_atomically() {
    let (mut receiver, _) = pair();
    let ops = operations(Prefix::GENESIS, &[10, 20]);
    receiver.retain(channel(1), &ops).unwrap();
    receiver.release(ops[0].prefix).unwrap();
    let before = receiver.report();
    assert_eq!(
        receiver.retract(channel(1).epoch, ops[1].prefix),
        Err(FlowError::Channel)
    );
    for keep in [
        Prefix::GENESIS,
        Prefix {
            digest: Digest::from_bytes([99; 32]),
            ..ops[1].prefix
        },
        operations(ops[1].prefix, &[1])[0].prefix,
    ] {
        assert_eq!(
            receiver.retract(channel(2).epoch, keep),
            Err(FlowError::History)
        );
        assert_eq!(receiver.report(), before);
    }
    receiver.retract(channel(2).epoch, ops[0].prefix).unwrap();
    assert_eq!(receiver.report().base, ops[0].prefix);
    assert_eq!(receiver.report().received, ops[0].prefix);
    assert_eq!(receiver.report().received_bytes, 0);
    receiver.retain(channel(2), &ops[1..]).unwrap();
}

#[test]
fn failed_reopen_keeps_old_sender_reservations() {
    let (receiver, mut sender) = pair();
    let ops = operations(Prefix::GENESIS, &[10, 20]);
    sender.record_send(channel(1), &ops).unwrap();
    let mut report = receiver.report();
    assert_eq!(
        sender.reopen(report, Prefix::GENESIS),
        Err(FlowError::Channel)
    );
    report.channel = channel(2);
    assert_eq!(
        sender.reopen(report, ops[0].prefix),
        Err(FlowError::History)
    );
    report.received = ops[0].prefix;
    report.received_bytes = 10;
    report.byte_limit = 9; // Advertised credit cannot precede its own receipt.
    assert_eq!(sender.reopen(report, ops[0].prefix), Err(FlowError::Report));
    assert_eq!(sender.channel(), channel(1));
    assert_eq!(sender.sent(), ops[1].prefix);
    assert_eq!(sender.outstanding().copied().collect::<Vec<_>>(), ops);
}

#[test]
fn duplicate_and_reordered_reports_never_multiply_credit() {
    let (mut receiver, mut sender) = pair();
    let initial = receiver.report();
    let ops = operations(Prefix::GENESIS, &[20, 20]);
    sender.record_send(channel(1), &ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    let receipt = receiver.report();
    receiver.release(ops[0].prefix).unwrap();
    let release = receiver.report();
    assert!(sender.observe(release).unwrap());
    for report in [receipt, release, initial, release] {
        assert!(!sender.observe(report).unwrap());
    }
    let next = operations(ops[1].prefix, &[20, 20, 20]);
    sender.record_send(channel(1), &next).unwrap();
    assert_eq!(
        sender.record_send(channel(1), &operations(next[2].prefix, &[1])),
        Err(FlowError::Capacity)
    );
}

#[test]
fn delayed_receipt_accounts_for_data_sent_after_the_snapshot() {
    let (mut receiver, mut sender) = pair();
    let ops = operations(Prefix::GENESIS, &[20, 20, 20, 20]);
    sender.record_send(channel(1), &ops[..2]).unwrap();
    receiver.retain(channel(1), &ops[..1]).unwrap();
    receiver.release(ops[0].prefix).unwrap();
    let delayed = receiver.report();
    sender.record_send(channel(1), &ops[2..]).unwrap();
    sender.observe(delayed).unwrap();
    assert_eq!(sender.outstanding().count(), 3);
    let next = operations(ops[3].prefix, &[20, 1]);
    sender.record_send(channel(1), &next[..1]).unwrap();
    assert_eq!(
        sender.record_send(channel(1), &next[1..]),
        Err(FlowError::Capacity)
    );
}

#[test]
fn count_and_bytes_are_independent_atomic_admission_limits() {
    for sizes in [&[1, 1, 1, 1, 1][..], &[51, 50][..]] {
        let (mut receiver, mut sender) = pair();
        let before = receiver.report();
        let ops = operations(Prefix::GENESIS, sizes);
        assert_eq!(
            sender.record_send(channel(1), &ops),
            Err(FlowError::Capacity)
        );
        assert_eq!(receiver.retain(channel(1), &ops), Err(FlowError::Capacity));
        assert_eq!(receiver.report(), before);
        assert_eq!(sender.outstanding().count(), 0);
        let valid = operations(Prefix::GENESIS, &[100]);
        sender.record_send(channel(1), &valid).unwrap();
        receiver.retain(channel(1), &valid).unwrap();
    }
}

#[test]
fn rejected_history_and_counters_do_not_partially_change_state() {
    let (mut receiver, mut sender) = pair();
    let before = receiver.report();
    let mut ops = operations(Prefix::GENESIS, &[10, 10]);
    ops[1].previous_digest = Digest::ZERO;
    assert_eq!(
        sender.record_send(channel(1), &ops),
        Err(FlowError::History)
    );
    assert_eq!(receiver.retain(channel(1), &ops), Err(FlowError::History));
    assert_eq!(sender.outstanding().count(), 0);
    assert_eq!(receiver.report(), before);
    let mut overflow = operations(Prefix::GENESIS, &[u64::MAX, 1]);
    assert_eq!(
        sender.record_send(channel(1), &overflow),
        Err(FlowError::Exhausted)
    );
    assert_eq!(
        receiver.retain(channel(1), &overflow),
        Err(FlowError::Exhausted)
    );
    overflow[0].body_bytes = 0;
    assert_eq!(
        sender.record_send(channel(1), &overflow),
        Err(FlowError::Invalid)
    );
    assert_eq!(receiver.report(), before);
}

#[test]
fn receipt_must_match_sent_prefix_digest_and_cumulative_bytes() {
    let (mut receiver, mut sender) = pair();
    let ops = operations(Prefix::GENESIS, &[10, 20]);
    sender.record_send(channel(1), &ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    let valid = receiver.report();
    let mut bad = valid;
    bad.received.digest = Digest::from_bytes([99; 32]);
    assert_eq!(sender.observe(bad), Err(FlowError::History));
    bad = valid;
    bad.received_bytes += 1;
    assert_eq!(sender.observe(bad), Err(FlowError::History));
    bad = valid;
    bad.received.op.0 += 1;
    assert!(sender.observe(bad).is_err());
    assert_eq!(sender.outstanding().count(), 2);
    assert!(sender.observe(valid).unwrap());
}

#[test]
fn epoch_and_view_changes_require_explicit_reopening() {
    let (mut receiver, mut sender) = pair();
    let ops = operations(Prefix::GENESIS, &[10]);
    sender.record_send(channel(1), &ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    sender.observe(receiver.report()).unwrap();
    let mut restarted = Receiver::new(channel(2), Prefix::GENESIS, limits()).unwrap();
    assert_eq!(sender.observe(restarted.report()), Err(FlowError::Channel));
    assert_eq!(restarted.retain(channel(1), &ops), Err(FlowError::Channel));
    let mut fresh = Sender::open(restarted.report(), limits(), Prefix::GENESIS).unwrap();
    assert_eq!(fresh.observe(receiver.report()), Err(FlowError::Channel));
    let mut later_view = channel(2);
    later_view.scope.view += 1;
    assert_eq!(fresh.record_send(later_view, &ops), Err(FlowError::Channel));
    fresh.record_send(channel(2), &ops).unwrap();
    restarted.retain(channel(2), &ops).unwrap();
}

#[test]
fn replacement_epoch_charges_retained_state_before_open() {
    let mut receiver = Receiver::new(channel(2), Prefix::GENESIS, limits()).unwrap();
    let retained = operations(Prefix::GENESIS, &[30, 30, 30]);
    receiver.retain(channel(2), &retained).unwrap();
    assert!(Sender::open(receiver.report(), limits(), Prefix::GENESIS).is_err());
    let mut sender = Sender::open(receiver.report(), limits(), retained[2].prefix).unwrap();
    let next = operations(retained[2].prefix, &[11]);
    assert_eq!(
        sender.record_send(channel(2), &next),
        Err(FlowError::Capacity)
    );
}

#[test]
fn release_requires_exact_retained_prefix_and_is_idempotent() {
    let (mut receiver, _) = pair();
    let ops = operations(Prefix::GENESIS, &[10, 20]);
    receiver.retain(channel(1), &ops).unwrap();
    let before = receiver.report();
    assert_eq!(
        receiver.release(Prefix {
            digest: Digest::ZERO,
            ..ops[0].prefix
        }),
        Err(FlowError::History)
    );
    assert_eq!(
        receiver.release(operations(ops[1].prefix, &[1])[0].prefix),
        Err(FlowError::History)
    );
    assert_eq!(receiver.report(), before);
    receiver.release(ops[0].prefix).unwrap();
    let after = receiver.report();
    receiver.release(ops[0].prefix).unwrap();
    assert_eq!(receiver.report(), after);
    assert_eq!(after.byte_limit, 110);
    assert_eq!(after.operation_limit, 5);
}

#[test]
fn changed_duplicate_and_retracted_reports_are_rejected_atomically() {
    let (mut receiver, mut sender) = pair();
    let ops = operations(Prefix::GENESIS, &[10, 20]);
    sender.record_send(channel(1), &ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    sender.observe(receiver.report()).unwrap();
    receiver.release(ops[0].prefix).unwrap();
    sender.observe(receiver.report()).unwrap();
    let current = receiver.report();
    let mut bad = current;
    bad.byte_limit -= 1; // Same revision, different content.
    assert_eq!(sender.observe(bad), Err(FlowError::Report));
    bad.revision += 1; // A newer revision cannot retract credit either.
    assert_eq!(sender.observe(bad), Err(FlowError::Report));
    bad = current;
    bad.revision += 1;
    bad.received = ops[0].prefix;
    bad.received_bytes = 10;
    assert_eq!(sender.observe(bad), Err(FlowError::Report));
    assert!(!sender.observe(current).unwrap());
    assert_eq!(sender.received(), ops[1].prefix);
}

#[test]
fn duplicate_data_never_consumes_another_reservation() {
    let (mut receiver, mut sender) = pair();
    let ops = operations(Prefix::GENESIS, &[10]);
    sender.record_send(channel(1), &ops).unwrap();
    receiver.retain(channel(1), &ops).unwrap();
    let before = receiver.report();
    assert_eq!(
        sender.record_send(channel(1), &ops),
        Err(FlowError::History)
    );
    assert_eq!(receiver.retain(channel(1), &ops), Err(FlowError::History));
    assert_eq!(receiver.report(), before);
    assert_eq!(sender.outstanding().count(), 1);
}

#[test]
fn another_peers_credit_exhaustion_does_not_block_healthy_delivery() {
    let (mut receiver, mut healthy) = pair();
    let (_, mut stalled) = pair();
    let ops = operations(Prefix::GENESIS, &[25; 64]);
    for (index, op) in ops.iter().enumerate() {
        healthy
            .record_send(channel(1), std::slice::from_ref(op))
            .unwrap();
        receiver
            .retain(channel(1), std::slice::from_ref(op))
            .unwrap();
        receiver.release(op.prefix).unwrap();
        healthy.observe(receiver.report()).unwrap();
        if index < 4 {
            stalled
                .record_send(channel(1), std::slice::from_ref(op))
                .unwrap();
        }
    }
    assert_eq!(healthy.received(), ops[63].prefix);
    assert_eq!(stalled.sent(), ops[3].prefix);
    assert_eq!(
        stalled.record_send(channel(1), &ops[4..5]),
        Err(FlowError::Capacity)
    );
}

#[test]
fn seeded_loss_reorder_and_release_schedules_preserve_credit_bounds() {
    // Ledger composition only, not the pending production probe/repair scheduler.
    for seed in 1..=256u64 {
        let (mut receiver, mut sender) = pair();
        let ops = operations(Prefix::GENESIS, &[20; 64]);
        let mut reports = Vec::with_capacity(8);
        let mut data = Vec::with_capacity(8);
        let mut applied = 0;
        let mut rng = seed;
        for _ in 0..1024 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let sent = sender.sent().op.0 as usize;
            let retained_count = receiver.report().received.op.0 as usize;
            match rng % 6 {
                0 if sent < ops.len() && data.len() < 8 => {
                    if sender.record_send(channel(1), &ops[sent..=sent]).is_ok() {
                        data.push(ops[sent]);
                    }
                }
                1 if !data.is_empty() => {
                    let op = data.swap_remove((rng as usize / 6) % data.len());
                    if op.prefix.op.0 == retained_count as u64 + 1 {
                        receiver.retain(channel(1), &[op]).unwrap();
                    } // Drop an out-of-order packet; sender reservation remains.
                }
                2 if reports.len() < 8 => reports.push(receiver.report()),
                3 if !reports.is_empty() => {
                    let report = reports.swap_remove((rng as usize / 6) % reports.len());
                    sender.observe(report).unwrap();
                }
                4 if applied < retained_count => {
                    receiver.release(ops[applied].prefix).unwrap();
                    applied += 1;
                }
                5 if retained_count < sent => {
                    // Harness requests the missing suffix, reusing its original charge.
                    receiver
                        .retain(channel(1), &ops[retained_count..=retained_count])
                        .unwrap();
                }
                _ => {}
            }
            let report = receiver.report();
            assert_eq!(report.operation_limit, applied as u64 + 4);
            assert_eq!(report.byte_limit, applied as u64 * 20 + 100);
            assert!(report.received.op.0 - applied as u64 <= 4);
            assert!(report.received_bytes - applied as u64 * 20 <= 100);
            assert!(sender.outstanding().count() <= 4);
        }
        // Restore ordered communication and application; no stale queue has authority.
        for _ in 0..128 {
            let retained_count = receiver.report().received.op.0 as usize;
            if applied < retained_count {
                receiver.release(ops[retained_count - 1].prefix).unwrap();
                applied = retained_count;
            }
            sender.observe(receiver.report()).unwrap();
            let sent = sender.sent().op.0 as usize;
            if sent < ops.len() {
                let _ = sender.record_send(channel(1), &ops[sent..=sent]);
            }
            if retained_count < sender.sent().op.0 as usize {
                receiver
                    .retain(channel(1), &ops[retained_count..=retained_count])
                    .unwrap();
            }
        }
        assert_eq!(applied, ops.len(), "seed {seed} failed to drain");
    }
}
