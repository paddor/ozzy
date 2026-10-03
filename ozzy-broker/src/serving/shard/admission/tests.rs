use super::*;
use ozzy_proto::GroupId;
use ozzy_replication::flow::{Channel, FlowError, Operation, ReceiveEpoch, Receiver};
use ozzy_replication::{Digest, OpNumber, PipelineLimits, Prefix, Scope};

mod fixture;
use fixture::{fixture, placement};

#[test]
fn canonical_demand_grows_without_extending_unchanged_transport_credit() {
    let mut f = fixture();
    f.actors.minimum_body_bytes = 128;
    f.finish();
    f.commands();
    f.begin().unwrap();
    let before = f.actors.receiver.report();
    assert!(before.byte_limit >= 128);
    f.actors.minimum_body_bytes = 512;
    f.finish();
    f.commands();
    f.begin().unwrap();
    let after = f.actors.receiver.report();
    assert_eq!(after.channel, before.channel);
    assert!(after.byte_limit >= 512);
    assert!(after.byte_limit > before.byte_limit);
}

#[test]
fn settled_replica_packet_preserves_its_backed_receive_epoch() {
    let mut f = fixture();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let report = f.actors.receiver.report();
    let promised = f.data.claimed_capacity();
    for _ in 0..3 {
        let charge = f.admission.buffers.maximum_retained_bytes();
        for _ in 0..2 {
            f.service
                .receive(f.prepare(report.channel), charge)
                .unwrap();
            drop(f.dequeue());
        }
        // The advertised tail has arrived. Its unused window stays backed.
        f.actors.demand = false;
        f.finish();
        f.commands();
        f.begin().unwrap();
        assert_eq!(f.actors.receiver.report(), report);
        assert_eq!(f.data.claimed_capacity(), promised);
        assert_eq!(f.admission.intake.spent().count(), 0);
    }
}

#[test]
fn deferred_replica_packet_does_not_replenish_until_actor_work_settles() {
    let mut f = fixture();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let report = f.actors.receiver.report();
    let charge = f.admission.buffers.maximum_retained_bytes();
    f.service
        .receive(f.prepare(report.channel), charge)
        .unwrap();
    let message = f.dequeue();
    let slot = f.admission.intake.spent().next().unwrap().0;
    f.admission.intake.defer(message).unwrap();
    f.finish();
    assert_eq!(f.actors.receiver.report(), report);
    assert!(f.admission.intake.spent().next().is_none());
    let message = f
        .admission
        .intake
        .retry_deferred(slot, &f.binding.links, &f.routes)
        .unwrap()
        .unwrap();
    drop(message);
    f.actors.replica_busy = true;
    f.finish();
    assert_eq!(f.admission.intake.spent().count(), 1);
    f.service
        .receive(f.prepare(report.channel), charge)
        .unwrap();
    assert!(
        f.service
            .receive(f.prepare(report.channel), charge)
            .is_err()
    );
    let second = f.dequeue();
    f.admission.intake.defer(second).unwrap();
    f.actors.replica_busy = false;
    let message = f
        .admission
        .intake
        .retry_deferred(slot, &f.binding.links, &f.routes)
        .unwrap()
        .unwrap();
    drop(message);
    f.finish();
    f.begin().unwrap();
    assert_eq!(f.actors.receiver.report(), report);
    f.service
        .receive(f.prepare(report.channel), charge)
        .unwrap();
    assert!(f.dequeue().current);
}

#[test]
fn credited_replica_packets_share_a_backed_dispatch_window() {
    let mut f = fixture();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let report = f.actors.receiver.report();
    assert!(report.operation_limit >= 2);
    let charge = f.admission.buffers.maximum_retained_bytes();
    f.service
        .receive(f.prepare(report.channel), charge)
        .unwrap();
    f.service
        .receive(f.prepare(report.channel), charge)
        .unwrap();
    assert!(f.dequeue().current);
    assert!(f.dequeue().current);
}

#[test]
fn tight_shard_capacity_falls_back_to_one_replica_packet() {
    let mut f = fixture();
    let held = f.data.try_lease(120 * 1024).unwrap();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let channel = f.actors.receiver.report().channel;
    let charge = f.admission.buffers.maximum_retained_bytes();
    f.service.receive(f.prepare(channel), charge).unwrap();
    assert!(f.service.receive(f.prepare(channel), charge).is_err());
    drop(f.dequeue());
    drop(held);
    f.finish();
    f.begin().unwrap();
    f.service.receive(f.prepare(channel), charge).unwrap();
}

#[test]
fn partial_replica_window_can_grow_canonical_credit() {
    let mut f = fixture();
    f.actors.minimum_body_bytes = 128;
    f.finish();
    f.commands();
    f.begin().unwrap();
    let report = f.actors.receiver.report();
    let charge = f.admission.buffers.maximum_retained_bytes();
    f.service
        .receive(f.prepare(report.channel), charge)
        .unwrap();
    drop(f.dequeue());
    f.actors.minimum_body_bytes = 512;
    f.finish();
    f.commands();
    f.begin().unwrap();
    assert!(f.actors.receiver.report().byte_limit > report.byte_limit);
    f.service
        .receive(f.prepare(report.channel), charge)
        .unwrap();
    assert!(f.dequeue().current);
}

#[test]
fn fenced_replica_window_keeps_queued_packet_until_dequeue() {
    let mut f = fixture();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let channel = f.actors.receiver.report().channel;
    let charge = f.admission.buffers.maximum_retained_bytes();
    for _ in 0..2 {
        f.service.receive(f.prepare(channel), charge).unwrap();
    }
    drop(f.dequeue());
    assert!(f.service.disconnect(f.broker));
    f.begin().unwrap();
    f.finish();
    assert!(f.admission.intake.has_spent());
    assert!(!f.dequeue().current);
    f.finish();
    assert!(!f.admission.intake.has_spent());
}

#[test]
fn replica_replenishment_keeps_aliases_charged_and_waits_for_memory() {
    let mut f = fixture();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let report = f.actors.receiver.report();
    let charge = f.admission.buffers.maximum_retained_bytes();
    f.service
        .receive(f.prepare(report.channel), charge)
        .unwrap();
    let message = f.dequeue();
    let alias = message.message.part_bytes(3).unwrap().slice(..1);
    drop(message);
    f.service
        .receive(f.prepare(report.channel), charge)
        .unwrap();
    drop(f.dequeue());
    let mut held = Vec::new();
    while let Ok(lease) = f.data.try_lease(4096) {
        held.push(lease);
    }
    f.finish();
    assert_eq!(f.actors.receiver.report(), report);
    assert_eq!(f.admission.intake.spent().count(), 1);
    drop(held);
    f.finish();
    assert_eq!(f.admission.intake.spent().count(), 0);
    let charged = f.data.claimed_capacity();
    drop(alias);
    assert_eq!(charged.bytes - f.data.claimed_capacity().bytes, charge);
    assert_eq!(f.actors.receiver.report(), report);
}

#[test]
fn consumed_replica_slot_cannot_replenish_an_obsolete_epoch() {
    let mut f = fixture();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let stale = f.actors.receiver.report().channel;
    let charge = f.admission.buffers.maximum_retained_bytes();
    f.service.receive(f.prepare(stale), charge).unwrap();
    drop(f.dequeue());
    f.actors
        .receiver
        .revoke_unused(ReceiveEpoch::new(2).unwrap())
        .unwrap();
    f.begin().unwrap();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let fresh = f.actors.receiver.report().channel;
    assert_ne!(fresh, stale);
    assert!(f.service.receive(f.prepare(stale), charge).is_err());
    f.service.receive(f.prepare(fresh), charge).unwrap();
    assert!(f.dequeue().current);
}

#[test]
fn stale_epoch_packet_cannot_spend_replacement_dispatch_credit() {
    let mut f = fixture();
    let stale = f.actors.receiver.report().channel;
    f.actors
        .receiver
        .revoke_unused(ReceiveEpoch::new(2).unwrap())
        .unwrap();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let fresh = f.actors.receiver.report();
    let promised = f.data.claimed_capacity();
    let charge = f.admission.buffers.maximum_retained_bytes();
    assert!(matches!(
        f.service.receive(f.prepare(stale), charge),
        Err(frontend::ReceiveError::Dispatch(
            frontend::Rejection::Fence(_)
        ))
    ));
    assert!(
        f.admission
            .intake
            .receive(&f.binding.links, &f.routes)
            .unwrap()
            .is_none()
    );
    f.finish();
    f.begin().unwrap();
    assert_eq!(f.actors.receiver.report(), fresh);
    assert_eq!(f.data.claimed_capacity(), promised);
    f.service.receive(f.prepare(fresh.channel), charge).unwrap();
    let received = f.dequeue();
    assert!(received.current);
    assert_eq!(received.request.route.class, Class::Data);
}

#[test]
fn epoch_change_during_installation_cannot_advertise_an_old_dispatch_fence() {
    let mut f = fixture();
    f.finish();
    f.actors
        .receiver
        .revoke_unused(ReceiveEpoch::new(2).unwrap())
        .unwrap();
    f.commands();
    f.begin().unwrap();
    assert_eq!(f.actors.grants, 0);
    assert!(f.admission.channels.is_empty());
    assert_eq!(
        f.data.claimed_capacity(),
        ozzy_runtime::memory::Quota::default()
    );
    f.finish();
    f.commands();
    f.begin().unwrap();
    assert_eq!(f.actors.grants, 1);
    assert_eq!(
        f.actors.receiver.report().channel.epoch,
        ReceiveEpoch::new(2).unwrap()
    );
    let charge = f.admission.buffers.maximum_retained_bytes();
    f.service
        .receive(f.prepare(f.actors.receiver.report().channel), charge)
        .unwrap();
    assert!(f.dequeue().current);
}

#[test]
fn wrong_view_and_configuration_cannot_spend_current_follower_credit() {
    let mut f = fixture();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let fresh = f.actors.receiver.report();
    let promised = f.data.claimed_capacity();
    let mut stale = [fresh.channel; 3];
    stale[0].scope.view += 1;
    stale[1].scope.configuration_epoch += 1;
    stale[2].scope.configuration_digest = Digest::from_bytes([8; 32]);
    let charge = f.admission.buffers.maximum_retained_bytes();
    for channel in stale {
        assert!(matches!(
            f.service.receive(f.prepare(channel), charge),
            Err(frontend::ReceiveError::Dispatch(
                frontend::Rejection::Fence(_)
            ))
        ));
    }
    f.finish();
    f.begin().unwrap();
    assert_eq!(f.actors.receiver.report(), fresh);
    assert_eq!(f.data.claimed_capacity(), promised);
    assert!(f.binding.requests.next_request().is_none());
    f.service.receive(f.prepare(fresh.channel), charge).unwrap();
    assert!(f.dequeue().current);
}

#[test]
fn protocol_credit_waits_for_dispatcher_installation() {
    let mut f = fixture();
    f.finish();
    let reserved = f.data.claimed_capacity();
    assert!(reserved.bytes > 0);
    f.finish();
    f.begin().unwrap();
    assert_eq!(f.actors.grants, 0);
    assert_eq!(f.actors.receiver.report().operation_limit, 0);
    assert_eq!(f.data.claimed_capacity(), reserved);
    f.commands();
    f.begin().unwrap();
    assert_eq!(f.actors.grants, 1);
    assert_eq!(f.actors.receiver.report().operation_limit, 64);
    assert_eq!(f.actors.receiver.report().byte_limit, 1024);
}

#[test]
fn stale_history_response_and_normal_packets_cannot_spend_selected_history_credit() {
    let mut f = fixture();
    let request = f.history_request();
    f.actors.history = Some(request);
    f.finish();
    f.commands();
    f.begin().unwrap();
    let promised = f.data.claimed_capacity();
    let charge = f.admission.buffers.maximum_retained_bytes();
    let mut stale = [request; 2];
    stale[0].request_id = ozzy_proto::RequestId::from_bytes([9; 16]);
    stale[1].source.generation = ozzy_replication::JournalGeneration(2);
    for old in stale {
        assert!(matches!(
            f.service.receive(f.history(old), charge),
            Err(frontend::ReceiveError::Dispatch(
                frontend::Rejection::Fence(_)
            ))
        ));
    }
    assert!(matches!(
        f.service
            .receive(f.prepare(f.actors.receiver.report().channel), charge),
        Err(frontend::ReceiveError::Dispatch(
            frontend::Rejection::Fence(_)
        ))
    ));
    f.finish();
    f.begin().unwrap();
    assert_eq!(f.actors.grants, 0);
    assert_eq!(f.data.claimed_capacity(), promised);
    assert!(f.binding.requests.next_request().is_none());
    f.service.receive(f.history(request), charge).unwrap();
    assert!(f.dequeue().current);
}

#[test]
fn history_change_during_installation_cannot_back_a_different_request() {
    let mut f = fixture();
    let mut request = f.history_request();
    f.actors.history = Some(request);
    f.finish();
    request.request_id = ozzy_proto::RequestId::from_bytes([9; 16]);
    f.actors.history = Some(request);
    f.commands();
    f.begin().unwrap();
    assert!(f.admission.channels.is_empty());
    assert_eq!(
        f.data.claimed_capacity(),
        ozzy_runtime::memory::Quota::default()
    );
    f.finish();
    f.commands();
    f.begin().unwrap();
    let charge = f.admission.buffers.maximum_retained_bytes();
    f.service.receive(f.history(request), charge).unwrap();
    assert!(f.dequeue().current);
}

#[test]
fn full_command_lane_rolls_back_both_promises_before_retry() {
    let mut f = fixture();
    let mut pending = Vec::new();
    for _ in 0..4 {
        pending.push(
            f.binding
                .port
                .try_route(ozzy_proto::directory::RouteState {
                    group: placement(0).group,
                    config_epoch: 1,
                    partition: placement(0).partition,
                    members: [ozzy_proto::NodeId::from_bytes([9; 16])].into(),
                    view: 0,
                    leader: Some(ozzy_proto::NodeId::from_bytes([9; 16])),
                })
                .unwrap(),
        );
    }
    f.finish();
    f.begin().unwrap();
    assert_eq!(f.actors.grants, 0);
    assert_eq!(
        f.data.claimed_capacity(),
        ozzy_runtime::memory::Quota::default()
    );
    f.commands();
    drop(pending);
    f.commands();
    f.finish();
    f.commands();
    f.begin().unwrap();
    assert_eq!(f.actors.grants, 1);
}

#[test]
fn changed_source_during_installation_cannot_publish_credit() {
    let mut f = fixture();
    f.finish();
    f.actors.peer = ozzy_proto::NodeId::from_bytes([3; 16]);
    f.commands();
    f.begin().unwrap();
    assert_eq!(f.actors.grants, 0);
    assert!(f.admission.channels.is_empty());
    assert_eq!(
        f.data.claimed_capacity(),
        ozzy_runtime::memory::Quota::default()
    );
}

#[test]
fn epoch_change_retires_old_promise_before_backing_new_credit() {
    let mut f = fixture();
    f.finish();
    f.commands();
    f.begin().unwrap();
    let next = Channel {
        epoch: ReceiveEpoch::new(2).unwrap(),
        ..f.actors.receiver.report().channel
    };
    f.actors.receiver.revoke_unused(next.epoch).unwrap();
    f.begin().unwrap();
    assert!(f.admission.channels.is_empty());
    assert_eq!(
        f.data.claimed_capacity(),
        ozzy_runtime::memory::Quota::default()
    );
    f.finish();
    assert_eq!(f.actors.receiver.report().operation_limit, 0);
    f.commands();
    f.begin().unwrap();
    assert_eq!(f.actors.receiver.report().channel, next);
    assert_eq!(f.actors.receiver.report().operation_limit, 64);
}

#[test]
fn failed_protocol_fence_retains_canonical_allowance_until_retry() {
    let mut f = fixture();
    f.finish();
    f.commands();
    f.begin().unwrap();
    f.actors.fail_revoke = true;
    let reserved = f.admission.partitions[0].1.capacity().remaining().bytes;
    assert!(f.service.disconnect(f.broker));
    assert!(f.begin().is_err());
    assert_eq!(
        f.admission.partitions[0].1.capacity().remaining().bytes,
        reserved
    );
    assert_eq!(f.actors.receiver.report().operation_limit, 64);
    f.actors.fail_revoke = false;
    f.begin().unwrap();
    assert_eq!(
        f.data.claimed_capacity(),
        ozzy_runtime::memory::Quota::default()
    );
    assert!(f.admission.channels.is_empty());
    assert_eq!(f.actors.receiver.report().operation_limit, 0);
}

#[test]
fn two_writers_settle_and_renew_independently() {
    let mut f = fixture();
    f.actors.demand = false;
    for writer in [4, 5] {
        f.request_writer(0, writer);
    }
    f.finish();
    f.commands();
    f.begin().unwrap();
    for writer in [4, 5] {
        f.service.receive(f.writer(0, writer), 4096).unwrap();
        drop(f.dequeue());
    }
    f.actors
        .busy
        .insert(ozzy_proto::ProducerId::from_bytes([4; 16]));
    f.finish();
    let spent: Vec<_> = f
        .admission
        .intake
        .spent()
        .map(|(_, _, request)| request.route.writer)
        .collect();
    assert_eq!(spent, [Some(ozzy_proto::ProducerId::from_bytes([4; 16]))]);
    f.commands();
    f.begin().unwrap();
    let idle: Vec<_> = f
        .admission
        .intake
        .idle_grants(&f.binding.links, 0, 16)
        .map(|(request, _)| request.route.writer)
        .collect();
    assert_eq!(idle, [Some(ozzy_proto::ProducerId::from_bytes([5; 16]))]);
    assert!(f.client_data.capacity().remaining().bytes > 2048);
}

#[test]
fn busy_partition_does_not_block_another_partitions_admission() {
    let mut f = fixture();
    f.actors.demand = false;
    f.request_writer(0, 4);
    f.finish();
    f.commands();
    f.begin().unwrap();
    f.service.receive(f.writer(0, 4), 4096).unwrap();
    drop(f.dequeue());
    f.actors
        .busy
        .insert(ozzy_proto::ProducerId::from_bytes([4; 16]));

    f.request_writer(1, 5);
    f.finish();
    f.commands();
    f.begin().unwrap();
    f.service.receive(f.writer(1, 5), 4096).unwrap();
    let received = f.dequeue();
    assert!(received.current);
    assert_eq!(received.request.route.placement, placement(1));
    drop(received);
    f.finish();
    assert_eq!(f.admission.intake.spent().count(), 1);
}

#[test]
fn exhausted_data_memory_preserves_broker_control_progress() {
    let mut f = fixture();
    f.actors.demand = false;
    let held = f.data.try_lease(131_072).unwrap();
    f.request_writer(0, 4);
    let probe = ozzy_replication::wire::FlowProbe {
        scope: f.actors.receiver.report().channel.scope,
        request_id: ozzy_proto::RequestId::from_bytes([6; 16]),
        tail: Prefix::GENESIS,
        available: OpNumber(1),
        minimum_body_bytes: 1,
    };
    let mut metadata = [0; 136];
    let encoded = ozzy_replication::wire::encode_flow_probe(
        f.broker.peer,
        f.broker.session,
        probe,
        &mut metadata,
        ozzy_replication::wire::WireLimits::default(),
    )
    .unwrap();
    let message = omq_tokio::Message::multipart([
        bytes::Bytes::copy_from_slice(f.broker.peer.as_bytes()),
        bytes::Bytes::copy_from_slice(&encoded.header),
        bytes::Bytes::copy_from_slice(&metadata),
        bytes::Bytes::new(),
    ]);
    assert!(matches!(
        f.service.receive(message.clone(), 4096),
        Err(frontend::ReceiveError::Dispatch(
            frontend::Rejection::NoGrant
        ))
    ));
    f.finish();
    f.commands();
    f.begin().unwrap();
    assert_eq!(
        f.client_data.capacity().remaining(),
        ozzy_runtime::memory::Quota::default()
    );
    f.service.receive(message, 4096).unwrap();
    let received = f.dequeue();
    assert!(received.current);
    assert_eq!(received.request.route.class, Class::Control);
    drop(received);
    f.finish();
    assert_eq!(f.actors.grants, 0);
    drop(held);
}

#[test]
fn changed_history_request_retires_its_old_backing() {
    let mut f = fixture();
    let history = ozzy_replication::wire::FetchOps {
        scope: f.actors.receiver.report().channel.scope,
        request_id: ozzy_proto::RequestId::from_bytes([8; 16]),
        source: ozzy_replication::LogSource {
            voter: f.broker.peer,
            generation: ozzy_replication::JournalGeneration(1),
            accepted: Prefix::GENESIS,
        },
        predecessor: Prefix::GENESIS,
        max_operations: 1,
        max_body_bytes: 1024,
    };
    f.actors.history = Some(history);
    f.finish();
    f.commands();
    f.begin().unwrap();
    assert_eq!(
        f.admission.channels[&placement(0).group].1,
        ReceivePurpose::History(history)
    );
    f.actors.history = Some(ozzy_replication::wire::FetchOps {
        request_id: ozzy_proto::RequestId::from_bytes([9; 16]),
        ..history
    });
    f.begin().unwrap();
    assert!(f.admission.channels.is_empty());
    assert_eq!(
        f.data.claimed_capacity(),
        ozzy_runtime::memory::Quota::default()
    );
    assert_eq!(f.actors.grants, 0);
}

#[test]
fn writer_settlement_keeps_a_retained_payload_alias_charged() {
    let mut f = fixture();
    f.actors.demand = false;
    f.request_writer(0, 4);
    f.finish();
    f.commands();
    f.begin().unwrap();
    f.service.receive(f.writer(0, 4), 4096).unwrap();
    let received = f.dequeue();
    let alias = received.message.part_bytes(3).unwrap().slice(..1);
    drop(received);
    f.finish();
    assert_eq!(f.admission.intake.spent().count(), 0);
    let held = f.data.claimed_capacity();
    drop(alias);
    assert_eq!(held.bytes - f.data.claimed_capacity().bytes, 4096);
}

#[test]
fn released_records_enlarge_an_idle_follower_window() {
    let channel = Channel {
        scope: Scope {
            group_id: GroupId::from_bytes([1; 16]),
            configuration_epoch: 1,
            configuration_digest: Digest::from_bytes([2; 32]),
            view: 1,
        },
        epoch: ReceiveEpoch::new(1).unwrap(),
    };
    let mut receiver = Receiver::new_reserved(
        channel,
        Prefix::GENESIS,
        PipelineLimits {
            max_operations: 64,
            max_body_bytes: 1024,
        },
    )
    .unwrap();
    let older = Operation {
        prefix: Prefix {
            op: OpNumber(1),
            digest: Digest::from_bytes([3; 32]),
        },
        previous_digest: Prefix::GENESIS.digest,
        body_bytes: 896,
    };
    receiver.grant(channel, 1, older.body_bytes).unwrap();
    receiver.retain(channel, &[older]).unwrap();
    receiver.grant(channel, 63, 128).unwrap();
    assert_eq!(
        receive_grant(receiver.report(), receiver.available(), 1024, 0),
        None
    );

    receiver.release(older.prefix).unwrap();
    let next = Operation {
        prefix: Prefix {
            op: OpNumber(2),
            digest: Digest::from_bytes([4; 32]),
        },
        previous_digest: older.prefix.digest,
        body_bytes: 512,
    };
    assert_eq!(receiver.retain(channel, &[next]), Err(FlowError::Capacity));
    let (operations, bytes) =
        receive_grant(receiver.report(), receiver.available(), 1024, 0).unwrap();
    receiver.grant(channel, operations, bytes).unwrap();
    receiver.retain(channel, &[next]).unwrap();
    assert_eq!(receiver.report().received, next.prefix);
}

#[test]
fn idle_follower_refill_preserves_shared_backing_and_independent_counts() {
    let report = Receiver::new_reserved(
        Channel {
            scope: Scope {
                group_id: GroupId::from_bytes([1; 16]),
                configuration_epoch: 1,
                configuration_digest: Digest::from_bytes([2; 32]),
                view: 1,
            },
            epoch: ReceiveEpoch::new(1).unwrap(),
        },
        Prefix::GENESIS,
        PipelineLimits {
            max_operations: 64,
            max_body_bytes: 1024,
        },
    )
    .unwrap()
    .report();
    let report = ozzy_replication::flow::Report {
        operation_limit: 64,
        byte_limit: 128,
        ..report
    };
    let available = PipelineLimits {
        max_operations: 0,
        max_body_bytes: 896,
    };
    assert_eq!(receive_grant(report, available, 128, 0), None);
    assert_eq!(receive_grant(report, available, 256, 0), Some((0, 128)));
    assert_eq!(receive_grant(report, available, 4096, 0), Some((0, 896)));
}

#[test]
fn follower_refills_count_and_byte_windows_only_at_their_low_watermarks() {
    let mut report = fixture().actors.receiver.report();
    report.operation_limit = 64;
    report.received = Prefix {
        op: OpNumber(31),
        digest: Digest::from_bytes([4; 32]),
    };
    report.received_bytes = 31;
    report.byte_limit = 1024;
    let available = PipelineLimits {
        max_operations: 32,
        max_body_bytes: 1024,
    };
    assert_eq!(receive_grant(report, available, 1024, 0), None);

    report.received.op = OpNumber(32);
    report.received_bytes = 32;
    assert_eq!(receive_grant(report, available, 1024, 0), Some((32, 0)));

    report.operation_limit = 96;
    report.received_bytes = 511;
    assert_eq!(receive_grant(report, available, 1024, 0), None);
    assert_eq!(
        receive_grant(report, available, 1024, 514),
        Some((0, 511)),
        "a larger next operation must not stall above the low watermark",
    );
    report.received_bytes = 512;
    assert_eq!(receive_grant(report, available, 1024, 0), Some((0, 512)));
    assert_eq!(
        receive_grant(
            report,
            PipelineLimits {
                max_operations: 0,
                max_body_bytes: 0
            },
            1024,
            514,
        ),
        None,
        "a low watermark cannot create unbacked capacity",
    );
}
