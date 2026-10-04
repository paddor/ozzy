#[path = "support/replication.rs"]
mod model;

use model::{Cluster, Error};
use ozzy_core::state::{CanonicalImagesError, StateError};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Barrier, CreatePartition, OpenProducer, OperationBody,
    RetentionPolicy,
};
use ozzy_proto::{
    MessageId, Offset, OperationId, OwnerEpoch, PartitionId, PartitionIncarnation, ProducerEpoch,
    ProducerId, ProducerSequence,
};
use ozzy_replication::{OpNumber, ReplicationError, Status};

fn cluster() -> Cluster {
    Cluster::new(256, 8, 8192, 32)
}

fn barrier(value: u128) -> OperationBody<'static> {
    OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes(value.to_be_bytes()),
    })
}

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([10; 16])
}

fn create() -> OperationBody<'static> {
    OperationBody::CreatePartition(CreatePartition {
        partition: partition(),
        stream: "events",
        topic: "orders",
        partition_id: PartitionId::new(0),
        owner_epoch: OwnerEpoch::new(1),
        retention: RetentionPolicy::default(),
    })
}

fn open() -> OperationBody<'static> {
    OperationBody::OpenProducer(OpenProducer {
        partition: partition(),
        producer_id: ProducerId::from_bytes([11; 16]),
        expected_epoch: None,
        new_epoch: ProducerEpoch::new(1),
        operation_id: OperationId::from_bytes([12; 16]),
    })
}

fn append(sequence: u64) -> OperationBody<'static> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::new(1),
            producer_id: ProducerId::from_bytes([11; 16]),
            producer_epoch: ProducerEpoch::new(1),
            first_sequence: ProducerSequence::new(sequence),
            first_offset: Offset::new(sequence),
            append_timestamp_millis: 123,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes(u128::from(sequence + 100).to_be_bytes()),
                parts: vec![
                    b"order.created".as_slice(),
                    b"{\"sku\":\"sku-42\",\"qty\":3}".as_slice(),
                ]
                .into(),
            }]
            .into(),
        }],
    })
}

#[test]
fn records_become_visible_only_after_durable_quorum_and_application() {
    let mut sim = cluster();
    sim.propose(&[create(), open(), append(0), append(1)])
        .unwrap();
    sim.send_prepare(1, 0, 4).unwrap();
    sim.deliver(0).unwrap();
    for replica in &sim.replicas[..2] {
        assert!(replica.images.committed().partition(partition()).is_none());
        assert_eq!(
            replica
                .images
                .speculative()
                .partition(partition())
                .unwrap()
                .next_offset,
            Offset::new(2)
        );
    }
    // Backup can finish first while the primary has not even started writing.
    sim.replicas[1].drain_disk();
    sim.send_ack(1).unwrap();
    sim.deliver(0).unwrap();
    assert_eq!(sim.replicas[0].core.snapshot().committed.op, OpNumber(0));
    sim.replicas[0].drain_disk();
    assert_eq!(sim.replicas[0].core.snapshot().committed.op, OpNumber(4));
    assert!(
        sim.replicas[0]
            .images
            .committed()
            .partition(partition())
            .is_none()
    );
    sim.replicas[0].apply().unwrap();
    assert_eq!(
        sim.replicas[0]
            .images
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::new(2)
    );
    assert!(
        sim.replicas[1]
            .images
            .committed()
            .partition(partition())
            .is_none()
    );
    sim.check();
    sim.heal();
}

#[test]
fn physical_writes_and_lost_sync_callbacks_cannot_cast_durable_votes() {
    let mut sim = cluster();
    sim.propose(&[barrier(1), barrier(2)]).unwrap();
    for backup in 1..3 {
        sim.send_prepare(backup, 0, 2).unwrap();
        sim.deliver(0).unwrap();
    }
    for replica in &mut sim.replicas {
        replica.write(1).unwrap();
        assert_eq!(replica.notify_write(), Err(Error::NotWritten));
        assert_eq!(replica.core.snapshot().journal.written, OpNumber(0));
        replica.write(1).unwrap();
        assert_eq!(replica.core.snapshot().journal.written, OpNumber(0));
        replica.notify_write().unwrap();
        let sync = replica.core.begin_sync().unwrap();
        assert_eq!(replica.notify_sync(sync), Err(Error::NotPersisted));
        replica.persist(sync).unwrap();
        assert_eq!(replica.core.snapshot().journal.durable, OpNumber(0));
    }
    for backup in 1..3 {
        sim.send_ack(backup).unwrap();
        sim.deliver(0).unwrap();
    }
    assert_eq!(sim.replicas[0].core.snapshot().committed.op, OpNumber(0));
    sim.check();
    sim.heal();
}

#[test]
fn delayed_barrier_completion_does_not_cover_later_appends() {
    let mut sim = cluster();
    sim.propose(&[barrier(1)]).unwrap();
    sim.replicas[0].write(1).unwrap();
    sim.replicas[0].notify_write().unwrap();
    let first = sim.replicas[0].core.begin_sync().unwrap();
    sim.propose(&[barrier(2)]).unwrap();
    sim.replicas[0].write(1).unwrap();
    sim.replicas[0].notify_write().unwrap();
    sim.send_prepare(1, 0, 2).unwrap();
    sim.deliver(0).unwrap();
    sim.replicas[1].drain_disk();
    sim.send_ack(1).unwrap();
    sim.deliver(0).unwrap();
    sim.replicas[0].persist(first).unwrap();
    sim.replicas[0].notify_sync(first).unwrap();
    assert_eq!(sim.replicas[0].core.snapshot().committed.op, OpNumber(1));
    assert_eq!(sim.replicas[0].stable.len(), 1);
    sim.check();
    sim.heal();
}

#[test]
fn prepare_gaps_duplicates_and_missing_commits_recover_by_bounded_retry() {
    let mut sim = cluster();
    sim.propose(&[barrier(1), barrier(2), barrier(3)]).unwrap();
    sim.send_prepare(1, 0, 2).unwrap();
    sim.send_prepare(1, 2, 1).unwrap();
    assert!(sim.deliver(1).is_err());
    assert_eq!(sim.replicas[1].accepted.len(), 0);
    sim.duplicate(0).unwrap();
    sim.deliver(1).unwrap();
    sim.deliver(0).unwrap();
    assert_eq!(sim.replicas[1].accepted.len(), 2);
    sim.replicas[0].drain_disk();
    sim.replicas[1].drain_disk();
    sim.send_ack(1).unwrap();
    sim.duplicate(0).unwrap();
    sim.deliver(0).unwrap();
    sim.deliver(0).unwrap();
    sim.replicas[0].apply().unwrap();
    sim.send_commit(1).unwrap();
    sim.drop_packet(0);
    assert_eq!(sim.replicas[1].core.snapshot().applied.op, OpNumber(0));
    sim.check();
    sim.heal();
}

#[test]
fn one_crashed_backup_does_not_block_the_healthy_quorum() {
    let mut sim = cluster();
    sim.replicas[2].power_cut();
    for round in 0..20 {
        sim.propose(&[barrier(round + 1)]).unwrap();
        sim.heal();
    }
    assert_eq!(sim.replicas[0].core.snapshot().applied.op, OpNumber(20));
    assert_eq!(sim.replicas[2].stable.len(), 0);
}

#[test]
fn lagging_backup_replays_more_than_pipeline_from_committed_journal() {
    let mut sim = cluster();
    for round in 0..24 {
        sim.propose(&[barrier(round as u128 + 1)]).unwrap();
        sim.send_prepare(1, round, 1).unwrap();
        sim.deliver(0).unwrap();
        sim.replicas[0].drain_disk();
        sim.replicas[1].drain_disk();
        sim.send_ack(1).unwrap();
        sim.deliver(0).unwrap();
        sim.replicas[0].apply().unwrap();
        sim.send_commit(1).unwrap();
        sim.deliver(0).unwrap();
        sim.replicas[1].apply().unwrap();
        sim.check();
    }
    assert_eq!(sim.replicas[2].accepted.len(), 0);
    assert_eq!(sim.replicas[0].core.snapshot().pending_operations, 0);
    // Twenty-four operations cannot fit the eight-operation pipeline. Each
    // replay chunk carries a committed prefix sourced from the primary's log.
    sim.heal();
    assert_eq!(sim.replicas[2].core.snapshot().applied.op, OpNumber(24));
}

#[test]
fn isolated_primary_cannot_commit_and_healing_restores_progress() {
    let mut sim = cluster();
    sim.propose(&[barrier(1)]).unwrap();
    sim.replicas[0].drain_disk();
    sim.links[0][1] = false;
    sim.links[0][2] = false;
    for backup in 1..3 {
        sim.send_prepare(backup, 0, 1).unwrap();
        sim.deliver(0).unwrap();
        sim.send_ack(backup).unwrap();
        sim.deliver(0).unwrap();
    }
    assert_eq!(sim.replicas[0].core.snapshot().committed.op, OpNumber(0));
    sim.check();
    sim.heal();
}

#[test]
fn primary_crash_after_reply_before_commit_keeps_quorum_prepared_bytes() {
    let mut sim = cluster();
    sim.propose(&[create(), open(), append(0)]).unwrap();
    sim.send_prepare(1, 0, 3).unwrap();
    sim.deliver(0).unwrap();
    sim.replicas[0].drain_disk();
    sim.replicas[1].drain_disk();
    sim.send_ack(1).unwrap();
    sim.deliver(0).unwrap();
    sim.replicas[0].apply().unwrap();
    let acknowledged = sim.replicas[0].stable.clone();
    sim.replicas[0].power_cut();
    assert_eq!(sim.replicas[1].stable, acknowledged);
    assert_eq!(sim.replicas[1].core.snapshot().committed.op, OpNumber(0));
    assert_eq!(sim.replicas[2].stable.len(), 0);
    assert_eq!(sim.propose(&[append(1)]), Err(Error::Offline));
    sim.check();
    // This is preservation evidence, NOT failover. View selection must later
    // retain these bytes even though the surviving backup calls them uncommitted.
}

#[test]
fn fencing_blocks_delayed_votes_but_allows_pending_disk_to_settle() {
    let mut sim = cluster();
    sim.propose(&[barrier(1)]).unwrap();
    sim.send_prepare(1, 0, 1).unwrap();
    sim.deliver(0).unwrap();
    sim.replicas[1].drain_disk();
    sim.send_ack(1).unwrap();
    sim.replicas[0].core.fence();
    sim.replicas[0].drain_disk();
    assert_eq!(
        sim.deliver(0),
        Err(Error::Replication(ReplicationError::NotNormal))
    );
    assert_eq!(
        sim.replicas[0].apply(),
        Err(Error::Replication(ReplicationError::NotNormal))
    );
    assert_eq!(sim.replicas[0].core.snapshot().journal.durable, OpNumber(1));
    assert_eq!(sim.replicas[0].core.snapshot().committed.op, OpNumber(0));
    sim.check();
}

#[test]
fn uncertain_sync_error_never_turns_physical_bytes_into_client_success() {
    let mut sim = cluster();
    sim.propose(&[barrier(1)]).unwrap();
    sim.send_prepare(1, 0, 1).unwrap();
    sim.deliver(0).unwrap();
    sim.replicas[1].drain_disk();
    sim.send_ack(1).unwrap();
    sim.deliver(0).unwrap();
    let primary = &mut sim.replicas[0];
    primary.write(1).unwrap();
    primary.notify_write().unwrap();
    let sync = primary.core.begin_sync().unwrap();
    primary.persist(sync).unwrap();
    primary.core.fail_io(sync.generation()).unwrap();
    assert!(primary.notify_sync(sync).is_err());
    assert_eq!(primary.core.snapshot().status, Status::Faulted);
    assert_eq!(primary.core.snapshot().committed.op, OpNumber(0));
    sim.check();
}

#[test]
fn application_validation_precedes_replication_admission_and_disk_work() {
    let mut sim = cluster();
    sim.propose(&[create(), open()]).unwrap();
    let before = sim.replicas[0].core.snapshot();
    assert_eq!(
        sim.propose(&[append(1)]),
        Err(Error::Application(CanonicalImagesError::State(
            StateError::ProducerSequenceMismatch
        )))
    );
    assert_eq!(sim.replicas[0].core.snapshot(), before);
    assert_eq!(sim.replicas[0].accepted.len(), 2);
    let mut malformed = sim.replicas[0].accepted.clone();
    malformed[1].body.pop();
    assert!(matches!(
        sim.replicas[1].admit(&malformed),
        Err(Error::Codec(_))
    ));
    assert_eq!(sim.replicas[1].core.snapshot().accepted.op, OpNumber(0));
    assert_eq!(sim.replicas[1].images.speculative().revision(), 0);
    sim.heal();
}

#[test]
fn operation_byte_network_and_retained_history_bounds_reject_atomically() {
    let mut sim = Cluster::new(2, 1, 16, 1);
    sim.propose(&[barrier(1)]).unwrap();
    let before = sim.replicas[0].core.snapshot();
    assert!(sim.propose(&[barrier(2)]).is_err());
    assert_eq!(sim.replicas[0].core.snapshot(), before);
    sim.send_prepare(1, 0, 1).unwrap();
    assert_eq!(sim.duplicate(0), Err(Error::Capacity));
    assert_eq!(sim.queued(), 1);
    sim.heal();
    sim.propose(&[barrier(2)]).unwrap();
    sim.heal();
    assert_eq!(sim.propose(&[barrier(3)]), Err(Error::Capacity));
    assert!(sim.propose(&[create()]).is_err());
    sim.check();
}

#[test]
fn seeded_network_and_disk_schedules_preserve_safety_then_converge() {
    // Reproducible burst faults, then fair delivery. Never silently swallow an
    // unexpected error; the allowed rejection classes are explicit below.
    for seed in 1..=64u64 {
        let mut rng = seed;
        let mut sim = cluster();
        sim.propose(&[create(), open()]).unwrap();
        sim.heal();
        for burst in 0..8 {
            for sequence in burst * 4..burst * 4 + 4 {
                sim.propose(&[append(sequence)]).unwrap();
            }
            let first = 2 + burst as usize * 4;
            for step in 0..96 {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let index = (rng as usize / 16) % 3;
                let result = match rng % 12 {
                    0 => sim.send_prepare(1 + index % 2, first + step % 4, 1),
                    1 => sim.send_prepare(1 + index % 2, first, 4),
                    2 => sim.replicas[index].write(1),
                    3 => sim.replicas[index].notify_write(),
                    4 => {
                        let ticket = sim.replicas[index].core.begin_sync().unwrap();
                        sim.replicas[index].persist(ticket)
                    }
                    5 => {
                        let ticket = sim.replicas[index].core.begin_sync().unwrap();
                        sim.replicas[index].notify_sync(ticket)
                    }
                    6 => sim.send_ack(1 + index % 2),
                    7 => sim.send_commit(1 + index % 2),
                    8 if sim.queued() > 0 => sim.deliver(rng as usize % sim.queued()),
                    9 if sim.queued() > 0 => sim.duplicate(rng as usize % sim.queued()),
                    10 if sim.queued() > 0 => {
                        sim.drop_packet(rng as usize % sim.queued());
                        Ok(())
                    }
                    _ => sim.replicas[index].apply(),
                };
                assert!(
                    matches!(
                        result,
                        Ok(())
                            | Err(Error::Capacity
                                | Error::NotWritten
                                | Error::NotPersisted
                                | Error::Replication(
                                    ReplicationError::HistoryGap
                                        | ReplicationError::HistoryUnavailable
                                )
                                | Error::Application(CanonicalImagesError::State(
                                    StateError::OperationNumberMismatch
                                )))
                    ),
                    "seed={seed} burst={burst} step={step} event={}: {result:?}",
                    rng % 12
                );
                sim.check();
            }
            sim.heal();
        }
        assert_eq!(
            sim.replicas[0]
                .images
                .committed()
                .partition(partition())
                .unwrap()
                .next_offset,
            Offset::new(32)
        );
    }
}
