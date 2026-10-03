use ozzy_core::state::{CanonicalState, StateError, StateLimits};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Barrier, CreatePartition, OpenProducer, OperationBody,
    RetentionPolicy,
};
use ozzy_proto::{
    MessageId, Offset, OperationId, OwnerEpoch, PartitionId, PartitionIncarnation, ProducerEpoch,
    ProducerId, ProducerSequence,
};
use ozzy_sim::canonical::{CanonicalRecovery, CanonicalSimulation, CanonicalSimulationError};

#[path = "canonical_state/shared.rs"]
mod shared;

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([0x10; 16])
}

fn producer() -> ProducerId {
    ProducerId::from_bytes([0x20; 16])
}

fn create() -> OperationBody<'static> {
    OperationBody::CreatePartition(CreatePartition {
        partition: partition(),
        stream: "stream",
        topic: "topic",
        partition_id: PartitionId::new(7),
        owner_epoch: OwnerEpoch::new(9),
        retention: RetentionPolicy::default(),
    })
}

fn open() -> OperationBody<'static> {
    OperationBody::OpenProducer(OpenProducer {
        partition: partition(),
        producer_id: producer(),
        expected_epoch: None,
        new_epoch: ProducerEpoch::new(3),
        operation_id: OperationId::from_bytes([0x30; 16]),
    })
}

fn append(first: u64, message: u8) -> OperationBody<'static> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::new(9),
            producer_id: producer(),
            producer_epoch: ProducerEpoch::new(3),
            first_sequence: ProducerSequence::new(first),
            first_offset: Offset::new(first),
            append_timestamp_millis: 123,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([message; 16]),
                parts: vec![b"payload".as_slice()].into(),
            }]
            .into(),
        }],
    })
}

fn simulation(pending_capacity: usize) -> CanonicalSimulation {
    CanonicalSimulation::new(StateLimits::default(), 16, pending_capacity)
}

#[test]
fn accepted_state_is_invisible_until_ordered_commit() {
    let mut sim = simulation(8);
    sim.admit(1, &create()).unwrap();
    sim.admit(2, &open()).unwrap();
    sim.admit(3, &append(0, 0x40)).unwrap();

    assert_eq!(
        sim.committed(),
        &CanonicalState::new(StateLimits::default())
    );
    assert_eq!(sim.speculative().revision(), 3);
    assert_eq!(
        sim.speculative()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::new(1)
    );

    sim.commit_through(2).unwrap();
    assert_eq!(sim.committed().revision(), 2);
    assert_eq!(sim.pending_len(), 1);
    assert_eq!(
        sim.committed().partition(partition()).unwrap().next_offset,
        Offset::ZERO
    );
    sim.commit_through(3).unwrap();
    assert_eq!(sim.committed(), sim.speculative());
    assert_eq!(sim.committed_identities(), sim.speculative_identities());
    assert_eq!(sim.pending_len(), 0);
    assert_eq!(
        sim.commit_through(4),
        Err(CanonicalSimulationError::CommitBeyondAccepted)
    );
    assert_eq!(
        sim.commit_through(2),
        Err(CanonicalSimulationError::CommitRegression)
    );
}

#[test]
fn suffix_replacement_is_atomic_and_releases_speculative_identities() {
    let mut sim = simulation(8);
    sim.admit(1, &create()).unwrap();
    sim.admit(2, &open()).unwrap();
    sim.commit_through(2).unwrap();
    sim.admit(3, &append(0, 0x40)).unwrap();
    sim.admit(
        4,
        &OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes([0x50; 16]),
        }),
    )
    .unwrap();

    let before = sim.clone();
    assert_eq!(
        sim.replace_suffix(&[(3, append(1, 0x41))]),
        Err(CanonicalSimulationError::State(
            StateError::ProducerSequenceMismatch
        ))
    );
    assert_eq!(sim, before);

    sim.replace_suffix(&[
        (3, append(0, 0x41)),
        (
            4,
            OperationBody::Barrier(Barrier {
                operation_id: OperationId::from_bytes([0x51; 16]),
            }),
        ),
    ])
    .unwrap();
    assert_eq!(sim.pending_len(), 2);
    assert_ne!(sim, before);
    sim.discard_suffix();
    assert_eq!(sim.committed(), sim.speculative());
    assert_eq!(sim.committed_identities(), sim.speculative_identities());
    sim.admit(3, &append(0, 0x40)).unwrap();
}

#[test]
fn pending_capacity_backpressures_before_state_mutation() {
    let mut sim = simulation(1);
    sim.admit(1, &create()).unwrap();
    let before = sim.clone();
    assert_eq!(
        sim.admit(2, &open()),
        Err(CanonicalSimulationError::PendingCapacity)
    );
    assert_eq!(sim, before);

    sim.commit_through(1).unwrap();
    sim.admit(2, &open()).unwrap();
}

#[test]
fn recovery_is_fail_closed_and_requires_a_committed_prefix() {
    let mut recovery = CanonicalRecovery::new(StateLimits::default(), 16, 8);
    recovery.apply(1, &create(), true).unwrap();
    recovery.apply(2, &open(), false).unwrap();
    assert_eq!(
        recovery.apply(3, &append(0, 0x40), true),
        Err(CanonicalSimulationError::CommittedAfterAccepted)
    );
    assert_eq!(
        recovery.finish(),
        Err(CanonicalSimulationError::RecoveryFaulted)
    );

    let mut invalid = CanonicalRecovery::new(StateLimits::default(), 16, 8);
    assert_eq!(
        invalid.apply(2, &create(), true),
        Err(CanonicalSimulationError::State(
            StateError::OperationNumberMismatch
        ))
    );
    assert_eq!(
        invalid.apply(1, &create(), true),
        Err(CanonicalSimulationError::RecoveryFaulted)
    );
}
