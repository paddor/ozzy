use super::*;
use crate::{Digest, OpNumber};
use ozzy_journal::operation::{CanonicalOperation, OperationKind, canonical_body_digest};
use ozzy_proto::{GroupId, NodeId};

fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([1; 16]),
        1,
        NodeId::from_bytes([2; 16]),
        Digest::from_bytes([3; 32]),
    )
    .unwrap()
}

fn driver(generation: u128) -> Driver {
    Driver::recover(
        configuration(),
        JournalGeneration(generation),
        Prefix::GENESIS,
        PipelineLimits {
            max_operations: 4,
            max_body_bytes: 64,
        },
    )
    .unwrap()
}

fn operation(predecessor: Prefix, id: u8) -> PreparedOperation {
    let body = [id; 16];
    let operation = CanonicalOperation {
        group_id: configuration().scope().group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: predecessor.op.0 + 1,
        previous_digest: predecessor.digest,
        kind: OperationKind::Barrier,
        body: &body,
    };
    PreparedOperation::from_verified(&operation, canonical_body_digest(operation.body))
}

#[test]
fn local_configuration_rejects_every_corrupted_byte_and_group_records() {
    let configuration = configuration();
    let bytes = configuration.encode();
    assert_eq!(Configuration::decode(&bytes).unwrap(), configuration);
    for index in 0..bytes.len() {
        let mut corrupted = bytes;
        corrupted[index] ^= 1;
        assert!(Configuration::decode(&corrupted).is_err(), "byte {index}");
    }
    assert!(crate::ConfigurationRecord::decode(&bytes).is_err());
    let group = crate::ConfigurationRecord::new(
        configuration.scope().group_id,
        1,
        std::array::from_fn(|index| crate::ConfiguredVoter {
            node_id: NodeId::from_bytes([index as u8 + 1; 16]),
            principal: Digest::from_bytes([index as u8 + 10; 32]),
        }),
    )
    .unwrap();
    assert!(Configuration::decode(&group.encode()).is_err());
    assert_ne!(
        configuration.scope().configuration_digest,
        group.configuration().scope().configuration_digest
    );
}

#[test]
fn local_confirmation_requires_captured_durability_then_application() {
    let mut owner = driver(1);
    let first = operation(Prefix::GENESIS, 1);
    let second = operation(first.prefix(), 2);
    let validation = owner.begin_validation().unwrap();
    let write = owner.prepare_validated(validation, &[first]).unwrap();
    assert_eq!(owner.snapshot().committed, Prefix::GENESIS);
    assert_eq!(
        owner.apply_through(first.prefix()),
        Err(Error::ApplyBeyondDurable)
    );
    owner.complete_write(write).unwrap();
    assert_eq!(owner.snapshot().committed, Prefix::GENESIS);
    let sync = owner.begin_sync().unwrap();
    let write2 = owner
        .prepare_validated(owner.begin_validation().unwrap(), &[second])
        .unwrap();
    owner.complete_write(write2).unwrap();
    owner.complete_sync(sync).unwrap();
    assert_eq!(owner.snapshot().committed, first.prefix());
    assert_eq!(owner.snapshot().applied, Prefix::GENESIS);
    assert_eq!(owner.snapshot().pending_operations, 2);
    assert_eq!(
        owner.apply_through(second.prefix()),
        Err(Error::ApplyBeyondDurable)
    );
    owner.apply_through(first.prefix()).unwrap();
    assert_eq!(owner.snapshot().pending_operations, 1);
    owner.complete_sync(owner.begin_sync().unwrap()).unwrap();
    owner.apply_through(second.prefix()).unwrap();
    owner.complete_sync(sync).unwrap();
    assert_eq!(owner.snapshot().committed, second.prefix());
    assert_eq!(owner.snapshot().pending_body_bytes, 0);
    assert_eq!(owner.snapshot().pending_operations, 0);
}

#[test]
fn local_stale_validation_reordered_and_foreign_completions_never_confirm() {
    let mut owner = driver(1);
    let first = operation(Prefix::GENESIS, 1);
    let second = operation(first.prefix(), 2);
    let old = owner.begin_validation().unwrap();
    let write = owner.prepare_validated(old, &[first]).unwrap();
    assert_eq!(
        owner.prepare_validated(old, &[second]),
        Err(Error::StaleValidation)
    );
    let write2 = owner
        .prepare_validated(owner.begin_validation().unwrap(), &[second])
        .unwrap();
    assert_eq!(
        owner.complete_write(write2),
        Err(Error::Journal(ProgressError::WriteGap))
    );
    assert_eq!(owner.snapshot().journal.written, OpNumber(0));
    owner.complete_write(write).unwrap();
    let sync = owner.begin_sync().unwrap();
    owner.complete_sync(sync).unwrap();
    let mut restarted = Driver::recover(
        configuration(),
        JournalGeneration(2),
        first.prefix(),
        PipelineLimits {
            max_operations: 4,
            max_body_bytes: 64,
        },
    )
    .unwrap();
    assert_eq!(
        restarted.complete_write(write2),
        Err(Error::Journal(ProgressError::StaleGeneration))
    );
    assert_eq!(
        restarted.complete_sync(sync),
        Err(Error::Journal(ProgressError::StaleGeneration))
    );
    assert_eq!(
        restarted.prepare_validated(old, &[second]),
        Err(Error::StaleValidation)
    );
    assert_eq!(restarted.snapshot().committed, first.prefix());
    owner.fail(JournalGeneration(1)).unwrap();
    assert_eq!(
        owner.begin_validation(),
        Err(Error::Journal(ProgressError::Faulted))
    );
    assert_eq!(
        owner.apply_through(first.prefix()),
        Err(Error::Journal(ProgressError::Faulted))
    );
}

#[test]
fn local_rejections_preserve_state_and_application_releases_bounded_capacity() {
    let mut owner = driver(1);
    let mut ops = Vec::new();
    let mut end = Prefix::GENESIS;
    for id in 1..=4 {
        let next = operation(end, id);
        end = next.prefix();
        ops.push(next);
    }
    let before = owner.snapshot();
    let mut invalid = ops.clone();
    invalid[3].previous_digest = Digest::ZERO;
    assert_eq!(
        owner.prepare_validated(owner.begin_validation().unwrap(), &invalid),
        Err(Error::Lineage)
    );
    assert_eq!(owner.snapshot(), before);
    let write = owner
        .prepare_validated(owner.begin_validation().unwrap(), &ops)
        .unwrap();
    let next = operation(end, 5);
    assert_eq!(
        owner.prepare_validated(owner.begin_validation().unwrap(), &[next]),
        Err(Error::Capacity)
    );
    owner.complete_write(write).unwrap();
    owner.complete_sync(owner.begin_sync().unwrap()).unwrap();
    assert_eq!(
        owner.prepare_validated(owner.begin_validation().unwrap(), &[next]),
        Err(Error::Capacity)
    );
    owner.apply_through(ops[1].prefix()).unwrap();
    owner
        .prepare_validated(owner.begin_validation().unwrap(), &[next])
        .unwrap();
    assert_eq!(owner.snapshot().pending_operations, 3);
    assert_eq!(owner.snapshot().pending_body_bytes, 48);
}
