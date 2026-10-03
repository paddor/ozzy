use ozzy_core::state::{IdentityIndex, IdentityKey, StateLimits};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Barrier, CreatePartition, OpenProducer, OperationBody,
    RetentionPolicy, encode_operation_body,
};
use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    CanonicalOperation, CanonicalRecoveryLimits, ChainPosition, DecodeLimits, Digest,
    GroupDirectory, GroupIdentity, LogPosition, MetadataLimits, OpenGroupJournal, OperationLimits,
    SegmentHeader, WriterPosition,
};
use ozzy_proto::{
    GroupId, MessageId, NodeId, Offset, OperationId, OwnerEpoch, PartitionId, PartitionIncarnation,
    ProducerEpoch, ProducerId, ProducerSequence, StoreId, VolumeId,
};
use tempfile::TempDir;

fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([0x10; 16]),
        replica_node_id: NodeId::from_bytes([0x11; 16]),
        volume_id: VolumeId::from_bytes([0x12; 16]),
        store_id: StoreId::from_bytes([0x13; 16]),
        store_generation: 1,
    }
}

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([0x20; 16])
}

fn producer() -> ProducerId {
    ProducerId::from_bytes([0x30; 16])
}

fn bodies() -> [OperationBody<'static>; 3] {
    [
        OperationBody::CreatePartition(CreatePartition {
            partition: partition(),
            stream: "stream",
            topic: "topic",
            partition_id: PartitionId::new(7),
            owner_epoch: OwnerEpoch::new(9),
            retention: RetentionPolicy::default(),
        }),
        OperationBody::OpenProducer(OpenProducer {
            partition: partition(),
            producer_id: producer(),
            expected_epoch: None,
            new_epoch: ProducerEpoch::new(3),
            operation_id: OperationId::from_bytes([0x40; 16]),
        }),
        OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: partition(),
                owner_epoch: OwnerEpoch::new(9),
                producer_id: producer(),
                producer_epoch: ProducerEpoch::new(3),
                first_sequence: ProducerSequence::new(0),
                first_offset: Offset::ZERO,
                append_timestamp_millis: 123,
                records: vec![AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([0x50; 16]),
                    parts: vec![b"header".as_slice(), b"payload".as_slice()].into(),
                }]
                .into(),
            }],
        }),
    ]
}

fn append_body(
    journal: &mut OpenGroupJournal,
    group_id: GroupId,
    op_number: u64,
    previous_digest: Digest,
    body: &OperationBody<'_>,
) -> WriterPosition {
    let encoded = encode_operation_body(body, OperationLimits::default()).unwrap();
    journal
        .append(&[CanonicalOperation {
            group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number,
            previous_digest,
            kind: body.kind(),
            body: &encoded,
        }])
        .unwrap()
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "linear storage recovery and post-recovery admission lifecycle"
)]
fn segment_replay_reconstructs_committed_and_accepted_images() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let identity = identity();
    let header = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 20 * 1024).unwrap();
    let directory = GroupDirectory::format_new(&root, identity, 1, &header).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();

    let mut chain = ChainPosition::GENESIS;
    let mut positions = Vec::new();
    for (index, body) in bodies().iter().enumerate() {
        let op_number = index as u64 + 1;
        let position = append_body(
            &mut journal,
            identity.group_id,
            op_number,
            chain.previous_digest(),
            body,
        );
        chain = position.next_chain();
        positions.push(position);
    }
    journal.sync_through(positions[2]).unwrap();

    let mut manifest = journal.directory().manifest().clone();
    manifest.generation += 1;
    manifest.parent_generation = journal.directory().manifest().generation;
    manifest.accepted = LogPosition {
        op_number: 3,
        digest: positions[2].next_chain().previous_digest(),
    };
    manifest.committed = LogPosition {
        op_number: 2,
        digest: positions[1].next_chain().previous_digest(),
    };
    let journal = journal.install_metadata(manifest).unwrap();

    let mut simulation = journal
        .recover_canonical_images(CanonicalRecoveryLimits {
            state: StateLimits::default(),
            retained_identities: 16,
            accepted_transitions: 16,
            ..CanonicalRecoveryLimits::default()
        })
        .unwrap();

    assert_eq!(simulation.committed().revision(), 2);
    assert_eq!(simulation.speculative().revision(), 3);
    assert_eq!(
        simulation
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::ZERO
    );
    assert_eq!(
        simulation
            .speculative()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::new(1)
    );
    simulation.commit_through(3).unwrap();
    assert_eq!(simulation.committed(), simulation.speculative());

    let mut tiny_overlay = journal
        .recover_canonical_images(CanonicalRecoveryLimits {
            state: StateLimits::default(),
            retained_identities: 1,
            accepted_transitions: 16,
            ..CanonicalRecoveryLimits::default()
        })
        .unwrap();
    assert_eq!(tiny_overlay.speculative().revision(), 3);
    assert!(tiny_overlay.committed_identities().overlay().is_empty());
    assert!(tiny_overlay.speculative_identities().overlay().is_empty());
    assert!(
        tiny_overlay
            .committed_identities()
            .lookup(IdentityKey::operation(OperationId::from_bytes([0x40; 16])))
            .unwrap()
            .is_some()
    );
    tiny_overlay.commit_through(3).unwrap();
    let fourth = OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::new(9),
            producer_id: producer(),
            producer_epoch: ProducerEpoch::new(3),
            first_sequence: ProducerSequence::new(1),
            first_offset: Offset::new(1),
            append_timestamp_millis: 124,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([0x51; 16]),
                parts: vec![b"next".as_slice()].into(),
            }]
            .into(),
        }],
    });
    tiny_overlay.admit(4, &fourth).unwrap();
    let fifth = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([0x52; 16]),
    });
    tiny_overlay.admit(5, &fifth).unwrap();
    let sixth = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([0x53; 16]),
    });
    assert!(tiny_overlay.admit(6, &sixth).is_err());

    drop(tiny_overlay);
    drop(simulation);
    drop(journal);
    let reopened = GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
    assert_eq!(reopened.manifest().accepted.op_number, 3);
}
