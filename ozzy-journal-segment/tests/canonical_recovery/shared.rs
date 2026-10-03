use super::*;
use ozzy_journal_segment::{IndexBuildLimits, JournalIndexBoundary};

fn other_producer() -> ProducerId {
    ProducerId::from_bytes([0x31; 16])
}

fn open_other() -> OperationBody<'static> {
    let OperationBody::OpenProducer(mut body) = open() else {
        unreachable!()
    };
    body.producer_id = other_producer();
    body.operation_id = OperationId::from_bytes([0x41; 16]);
    OperationBody::OpenProducer(body)
}

fn writer_record(writer: ProducerId, sequence: u64, offset: u64) -> OperationBody<'static> {
    let OperationBody::Append(mut body) = append(offset as u8 + 1, sequence, offset) else {
        unreachable!()
    };
    body.batches[0].producer_id = writer;
    OperationBody::Append(body)
}

#[test]
fn interleaved_writer_results_recover_from_replay_and_checkpoint_plus_tail() {
    for checkpoint in [false, true] {
        let temporary = TempDir::new().unwrap();
        let mut journal = format(&temporary);
        let mut state = CanonicalState::new(StateLimits::default());
        let mut identities = MemoryIdentityIndex::new(16);
        let mut chain = ChainPosition::GENESIS;
        for body in [create(), open(), open_other()] {
            chain = append_body(&mut journal, chain, &body);
            apply_body(&mut state, &mut identities, &body);
        }
        journal = journal.roll_active(64 * 1024).unwrap();
        let assignments = [
            (producer(), 0, 0),
            (other_producer(), 0, 1),
            (producer(), 1, 2),
            (other_producer(), 1, 3),
            (producer(), 2, 4),
            (producer(), 3, 5),
        ];
        for (writer, sequence, offset) in assignments {
            let body = writer_record(writer, sequence, offset);
            chain = append_body(&mut journal, chain, &body);
            apply_body(&mut state, &mut identities, &body);
            if checkpoint && offset == 2 {
                journal = publish(journal, chain);
                let id = CheckpointId::from_bytes([0x60; 16]);
                journal
                    .canonical_checkpoint_plan(id, 1024)
                    .unwrap()
                    .build_canonical(
                        &state,
                        StateSnapshotLimits::default(),
                        CheckpointLimits::default(),
                    )
                    .unwrap();
                journal = journal
                    .install_canonical_checkpoint(
                        id,
                        StateLimits::default(),
                        StateSnapshotLimits::default(),
                        CheckpointLimits::default(),
                    )
                    .unwrap();
                journal = journal.roll_active(64 * 1024).unwrap();
            }
        }
        drop(publish(journal, chain));
        let reopened = GroupDirectory::open(
            temporary.path().join("group"),
            identity(),
            MetadataLimits::default(),
        )
        .unwrap()
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
        let images = reopened
            .recover_canonical_images(CanonicalRecoveryLimits::default())
            .unwrap();
        assert_eq!(images.committed(), &state);
        let index = reopened
            .build_index_snapshot(JournalIndexBoundary::Committed, IndexBuildLimits::default())
            .unwrap();
        for (writer, sequence, offset) in assignments {
            let original = images
                .committed()
                .partition(partition())
                .unwrap()
                .producer(writer)
                .unwrap()
                .result_offset(ProducerSequence::new(sequence))
                .unwrap();
            assert_eq!(original, Offset::new(offset));
            let (record, _) = index
                .read_offset_with_position(partition(), original)
                .unwrap()
                .unwrap();
            assert_eq!(record.producer_id, writer);
            assert_eq!(record.producer_sequence, ProducerSequence::new(sequence));
            assert_eq!(
                record.message_id,
                MessageId::from_bytes([offset as u8 + 1; 16])
            );
            assert_eq!(record.parts[0].as_ref(), b"payload");
        }
    }
}
