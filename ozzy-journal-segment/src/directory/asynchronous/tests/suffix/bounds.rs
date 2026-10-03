use super::*;
use ozzy_journal::operation::{
    CreatePartition, OperationBody, RetentionPolicy, encode_operation_body,
};
use ozzy_proto::{OwnerEpoch, PartitionId, PartitionIncarnation};

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "three physical/decoded boundary cases share one fixture"
)]
fn async_suffix_splits_only_at_operation_boundaries_and_obeys_decoded_segment_limits() {
    for case in 0..3 {
        if case == 2 && !cfg!(feature = "lz4") {
            continue;
        }
        let (mut controller, mut journal) = empty_journal();
        let io = journal.access.io.clone();
        journal.limits.operations.max_name_bytes = 4096;
        let count = if case == 2 {
            4
        } else if case == 1 {
            1
        } else {
            2
        };
        let name = "orders".repeat(match case {
            0 => 160,
            1 => 320,
            _ => 500,
        });
        if case == 2 {
            journal.limits.decode.max_decoded_body_bytes = 8192;
            journal.limits.decode.max_group_decoded_body_bytes = 8192;
            journal.limits.decode.max_segment_decoded_body_bytes = 8192;
        }
        let bounds = journal.limits;
        let bodies = (1..=count)
            .map(|id| {
                encode_operation_body(
                    &OperationBody::CreatePartition(CreatePartition {
                        partition: PartitionIncarnation::from_bytes([id; 16]),
                        stream: &name,
                        topic: &name,
                        partition_id: PartitionId::new(u32::from(id)),
                        owner_epoch: OwnerEpoch::INITIAL,
                        retention: RetentionPolicy::default(),
                    }),
                    bounds.operations,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let mut previous = LogPosition::GENESIS;
        let ops = bodies
            .iter()
            .map(|body| {
                let op = CanonicalOperation {
                    group_id: identity().group_id,
                    configuration_epoch: 1,
                    original_view: 1,
                    op_number: previous.op_number + 1,
                    previous_digest: previous.digest,
                    kind: OperationKind::CreatePartition,
                    body,
                };
                previous = position(op);
                op
            })
            .collect::<Vec<_>>();
        let mut replacement = request(&journal, previous);
        if case == 2 {
            replacement.body_encoding = BodyEncoding::Lz4 {
                min_savings_bytes: 0,
            };
        }
        let mut staging = drive(
            &mut controller,
            journal.begin_suffix_replacement(
                replacement,
                previous,
                SuffixStreamLimits {
                    max_group_body_bytes: 8192,
                    ..stream_limits()
                },
            ),
        )
        .unwrap();
        if case == 1 {
            assert!(matches!(
                drive(&mut controller, staging.append_chunk(&ops)),
                Err(Error::Writer(WriterError::Codec(
                    CodecError::GroupExceedsSegment
                )))
            ));
            assert!(matches!(
                drive(&mut controller, staging.finish()),
                Err(Error::StagingFaulted)
            ));
            let journal = drive(&mut controller, open(io, 12)).unwrap();
            assert_eq!(journal.current(), replacement.expected_current);
            continue;
        }
        for chunk in ops.chunks(if case == 2 { 1 } else { 2 }) {
            drive(&mut controller, staging.append_chunk(chunk)).unwrap();
        }
        let journal = drive(&mut controller, staging.finish()).unwrap();
        assert_eq!(journal.manifest.segments.len(), count as usize);
        assert_eq!(journal.committed_position().unwrap(), previous);
        drop(journal);
        let journal = drive(
            &mut controller,
            Journal::open(
                "/group".into(),
                io,
                identity(),
                Some(CONFIG),
                JournalGeneration(12),
                bounds,
            ),
        )
        .unwrap();
        assert_eq!(journal.accepted_position().unwrap(), previous);
        assert_eq!(
            drive(&mut controller, replay(&journal)).len(),
            count as usize
        );
    }
}

#[test]
fn async_suffix_requires_exact_authority_and_valid_bounds_before_creating_files() {
    for case in 0..7 {
        let (mut controller, journal) = empty_journal();
        let selected = journal.current();
        let io = journal.access.io.clone();
        let mut replacement = request(&journal, LogPosition::GENESIS);
        let mut bounds = stream_limits();
        match case {
            0 => replacement.writer_generation = journal.writer.written_position().generation(),
            1 => replacement.expected_current.generation += 1,
            2 => {
                replacement.protected_committed = LogPosition {
                    op_number: 1,
                    digest: Digest::from_bytes([1; 32]),
                }
            }
            3 => replacement.last_normal_view = 3,
            4 => bounds.max_group_operations = journal.limits.decode.max_entries + 1,
            5 => bounds.max_staged_bytes = 8191,
            6 => bounds.max_segments = 0,
            _ => unreachable!(),
        }
        assert!(
            drive(
                &mut controller,
                journal.begin_suffix_replacement(replacement, LogPosition::GENESIS, bounds)
            )
            .is_err()
        );
        assert!(
            controller
                .image()
                .bytes(Path::new("/group/segments/2.log"), false)
                .is_err()
        );
        let journal = drive(&mut controller, open(io, 12)).unwrap();
        assert_eq!(journal.current(), selected);
    }
}
