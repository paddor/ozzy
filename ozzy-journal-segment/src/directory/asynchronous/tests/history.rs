use super::*;
use crate::{ChainPosition, HistoryError, StorageValidationBudget, logical_operation_digest};
use std::time::Duration;

fn budget(bytes: usize) -> StorageValidationBudget {
    StorageValidationBudget {
        max_read_bytes: bytes,
        max_work: Duration::MAX,
    }
}

async fn group(
    journal: &mut Journal,
    bodies: &[[u8; 16]],
    encoding: BodyEncoding,
) -> Vec<LogPosition> {
    let mut chain = journal.writer.written_position().next_chain();
    let mut positions = Vec::new();
    let operations = bodies
        .iter()
        .map(|body| {
            let op = CanonicalOperation {
                op_number: chain.next_op_number(),
                previous_digest: chain.previous_digest(),
                body,
                ..operation(journal)
            };
            let digest = logical_operation_digest(&op);
            positions.push(LogPosition {
                op_number: op.op_number,
                digest,
            });
            chain = ChainPosition::new(op.op_number + 1, digest);
            op
        })
        .collect::<Vec<_>>();
    let position = journal.append(&operations, encoding).await.unwrap();
    journal.sync_through(position).await.unwrap();
    positions
}

#[test]
fn async_validation_bounds_reads_and_preserves_frozen_tail_with_tiny_time_budget() {
    for encoding in [
        BodyEncoding::Raw,
        BodyEncoding::Lz4 {
            min_savings_bytes: 0,
        },
    ]
    .into_iter()
    .filter(|encoding| encoding.is_supported())
    {
        for max_read_bytes in [257, 1024, 8192] {
            let (mut controller, mut journal) = empty_journal();
            drive(&mut controller, group(&mut journal, &[[1; 16]], encoding));
            drive(&mut controller, group(&mut journal, &[[2; 16]], encoding));
            drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
            let positions = drive(
                &mut controller,
                group(&mut journal, &[[3; 16], [4; 16]], encoding),
            );
            let mut validation =
                drive(&mut controller, journal.begin_storage_validation(32768)).unwrap();
            drive(&mut controller, group(&mut journal, &[[5; 16]], encoding));
            let bounds = StorageValidationBudget {
                max_read_bytes,
                max_work: Duration::from_nanos(1),
            };
            let mut total = 0;
            let mut complete = 0;
            for _ in 0..200 {
                let Some(step) = drive(
                    &mut controller,
                    validation.validate_next_with_budget(bounds),
                )
                .unwrap() else {
                    break;
                };
                assert!(step.checked_bytes <= max_read_bytes as u64);
                assert_eq!(step.through, positions[1]);
                total += step.checked_bytes;
                complete += usize::from(step.segment_complete);
            }
            assert!(
                drive(&mut controller, validation.validate_next())
                    .unwrap()
                    .is_none()
            );
            assert_eq!(total, 20 * 1024);
            assert_eq!(complete, 2);
        }
    }
}

#[test]
fn async_validation_detects_body_damage_after_reading_its_header() {
    let (mut controller, mut journal) = empty_journal();
    drive(
        &mut controller,
        group(&mut journal, &[[1; 16], [2; 16]], BodyEncoding::Raw),
    );
    let mut validation = drive(&mut controller, journal.begin_storage_validation(32768)).unwrap();
    let first = drive(
        &mut controller,
        validation.validate_next_with_budget(budget(4096)),
    )
    .unwrap()
    .unwrap();
    assert!(!first.segment_complete);
    assert_eq!(first.remaining_segments, 1);
    let file = drive(
        &mut controller,
        journal.access.open(
            journal.root().join("segments/1.log"),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(
            &file,
            crate::SEGMENT_HEADER_BYTES as u64 + crate::ENTRY_HEADER_BYTES as u64,
            &[99],
        ),
    )
    .unwrap();
    assert!(
        drive(
            &mut controller,
            validation.validate_next_with_budget(budget(4096))
        )
        .is_err()
    );
}

#[test]
fn async_history_caches_exact_frozen_segments_and_exports_bounded_groups() {
    for encoding in [
        BodyEncoding::Raw,
        BodyEncoding::Lz4 {
            min_savings_bytes: 0,
        },
    ]
    .into_iter()
    .filter(|e| e.is_supported())
    {
        let (mut controller, mut journal) = empty_journal();
        let first = drive(
            &mut controller,
            group(&mut journal, &[[1; 16], [2; 16], [3; 16]], encoding),
        );
        drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        let second = drive(
            &mut controller,
            group(&mut journal, &[[4; 16], [5; 16]], encoding),
        );
        let mut history = journal.freeze_history(32768).unwrap();
        assert!(controller.jobs().is_empty(), "capture submits no I/O");
        assert_eq!(history.identity(), identity());
        assert_eq!(history.generation(), JournalGeneration(1));
        assert_eq!(history.predecessor(), LogPosition::GENESIS);
        assert_eq!(history.through(), second[1]);
        let end = journal.writer.written_position().end_offset();
        drive(&mut controller, group(&mut journal, &[[6; 16]], encoding));
        // Damage only the later group; this frozen source must never read it.
        let handle = drive(
            &mut controller,
            journal.access.open(
                journal.root().join("segments/2.log"),
                ozzy_io::OpenMode::ReadWrite,
                false,
                false,
            ),
        )
        .unwrap();
        drive(
            &mut controller,
            journal.access.write_all(&handle, end, &[99; 8]),
        )
        .unwrap();
        let mut predecessor = LogPosition::GENESIS;
        let mut reads = 0;
        for (at, expected) in first.iter().chain(&second).enumerate() {
            let chunk = drive_with(
                &mut controller,
                history.read_after(predecessor, 1, 16),
                |op| {
                    if matches!(op, Operation::Read { .. }) {
                        reads += 1;
                    }
                    Effect::Normal
                },
            )
            .unwrap();
            assert_eq!(chunk.end(), *expected);
            let op = chunk.operations().next().unwrap();
            assert_eq!(op.body, &[at as u8 + 1; 16]);
            assert_eq!(logical_operation_digest(&op), expected.digest);
            predecessor = chunk.end();
            let queries = drive_with(
                &mut controller,
                history.position(expected.op_number),
                |_| panic!("cached lookup performed I/O"),
            );
            assert_eq!(queries.unwrap(), Some(*expected));
        }
        assert_eq!(
            reads, 4,
            "one 8 KiB read per captured segment, in 4 KiB jobs"
        );
        assert_eq!(drive(&mut controller, history.position(6)).unwrap(), None);
        assert!(matches!(
            drive(
                &mut controller,
                history.read_after(history.through(), 1, 16)
            ),
            Err(HistoryError::Predecessor)
        ));
        assert!(matches!(
            drive(
                &mut controller,
                history.read_after(LogPosition::GENESIS, 1, 15)
            ),
            Err(HistoryError::BodyBudget {
                required: 16,
                available: 15
            })
        ));
        assert!(matches!(
            drive(
                &mut controller,
                history.read_after(LogPosition::GENESIS, 0, 16)
            ),
            Err(HistoryError::Capacity)
        ));
    }
}

#[test]
fn refreshed_history_checks_only_new_groups_and_rejects_damaged_suffix() {
    let (mut controller, mut journal) = empty_journal();
    let first = drive(
        &mut controller,
        group(&mut journal, &[[1; 16], [2; 16]], BodyEncoding::Raw),
    );
    let mut old = journal.freeze_history(32768).unwrap();
    assert_eq!(
        drive(&mut controller, old.position(first[1].op_number)).unwrap(),
        Some(first[1])
    );
    let old_end = journal.writer.written_position().end_offset();
    let second = drive(
        &mut controller,
        group(&mut journal, &[[3; 16], [4; 16]], BodyEncoding::Raw),
    );
    let mut refreshed = journal.freeze_history(32768).unwrap();
    assert!(refreshed.reuse_checked_prefix(&mut old).unwrap());
    let mut read_offsets = Vec::new();
    let chunk = drive_with(
        &mut controller,
        refreshed.read_after(first[1], 2, 32),
        |operation| {
            if let Operation::Read { offset, .. } = operation {
                read_offsets.push(*offset);
            }
            Effect::Normal
        },
    )
    .unwrap();
    assert_eq!(chunk.end(), second[1]);
    assert_ne!(read_offsets.len(), 0);
    assert!(read_offsets.iter().all(|offset| *offset >= old_end));

    let second_end = journal.writer.written_position().end_offset();
    drive(
        &mut controller,
        group(&mut journal, &[[5; 16]], BodyEncoding::Raw),
    );
    let mut damaged = journal.freeze_history(32768).unwrap();
    assert!(damaged.reuse_checked_prefix(&mut refreshed).unwrap());
    let file = drive(
        &mut controller,
        journal.access.open(
            journal.root().join("segments/1.log"),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(&file, second_end, &[99; 8]),
    )
    .unwrap();
    assert!(drive(&mut controller, damaged.read_after(second[1], 1, 16)).is_err());
}

#[test]
fn async_history_validates_the_whole_physical_group_before_exposing_a_prefix() {
    let (mut controller, mut journal) = empty_journal();
    let positions = drive(
        &mut controller,
        group(
            &mut journal,
            &[[1; 16], [2; 16], [3; 16]],
            BodyEncoding::Raw,
        ),
    );
    let mut prefix = drive(
        &mut controller,
        journal.freeze_history_through(positions[0], 32768),
    )
    .unwrap();
    let chunk = drive(
        &mut controller,
        prefix.read_after(LogPosition::GENESIS, 32, 1024),
    )
    .unwrap();
    assert_eq!(chunk.operations().len(), 1);
    assert_eq!(chunk.end(), positions[0]);
    let mut untouched = journal.freeze_history(32768).unwrap();
    // Same first operation, valid replacement checksums, conflicting later body.
    let bodies = [[1; 16], [2; 16], [9; 16]];
    let mut chain = ChainPosition::GENESIS;
    let ops = bodies
        .iter()
        .map(|body| {
            let op = CanonicalOperation {
                body,
                op_number: chain.next_op_number(),
                previous_digest: chain.previous_digest(),
                ..operation(&journal)
            };
            chain = ChainPosition::new(op.op_number + 1, logical_operation_digest(&op));
            op
        })
        .collect::<Vec<_>>();
    let encoded = crate::encode_group(
        journal.writer.header(),
        1,
        crate::SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &ops,
    )
    .unwrap();
    let file = drive(
        &mut controller,
        journal.access.open(
            journal.root().join("segments/1.log"),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(
            &file,
            crate::SEGMENT_HEADER_BYTES as u64,
            encoded.as_bytes(),
        ),
    )
    .unwrap();
    assert!(matches!(
        drive(&mut controller, untouched.position(1)),
        Err(HistoryError::Source)
    ));
    assert!(matches!(
        drive(
            &mut controller,
            journal.freeze_history_through(positions[0], 32768)
        ),
        Err(HistoryError::Source)
    ));
}

#[test]
fn async_written_history_never_claims_stability_and_capture_bounds_are_enforced() {
    let (mut controller, io) = setup(Image::default());
    let mut limits = limits();
    limits.io.write_mode = SegmentWriteMode::Buffered;
    let mut journal = drive(
        &mut controller,
        Journal::format(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits,
        ),
    )
    .unwrap();
    let op = operation(&journal);
    let written = drive(&mut controller, journal.append(&[op], BodyEncoding::Raw)).unwrap();
    assert!(matches!(
        journal.freeze_history(32768),
        Err(HistoryError::Unsettled)
    ));
    assert!(matches!(
        journal.freeze_written_history(32767),
        Err(HistoryError::Capacity)
    ));
    let mut history = journal.freeze_written_history(32768).unwrap();
    assert_eq!(
        drive(
            &mut controller,
            history.read_after(LogPosition::GENESIS, 1, 16)
        )
        .unwrap()
        .operations()
        .len(),
        1
    );
    assert_eq!(journal.accepted_position().unwrap(), LogPosition::GENESIS);
    drive(&mut controller, journal.sync_through(written)).unwrap();
    assert!(journal.freeze_history(32768).is_ok());
}

#[test]
fn async_history_cancellation_discards_partial_cache_without_losing_file_protection() {
    let (mut controller, io) = setup(super::retention::retained_baseline());
    let mut journal = drive(&mut controller, open(io.clone(), 3)).unwrap();
    let mut history = journal.freeze_history(32768).unwrap();
    drive(
        &mut controller,
        journal.retire_sealed_prefix(
            &crate::RetentionFloors::new(vec![]).unwrap(),
            crate::AsyncRetirementBudget {
                max_segments: 2,
                max_read_bytes: 65536,
            },
        ),
    )
    .unwrap();
    assert_eq!(
        drive(&mut controller, journal.reclaim_unreferenced_segments(2))
            .unwrap()
            .pinned_segment_ids,
        [1, 2]
    );
    let mut future = Box::pin(history.position(1));
    let mut reads = 0;
    loop {
        assert!(poll(future.as_mut()).is_pending());
        let (id, _) = controller.jobs()[0];
        if matches!(
            controller.operation(id).unwrap().unprotected(),
            Operation::Read { .. }
        ) {
            reads += 1;
            if reads == 2 {
                drop(future);
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
                break;
            }
        }
        controller.execute(id, Effect::Normal).unwrap();
        controller.deliver(id).unwrap();
    }
    assert_eq!(
        drive(&mut controller, history.position(1))
            .unwrap()
            .unwrap()
            .op_number,
        1
    );
    drop(journal);
    assert!(drive(&mut controller, open(io.clone(), 4)).is_err());
    drop(history);
    let mut journal = drive(&mut controller, open(io, 5)).unwrap();
    assert_eq!(
        drive(&mut controller, journal.reclaim_unreferenced_segments(2))
            .unwrap()
            .removed_segment_ids,
        [1, 2]
    );
}

#[test]
fn async_storage_validation_obeys_byte_budgets_and_rechecks_cached_bytes() {
    let (mut controller, mut journal) = empty_journal();
    drive(
        &mut controller,
        group(&mut journal, &[[1; 16], [2; 16]], BodyEncoding::Raw),
    );
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    drive(
        &mut controller,
        group(&mut journal, &[[3; 16]], BodyEncoding::Raw),
    );
    let mut history = journal.freeze_history(32768).unwrap();
    drive(&mut controller, history.position(1)).unwrap();
    let mut validation = drive(&mut controller, journal.begin_storage_validation(32768)).unwrap();
    let mut total = 0;
    let mut completed = Vec::new();
    while let Some(step) = drive(
        &mut controller,
        validation.validate_next_with_budget(budget(1500)),
    )
    .unwrap()
    {
        assert!(step.checked_bytes <= 1500);
        assert_eq!(step.current, journal.current());
        assert_eq!(step.generation, JournalGeneration(1));
        assert_eq!(step.through, history.through());
        total += step.checked_bytes;
        if step.segment_complete {
            completed.push(step.segment_id);
        }
    }
    assert_eq!(total, 16384);
    assert_eq!(completed, [1, 2]);
    let file = drive(
        &mut controller,
        journal.access.open(
            journal.root().join("segments/1.log"),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(
            &file,
            crate::SEGMENT_HEADER_BYTES as u64 + crate::ENTRY_HEADER_BYTES as u64,
            &[99],
        ),
    )
    .unwrap();
    // Existing cached history remains immutable. A new scrub must read the disk.
    assert!(
        drive(&mut controller, history.position(1))
            .unwrap()
            .is_some()
    );
    let mut validation = drive(&mut controller, journal.begin_storage_validation(32768)).unwrap();
    assert!(drive(&mut controller, validation.validate_next()).is_err());
    assert!(matches!(
        drive(&mut controller, validation.validate_next()),
        Err(HistoryError::Source)
    ));
}

#[test]
fn canceled_and_failed_async_validation_steps_cannot_resume() {
    let (mut controller, mut journal) = empty_journal();
    drive(
        &mut controller,
        group(&mut journal, &[[1; 16]], BodyEncoding::Raw),
    );
    for cancel in [false, true] {
        let mut validation =
            drive(&mut controller, journal.begin_storage_validation(32768)).unwrap();
        if cancel {
            let mut step = Box::pin(validation.validate_next());
            assert!(poll(step.as_mut()).is_pending());
            let (id, _) = controller.jobs()[0];
            controller.execute(id, Effect::Normal).unwrap();
            drop(step);
            controller.deliver(id).unwrap();
        } else {
            assert!(
                drive_with(&mut controller, validation.validate_next(), |op| {
                    if matches!(op, Operation::Read { .. }) {
                        Effect::FailBefore(io::ErrorKind::Other)
                    } else {
                        Effect::Normal
                    }
                })
                .is_err()
            );
        }
        assert!(matches!(
            drive(&mut controller, validation.validate_next()),
            Err(HistoryError::Source)
        ));
    }
    let mut fresh = drive(&mut controller, journal.begin_storage_validation(32768)).unwrap();
    assert!(
        drive(&mut controller, fresh.validate_next())
            .unwrap()
            .unwrap()
            .segment_complete
    );
}

pub(super) async fn check_real_history(journal: &mut Journal) {
    let mut history = journal.freeze_history(32768).unwrap();
    let mut position = LogPosition::GENESIS;
    while position != history.through() {
        let chunk = history.read_after(position, 4, 64).await.unwrap();
        assert!(
            chunk
                .operations()
                .all(|op| op.kind == OperationKind::Barrier)
        );
        position = chunk.end();
    }
    assert_eq!(position.op_number, 2);
    assert_eq!(history.position(1).await.unwrap().unwrap().op_number, 1);
    let mut validation = journal.begin_storage_validation(32768).await.unwrap();
    let mut total = 0;
    while let Some(step) = validation
        .validate_next_with_budget(budget(1024))
        .await
        .unwrap()
    {
        assert!(step.checked_bytes <= 1024);
        total += step.checked_bytes;
    }
    assert_eq!(total, 16384);
}

#[test]
fn async_history_short_reads_are_completed_and_zero_reads_cannot_seed_cache() {
    let (mut controller, mut journal) = empty_journal();
    let positions = drive(
        &mut controller,
        group(&mut journal, &[[1; 16]], BodyEncoding::Raw),
    );
    let mut history = journal.freeze_history(32768).unwrap();
    let mut reads = 0;
    assert_eq!(
        drive_with(&mut controller, history.position(1), |op| {
            if matches!(op, Operation::Read { .. }) {
                reads += 1;
                Effect::Short(127)
            } else {
                Effect::Normal
            }
        })
        .unwrap(),
        Some(positions[0])
    );
    assert!(reads > 2);
    let mut history = journal.freeze_history(32768).unwrap();
    assert!(
        drive_with(&mut controller, history.position(1), |op| {
            if matches!(op, Operation::Read { .. }) {
                Effect::Short(0)
            } else {
                Effect::Normal
            }
        })
        .is_err()
    );
    assert_eq!(
        drive(&mut controller, history.position(1)).unwrap(),
        Some(positions[0])
    );

    let mut validation = drive(&mut controller, journal.begin_storage_validation(32768)).unwrap();
    let mut complete = false;
    for _ in 0..16 {
        let step = drive_with(
            &mut controller,
            validation.validate_next_with_budget(StorageValidationBudget {
                max_read_bytes: usize::MAX,
                max_work: Duration::from_nanos(1),
            }),
            |op| {
                if matches!(op, Operation::Read { .. }) {
                    Effect::Short(127)
                } else {
                    Effect::Normal
                }
            },
        )
        .unwrap()
        .unwrap();
        if step.segment_complete {
            complete = true;
            break;
        }
    }
    assert!(complete, "tiny processing budgets must still make progress");
}
