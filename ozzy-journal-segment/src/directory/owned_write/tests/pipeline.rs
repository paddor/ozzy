use super::*;
use crate::JournalWriteEvent::{WriteFinished, WriteStarted, WritebackFinished, WritebackStarted};

#[test]
fn detached_writeback_crosses_range_boundary_without_claiming_durability() {
    let (_temporary, mut journal) =
        crate::directory::progress_tests::journal_mode_capacity(true, 8 * 1024 * 1024);
    journal
        .set_write_mode(crate::SegmentWriteMode::Buffered)
        .unwrap();
    let mut pipeline = journal.begin_write_pipeline(300).unwrap();
    let chunks = reserve(&mut pipeline, 300, BodyEncoding::Raw);
    assert!(chunks.last().unwrap().plan.after.end_offset() > 4 * 1024 * 1024);
    let mut hint = None;
    for completion in PreparedJournalWrite::write_batch_scheduling(
        chunks,
        std::num::NonZeroUsize::MAX,
        |captured| {
            hint = captured;
            Ok(())
        },
        |_| {},
    ) {
        pipeline.complete(completion).unwrap();
    }
    let mut journal = pipeline.finish().unwrap();
    hint.expect("completed 4 MiB range").start().unwrap();
    assert_eq!(journal.written_position().unwrap().op_number, 300);
    assert_eq!(journal.accepted_position().unwrap().op_number, 0);
    journal.sync_through(journal.begin_sync()).unwrap();
    let expected = journal.accepted_position().unwrap();
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    drop(journal);
    let recovered = crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default())
        .unwrap()
        .recover(
            ozzy_journal::progress::JournalGeneration(2),
            crate::DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(recovered.accepted_position().unwrap(), expected);
}

#[test]
fn buffered_pipeline_exposes_written_history_without_durability() {
    let (_temporary, mut journal) = journal_mode(true);
    journal
        .set_write_mode(crate::SegmentWriteMode::Buffered)
        .unwrap();
    let mut pipeline = journal.begin_write_pipeline(4).unwrap();
    let chunks = reserve(&mut pipeline, 2, BodyEncoding::Raw);
    #[cfg(target_os = "linux")]
    for chunk in &chunks {
        let flags = rustix::fs::fcntl_getfl(&chunk.file).unwrap();
        assert_eq!(flags.bits() & libc::O_DSYNC as u32, 0);
    }
    for completed in PreparedJournalWrite::write_batch(chunks) {
        pipeline.complete(completed).unwrap();
    }
    let mut journal = pipeline.finish().unwrap();
    assert_eq!(journal.written_position().unwrap().op_number, 2);
    assert_eq!(journal.accepted_position().unwrap().op_number, 0);
    assert!(matches!(
        journal.freeze_history(1024 * 1024),
        Err(crate::HistoryError::Unsettled)
    ));
    let mut history = journal.freeze_written_history(1024 * 1024).unwrap();
    assert_eq!(history.through().op_number, 2);
    assert_eq!(
        history.position(2).unwrap().unwrap(),
        journal.written_position().unwrap()
    );
    journal.sync_through(journal.begin_sync()).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 2);
    assert!(journal.freeze_history(1024 * 1024).is_ok());
}

fn reserve(
    pipeline: &mut JournalWritePipeline,
    count: u64,
    encoding: BodyEncoding,
) -> Vec<PreparedJournalWrite> {
    let mut previous = Digest::ZERO;
    (1..=count)
        .map(|number| {
            let bytes = body(number);
            let op = operation(pipeline.journal(), number, previous, &bytes);
            previous = logical_operation_digest(&op);
            let group = if !pipeline.journal().writer.data_sync() && encoding == BodyEncoding::Raw {
                pipeline
                    .preencode_shared_raw(vec![SharedJournalOperation {
                        header: op.header(),
                        body_digest: canonical_body_digest(op.body),
                        body: bytes.into(),
                    }])
                    .unwrap()
            } else {
                pipeline.preencode(&[op], encoding).unwrap()
            };
            pipeline.prepare(group).unwrap()
        })
        .collect()
}

#[test]
fn four_reserved_chunks_publish_only_ordered_completions_and_recover() {
    for encoding in [
        BodyEncoding::Raw,
        BodyEncoding::Lz4 {
            min_savings_bytes: 64,
        },
    ]
    .into_iter()
    .filter(|encoding| encoding.is_supported())
    {
        let (_temporary, journal) = journal_mode(true);
        let root = journal.directory.root.clone();
        let identity = journal.directory.identity;
        let mut pipeline = journal.begin_write_pipeline(4).unwrap();
        let chunks = reserve(&mut pipeline, 4, encoding);
        assert_eq!(pipeline.pending(), 4);
        assert!(pipeline.preencode(&[], encoding).is_err());
        assert_eq!(pipeline.journal().accepted_position().unwrap().op_number, 0);
        let mut observed = Vec::new();
        let completions = PreparedJournalWrite::write_batch_observing(
            chunks,
            std::num::NonZeroUsize::new(512).unwrap(),
            |event| observed.push(event),
        );
        assert_eq!(
            observed,
            [
                WriteStarted,
                WriteFinished,
                WritebackStarted,
                WritebackFinished
            ]
        );
        assert_eq!(pipeline.journal().accepted_position().unwrap().op_number, 0);
        for (index, completion) in completions.into_iter().enumerate() {
            assert!(completion.succeeded());
            let locations = pipeline.complete(completion).unwrap();
            assert_eq!(locations.len(), 1);
            assert_eq!(locations[0].op_number, index as u64 + 1);
            assert_eq!(
                pipeline.journal().accepted_position().unwrap().op_number,
                index as u64 + 1
            );
        }
        let journal = pipeline.finish().unwrap();
        let position = journal.accepted_position().unwrap();
        drop(journal);
        let recovered =
            crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default())
                .unwrap()
                .recover(
                    ozzy_journal::progress::JournalGeneration(2),
                    crate::DecodeLimits::default(),
                    OperationLimits::default(),
                )
                .unwrap();
        assert_eq!(recovered.accepted_position().unwrap(), position);
    }
}

#[test]
fn failed_reordered_and_foreign_completions_fence_the_pipeline() {
    for failure in [0, 1, 2] {
        let (_temporary, journal) = journal_mode(true);
        let mut pipeline = journal.begin_write_pipeline(4).unwrap();
        let mut writes = reserve(&mut pipeline, 2, BodyEncoding::Raw).into_iter();
        let first = writes.next().unwrap();
        let second = writes.next().unwrap();
        let bad = match failure {
            0 => first.fail(),
            1 => second.write(),
            _ => {
                let (_foreign_root, journal) = journal_mode(true);
                let mut foreign = journal.begin_write_pipeline(1).unwrap();
                reserve(&mut foreign, 1, BodyEncoding::Raw)
                    .pop()
                    .unwrap()
                    .write()
            }
        };
        assert!(pipeline.complete(bad).is_err());
        assert_eq!(pipeline.journal().accepted_position().unwrap().op_number, 0);
        assert!(pipeline.preencode(&[], BodyEncoding::Raw).is_err());
        assert!(pipeline.finish().is_err());
    }
}

#[test]
fn vectored_writes_reject_nonconsecutive_chunks_without_writing() {
    let (_temporary, journal) = journal_mode(true);
    let path = journal.active_segment_path();
    let before = std::fs::read(&path).unwrap();
    let mut pipeline = journal.begin_write_pipeline(4).unwrap();
    let mut writes = reserve(&mut pipeline, 2, BodyEncoding::Raw);
    writes.reverse();
    let mut observed = Vec::new();
    let completed =
        PreparedJournalWrite::write_batch_observing(writes, std::num::NonZeroUsize::MAX, |event| {
            observed.push(event);
        });
    assert!(observed.is_empty());
    assert!(completed.iter().all(|done| !done.succeeded()));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(pipeline.journal().accepted_position().unwrap().op_number, 0);
}

#[test]
fn failed_write_finishes_observation_without_starting_writeback() {
    let (_temporary, journal) = journal_mode(true);
    let path = journal.active_segment_path();
    let before = std::fs::read(&path).unwrap();
    let mut pipeline = journal.begin_write_pipeline(4).unwrap();
    let mut writes = reserve(&mut pipeline, 2, BodyEncoding::Raw);
    writes[0].file = std::sync::Arc::new(std::fs::File::open(&path).unwrap());
    let mut observed = Vec::new();
    let completed =
        PreparedJournalWrite::write_batch_observing(writes, std::num::NonZeroUsize::MAX, |event| {
            observed.push(event);
        });
    assert_eq!(observed, [WriteStarted, WriteFinished]);
    assert!(completed.iter().all(|done| !done.succeeded()));
    assert_eq!(std::fs::read(path).unwrap(), before);
    for done in completed {
        assert!(pipeline.complete(done).is_err());
    }
}

#[test]
fn failure_after_a_complete_chunk_does_not_publish_any_of_the_physical_batch() {
    let (_temporary, journal) = journal_mode(true);
    let path = journal.active_segment_path();
    let before = std::fs::read(&path).unwrap();
    let mut pipeline = journal.begin_write_pipeline(4).unwrap();
    let writes = reserve(&mut pipeline, 4, BodyEncoding::Raw);
    let completed = PreparedJournalWrite::write_batch_with(writes, |chunks| {
        let first = &mut chunks[0];
        first
            .bytes
            .write(&mut first.file, first.plan.before.end_offset())?;
        Err(std::io::Error::from(std::io::ErrorKind::StorageFull))
    });
    assert_ne!(std::fs::read(&path).unwrap(), before);
    assert_eq!(completed.len(), 4);
    assert!(completed.iter().all(|done| !done.succeeded()));
    for completion in completed {
        assert!(pipeline.complete(completion).is_err());
        assert_eq!(pipeline.journal().accepted_position().unwrap().op_number, 0);
    }
    assert!(pipeline.finish().is_err());
}

#[test]
fn rejected_writeback_schedule_fences_all_written_chunks() {
    let (_temporary, mut journal) = journal_mode(true);
    journal
        .set_write_mode(crate::SegmentWriteMode::Buffered)
        .unwrap();
    let mut pipeline = journal.begin_write_pipeline(2).unwrap();
    let chunks = reserve(&mut pipeline, 2, BodyEncoding::Raw);
    let completed = PreparedJournalWrite::write_batch_scheduling(
        chunks,
        std::num::NonZeroUsize::MAX,
        |_| Err(std::io::Error::other("scheduler stopped")),
        |_| {},
    );
    assert!(completed.iter().all(|done| !done.succeeded()));
    assert!(
        pipeline
            .complete(completed.into_iter().next().unwrap())
            .is_err()
    );
    assert!(pipeline.finish().is_err());
}
