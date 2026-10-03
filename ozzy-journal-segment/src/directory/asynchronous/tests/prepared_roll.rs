use super::*;

fn buffered() -> (Controller, Journal) {
    let (mut controller, io) = setup(Image::default());
    let mut bounds = limits();
    bounds.io.write_mode = SegmentWriteMode::Buffered;
    let journal = drive(
        &mut controller,
        Journal::format(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            bounds,
        ),
    )
    .unwrap();
    (controller, journal)
}

pub(super) async fn roll_real(journal: Journal) -> Journal {
    let mut segment = journal
        .prepare_next_segment(32768, 4)
        .unwrap()
        .prepare()
        .await
        .unwrap();
    segment.zero_range(0..32768).await.unwrap();
    let (pending, work) = journal
        .begin_owned_roll_with(32768, 4, Some(segment))
        .unwrap();
    pending.complete(work.publish().await).unwrap()
}

#[test]
fn async_detached_roll_keeps_written_predecessor_readable_until_exact_installation() {
    let (mut controller, mut journal) = buffered();
    let io = journal.access.io.clone();
    let preparation = journal.prepare_next_segment(32768, 4).unwrap();
    assert!(controller.jobs().is_empty());
    let mut segment = drive(&mut controller, preparation.prepare()).unwrap();
    assert_eq!(segment.capacity(), 32768);
    drive(&mut controller, segment.zero_range(0..32768)).unwrap();
    assert!(drive(&mut controller, segment.zero_range(32768..32769)).is_err());
    let op = operation(&journal);
    drive(&mut controller, journal.append(&[op], BodyEncoding::Raw)).unwrap();
    let written = journal.written_position().unwrap();
    let selected = journal.current();
    let mut history = journal.freeze_written_history(32768).unwrap();
    let (mut pending, work) = journal
        .begin_owned_roll_with(32768, 4, Some(segment))
        .unwrap();
    let following = CanonicalOperation {
        body: &[2; 16],
        ..operation(pending.journal())
    };
    let group = pending
        .begin_group_encoding()
        .unwrap()
        .encode(
            &mut crate::JournalGroupEncoder::default(),
            &[following],
            BodyEncoding::Raw,
        )
        .unwrap();
    let completed = drive(&mut controller, work.publish());
    assert!(completed.succeeded());
    assert_eq!(pending.journal().current(), selected);
    assert_eq!(pending.journal().writer.header().segment_id(), 1);
    assert_eq!(
        pending.journal().accepted_position().unwrap(),
        LogPosition::GENESIS
    );
    assert_eq!(
        drive(&mut controller, history.position(1)).unwrap(),
        Some(written)
    );
    let journal = pending.complete(completed).unwrap();
    assert_eq!(journal.writer.header().segment_id(), 2);
    assert_eq!(journal.accepted_position().unwrap(), written);
    assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
    let mut pipeline = journal.begin_write_pipeline(1).unwrap();
    let prepared = pipeline.prepare(group, 0).unwrap();
    pipeline
        .complete(drive(&mut controller, prepared.write()))
        .unwrap();
    // Buffered mode is preserved on the new descriptor.
    assert_eq!(pipeline.journal().written_position().unwrap().op_number, 2);
    assert_eq!(pipeline.journal().accepted_position().unwrap(), written);
    drive(&mut controller, pipeline.sync_installed()).unwrap();
    drop(history);
    drop(pipeline.finish().unwrap());
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 2);
    assert_eq!(journal.manifest.segments.len(), 2);
}

#[test]
fn async_prepared_files_are_pinned_and_failed_or_canceled_zeroing_is_never_selected() {
    for cancel in [false, true] {
        let (mut controller, mut journal) = empty_journal();
        drive(&mut controller, append_confirmed(&mut journal));
        let mut segment = drive(
            &mut controller,
            journal.prepare_next_segment(32768, 4).unwrap().prepare(),
        )
        .unwrap();
        assert_eq!(
            drive(&mut controller, journal.reclaim_unreferenced_segments(4))
                .unwrap()
                .pinned_segment_ids,
            [2]
        );
        if cancel {
            let mut future = Box::pin(segment.zero_range(0..8192));
            assert!(poll(future.as_mut()).is_pending());
            drop(future);
            // Let the canceled physical write settle before inspecting selection.
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        } else {
            assert!(
                drive_with(&mut controller, segment.zero_range(0..8192), |_| {
                    Effect::FailAfter(io::ErrorKind::Other)
                })
                .is_err()
            );
        }
        assert!(drive(&mut controller, segment.zero_range(0..4096)).is_err());
        let (pending, work) = journal
            .begin_owned_roll_with(32768, 4, Some(segment))
            .unwrap();
        let completed = drive(&mut controller, work.publish());
        let mut journal = pending.complete(completed).unwrap();
        assert_eq!(
            journal.writer.header().segment_id(),
            3,
            "faulted file remains unselected"
        );
        assert_eq!(
            drive(&mut controller, journal.reclaim_unreferenced_segments(4))
                .unwrap()
                .removed_segment_ids,
            [2]
        );
    }
}

#[test]
fn async_detached_roll_rejects_foreign_results_and_probes_past_stale_preparations() {
    let (mut controller, mut journal) = empty_journal();
    drive(&mut controller, append_confirmed(&mut journal));
    let io = journal.access.io.clone();
    let mut other = drive(
        &mut controller,
        Journal::format(
            "/other".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        ),
    )
    .unwrap();
    drive(&mut controller, append_confirmed(&mut other));
    let segment = drive(
        &mut controller,
        journal.prepare_next_segment(32768, 4).unwrap().prepare(),
    )
    .unwrap();
    let (left, left_work) = journal
        .begin_owned_roll_with(32768, 4, Some(segment))
        .unwrap();
    let (right, right_work) = other.begin_owned_roll(32768, 4).unwrap();
    let left_done = drive(&mut controller, left_work.publish());
    let right_done = drive(&mut controller, right_work.publish());
    assert!(left.complete(right_done).is_err());
    assert!(right.complete(left_done).is_err());

    let (mut controller, mut journal) = empty_journal();
    drive(&mut controller, append_confirmed(&mut journal));
    let stale = drive(
        &mut controller,
        journal.prepare_next_segment(32768, 4).unwrap().prepare(),
    )
    .unwrap();
    // Another ordinary roll changes the predecessor. The stale file remains
    // pinned but cannot be used as a successor to the new segment.
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    assert_eq!(journal.writer.header().segment_id(), 3);
    drive(&mut controller, append_confirmed(&mut journal));
    let (pending, work) = journal
        .begin_owned_roll_with(32768, 4, Some(stale))
        .unwrap();
    let journal = pending
        .complete(drive(&mut controller, work.publish()))
        .unwrap();
    assert_eq!(journal.writer.header().segment_id(), 4);
}

#[test]
fn async_detached_roll_failure_preserves_old_readable_image_and_requires_reopen() {
    let (mut controller, mut journal) = empty_journal();
    drive(&mut controller, append_confirmed(&mut journal));
    let io = journal.access.io.clone();
    let current = journal.current();
    let (pending, work) = journal.begin_owned_roll(32768, 4).unwrap();
    let mut failed = false;
    let completed = drive_with(&mut controller, work.publish(), |op| {
        if let Operation::Rename { destination, .. } = op
            && destination == Path::new("/group/CURRENT")
        {
            failed = true;
            Effect::FailAfter(io::ErrorKind::Other)
        } else {
            Effect::Normal
        }
    });
    assert!(failed);
    assert!(!completed.succeeded());
    assert_eq!(pending.journal().current(), current);
    assert_eq!(pending.journal().writer.header().segment_id(), 1);
    assert!(pending.complete(completed).is_err());
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    assert_eq!(journal.writer.header().segment_id(), 2);
}

#[test]
fn async_preparation_and_roll_crash_cuts_preserve_confirmed_predecessor() {
    let image = baseline();
    for ready in [false, true] {
        for immediate in [false, true] {
            let mut finished = false;
            for cut in 0..600 {
                let (mut controller, io) = setup(image.clone());
                let journal = drive(&mut controller, open(io, 2)).unwrap();
                let done = super::recovery::run_cut(
                    &mut controller,
                    async {
                        let segment = if ready {
                            let mut segment =
                                journal.prepare_next_segment(32768, 4)?.prepare().await?;
                            segment.zero_range(0..32768).await?;
                            Some(segment)
                        } else {
                            None
                        };
                        let (pending, work) = journal.begin_owned_roll_with(32768, 4, segment)?;
                        pending.complete(work.publish().await)
                    },
                    cut,
                    immediate,
                );
                let (mut controller, io) = setup(controller.crash(true).unwrap().0);
                let journal = drive(&mut controller, open(io, 3)).unwrap();
                assert_eq!(journal.accepted_position().unwrap().op_number, 1);
                assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
                assert!(matches!(journal.writer.header().segment_id(), 1 | 2));
                if done {
                    assert_eq!(journal.writer.header().segment_id(), 2);
                    finished = true;
                    break;
                }
            }
            assert!(finished);
        }
    }
}
