use super::*;
use crate::{
    AsyncJournalWritePipeline as Pipeline, AsyncPreparedJournalWrite as Prepared,
    JournalGroupEncoder, PreencodedJournalGroup, SharedJournalOperation, logical_operation_digest,
};
use ozzy_journal::operation::canonical_body_digest;
use std::num::NonZeroUsize;

fn reserve(
    pipeline: &mut Pipeline,
    count: u8,
    encoding: BodyEncoding,
    shared: bool,
) -> Vec<Prepared> {
    let mut encoder = JournalGroupEncoder::default();
    let mut chain = pipeline.journal().writer.written_position().next_chain();
    (1..=count)
        .map(|id| {
            let body = vec![id; 16];
            let op = CanonicalOperation {
                op_number: chain.next_op_number(),
                previous_digest: chain.previous_digest(),
                body: &body,
                ..operation(pipeline.journal())
            };
            chain = crate::ChainPosition::new(op.op_number + 1, logical_operation_digest(&op));
            let work = pipeline.begin_group_encoding().unwrap();
            let group = if shared {
                work.encode_shared_raw(vec![SharedJournalOperation {
                    header: op.header(),
                    body_digest: canonical_body_digest(op.body),
                    body: body.into(),
                }])
                .unwrap()
            } else {
                work.encode(&mut encoder, &[op], encoding).unwrap()
            };
            pipeline
                .prepare(group, if shared { 16 } else { 0 })
                .unwrap()
        })
        .collect()
}

fn next_group(pipeline: &mut Pipeline) -> PreencodedJournalGroup {
    let op = operation(pipeline.journal());
    pipeline
        .begin_group_encoding()
        .unwrap()
        .encode(
            &mut JournalGroupEncoder::default(),
            &[op],
            BodyEncoding::Raw,
        )
        .unwrap()
}

#[test]
fn async_pipeline_detached_sync_protects_only_its_installed_prefix() {
    for mode in [SegmentWriteMode::DataSync, SegmentWriteMode::Buffered] {
        let (mut controller, io) = setup(Image::default());
        let mut options = limits();
        options.io.write_mode = mode;
        let journal = drive(
            &mut controller,
            Journal::format(
                "/group".into(),
                io,
                spec(CommitMode::External),
                JournalGeneration(1),
                options,
            ),
        )
        .unwrap();
        let mut pipeline = journal.begin_write_pipeline(2).unwrap();
        let mut writes = reserve(&mut pipeline, 2, BodyEncoding::Raw, true).into_iter();
        let first = drive(&mut controller, writes.next().unwrap().write());
        pipeline.complete(first).unwrap();
        let through = pipeline.journal().writer().written_position();
        let work = pipeline.prepare_sync().unwrap();
        assert!(pipeline.sync_pending());
        assert!(pipeline.prepare_sync().is_err());
        assert!(drive(&mut controller, pipeline.sync_installed()).is_err());
        assert!(drive(&mut controller, pipeline.publish_durable_progress()).is_err());
        let second = drive(&mut controller, writes.next().unwrap().write());
        pipeline.complete(second).unwrap();
        assert_eq!(pipeline.journal().written_position().unwrap().op_number, 2);
        let done = drive(&mut controller, work.publish());
        assert_eq!(pipeline.complete_sync(done).unwrap(), through);
        assert!(!pipeline.sync_pending());
        assert_eq!(
            pipeline
                .journal()
                .evidence
                .as_ref()
                .unwrap()
                .protected(pipeline.journal().manifest())
                .unwrap()
                .op_number,
            1
        );
        let work = pipeline.prepare_sync().unwrap();
        let done = drive(&mut controller, work.publish());
        assert_eq!(
            pipeline
                .complete_sync(done)
                .unwrap()
                .next_chain()
                .next_op_number(),
            3
        );
        let journal = pipeline.finish().unwrap();
        assert_eq!(
            journal
                .evidence
                .as_ref()
                .unwrap()
                .protected(journal.manifest())
                .unwrap()
                .op_number,
            2
        );
    }
}

#[test]
fn async_pipeline_foreign_sync_completion_and_uncertain_publication_fence() {
    let (mut controller, journal) = empty_journal();
    let mut original = journal.begin_write_pipeline(1).unwrap();
    let work = original.prepare_sync().unwrap();
    let done = drive(&mut controller, work.publish());
    let (_, journal) = empty_journal();
    let mut foreign = journal.begin_write_pipeline(1).unwrap();
    assert!(foreign.complete_sync(done).is_err());
    assert!(foreign.is_faulted());
    assert!(foreign.finish().is_err());
    assert!(original.sync_pending());
    assert!(original.finish().is_err());

    let (mut controller, journal) = empty_journal();
    let mut pipeline = journal.begin_write_pipeline(1).unwrap();
    let write = reserve(&mut pipeline, 1, BodyEncoding::Raw, true)
        .pop()
        .unwrap();
    let done = drive(&mut controller, write.write());
    pipeline.complete(done).unwrap();
    let work = pipeline.prepare_sync().unwrap();
    let mut failed = false;
    let done = drive_with(&mut controller, work.publish(), |operation| {
        if matches!(operation, Operation::Write { .. }) && !failed {
            failed = true;
            Effect::FailAfter(io::ErrorKind::Other)
        } else {
            Effect::Normal
        }
    });
    assert!(failed);
    assert!(pipeline.complete_sync(done).is_err());
    assert!(pipeline.is_faulted());
    assert!(pipeline.finish().is_err());
}

pub(super) async fn append_real_shared(journal: Journal) -> Journal {
    let mut pipeline = journal.begin_write_pipeline(1).unwrap();
    let work = reserve(&mut pipeline, 1, BodyEncoding::Raw, true);
    let completed = Prepared::write_batch(work, NonZeroUsize::new(4096).unwrap()).await;
    for completion in completed {
        pipeline.complete(completion).unwrap();
    }
    pipeline.publish_durable_progress().await.unwrap();
    pipeline.finish().unwrap()
}

#[test]
fn async_pipeline_batch_splits_at_device_byte_share_and_any_failure_confirms_none() {
    for fail in [false, true] {
        let (mut controller, journal) = empty_journal();
        let mut pipeline = journal.begin_write_pipeline(2).unwrap();
        let mut previous = Digest::ZERO;
        let mut work = Vec::new();
        for number in 1..=2 {
            let body = vec![number; 16];
            let op = CanonicalOperation {
                op_number: u64::from(number),
                previous_digest: previous,
                body: &body,
                ..operation(pipeline.journal())
            };
            previous = logical_operation_digest(&op);
            let group = pipeline
                .begin_group_encoding()
                .unwrap()
                .encode_shared_raw(vec![SharedJournalOperation {
                    header: op.header(),
                    body_digest: canonical_body_digest(op.body),
                    body: body.into(),
                }])
                .unwrap();
            // Two such backings cannot share this device's 2 MiB data budget.
            work.push(pipeline.prepare(group, 1_500_000).unwrap());
        }
        let mut writes = 0;
        let completed = drive_with(
            &mut controller,
            Prepared::write_batch(work, NonZeroUsize::MAX),
            |op| {
                if matches!(op, Operation::Write { .. }) {
                    writes += 1;
                }
                if fail && writes == 2 {
                    Effect::FailAfter(io::ErrorKind::Other)
                } else {
                    Effect::Normal
                }
            },
        );
        assert_eq!(writes, 2);
        for completion in completed {
            assert_eq!(completion.succeeded(), !fail);
            assert_eq!(pipeline.complete(completion).is_ok(), !fail);
        }
        assert_eq!(
            pipeline.journal().written_position().unwrap().op_number,
            if fail { 0 } else { 2 }
        );
        assert_eq!(pipeline.finish().is_ok(), !fail);
    }
}

#[test]
fn async_pipeline_power_loss_discards_a_later_completed_write_above_a_hole() {
    for earlier in [false, true] {
        let (mut controller, journal) = empty_journal();
        let mut pipeline = journal.begin_write_pipeline(2).unwrap();
        let mut work = reserve(&mut pipeline, 2, BodyEncoding::Raw, true);
        let second = work.pop().unwrap();
        let first = work.pop().unwrap();
        let completed = drive(&mut controller, second.write());
        assert!(completed.succeeded());
        if earlier {
            pipeline
                .complete(drive(&mut controller, first.write()))
                .unwrap();
        } else {
            drop(first);
        }
        drop(completed);
        drop(pipeline);
        let (mut controller, io) = setup(controller.crash(true).unwrap().0);
        let journal = drive(&mut controller, open(io, 2)).unwrap();
        assert_eq!(
            journal.accepted_position().unwrap().op_number,
            if earlier { 2 } else { 0 }
        );
        assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
    }
}

#[test]
fn async_pipeline_batches_shared_and_encoded_output_with_exact_ordered_completions() {
    for (encoding, shared, limit) in [
        (BodyEncoding::Raw, false, 8192),
        (BodyEncoding::Raw, true, 512),
        (
            BodyEncoding::Lz4 {
                min_savings_bytes: 0,
            },
            false,
            16384,
        ),
    ]
    .into_iter()
    .filter(|(e, _, _)| e.is_supported())
    {
        let (mut controller, journal) = empty_journal();
        let io = journal.access.io.clone();
        let mut pipeline = journal.begin_write_pipeline(4).unwrap();
        let work = reserve(&mut pipeline, 4, encoding, shared);
        assert_eq!(pipeline.pending(), 4);
        assert!(controller.jobs().is_empty(), "reservation is memory only");
        let group = next_group(&mut pipeline);
        assert!(pipeline.prepare(group, 0).is_err());
        let mut writes = 0;
        let completed = drive_with(
            &mut controller,
            Prepared::write_batch(work, NonZeroUsize::new(limit).unwrap()),
            |op| {
                if let Operation::Write { data, .. } = op {
                    assert!(data.len() <= limit);
                    writes += 1;
                }
                Effect::Normal
            },
        );
        assert_eq!(writes, 16384 / limit);
        assert_eq!(
            pipeline.journal().accepted_position().unwrap(),
            LogPosition::GENESIS
        );
        for (index, completed) in completed.into_iter().enumerate() {
            assert!(completed.succeeded());
            let locations = pipeline.complete(completed).unwrap();
            assert_eq!(locations.len(), 1);
            assert_eq!(locations[0].op_number, index as u64 + 1);
            assert_eq!(
                pipeline.journal().accepted_position().unwrap().op_number,
                index as u64 + 1
            );
        }
        drive(&mut controller, pipeline.publish_durable_progress()).unwrap();
        let journal = pipeline.finish().unwrap();
        assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
        drop(journal);
        let journal = drive(&mut controller, open(io, 2)).unwrap();
        assert_eq!(journal.accepted_position().unwrap().op_number, 4);
    }
}

#[test]
fn async_pipeline_handles_reversed_physical_completion_but_rejects_reversed_installation() {
    for ordered in [false, true] {
        let (mut controller, journal) = empty_journal();
        let mut pipeline = journal.begin_write_pipeline(2).unwrap();
        let mut work = reserve(&mut pipeline, 2, BodyEncoding::Raw, false);
        let mut second = Box::pin(work.pop().unwrap().write());
        let mut first = Box::pin(work.pop().unwrap().write());
        assert!(poll(first.as_mut()).is_pending());
        assert!(poll(second.as_mut()).is_pending());
        let jobs = controller.jobs();
        assert_eq!(jobs.len(), 2);
        for &(id, _) in jobs.iter().rev() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        let Poll::Ready(first) = poll(first.as_mut()) else {
            panic!("first ready");
        };
        let Poll::Ready(second) = poll(second.as_mut()) else {
            panic!("second ready");
        };
        assert_eq!(
            pipeline.journal().written_position().unwrap(),
            LogPosition::GENESIS
        );
        if ordered {
            pipeline.complete(first).unwrap();
            pipeline.complete(second).unwrap();
            assert_eq!(
                pipeline
                    .finish()
                    .unwrap()
                    .accepted_position()
                    .unwrap()
                    .op_number,
                2
            );
        } else {
            assert!(pipeline.complete(second).is_err());
            assert!(pipeline.complete(first).is_err());
            assert!(pipeline.finish().is_err());
        }
    }
}

#[test]
fn async_pipeline_buffered_results_and_durable_evidence_exclude_later_reservations() {
    let (mut controller, io) = setup(Image::default());
    let mut bounds = limits();
    bounds.io.write_mode = SegmentWriteMode::Buffered;
    let journal = drive(
        &mut controller,
        Journal::format(
            "/group".into(),
            io.clone(),
            spec(CommitMode::External),
            JournalGeneration(1),
            bounds,
        ),
    )
    .unwrap();
    let mut pipeline = journal.begin_write_pipeline(2).unwrap();
    let mut work = reserve(&mut pipeline, 2, BodyEncoding::Raw, true);
    let later = work.pop().unwrap();
    let completed = drive(&mut controller, work.pop().unwrap().write());
    pipeline.complete(completed).unwrap();
    assert_eq!(pipeline.journal().written_position().unwrap().op_number, 1);
    assert_eq!(
        pipeline.journal().accepted_position().unwrap(),
        LogPosition::GENESIS
    );
    assert!(pipeline.journal().freeze_history(32768).is_err());
    drive(&mut controller, pipeline.sync_installed()).unwrap();
    drive(&mut controller, pipeline.publish_durable_progress()).unwrap();
    assert_eq!(pipeline.journal().accepted_position().unwrap().op_number, 1);
    drop(later);
    assert!(pipeline.finish().is_err());
    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 1);
    assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
}

#[test]
fn async_pipeline_lost_failed_and_foreign_completions_never_restore_mutation() {
    for case in 0..4 {
        let (mut controller, journal) = empty_journal();
        let io = journal.access.io.clone();
        let mut pipeline = journal.begin_write_pipeline(2).unwrap();
        let work = reserve(&mut pipeline, 1, BodyEncoding::Raw, false)
            .pop()
            .unwrap();
        match case {
            0 => drop(work),
            1 => {
                let completed = drive_with(&mut controller, work.write(), |_| Effect::Short(100));
                assert!(!completed.succeeded());
                assert!(pipeline.complete(completed).is_err());
            }
            2 => {
                let mut future = Box::pin(work.write());
                assert!(poll(future.as_mut()).is_pending());
                drop(future);
            }
            3 => {
                let journal = drive(
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
                let mut other = journal.begin_write_pipeline(1).unwrap();
                let foreign = reserve(&mut other, 1, BodyEncoding::Raw, false)
                    .pop()
                    .unwrap();
                let completed = drive(&mut controller, foreign.write());
                assert!(pipeline.complete(completed).is_err());
                drop(work);
            }
            _ => unreachable!(),
        }
        assert!(pipeline.finish().is_err());
    }
}

#[test]
fn canceled_async_pipeline_write_keeps_shared_body_until_physical_completion() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Owned {
        bytes: Vec<u8>,
        dropped: std::sync::Arc<AtomicUsize>,
    }
    impl AsRef<[u8]> for Owned {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Owned {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (mut controller, journal) = empty_journal();
    let mut pipeline = journal.begin_write_pipeline(1).unwrap();
    let dropped = std::sync::Arc::new(AtomicUsize::new(0));
    let body = bytes::Bytes::from_owner(Owned {
        bytes: vec![1; 16],
        dropped: dropped.clone(),
    });
    let header = operation(pipeline.journal()).header();
    let group = pipeline
        .begin_group_encoding()
        .unwrap()
        .encode_shared_raw(vec![SharedJournalOperation {
            header,
            body_digest: canonical_body_digest(&body),
            body,
        }])
        .unwrap();
    let work = pipeline.prepare(group, 16).unwrap();
    let mut future = Box::pin(work.write());
    assert!(poll(future.as_mut()).is_pending());
    drop(future);
    drop(pipeline);
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    let jobs = controller.jobs();
    for (id, _) in jobs {
        controller.execute(id, Effect::Normal).unwrap();
        controller.deliver(id).unwrap();
    }
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn async_pipeline_reuses_preencoded_group_after_roll_and_rejects_impossible_charges() {
    let (mut controller, io) = setup(Image::default());
    let mut format = spec(CommitMode::External);
    format.first_segment =
        SegmentHeader::new(identity().group_id, 1, None, Digest::ZERO, 8192).unwrap();
    let journal = drive(
        &mut controller,
        Journal::format("/group".into(), io, format, JournalGeneration(1), limits()),
    )
    .unwrap();
    let mut pipeline = journal.begin_write_pipeline(2).unwrap();
    let work = reserve(&mut pipeline, 1, BodyEncoding::Raw, false)
        .pop()
        .unwrap();
    pipeline
        .complete(drive(&mut controller, work.write()))
        .unwrap();
    let group = next_group(&mut pipeline);
    assert!(pipeline.check(&group).is_err());
    let mut journal = pipeline.finish().unwrap();
    drive(&mut controller, journal.roll_active(8192, 4)).unwrap();
    let mut pipeline = journal.begin_write_pipeline(1).unwrap();
    let prepared = pipeline.prepare(group, 0).unwrap();
    pipeline
        .complete(drive(&mut controller, prepared.write()))
        .unwrap();
    assert_eq!(
        pipeline
            .finish()
            .unwrap()
            .accepted_position()
            .unwrap()
            .op_number,
        2
    );

    let (mut controller, journal) = empty_journal();
    let mut pipeline = journal.begin_write_pipeline(1).unwrap();
    let op = operation(pipeline.journal());
    let group = pipeline
        .begin_group_encoding()
        .unwrap()
        .encode_shared_raw(vec![SharedJournalOperation {
            header: op.header(),
            body: bytes::Bytes::copy_from_slice(op.body),
            body_digest: canonical_body_digest(op.body),
        }])
        .unwrap();
    assert!(pipeline.prepare(group, 4 * 1024 * 1024).is_err());
    assert_eq!(pipeline.pending(), 0);
    assert!(controller.jobs().is_empty());
    assert!(pipeline.finish().is_ok());
}
