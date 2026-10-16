use super::*;
mod bounds;
mod checkpoint_transfer;
mod crashes;
use crate::{
    SuffixReplacement, SuffixReplacementError as Error, SuffixStreamLimits,
    logical_operation_digest,
};

const CONFIG: &[u8] = b"test deployment";
const BODIES: [[u8; 16]; 12] = [
    [1; 16], [2; 16], [3; 16], [4; 16], [5; 16], [6; 16], [7; 16], [8; 16], [9; 16], [10; 16],
    [11; 16], [12; 16],
];

fn stream_request(replacement: SuffixReplacement, accepted: LogPosition) -> crate::SuffixRequest {
    crate::SuffixRequest {
        replacement,
        accepted,
        limits: stream_limits(),
    }
}

fn stream_limits() -> SuffixStreamLimits {
    SuffixStreamLimits {
        max_group_operations: 2,
        max_group_body_bytes: 128,
        max_segments: 16,
        max_staged_bytes: 128 * 1024,
        max_source_segment_bytes: 32768,
        max_orphan_probes: 8,
    }
}

fn operations(
    mut previous: LogPosition,
    bodies: &'static [[u8; 16]],
    view: u64,
) -> Vec<CanonicalOperation<'static>> {
    bodies
        .iter()
        .map(|body| {
            let op = CanonicalOperation {
                group_id: identity().group_id,
                configuration_epoch: 1,
                original_view: view,
                op_number: previous.op_number + 1,
                previous_digest: previous.digest,
                kind: OperationKind::Barrier,
                body,
            };
            previous = position(op);
            op
        })
        .collect()
}

fn position(op: CanonicalOperation<'_>) -> LogPosition {
    LogPosition {
        op_number: op.op_number,
        digest: logical_operation_digest(&op),
    }
}

fn request(journal: &Journal, committed: LogPosition) -> SuffixReplacement {
    SuffixReplacement {
        expected_current: journal.current(),
        protected_committed: journal.committed_position().unwrap(),
        promised_view: 2,
        last_normal_view: 2,
        committed,
        writer_generation: JournalGeneration(9),
        segment_capacity: 8192,
        body_encoding: BodyEncoding::Raw,
    }
}

async fn replay(journal: &Journal) -> Vec<(u64, Vec<u8>)> {
    let mut result = Vec::new();
    journal
        .replay_accepted(|item| {
            result.push((item.operation.op_number, item.operation.body.to_vec()));
            Ok::<_, io::Error>(())
        })
        .await
        .unwrap();
    result
}

pub(super) async fn check_real_suffix(journal: Journal) -> Journal {
    let protected = journal.committed_position().unwrap();
    let accepted = journal.accepted_position().unwrap();
    let selected = CanonicalOperation {
        op_number: protected.op_number + 1,
        previous_digest: protected.digest,
        ..operation(&journal)
    };
    assert_eq!(position(selected), accepted);
    let replacement = SuffixReplacement {
        segment_capacity: 32768,
        ..request(&journal, protected)
    };
    let mut installer = journal
        .begin_suffix_replacement(stream_request(replacement, accepted))
        .await
        .unwrap();
    installer.append_chunk(&[selected]).await.unwrap();
    let journal = Box::pin(installer.finish()).await.unwrap();
    assert_eq!(journal.accepted_position().unwrap(), accepted);
    assert_eq!(journal.committed_position().unwrap(), protected);
    assert_eq!(journal.manifest.last_normal_view, 2);
    journal
}

#[test]
fn async_suffix_preserves_exact_protected_fragment_and_old_captured_history() {
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
        let io = journal.access.io.clone();
        let old = operations(LogPosition::GENESIS, &BODIES[..6], 0);
        for (index, chunk) in old.chunks(2).enumerate() {
            let written = drive(&mut controller, journal.append(chunk, encoding)).unwrap();
            drive(&mut controller, journal.sync_through(written)).unwrap();
            if index < 2 {
                drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
            }
        }
        let mut next = journal.next_manifest().unwrap();
        next.accepted = position(old[5]);
        next.committed = position(old[2]);
        drive(&mut controller, journal.install_metadata(next)).unwrap();
        let selected = journal.current();
        let earlier = journal.manifest.segments[0];
        let originals = (1..=3)
            .map(|id| {
                controller
                    .image()
                    .bytes(Path::new(&format!("/group/segments/{id}.log")), false)
                    .unwrap()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        let mut history = journal.freeze_history(32768).unwrap();
        let new = operations(position(old[2]), &BODIES[6..], 1);
        let mut replacement = request(&journal, position(new[3]));
        replacement.body_encoding = encoding;
        let mut installer = drive(
            &mut controller,
            journal.begin_suffix_replacement(stream_request(replacement, position(new[5]))),
        )
        .unwrap();
        for chunk in new.chunks(2) {
            drive(&mut controller, installer.append_chunk(chunk)).unwrap();
        }
        assert_eq!(
            crate::decode_current(
                controller
                    .image()
                    .bytes(Path::new("/group/CURRENT"), false)
                    .unwrap()
            )
            .unwrap(),
            selected
        );
        let mut journal = drive(&mut controller, installer.finish()).unwrap();
        assert_eq!(journal.manifest.segments[0], earlier);
        assert_eq!(journal.accepted_position().unwrap(), position(new[5]));
        assert_eq!(journal.committed_position().unwrap(), position(new[3]));
        assert_eq!(journal.manifest.last_normal_view, 2);
        assert_eq!(
            drive(&mut controller, history.position(6)).unwrap(),
            Some(position(old[5]))
        );
        for (index, expected) in originals.iter().enumerate() {
            assert_eq!(
                controller
                    .image()
                    .bytes(
                        Path::new(&format!("/group/segments/{}.log", index + 1)),
                        false
                    )
                    .unwrap(),
                expected
            );
        }
        let expected = old[..3]
            .iter()
            .chain(&new)
            .map(|op| (op.op_number, op.body.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(drive(&mut controller, replay(&journal)), expected);
        assert_eq!(
            drive(&mut controller, journal.reclaim_unreferenced_segments(8))
                .unwrap()
                .removed_segment_ids,
            [] as [u64; 0]
        );
        drop(history);
        assert_eq!(
            drive(&mut controller, journal.reclaim_unreferenced_segments(8))
                .unwrap()
                .removed_segment_ids,
            [2, 3]
        );
        drop(journal);
        let journal = drive(&mut controller, open(io, 12)).unwrap();
        assert_eq!(drive(&mut controller, replay(&journal)), expected);
    }
}

#[test]
fn async_suffix_rejects_whole_bad_chunks_before_writing_and_can_abort_healthy_staging() {
    let (mut controller, journal) = empty_journal();
    let selected = journal.current();
    let old = controller
        .image()
        .bytes(Path::new("/group/segments/1.log"), false)
        .unwrap()
        .to_vec();
    let ops = operations(LogPosition::GENESIS, &BODIES[..4], 1);
    let replacement = request(&journal, position(ops[2]));
    let mut installer = drive(
        &mut controller,
        journal.begin_suffix_replacement(stream_request(replacement, position(ops[3]))),
    )
    .unwrap();
    let wrong = CanonicalOperation {
        body: &[99; 16],
        ..ops[1]
    };
    assert!(
        matches!(
            drive(&mut controller, installer.append_chunk(&[ops[0], wrong])),
            Ok(())
        ),
        "intermediate prefix not yet anchored"
    );
    // The next chunk exposes that wrong predecessor. It cannot change the staged
    // prefix; callers must abandon a semantically wrong donor stream.
    assert!(matches!(
        drive(&mut controller, installer.append_chunk(&ops[2..])),
        Err(Error::SelectedSuffixMismatch)
    ));
    let journal = drive(&mut controller, installer.abort()).unwrap();
    assert_eq!(journal.current(), selected);
    assert_eq!(
        controller
            .image()
            .bytes(Path::new("/group/segments/1.log"), false)
            .unwrap(),
        old
    );
    assert!(
        controller
            .image()
            .bytes(Path::new("/group/segments/2.log"), false)
            .is_err()
    );
    let mut installer = drive(
        &mut controller,
        journal.begin_suffix_replacement(stream_request(replacement, position(ops[3]))),
    )
    .unwrap();
    let wrong = CanonicalOperation {
        previous_digest: Digest::ZERO,
        ..ops[1]
    };
    assert!(matches!(
        drive(&mut controller, installer.append_chunk(&[ops[0], wrong])),
        Err(Error::SelectedSuffixMismatch)
    ));
    let wrong = CanonicalOperation {
        body: &[1; 1],
        ..ops[0]
    };
    assert!(matches!(
        drive(&mut controller, installer.append_chunk(&[wrong])),
        Err(Error::Operation(_))
    ));
    assert!(matches!(
        drive(&mut controller, installer.append_chunk(&ops)),
        Err(Error::LimitExceeded { .. })
    ));
    drive(&mut controller, installer.append_chunk(&ops[..2])).unwrap();
    let wrong = CanonicalOperation {
        body: &[99; 16],
        ..ops[2]
    };
    assert!(matches!(
        drive(&mut controller, installer.append_chunk(&[wrong])),
        Err(Error::CommitNotSelected)
    ));
    drive(&mut controller, installer.append_chunk(&ops[2..])).unwrap();
    let journal = drive(&mut controller, installer.finish()).unwrap();
    assert_eq!(journal.accepted_position().unwrap(), position(ops[3]));
}

#[test]
fn async_suffix_canceled_or_failed_write_fences_staging_but_not_old_selection() {
    for cancel in [false, true] {
        let (mut controller, journal) = empty_journal();
        let io = journal.access.io.clone();
        let selected = journal.current();
        let ops = operations(LogPosition::GENESIS, &BODIES[..2], 1);
        let replacement = request(&journal, position(ops[0]));
        let mut installer = drive(
            &mut controller,
            journal.begin_suffix_replacement(stream_request(replacement, position(ops[1]))),
        )
        .unwrap();
        if cancel {
            let mut future = Box::pin(installer.append_chunk(&ops));
            assert!(poll(future.as_mut()).is_pending());
            let (id, Stage::Queued) = controller.jobs()[0] else {
                panic!("queued write");
            };
            assert!(matches!(
                controller.operation(id).unwrap().unprotected(),
                Operation::Write { .. }
            ));
            controller.execute(id, Effect::Normal).unwrap();
            drop(future);
            controller.deliver(id).unwrap();
        } else {
            assert!(
                drive_with(&mut controller, installer.append_chunk(&ops), |op| {
                    if matches!(op, Operation::Write { .. }) {
                        Effect::FailAfter(io::ErrorKind::Other)
                    } else {
                        Effect::Normal
                    }
                })
                .is_err()
            );
        }
        assert!(matches!(
            drive(&mut controller, installer.append_chunk(&ops)),
            Err(Error::StagingFaulted)
        ));
        assert!(matches!(
            drive(&mut controller, installer.abort()),
            Err(Error::StagingFaulted)
        ));
        let journal = drive(&mut controller, open(io, 12)).unwrap();
        assert_eq!(journal.current(), selected);
        assert_eq!(journal.accepted_position().unwrap(), LogPosition::GENESIS);
    }
}

#[test]
fn async_suffix_quota_and_probe_bounds_leave_old_generation_selected() {
    let (mut controller, journal) = empty_journal();
    let io = journal.access.io.clone();
    let selected = journal.current();
    let ops = operations(LogPosition::GENESIS, &BODIES[..4], 1);
    let replacement = request(&journal, LogPosition::GENESIS);
    let mut installer = drive(
        &mut controller,
        journal.begin_suffix_replacement(crate::SuffixRequest {
            replacement,
            accepted: position(ops[3]),
            limits: SuffixStreamLimits {
                max_staged_bytes: 8192,
                ..stream_limits()
            },
        }),
    )
    .unwrap();
    drive(&mut controller, installer.append_chunk(&ops[..2])).unwrap();
    assert!(matches!(
        drive(&mut controller, installer.append_chunk(&ops[2..])),
        Err(Error::StagingQuota { .. })
    ));
    drop(installer);
    let journal = drive(&mut controller, open(io.clone(), 12)).unwrap();
    assert_eq!(journal.current(), selected);
    assert!(matches!(
        drive(
            &mut controller,
            journal.begin_suffix_replacement(crate::SuffixRequest {
                replacement,
                accepted: position(ops[3]),
                limits: SuffixStreamLimits {
                    max_orphan_probes: 1,
                    ..stream_limits()
                }
            })
        ),
        Err(Error::ReplacementSegmentConflict)
    ));
    let journal = drive(&mut controller, open(io, 13)).unwrap();
    assert_eq!(journal.current(), selected);
    let mut installer = drive(
        &mut controller,
        journal.begin_suffix_replacement(stream_request(replacement, position(ops[3]))),
    )
    .unwrap();
    drive(&mut controller, installer.append_chunk(&ops[..2])).unwrap();
    assert!(matches!(
        drive(&mut controller, installer.finish()),
        Err(Error::IncompleteSuffix)
    ));
}

#[test]
fn async_recovery_suffix_keeps_marker_until_exact_private_configuration_publication() {
    let (mut controller, io) = setup(Image::default());
    let journal = drive(
        &mut controller,
        Journal::format_recovering(
            "/group".into(),
            io.clone(),
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        ),
    )
    .unwrap();
    let ops = operations(LogPosition::GENESIS, &BODIES[..4], 1);
    let replacement = request(&journal, position(ops[0]));
    let mut installer = drive(
        &mut controller,
        journal.begin_recovery_replacement(CONFIG, stream_request(replacement, position(ops[3]))),
    )
    .unwrap();
    for chunk in ops.chunks(2) {
        drive(&mut controller, installer.append_chunk(chunk)).unwrap();
    }
    let mut journal = drive(&mut controller, installer.finish()).unwrap();
    assert_eq!(
        journal.configuration().unwrap(),
        crate::directory::recovery::recovery_marker(CONFIG).unwrap()
    );
    let publication = crate::RecoveryPublication {
        current: journal.current(),
        generation: JournalGeneration(9),
        view: 2,
        accepted: position(ops[3]),
        committed: position(ops[0]),
    };
    let candidate = drive(
        &mut controller,
        journal.publish_recovered_configuration(
            CONFIG,
            publication,
            super::recovery::recovery_limits(),
        ),
    )
    .unwrap();
    assert_eq!(candidate.committed_images().committed().revision(), 1);
    assert_eq!(candidate.accepted_position(), position(ops[3]));
    drop(candidate);
    drop(journal);
    let journal = drive(&mut controller, open(io, 12)).unwrap();
    assert_eq!(journal.accepted_position().unwrap(), position(ops[3]));
    assert_eq!(journal.committed_position().unwrap(), position(ops[0]));
}

#[test]
fn replacement_preserves_earlier_sealed_segments_and_supports_empty_selected_suffix() {
    for inside_group in [false, true] {
        let (mut controller, mut journal) = empty_journal();
        let ops = operations(LogPosition::GENESIS, &BODIES[..4], 0);
        let written = drive(
            &mut controller,
            journal.append(&ops[..2], BodyEncoding::Raw),
        )
        .unwrap();
        drive(&mut controller, journal.sync_through(written)).unwrap();
        drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        let earlier = journal.manifest.segments[0];
        let sealed = Path::new("/group/segments/1.log");
        let original = controller.image().bytes(sealed, false).unwrap().to_vec();
        let written = drive(
            &mut controller,
            journal.append(&ops[2..], BodyEncoding::Raw),
        )
        .unwrap();
        drive(&mut controller, journal.sync_through(written)).unwrap();
        let protected = position(ops[if inside_group { 2 } else { 1 }]);
        let mut next = journal.next_manifest().unwrap();
        next.accepted = position(ops[3]);
        next.committed = protected;
        drive(&mut controller, journal.install_metadata(next)).unwrap();
        let replacement = request(&journal, protected);
        let installer = drive(
            &mut controller,
            journal.begin_suffix_replacement(stream_request(replacement, protected)),
        )
        .unwrap();
        let journal = drive(&mut controller, installer.finish()).unwrap();
        assert_eq!(journal.manifest.segments[0], earlier);
        assert_eq!(controller.image().bytes(sealed, false).unwrap(), original);
        assert_eq!(journal.accepted_position().unwrap(), protected);
        assert_eq!(journal.committed_position().unwrap(), protected);
        drop(journal);
        let (mut controller, io) = setup(controller.crash(true).unwrap().0);
        let journal = drive(&mut controller, open(io, 10)).unwrap();
        assert_eq!(journal.accepted_position().unwrap(), protected);
    }
}

fn recovering(controller: &mut Controller, io: &Local) -> Journal {
    drive(
        controller,
        Journal::format_recovering(
            "/group".into(),
            io.clone(),
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        ),
    )
    .unwrap()
}

#[test]
fn fresh_recovery_replaces_unadmitted_attempt_not_ordinary_voter_history() {
    use super::recovery::{recovery_limits, reopen_replacement};
    let (mut controller, io) = setup(Image::default());
    let journal = recovering(&mut controller, &io);
    let ops = operations(LogPosition::GENESIS, &BODIES[..1], 2);
    let accepted = position(ops[0]);
    let mut replacement = SuffixReplacement {
        writer_generation: JournalGeneration(2),
        ..request(&journal, accepted)
    };
    let mut installer = drive(
        &mut controller,
        journal.begin_suffix_replacement(stream_request(replacement, accepted)),
    )
    .unwrap();
    drive(&mut controller, installer.append_chunk(&ops)).unwrap();
    // Stop before publishing the voter configuration.
    drop(drive(&mut controller, installer.finish()).unwrap());

    let journal = drive(&mut controller, reopen_replacement(io.clone(), 3)).unwrap();
    replacement.expected_current = journal.current();
    replacement.writer_generation = JournalGeneration(4);
    replacement.promised_view = 3;
    replacement.last_normal_view = 3;
    replacement.committed = LogPosition::GENESIS;
    // Ordinary suffix installation still cannot lower even this local commit floor.
    assert!(matches!(
        drive(
            &mut controller,
            journal.begin_suffix_replacement(crate::SuffixRequest {
                replacement,
                accepted,
                limits: stream_limits()
            })
        ),
        Err(Error::ProtectedCommitMismatch)
    ));
    let journal = drive(&mut controller, reopen_replacement(io.clone(), 3)).unwrap();
    let mut installer = drive(
        &mut controller,
        journal.begin_recovery_replacement(CONFIG, stream_request(replacement, accepted)),
    )
    .unwrap();
    drive(&mut controller, installer.append_chunk(&ops)).unwrap();
    let mut journal = drive(&mut controller, installer.finish()).unwrap();
    assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
    let publication = crate::RecoveryPublication {
        current: journal.current(),
        generation: JournalGeneration(4),
        view: 3,
        accepted,
        committed: LogPosition::GENESIS,
    };
    let candidate = drive(
        &mut controller,
        journal.publish_recovered_configuration(CONFIG, publication, recovery_limits()),
    )
    .unwrap();
    assert_eq!(candidate.accepted_position(), accepted);
    drop(candidate);
    replacement.expected_current = journal.current();
    replacement.writer_generation = JournalGeneration(5);
    assert!(matches!(
        drive(
            &mut controller,
            journal.begin_recovery_replacement(
                CONFIG,
                crate::SuffixRequest {
                    replacement,
                    accepted,
                    limits: stream_limits()
                }
            )
        ),
        Err(Error::Directory(DirectoryError::ConfigurationMismatch))
    ));
}

#[test]
fn syntactically_valid_duplicate_identity_cannot_publish_voter_configuration() {
    use super::recovery::{recovery_limits, reopen_replacement};
    const SAME: [[u8; 16]; 2] = [[1; 16]; 2];
    let (mut controller, io) = setup(Image::default());
    let journal = recovering(&mut controller, &io);
    let ops = operations(LogPosition::GENESIS, &SAME, 1);
    let accepted = position(ops[1]);
    let replacement = request(&journal, accepted);
    let mut installer = drive(
        &mut controller,
        journal.begin_recovery_replacement(CONFIG, stream_request(replacement, accepted)),
    )
    .unwrap();
    drive(&mut controller, installer.append_chunk(&ops)).unwrap();
    let mut journal = drive(&mut controller, installer.finish()).unwrap();
    assert_eq!(journal.accepted_position().unwrap(), accepted);
    let publication = crate::RecoveryPublication {
        current: journal.current(),
        generation: JournalGeneration(9),
        view: 2,
        accepted,
        committed: accepted,
    };
    let error = drive(
        &mut controller,
        journal.publish_recovered_configuration(CONFIG, publication, recovery_limits()),
    )
    .err()
    .unwrap();
    assert!(
        matches!(error, crate::RecoveryPublicationError::Canonical(_)),
        "{error:?}"
    );
    assert!(
        !controller
            .image()
            .exists(Path::new("/group/.CONFIGURATION.recovered.9.tmp"), false)
    );
    drop(journal);
    assert!(drive(&mut controller, open(io.clone(), 10)).is_err());
    let journal = drive(&mut controller, reopen_replacement(io, 11)).unwrap();
    assert_eq!(journal.current(), publication.current);
    assert_eq!(journal.manifest.accepted, accepted);
}
