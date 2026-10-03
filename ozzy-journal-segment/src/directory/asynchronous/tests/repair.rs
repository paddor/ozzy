use super::*;
mod real;
use crate::{AsyncSealedRepair, ChainPosition, SealedRepairLimits, logical_operation_digest};

const CONFIG: &[u8] = b"test deployment";
const BODIES: [[u8; 16]; 8] = [
    [1; 16], [2; 16], [3; 16], [4; 16], [5; 16], [6; 16], [7; 16], [8; 16],
];

struct Fixture {
    image: Image,
    operations: Vec<CanonicalOperation<'static>>,
    accepted: LogPosition,
}

fn repair_limits(encoding: BodyEncoding) -> SealedRepairLimits {
    SealedRepairLimits {
        max_segment_bytes: 32768,
        max_chunk_operations: 2,
        max_chunk_body_bytes: 1024,
        max_staged_bytes: 65536,
        max_orphan_probes: 4,
        body_encoding: encoding,
    }
}

fn fixture(encoding: BodyEncoding) -> Fixture {
    let (mut controller, mut journal) = empty_journal();
    let mut chain = ChainPosition::GENESIS;
    let operations = BODIES
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
    for (index, ops) in operations.chunks(2).enumerate() {
        let position = drive(&mut controller, journal.append(ops, encoding)).unwrap();
        drive(&mut controller, journal.sync_through(position)).unwrap();
        drive(&mut controller, journal.publish_durable_progress()).unwrap();
        if index < 3 {
            drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        }
    }
    let accepted = journal.accepted_position().unwrap();
    for (id, offset) in [(1, 0), (3, 4096 + 192)] {
        let handle = drive(
            &mut controller,
            journal.access.open(
                journal.root().join(format!("segments/{id}.log")),
                ozzy_io::OpenMode::ReadWrite,
                false,
                false,
            ),
        )
        .unwrap();
        drive(
            &mut controller,
            journal.access.write_all(&handle, offset, &[99]),
        )
        .unwrap();
        drive(&mut controller, journal.access.sync(&handle)).unwrap();
        drop(handle);
    }
    drop(journal);
    Fixture {
        image: controller.crash(true).unwrap().0,
        operations,
        accepted,
    }
}

async fn directory(io: Local) -> Result<RecoveryDirectory, DirectoryError> {
    RecoveryDirectory::open_for_repair("/group".into(), io, identity(), CONFIG, limits()).await
}

async fn start(
    io: Local,
    accepted: LogPosition,
    limits: SealedRepairLimits,
) -> Result<AsyncSealedRepair, DirectoryError> {
    directory(io)
        .await?
        .quarantine_for_recovery(CONFIG)
        .await?
        .begin_sealed_repair(CONFIG, JournalGeneration(9), 7, accepted, limits)
        .await
}

async fn fill(repair: &mut AsyncSealedRepair, operations: &[CanonicalOperation<'_>]) -> usize {
    let mut transferred = 0;
    while let Some(range) = repair.pending().unwrap() {
        let index = range.after.op_number as usize;
        repair.append(&operations[index..=index]).await.unwrap();
        transferred += 1;
    }
    transferred
}

fn unchanged(fixture: &Fixture, controller: &Controller) {
    for id in 1..=4 {
        let path = PathBuf::from(format!("/group/segments/{id}.log"));
        assert_eq!(
            controller.image().bytes(&path, false).unwrap(),
            fixture.image.bytes(&path, false).unwrap(),
            "{id}"
        );
    }
}

fn nonvoting(controller: &Controller) {
    assert_eq!(
        controller
            .image()
            .bytes(Path::new("/group/CONFIGURATION"), false)
            .unwrap(),
        crate::directory::recovery::recovery_marker(CONFIG).unwrap()
    );
}

#[test]
fn async_sealed_repair_salvages_fragments_and_preserves_logical_history() {
    for encoding in [
        BodyEncoding::Raw,
        BodyEncoding::Lz4 {
            min_savings_bytes: 0,
        },
    ]
    .into_iter()
    .filter(|encoding| encoding.is_supported())
    {
        let fixture = fixture(encoding);
        let (mut controller, io) = setup(fixture.image.clone());
        let mut directory = drive(&mut controller, directory(io.clone())).unwrap();
        let ranges = drive(&mut controller, directory.sealed_damage(32768))
            .unwrap()
            .unwrap();
        assert_eq!(
            ranges.iter().map(|r| r.segment_id).collect::<Vec<_>>(),
            [1, 3]
        );
        drop(directory);
        let mut repair = drive(
            &mut controller,
            start(io.clone(), fixture.accepted, repair_limits(encoding)),
        )
        .unwrap();
        nonvoting(&controller);
        let transferred = drive(&mut controller, fill(&mut repair, &fixture.operations));
        if encoding == BodyEncoding::Raw {
            assert_eq!(transferred, 1);
        }
        assert!(transferred <= 2);
        let mut journal = drive(
            &mut controller,
            repair.finish(CONFIG, super::recovery::recovery_limits()),
        )
        .unwrap();
        assert_eq!(journal.accepted_position().unwrap(), fixture.accepted);
        assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
        assert_eq!(journal.manifest.last_normal_view, 0);
        assert_eq!(journal.manifest.promised_view, 7);
        assert_eq!(
            journal
                .manifest
                .segments
                .iter()
                .map(|r| r.file_generation)
                .collect::<Vec<_>>(),
            [1, 0, 1, 0]
        );
        unchanged(&fixture, &controller);
        let mut replayed = Vec::new();
        drive(
            &mut controller,
            journal.replay_accepted(|item| {
                replayed.push((item.operation.op_number, item.operation.body.to_vec()));
                Ok::<_, io::Error>(())
            }),
        )
        .unwrap();
        assert_eq!(
            replayed,
            fixture
                .operations
                .iter()
                .map(|op| (op.op_number, op.body.to_vec()))
                .collect::<Vec<_>>()
        );
        let captured = journal.capture_sealed_segments(&[1, 3]).unwrap();
        assert!(
            drive(&mut controller, journal.reclaim_unreferenced_segments(4))
                .unwrap()
                .removed_segment_ids
                .is_empty()
        );
        drop(captured);
        assert_eq!(
            drive(&mut controller, journal.reclaim_unreferenced_segments(4))
                .unwrap()
                .removed_segment_ids,
            [1, 3]
        );
        drop(journal);
        let journal = drive(&mut controller, open(io, 10)).unwrap();
        assert_eq!(journal.accepted_position().unwrap(), fixture.accepted);
    }
}

#[test]
fn async_sealed_repair_rejects_wrong_incomplete_and_oversized_donor_history() {
    let fixture = fixture(BodyEncoding::Raw);
    for case in 0..5 {
        let (mut controller, io) = setup(fixture.image.clone());
        let mut limits = repair_limits(BodyEncoding::Raw);
        if case == 2 {
            limits.max_staged_bytes = 32768;
        }
        let mut repair = drive(&mut controller, start(io, fixture.accepted, limits)).unwrap();
        match case {
            0 => assert!(
                drive(
                    &mut controller,
                    repair.finish(CONFIG, super::recovery::recovery_limits())
                )
                .is_err()
            ),
            1 => {
                let wrong = CanonicalOperation {
                    body: &[99; 16],
                    ..fixture.operations[4]
                };
                assert!(drive(&mut controller, repair.append(&[wrong])).is_err());
                assert!(repair.pending().is_err());
            }
            2 => assert!(drive(&mut controller, repair.append(&fixture.operations[4..5])).is_err()),
            3 => assert!(drive(&mut controller, repair.append(&fixture.operations[..3])).is_err()),
            4 => {
                let mut future = Box::pin(repair.append(&fixture.operations[4..5]));
                assert!(poll(future.as_mut()).is_pending());
                drop(future);
                assert!(repair.pending().is_err());
                assert!(drive(&mut controller, repair.append(&fixture.operations[4..5])).is_err());
            }
            _ => unreachable!(),
        }
        nonvoting(&controller);
        unchanged(&fixture, &controller);
    }
}

#[test]
fn async_sealed_repair_distinguishes_active_damage_and_device_failures() {
    let fixture = fixture(BodyEncoding::Raw);
    let (mut controller, io) = setup(fixture.image.clone());
    let access = Access {
        io: io.clone(),
        protection: None,
    };
    let mut directory = drive(&mut controller, directory(io)).unwrap();
    let error = drive_with(&mut controller, directory.sealed_damage(32768), |op| {
        if let Operation::Open { path, .. } = op
            && path == Path::new("/group/segments/1.log")
        {
            Effect::FailBefore(io::ErrorKind::PermissionDenied)
        } else {
            Effect::Normal
        }
    })
    .unwrap_err();
    assert!(
        matches!(error, DirectoryError::Io(error) if error.kind() == io::ErrorKind::PermissionDenied)
    );
    let handle = drive(
        &mut controller,
        access.open(
            "/group/segments/4.log".into(),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        access.write_all(&handle, 4096 + 192, &[99]),
    )
    .unwrap();
    assert!(
        drive(&mut controller, directory.sealed_damage(32768))
            .unwrap()
            .is_none()
    );
}

#[test]
fn async_sealed_repair_publication_crash_cuts_keep_complete_nonvoting_or_repaired_history() {
    let fixture = fixture(BodyEncoding::Raw);
    for immediate in [false, true] {
        let mut completed = false;
        for cut in 0..1600 {
            let (mut controller, io) = setup(fixture.image.clone());
            let done = super::recovery::run_cut(
                &mut controller,
                async {
                    let mut repair =
                        start(io, fixture.accepted, repair_limits(BodyEncoding::Raw)).await?;
                    fill(&mut repair, &fixture.operations).await;
                    Box::pin(repair.finish(CONFIG, super::recovery::recovery_limits())).await
                },
                cut,
                immediate,
            );
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            unchanged(&fixture, &controller);
            let configuration = controller
                .image()
                .bytes(Path::new("/group/CONFIGURATION"), false)
                .unwrap();
            if configuration == CONFIG {
                // Real configuration can be old (before quarantine) or fully
                // repaired. Old damaged history must still fail strict open.
                let directory = drive(&mut controller, directory(io.clone())).unwrap();
                let generations = directory
                    .manifest()
                    .segments
                    .iter()
                    .map(|r| r.file_generation)
                    .collect::<Vec<_>>();
                drop(directory);
                if generations == [0, 0, 0, 0] {
                    assert!(!done);
                    assert!(drive(&mut controller, open(io, 12)).is_err());
                } else {
                    assert_eq!(generations, [1, 0, 1, 0]);
                    let journal = drive(&mut controller, open(io, 12)).unwrap();
                    assert_eq!(journal.accepted_position().unwrap(), fixture.accepted);
                    assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
                }
            } else {
                assert!(!done);
                nonvoting(&controller);
                assert!(drive(&mut controller, open(io.clone(), 12)).is_err());
                let directory = drive(
                    &mut controller,
                    RecoveryDirectory::open_recovering(
                        "/group".into(),
                        io,
                        identity(),
                        CONFIG,
                        limits(),
                    ),
                )
                .unwrap();
                let generations = directory
                    .manifest()
                    .segments
                    .iter()
                    .map(|r| r.file_generation)
                    .collect::<Vec<_>>();
                assert!(generations == [0, 0, 0, 0] || generations == [1, 0, 1, 0]);
            }
            if done {
                completed = true;
                break;
            }
        }
        assert!(completed);
    }
}
