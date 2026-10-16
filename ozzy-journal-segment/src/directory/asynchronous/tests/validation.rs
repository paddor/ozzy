use super::*;
use crate::{GROUP_CONFIGURATION_MAX_BYTES, WriterError};
use ozzy_proto::CheckpointId;

fn format_with(mode: CommitMode, chosen: Limits) -> (Controller, Journal) {
    let (mut controller, io) = setup(Image::default());
    let journal = drive(
        &mut controller,
        Journal::format(
            "/group".into(),
            io,
            spec(mode),
            JournalGeneration(1),
            chosen,
        ),
    )
    .unwrap();
    (controller, journal)
}

async fn open_with(io: Local, generation: u128, chosen: Limits) -> Result<Journal, DirectoryError> {
    Journal::open(
        "/group".into(),
        io,
        crate::ExpectedStore {
            identity: identity(),
            configuration: Some(b"test deployment"),
        },
        JournalGeneration(generation),
        chosen,
    )
    .await
}

fn reopened(controller: Controller) -> (Controller, Journal) {
    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    (controller, journal)
}

fn installed_second_manifest(controller: &Controller) -> bool {
    controller
        .image()
        .exists(Path::new("/group/MANIFEST.2"), false)
}

#[test]
fn format_refuses_existing_stores_and_configuration_lengths_before_creating_files() {
    let (mut controller, io) = setup(baseline());
    assert!(matches!(
        drive(
            &mut controller,
            Journal::format(
                "/group".into(),
                io,
                spec(CommitMode::External),
                JournalGeneration(2),
                limits(),
            ),
        ),
        Err(DirectoryError::StoreAlreadyExists)
    ));
    let (_, journal) = reopened(controller);
    assert_eq!(journal.accepted_position().unwrap().op_number, 1);
    drop(journal);

    for length in [0, GROUP_CONFIGURATION_MAX_BYTES + 1] {
        let (mut controller, io) = setup(Image::default());
        let mut format = spec(CommitMode::External);
        format.configuration = vec![7; length];
        assert!(matches!(
            drive(
                &mut controller,
                Journal::format("/group".into(), io, format, JournalGeneration(1), limits()),
            ),
            Err(DirectoryError::ConfigurationLength)
        ));
        assert!(!controller.image().exists(Path::new("/group"), false));
    }
    let (mut controller, io) = setup(baseline());
    assert!(matches!(
        drive(
            &mut controller,
            Journal::open(
                "/group".into(),
                io,
                crate::ExpectedStore {
                    identity: identity(),
                    configuration: Some(b""),
                },
                JournalGeneration(2),
                limits(),
            ),
        ),
        Err(DirectoryError::ConfigurationLength)
    ));
}

#[test]
fn recovery_removes_only_recognized_abandoned_staging_workspaces() {
    let (mut controller, journal) = empty_journal();
    let staging = Path::new("/group/staging");
    let index = staging.join("index-1-123-0");
    let checkpoint = staging.join(format!(".checkpoint-{}-123-0.tmp", "31".repeat(16)));
    let operator = staging.join("index-01-123-0");
    for directory in [&index, &checkpoint, &operator] {
        drive(
            &mut controller,
            journal.access.done(Operation::CreateDirectory {
                path: directory.clone(),
            }),
        )
        .unwrap();
    }
    for directory in [&index, &checkpoint] {
        let file = drive(
            &mut controller,
            journal.access.open(
                directory.join("partial"),
                ozzy_io::OpenMode::CreateNew,
                false,
                false,
            ),
        )
        .unwrap();
        drive(
            &mut controller,
            journal.access.write_all(&file, 0, b"partial"),
        )
        .unwrap();
        drive(&mut controller, journal.access.sync(&file)).unwrap();
        drive(
            &mut controller,
            journal.access.sync_directory(directory.clone()),
        )
        .unwrap();
    }
    drive(
        &mut controller,
        journal.access.sync_directory(staging.to_path_buf()),
    )
    .unwrap();
    drop(journal);

    let (controller, journal) = reopened(controller);
    drop(journal);
    let image = controller.crash(true).unwrap().0;
    assert!(!image.exists(&index, true));
    assert!(!image.exists(&checkpoint, true));
    assert!(image.exists(&operator, true));
}

#[test]
fn local_commit_mode_needs_one_data_sync_and_recovers_complete_groups() {
    let (mut controller, mut journal) = format_with(CommitMode::LocalDurable, limits());
    let op = operation(&journal);
    let first = drive(&mut controller, journal.append(&[op], BodyEncoding::Raw)).unwrap();
    drive(&mut controller, journal.sync_through(first)).unwrap();
    assert_eq!(journal.committed_position().unwrap().op_number, 1);
    assert_eq!(journal.manifest.committed, LogPosition::GENESIS);
    assert!(matches!(
        journal.prepare_checkpoint_source(
            CheckpointId::from_bytes([0x73; 16]),
            Digest::from_bytes([0x74; 32]),
            1024,
        ),
        Err(DirectoryError::LocalProgressUnpublished)
    ));
    drop(journal);

    let (mut controller, mut journal) = reopened(controller);
    assert_eq!(journal.current().generation, 2);
    assert_eq!(journal.manifest.committed.op_number, 1);
    let mut replayed = Vec::new();
    drive(
        &mut controller,
        journal.replay_accepted(|item| {
            replayed.push((item.operation.op_number, item.committed));
            Ok::<_, io::Error>(())
        }),
    )
    .unwrap();
    assert_eq!(replayed, [(1, true)]);
    let op = operation(&journal);
    let second = drive(&mut controller, journal.append(&[op], BodyEncoding::Raw)).unwrap();
    drive(&mut controller, journal.sync_through(second)).unwrap();
    assert_eq!(journal.current().generation, 2);
    drive(&mut controller, journal.publish_progress()).unwrap();
    assert_eq!(journal.current().generation, 3);
    assert_eq!(journal.manifest.committed.op_number, 2);
}

#[test]
fn metadata_install_requires_an_exact_durable_position() {
    let (mut controller, mut journal) = empty_journal();
    let op = operation(&journal);
    let written = drive(&mut controller, journal.append(&[op], BodyEncoding::Raw)).unwrap();
    let digest = written.next_chain().previous_digest();
    let mut next = journal.next_manifest().unwrap();
    next.accepted = LogPosition {
        op_number: 1,
        digest,
    };
    assert!(matches!(
        drive(&mut controller, journal.install_metadata(next.clone())),
        Err(DirectoryError::PositionNotDurable(1))
    ));
    drive(&mut controller, journal.sync_through(written)).unwrap();
    next.accepted.digest = Digest::from_bytes([0xfe; 32]);
    assert!(matches!(
        drive(&mut controller, journal.install_metadata(next)),
        Err(DirectoryError::PositionMismatch(1))
    ));
    assert_eq!(journal.current().generation, 1);
    assert!(!installed_second_manifest(&controller));
}

#[test]
fn ordinary_metadata_install_never_regresses_hard_state() {
    let (mut controller, mut journal) = empty_journal();
    let written = drive(&mut controller, append_confirmed(&mut journal));
    let mut next = journal.next_manifest().unwrap();
    next.accepted = position_before(written.next_chain()).unwrap();
    next.committed = next.accepted;
    drive(&mut controller, journal.install_metadata(next)).unwrap();
    let installed = journal.current().generation;

    let mut regressed = journal.next_manifest().unwrap();
    regressed.accepted = LogPosition::GENESIS;
    regressed.committed = LogPosition::GENESIS;
    assert!(matches!(
        drive(&mut controller, journal.install_metadata(regressed)),
        Err(DirectoryError::HardStateRegression)
    ));
    assert_eq!(journal.current().generation, installed);
    assert!(!journal.is_faulted());
    drop(journal);
    let (_, journal) = reopened(controller);
    assert_eq!(journal.manifest.committed.op_number, 1);
}

#[test]
fn generic_metadata_cannot_change_configuration_or_commit_mode() {
    let (mut controller, mut journal) = empty_journal();
    let mut epoch = journal.next_manifest().unwrap();
    epoch.configuration_epoch = 2;
    assert!(matches!(
        drive(&mut controller, journal.install_metadata(epoch)),
        Err(DirectoryError::ConfigurationChangeRequiresInstall)
    ));
    let mut evidence = journal.next_manifest().unwrap();
    evidence.durable_evidence = !evidence.durable_evidence;
    assert!(matches!(
        drive(&mut controller, journal.install_metadata(evidence)),
        Err(DirectoryError::ConfigurationChangeRequiresInstall)
    ));
    let mut mode = journal.next_manifest().unwrap();
    mode.commit_mode = CommitMode::LocalDurable;
    assert!(matches!(
        drive(&mut controller, journal.install_metadata(mode)),
        Err(DirectoryError::CommitModeChange)
    ));
    assert_eq!(journal.current().generation, 1);
    assert!(!installed_second_manifest(&controller));
}

#[test]
fn roll_rejects_unsynchronized_or_empty_active_segment() {
    let (mut controller, mut journal) = empty_journal();
    assert!(matches!(
        drive(&mut controller, journal.roll_active(32768, 4)),
        Err(DirectoryError::EmptyActiveSegment)
    ));
    let op = operation(&journal);
    drive(&mut controller, journal.append(&[op], BodyEncoding::Raw)).unwrap();
    assert!(matches!(
        drive(&mut controller, journal.roll_active(32768, 4)),
        Err(DirectoryError::ActiveSegmentNotDurable)
    ));
    assert!(!journal.is_faulted());
    assert!(
        !controller
            .image()
            .exists(Path::new("/group/segments/2.log"), false)
    );
}

#[test]
fn decoded_body_limit_forces_a_recoverable_segment_roll() {
    let mut chosen = limits();
    chosen.decode.max_decoded_body_bytes = 16;
    chosen.decode.max_group_decoded_body_bytes = 16;
    chosen.decode.max_segment_decoded_body_bytes = 16;
    let (mut controller, mut journal) = format_with(CommitMode::External, chosen);
    let first = drive(&mut controller, append_confirmed(&mut journal));
    let second = operation(&journal);
    assert!(matches!(
        drive(
            &mut controller,
            journal.append(&[second], BodyEncoding::Raw)
        ),
        Err(DirectoryError::Codec(CodecError::SegmentDecodedBodyLimit {
            actual: 32,
            limit: 16
        }))
    ));
    assert_eq!(journal.writer.written_position(), first);
    assert!(!journal.is_faulted());

    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    drive(&mut controller, append_confirmed(&mut journal));
    drop(journal);
    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let recovered = drive(&mut controller, open_with(io, 2, chosen)).unwrap();
    assert_eq!(recovered.writer.written_position().group_number(), 2);
    assert_eq!(recovered.manifest.segments.len(), 2);
}

/// The journal refuses `invalid`, the raw writer stores it, and reopening the
/// synchronized bytes must refuse it again with `rejected`.
fn recovery_rejects(
    change: impl Fn(&mut CanonicalOperation<'static>),
    rejected: impl Fn(&DirectoryError) -> bool,
) {
    let (mut controller, mut journal) = empty_journal();
    let mut invalid = operation(&journal);
    change(&mut invalid);
    let error = drive(
        &mut controller,
        journal.append(&[invalid], BodyEncoding::Raw),
    )
    .err()
    .unwrap();
    assert!(rejected(&error), "append: {error:?}");
    assert_eq!(
        journal.writer.written_position().end_offset(),
        crate::SEGMENT_HEADER_BYTES as u64
    );
    assert!(!journal.is_faulted());
    let written = drive(
        &mut controller,
        journal.writer.append(&[invalid], BodyEncoding::Raw),
    )
    .unwrap();
    drive(&mut controller, journal.writer.sync_through(written)).unwrap();
    drop(journal);

    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let error = drive(&mut controller, open(io, 2)).err().unwrap();
    assert!(rejected(&error), "recovery: {error:?}");
}

#[test]
fn recovery_rejects_physically_valid_noncanonical_operation_body() {
    recovery_rejects(
        |operation| operation.body = &b"bad"[..],
        |error| {
            matches!(
                error,
                DirectoryError::Operation(_) | DirectoryError::Writer(WriterError::Operation(_))
            )
        },
    );
}

#[test]
fn recovery_enforces_operation_configuration_and_promised_view() {
    recovery_rejects(
        |operation| operation.configuration_epoch = 2,
        |error| {
            matches!(
                error,
                DirectoryError::Writer(WriterError::ConfigurationMismatch { op_number: 1, .. })
            )
        },
    );
    recovery_rejects(
        |operation| operation.original_view = 1,
        |error| {
            matches!(
                error,
                DirectoryError::Writer(WriterError::ViewBeyondPromise { op_number: 1, .. })
            )
        },
    );
}
