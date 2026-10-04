use super::*;
use crate::{
    AsyncRecoveryDirectory as Recovery, CanonicalRecoveryLimits, RecoveryPublication,
    RecoveryPublicationError,
};

const CONFIG: &[u8] = b"test deployment";

mod checkpoint_quarantine;

pub(super) fn recovery_limits() -> CanonicalRecoveryLimits {
    CanonicalRecoveryLimits {
        index: super::indexes::build_limits(),
        checkpoint: limits().checkpoint,
        retained_identities: 4,
        accepted_transitions: 4,
        ..CanonicalRecoveryLimits::default()
    }
}
fn publication(journal: &Journal) -> RecoveryPublication {
    RecoveryPublication {
        current: journal.current(),
        generation: journal.writer.written_position().generation(),
        view: journal.manifest.promised_view,
        accepted: journal.manifest.accepted,
        committed: journal.manifest.committed,
    }
}
async fn repair_directory(io: Local) -> Result<Recovery, DirectoryError> {
    Recovery::open_for_repair("/group".into(), io, identity(), CONFIG, limits()).await
}
async fn recovering_directory(io: Local) -> Result<Recovery, DirectoryError> {
    Recovery::open_recovering("/group".into(), io, identity(), CONFIG, limits()).await
}
fn damaged() -> Image {
    let (mut controller, io) = setup(baseline());
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    let file = drive(
        &mut controller,
        journal.access.open(
            "/group/segments/1.log".into(),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(&mut controller, journal.access.write_all(&file, 0, &[99])).unwrap();
    drive(&mut controller, journal.access.sync(&file)).unwrap();
    let leftover = drive(
        &mut controller,
        journal.access.open(
            "/group/segments/2.log".into(),
            ozzy_io::OpenMode::CreateNew,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(&leftover, 0, b"leftover"),
    )
    .unwrap();
    drive(&mut controller, journal.access.sync(&leftover)).unwrap();
    drive(
        &mut controller,
        journal.access.sync_directory("/group/segments".into()),
    )
    .unwrap();
    drop(journal);
    drop(file);
    drop(leftover);
    controller.crash(true).unwrap().0
}
fn replacement() -> Image {
    let (mut controller, io) = setup(Image::default());
    let mut journal = drive(
        &mut controller,
        Journal::format_recovering(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        ),
    )
    .unwrap();
    drive(&mut controller, append_confirmed(&mut journal));
    drive(&mut controller, journal.publish_progress()).unwrap();
    drop(journal);
    controller.crash(true).unwrap().0
}
async fn reopen_replacement(io: Local, generation: u128) -> Result<Journal, DirectoryError> {
    let marker = crate::directory::recovery::recovery_marker(CONFIG)?;
    Journal::open(
        "/group".into(),
        io,
        identity(),
        Some(&marker),
        JournalGeneration(generation),
        limits(),
    )
    .await
}

#[test]
fn async_quarantine_preserves_damaged_files_and_skips_leftover_names() {
    let (mut controller, io) = setup(damaged());
    let before = controller
        .image()
        .bytes(Path::new("/group/segments/1.log"), false)
        .unwrap()
        .to_vec();
    assert!(drive(&mut controller, open(io.clone(), 3)).is_err());
    assert!(drive(&mut controller, recovering_directory(io.clone())).is_err());
    let directory = drive(&mut controller, repair_directory(io.clone())).unwrap();
    let directory = drive(&mut controller, directory.quarantine_for_recovery(CONFIG)).unwrap();
    assert_eq!(directory.manifest().segments[0].segment_id, 1);
    let journal = drive_with(
        &mut controller,
        directory.recover_nonvoting(CONFIG, JournalGeneration(4), 3),
        |operation| {
            if let Operation::Open { path, .. } = operation {
                assert_ne!(path, Path::new("/group/segments/1.log"));
            }
            Effect::Normal
        },
    )
    .unwrap();
    assert_eq!(journal.manifest.segments[0].segment_id, 3);
    assert_eq!(journal.accepted_position().unwrap(), LogPosition::GENESIS);
    assert_eq!(
        controller
            .image()
            .bytes(Path::new("/group/segments/1.log"), false)
            .unwrap(),
        before
    );
    assert_eq!(
        controller
            .image()
            .bytes(Path::new("/group/segments/2.log"), false)
            .unwrap(),
        b"leftover"
    );
    drop(journal);
    assert!(drive(&mut controller, open(io.clone(), 5)).is_err());
    assert!(drive(&mut controller, recovering_directory(io)).is_ok());
}

#[test]
fn async_configuration_publication_requires_exact_private_image() {
    let (mut controller, io) = setup(replacement());
    assert!(drive(&mut controller, open(io.clone(), 2)).is_err());
    let mut journal = drive(&mut controller, reopen_replacement(io.clone(), 3)).unwrap();
    let authority = publication(&journal);
    let wrong = RecoveryPublication {
        generation: JournalGeneration(9),
        ..authority
    };
    assert!(matches!(
        drive(
            &mut controller,
            journal.publish_recovered_configuration(CONFIG, wrong, recovery_limits())
        ),
        Err(RecoveryPublicationError::ImageMismatch)
    ));
    assert!(!journal.is_faulted());
    let candidate = drive(
        &mut controller,
        journal.publish_recovered_configuration(CONFIG, authority, recovery_limits()),
    )
    .unwrap();
    assert_eq!(journal.configuration(), Some(CONFIG));
    assert_eq!(candidate.committed_images().committed().revision(), 0);
    assert_eq!(candidate.accepted_position().op_number, 1);
    assert!(matches!(
        candidate.activate(&journal),
        Err(crate::CanonicalStateRecoveryError::CommitNotPublished)
    ));
    drop(journal);
    assert!(drive(&mut controller, recovering_directory(io.clone())).is_err());
    let journal = drive(&mut controller, open(io, 4)).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 1);
    assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
}

#[test]
fn async_recovery_never_bypasses_authority_or_checkpoint_validation() {
    for name in ["identity", "CURRENT", "DURABLE", "CONFIGURATION"] {
        let (mut controller, io) = setup(damaged());
        let access = Access {
            io: io.clone(),
            protection: None,
            readers: std::rc::Rc::default(),
        };
        let handle = drive(
            &mut controller,
            access.open(
                Path::new("/group").join(name),
                ozzy_io::OpenMode::ReadWrite,
                false,
                false,
            ),
        )
        .unwrap();
        let length = if name == "DURABLE" {
            crate::directory::evidence::FILE_BYTES
        } else {
            4096
        };
        drive(
            &mut controller,
            access.write_all(&handle, 0, &vec![0; length]),
        )
        .unwrap();
        drop(handle);
        assert!(
            drive(&mut controller, repair_directory(io)).is_err(),
            "{name}"
        );
    }
    let (mut controller, io) = setup(super::retention::retained_baseline());
    let journal = drive(&mut controller, open(io.clone(), 3)).unwrap();
    let id = journal.manifest.checkpoint.unwrap().checkpoint_id;
    let file = drive(
        &mut controller,
        journal.access.open(
            journal
                .root()
                .join("checkpoints")
                .join(crate::checkpoint_name(id))
                .join("manifest"),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(&mut controller, journal.access.write_all(&file, 0, &[99])).unwrap();
    drop(file);
    drop(journal);
    assert!(drive(&mut controller, repair_directory(io)).is_err());
}

#[test]
fn failed_async_recovery_publication_fences_even_after_configuration_rename() {
    let (mut controller, io) = setup(replacement());
    let mut journal = drive(&mut controller, reopen_replacement(io.clone(), 3)).unwrap();
    let authority = publication(&journal);
    let mut failed = false;
    let result = drive_with(
        &mut controller,
        journal.publish_recovered_configuration(CONFIG, authority, recovery_limits()),
        |operation| {
            if let Operation::Rename { destination, .. } = operation
                && destination == Path::new("/group/CONFIGURATION")
            {
                failed = true;
                Effect::FailAfter(std::io::ErrorKind::Other)
            } else {
                Effect::Normal
            }
        },
    );
    assert!(failed);
    assert!(result.is_err());
    assert!(journal.is_faulted());
    assert!(
        drive(
            &mut controller,
            journal.publish_recovered_configuration(CONFIG, authority, recovery_limits())
        )
        .is_err()
    );
    drop(journal);
    let journal = drive(&mut controller, open(io, 4)).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 1);
    assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
}

pub(super) fn run_cut<T, E: std::fmt::Debug>(
    controller: &mut Controller,
    future: impl std::future::Future<Output = Result<T, E>>,
    cut: usize,
    immediate: bool,
) -> bool {
    let mut future = std::pin::pin!(future);
    for _ in 0..cut {
        if let Poll::Ready(result) = poll(future.as_mut()) {
            result.unwrap();
            return true;
        }
        let (id, stage) = controller.jobs()[0];
        match stage {
            Stage::Queued => {
                controller.execute(id, Effect::Normal).unwrap();
                if immediate {
                    controller.deliver(id).unwrap();
                }
            }
            Stage::Executed => controller.deliver(id).unwrap(),
        }
    }
    false
}

#[test]
fn async_quarantine_and_private_reset_crash_cuts_never_restore_voting_configuration() {
    let image = damaged();
    let original = image
        .bytes(Path::new("/group/segments/1.log"), false)
        .unwrap()
        .to_vec();
    let marker = crate::directory::recovery::recovery_marker(CONFIG).unwrap();
    for immediate in [false, true] {
        let mut completed = false;
        for cut in 0..512 {
            let (mut controller, io) = setup(image.clone());
            let directory = drive(&mut controller, repair_directory(io)).unwrap();
            let done = run_cut(
                &mut controller,
                async {
                    directory
                        .quarantine_for_recovery(CONFIG)
                        .await?
                        .recover_nonvoting(CONFIG, JournalGeneration(3), 4)
                        .await
                },
                cut,
                immediate,
            );
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            assert_eq!(
                controller
                    .image()
                    .bytes(Path::new("/group/segments/1.log"), false)
                    .unwrap(),
                original
            );
            let configuration = controller
                .image()
                .bytes(Path::new("/group/CONFIGURATION"), false)
                .unwrap();
            if configuration == CONFIG {
                assert!(!done);
                let directory = drive(&mut controller, repair_directory(io)).unwrap();
                assert_eq!(directory.manifest().segments[0].segment_id, 1);
            } else {
                assert_eq!(configuration, marker);
                assert!(drive(&mut controller, open(io.clone(), 9)).is_err());
                let directory = drive(&mut controller, recovering_directory(io)).unwrap();
                assert!(matches!(directory.manifest().segments[0].segment_id, 1 | 3));
            }
            if done {
                completed = true;
                break;
            }
        }
        assert!(completed);
    }
}

#[test]
fn async_recovered_configuration_crash_cuts_select_only_complete_validated_images() {
    let image = replacement();
    let marker = crate::directory::recovery::recovery_marker(CONFIG).unwrap();
    for immediate in [false, true] {
        let mut completed = false;
        for cut in 0..512 {
            let (mut controller, io) = setup(image.clone());
            let mut journal = drive(&mut controller, reopen_replacement(io, 3)).unwrap();
            let authority = publication(&journal);
            let done = run_cut(
                &mut controller,
                journal.publish_recovered_configuration(CONFIG, authority, recovery_limits()),
                cut,
                immediate,
            );
            drop(journal);
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let configuration = controller
                .image()
                .bytes(Path::new("/group/CONFIGURATION"), false)
                .unwrap();
            if configuration == CONFIG {
                let journal = drive(&mut controller, open(io, 9)).unwrap();
                assert_eq!(journal.accepted_position().unwrap().op_number, 1);
                assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
            } else {
                assert!(!done);
                assert_eq!(configuration, marker);
                assert!(drive(&mut controller, open(io.clone(), 9)).is_err());
                assert!(drive(&mut controller, recovering_directory(io)).is_ok());
            }
            if done {
                completed = true;
                break;
            }
        }
        assert!(completed);
    }
}
