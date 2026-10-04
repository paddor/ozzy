use super::*;
use crate::directory::segment_name;
use crate::{AsyncRetirementBudget, RetentionFloors};

#[test]
fn async_quarantine_resets_checkpoint_selection_and_preserves_old_files() {
    for retire in [false, true] {
        let (mut controller, io) = setup(super::super::retention::retained_baseline());
        let mut journal = drive(&mut controller, open(io.clone(), 3)).unwrap();
        if retire {
            drive(
                &mut controller,
                journal.retire_sealed_prefix(
                    &RetentionFloors::new(vec![]).unwrap(),
                    AsyncRetirementBudget {
                        max_segments: 2,
                        max_read_bytes: 65536,
                    },
                ),
            )
            .unwrap();
            drive(&mut controller, journal.reclaim_unreferenced_segments(2)).unwrap();
        }
        let checkpoint = journal.manifest.checkpoint.unwrap();
        let checkpoint_path = journal
            .root()
            .join("checkpoints")
            .join(crate::checkpoint_name(checkpoint.checkpoint_id))
            .join("CHUNK.00000000");
        let segment_id = journal.manifest.segments.last().unwrap().segment_id;
        let segment_path = journal.root().join(segment_name(segment_id));
        let old_files = [checkpoint_path, segment_path].map(|path| {
            let bytes = controller.image().bytes(&path, false).unwrap().to_vec();
            (path, bytes)
        });
        drop(journal);
        let directory = drive(&mut controller, repair_directory(io.clone())).unwrap();
        let directory = drive(&mut controller, directory.quarantine_for_recovery(CONFIG)).unwrap();
        let journal = drive(
            &mut controller,
            directory.recover_nonvoting(CONFIG, JournalGeneration(4), 3),
        )
        .unwrap();
        assert!(journal.manifest.checkpoint.is_none());
        assert!(matches!(
            journal.capture_checkpoint(),
            Err(DirectoryError::RetentionRequiresCheckpoint)
        ));
        assert_eq!(journal.accepted_position().unwrap(), LogPosition::GENESIS);
        assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
        assert!(journal.manifest.segments[0].segment_id > segment_id);
        for (path, bytes) in old_files {
            assert_eq!(controller.image().bytes(&path, false).unwrap(), bytes);
        }
        drop(journal);
        let (mut controller, io) = setup(controller.crash(true).unwrap().0);
        assert!(drive(&mut controller, open(io.clone(), 5)).is_err());
        let journal = drive(&mut controller, reopen_replacement(io, 6)).unwrap();
        assert!(journal.manifest.checkpoint.is_none());
        assert_eq!(journal.accepted_position().unwrap(), LogPosition::GENESIS);
    }
}

#[test]
fn async_checkpoint_reset_crash_cuts_preserve_a_valid_nonvoting_selection() {
    let image = super::super::retention::retained_baseline();
    for immediate in [false, true] {
        let mut completed = false;
        for cut in 0..512 {
            let (mut controller, io) = setup(image.clone());
            let directory = drive(&mut controller, repair_directory(io)).unwrap();
            let old = directory.manifest().clone();
            let directory =
                drive(&mut controller, directory.quarantine_for_recovery(CONFIG)).unwrap();
            let done = run_cut(
                &mut controller,
                directory.recover_nonvoting(CONFIG, JournalGeneration(3), 4),
                cut,
                immediate,
            );
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            assert!(drive(&mut controller, open(io.clone(), 9)).is_err());
            let directory = drive(&mut controller, recovering_directory(io)).unwrap();
            let selected = directory.manifest();
            if selected.generation == old.generation {
                assert!(!done);
                assert_eq!(selected, &old);
            } else {
                assert!(selected.checkpoint.is_none());
                assert_eq!(selected.accepted, LogPosition::GENESIS);
                assert_eq!(selected.committed, LogPosition::GENESIS);
                assert!(selected.segments[0].segment_id > old.segments.last().unwrap().segment_id);
            }
            if done {
                completed = true;
                break;
            }
        }
        assert!(completed);
    }
}
