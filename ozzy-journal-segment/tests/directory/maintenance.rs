use super::*;
use ozzy_journal_segment::{MaintenanceBudget, OpenGroupJournal};
use std::time::Duration;

fn fresh(root: &std::path::Path) -> OpenGroupJournal {
    let expected = identity(0x35);
    GroupDirectory::format_new(root, expected, 1, &first_segment(expected))
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap()
}

fn orphan_pass(journal: &OpenGroupJournal, max_steps: usize) -> (usize, bool) {
    let mut cursor = journal.begin_orphan_cleanup().unwrap();
    let mut removed = 0;
    for _ in 0..max_steps {
        let step = journal
            .cleanup_orphan_step(
                &mut cursor,
                MaintenanceBudget {
                    max_entries: 1,
                    max_work: Duration::from_nanos(1),
                },
            )
            .unwrap();
        assert!(step.work_units <= 1);
        assert!(step.work_units > 0 || step.complete);
        removed += step.removed_files;
        if step.complete {
            return (removed, true);
        }
    }
    (removed, false)
}

#[test]
fn prepared_successor_survives_both_orphan_cleanup_paths() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let journal = fresh(&root);
    let prepared = journal
        .prepare_next_segment(8192, 4)
        .unwrap()
        .prepare()
        .unwrap();
    assert!(root.join("segments/2.log").exists());
    assert!(
        journal
            .reclaim_unreferenced_segments()
            .unwrap()
            .removed_segment_ids
            .is_empty()
    );
    assert_eq!(orphan_pass(&journal, 100), (0, true));
    drop(prepared);
    assert_eq!(orphan_pass(&journal, 100), (1, true));
    assert!(!root.join("segments/2.log").exists());
}

#[test]
fn interrupted_orphan_walk_preserves_selected_history_and_finishes_after_reopen() {
    for cut in 0..24 {
        let volume = TempDir::new().unwrap();
        let root = volume.path().join("group");
        let journal = fresh(&root);
        let selected = fs::read(root.join("segments/1.log")).unwrap();
        fs::write(root.join("segments/9.log"), b"unselected payload").unwrap();
        for id in [9, 10] {
            fs::write(
                root.join(format!("indexes/{id}-{}.idx", "ab".repeat(32))),
                b"unselected index",
            )
            .unwrap();
        }
        let checkpoint = root.join("checkpoints").join("77".repeat(16));
        fs::create_dir(&checkpoint).unwrap();
        for chunk in 0..8 {
            fs::write(
                checkpoint.join(format!("CHUNK.{chunk:08}")),
                b"partial checkpoint",
            )
            .unwrap();
        }
        fs::write(root.join("segments/unknown"), b"preserve").unwrap();
        orphan_pass(&journal, cut);
        // Source deletion can never precede deletion of its derived index.
        if !root.join("segments/9.log").exists() {
            assert!(
                !root
                    .join(format!("indexes/9-{}.idx", "ab".repeat(32)))
                    .exists()
            );
        }
        drop(journal);
        let journal = GroupDirectory::open(&root, identity(0x35), MetadataLimits::default())
            .unwrap()
            .recover(
                JournalGeneration(2),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        assert!(orphan_pass(&journal, 100).1);
        assert!(!checkpoint.exists());
        assert!(!root.join("segments/9.log").exists());
        assert_eq!(fs::read(root.join("segments/1.log")).unwrap(), selected);
        assert_eq!(
            fs::read(root.join("segments/unknown")).unwrap(),
            b"preserve"
        );
        assert_eq!(fs::read_dir(root.join("indexes")).unwrap().count(), 0);
    }
}

#[test]
fn live_checkpoint_build_image_blocks_incremental_deletion() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let mut journal = fresh(&root);
    let written = journal
        .append(&[operation(identity(0x35), 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(written).unwrap();
    let mut next = next_manifest(journal.directory());
    next.accepted = LogPosition {
        op_number: 1,
        digest: written.next_chain().previous_digest(),
    };
    next.committed = next.accepted;
    journal = journal.install_metadata(next).unwrap();
    let image = journal
        .checkpoint_plan(
            CheckpointId::from_bytes([0x76; 16]),
            Digest::from_bytes([0x75; 32]),
            4,
        )
        .unwrap()
        .build(b"state across many chunks", CheckpointLimits::default())
        .unwrap();
    assert_eq!(orphan_pass(&journal, 100), (0, true));
    drop(image);
    assert!(orphan_pass(&journal, 100).0 > 0);
    assert_eq!(fs::read_dir(root.join("checkpoints")).unwrap().count(), 0);
}

#[test]
fn retirement_refuses_insufficient_scan_budget_or_damaged_checkpoint() {
    for damage in [false, true] {
        let volume = TempDir::new().unwrap();
        let root = volume.path().join("group");
        let mut journal = fresh(&root);
        let written = journal
            .append(&[operation(identity(0x35), 1, Digest::ZERO)])
            .unwrap();
        journal.sync_through(written).unwrap();
        let mut next = next_manifest(journal.directory());
        next.accepted = LogPosition {
            op_number: 1,
            digest: written.next_chain().previous_digest(),
        };
        next.committed = next.accepted;
        journal = journal
            .install_metadata(next)
            .unwrap()
            .roll_active(8192)
            .unwrap();
        let id = CheckpointId::from_bytes([0x74; 16]);
        let image = journal
            .checkpoint_plan(id, Digest::from_bytes([0x73; 32]), 8)
            .unwrap()
            .build(b"state bytes", CheckpointLimits::default())
            .unwrap();
        journal = journal
            .install_checkpoint(id, CheckpointLimits::default())
            .unwrap();
        drop(image);
        let before = fs::read(root.join("CURRENT")).unwrap();
        if damage {
            fs::write(
                root.join("checkpoints")
                    .join(ozzy_journal_segment::checkpoint_name(id))
                    .join("CHUNK.00000000"),
                b"corrupt!",
            )
            .unwrap();
        }
        let result = journal.retire_sealed_prefix(
            &RetentionFloors::new(vec![]).unwrap(),
            ozzy_journal_segment::RetentionScanBudget {
                max_segments: 1,
                max_read_bytes: if damage { 8192 } else { 4096 },
                max_work: Duration::MAX,
            },
            CheckpointLimits::default(),
        );
        if damage {
            assert!(matches!(result, Err(DirectoryError::Checkpoint(_))));
        } else {
            assert!(matches!(result, Err(DirectoryError::RetentionScanBudget)));
        }
        assert_eq!(fs::read(root.join("CURRENT")).unwrap(), before);
        assert!(root.join("segments/1.log").exists());
    }
}
