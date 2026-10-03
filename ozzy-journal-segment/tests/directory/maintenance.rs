use super::*;
use ozzy_journal_segment::OpenGroupJournal;
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
