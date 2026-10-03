use super::*;
use crate::directory::progress_tests::{FailAt, append, journal_mode};
use crate::directory::{MetadataLimits, PersistencePhase};
use std::fs;

const CONFIG: &[u8] = b"test configuration";

#[test]
fn quarantine_cuts_never_select_empty_voting_history() {
    for phase in [
        PersistencePhase::CurrentTemporarySynced,
        PersistencePhase::CurrentRenamed,
        PersistencePhase::CurrentDirectorySynced,
    ] {
        let (_temporary, mut journal) = journal_mode(true);
        let written = append(&mut journal, 1);
        journal.sync_through(written).unwrap();
        journal.publish_durable_progress().unwrap();
        let root = journal.directory.root.clone();
        let identity = journal.directory.identity;
        drop(journal);
        let path = root.join(segment_name(1));
        let mut damaged = fs::read(&path).unwrap();
        damaged[4096..].fill(0);
        fs::write(&path, &damaged).unwrap();
        let directory = GroupDirectory::open_with_configuration(
            &root,
            identity,
            MetadataLimits::default(),
            CONFIG,
        )
        .unwrap();
        assert!(
            directory
                .quarantine_observing(CONFIG, &mut FailAt(phase))
                .is_err()
        );
        if let Ok(directory) = GroupDirectory::open_with_configuration(
            &root,
            identity,
            MetadataLimits::default(),
            CONFIG,
        ) {
            assert!(
                directory
                    .recover(
                        JournalGeneration(2),
                        DecodeLimits::default(),
                        OperationLimits::default()
                    )
                    .is_err()
            );
        } else {
            let directory =
                GroupDirectory::open_recovering(&root, identity, MetadataLimits::default(), CONFIG)
                    .unwrap();
            let journal = directory
                .recover_nonvoting(
                    CONFIG,
                    JournalGeneration(2),
                    DecodeLimits::default(),
                    OperationLimits::default(),
                    16,
                )
                .unwrap();
            assert_eq!(journal.accepted_position().unwrap(), LogPosition::GENESIS);
            drop(journal);
            assert!(
                GroupDirectory::open_with_configuration(
                    &root,
                    identity,
                    MetadataLimits::default(),
                    CONFIG
                )
                .is_err()
            );
        }
        assert_eq!(fs::read(path).unwrap(), damaged);
    }
}

#[test]
fn configured_store_cannot_skip_quarantine_and_reset_its_history() {
    let (_temporary, journal) = journal_mode(true);
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    drop(journal);
    let directory =
        GroupDirectory::open_with_configuration(root, identity, MetadataLimits::default(), CONFIG)
            .unwrap();
    assert!(
        directory
            .recover_nonvoting(
                CONFIG,
                JournalGeneration(2),
                DecodeLimits::default(),
                OperationLimits::default(),
                16
            )
            .is_err()
    );
}
