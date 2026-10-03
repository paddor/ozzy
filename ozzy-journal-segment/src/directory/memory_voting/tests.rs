use super::*;
use crate::directory::progress_tests::{append, journal_mode};

#[test]
fn running_marker_rejects_an_intact_but_potentially_stale_disk_prefix() {
    let (_temporary, mut journal) = journal_mode(true);
    assert!(journal.require_drained_memory_history().is_err());
    journal.publish_drained_memory_history().unwrap();
    journal.require_drained_memory_history().unwrap();
    journal.mark_memory_voting_running().unwrap();
    assert!(matches!(
        journal.require_drained_memory_history(),
        Err(DirectoryError::MemoryHistoryUnproven),
    ));
    let written = append(&mut journal, 1);
    journal.sync_through(written).unwrap();
    journal.publish_durable_progress().unwrap();
    // Even a fully persisted observed prefix cannot prove no later RAM vote existed.
    assert!(journal.require_drained_memory_history().is_err());
    journal.publish_drained_memory_history().unwrap();
    journal.require_drained_memory_history().unwrap();
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    drop(journal);
    let mut reopened =
        crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default())
            .unwrap()
            .recover(
                ozzy_journal::progress::JournalGeneration(2),
                crate::DecodeLimits::default(),
                crate::OperationLimits::default(),
            )
            .unwrap();
    reopened.require_drained_memory_history().unwrap();
    reopened.mark_memory_voting_running().unwrap();
    drop(reopened);
    let crashed = crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default())
        .unwrap()
        .recover(
            ozzy_journal::progress::JournalGeneration(3),
            crate::DecodeLimits::default(),
            crate::OperationLimits::default(),
        )
        .unwrap();
    assert!(crashed.require_drained_memory_history().is_err());
}

#[test]
fn old_clean_evidence_cannot_authorize_a_newer_journal_or_other_configuration() {
    let (_temporary, mut journal) = journal_mode(true);
    journal.publish_drained_memory_history().unwrap();
    let path = journal.directory.root.join(NAME);
    let old = std::fs::read(&path).unwrap();
    append(&mut journal, 1);
    journal.publish_drained_memory_history().unwrap();
    let current = std::fs::read(&path).unwrap();
    std::fs::write(&path, old).unwrap();
    assert!(journal.require_drained_memory_history().is_err());
    std::fs::write(&path, current).unwrap();
    journal.require_drained_memory_history().unwrap();
    journal.directory.configuration = Some(b"other membership".as_slice().into());
    assert!(journal.require_drained_memory_history().is_err());
}

#[test]
fn every_corrupted_byte_missing_file_and_temporary_clean_file_fail_closed() {
    let (_temporary, mut journal) = journal_mode(true);
    journal.publish_drained_memory_history().unwrap();
    let path = journal.directory.root.join(NAME);
    let original = std::fs::read(&path).unwrap();
    for index in 0..original.len() {
        let mut bytes = original.clone();
        bytes[index] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(
            journal.require_drained_memory_history().is_err(),
            "byte {index}"
        );
    }
    std::fs::write(journal.directory.root.join(TEMPORARY), &original).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(journal.require_drained_memory_history().is_err());
    for bytes in [&original[..BYTES - 1], &[0; BYTES + 1][..]] {
        std::fs::write(&path, bytes).unwrap();
        assert!(journal.require_drained_memory_history().is_err());
    }
}
