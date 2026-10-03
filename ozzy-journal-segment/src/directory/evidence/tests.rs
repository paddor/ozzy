use super::super::progress_tests::{FailAt, append, journal_mode};
use super::*;
use crate::directory::{
    DecodeLimits, JournalGeneration, MetadataLimits, OpenOptions, OperationLimits,
    PersistencePhase, fs, segment_name,
};

#[test]
fn repeated_confirmations_have_constant_metadata_files_and_protect_the_latest_tail() {
    let (_temporary, mut journal) = journal_mode(true);
    let generation = journal.directory.current.generation;
    let count = fs::read_dir(&journal.directory.root).unwrap().count();
    for id in 1..=128 {
        let position = append(&mut journal, id);
        journal.sync_through(position).unwrap();
        journal.publish_durable_progress().unwrap();
        assert_eq!(journal.directory.current.generation, generation);
        assert_eq!(
            fs::read_dir(&journal.directory.root).unwrap().count(),
            count
        );
    }
    assert_eq!(
        protected(&journal.directory.root, &journal.directory.manifest)
            .unwrap()
            .op_number,
        128
    );
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    let end = journal.writer.durable_position().end_offset();
    drop(journal);
    let path = root.join(segment_name(1));
    OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(end - 4096)
        .unwrap();
    let directory = GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
    assert!(
        directory
            .recover(
                JournalGeneration(2),
                DecodeLimits::default(),
                OperationLimits::default()
            )
            .is_err()
    );
}

#[test]
fn mandatory_evidence_damage_never_uses_old_manifest_or_temporary() {
    for damage in 0..5 {
        let (_temporary, mut journal) = journal_mode(true);
        let first = append(&mut journal, 1);
        journal.sync_through(first).unwrap();
        journal.publish_durable_progress().unwrap();
        let root = journal.directory.root.clone();
        let identity = journal.directory.identity;
        let path = root.join(NAME);
        let mut bytes = fs::read(&path).unwrap();
        fs::write(root.join(".DURABLE.tmp"), &bytes).unwrap();
        drop(journal);
        match damage {
            0 => {
                bytes[80] ^= 1;
                bytes[COPY_STRIDE + 80] ^= 1;
                fs::write(&path, bytes).unwrap();
            }
            1 => {
                fs::write(&path, &bytes[..FILE_BYTES - 1]).unwrap();
            }
            2 => {
                fs::write(&path, vec![0; FILE_BYTES]).unwrap();
            }
            3 => {
                fs::remove_file(&path).unwrap();
            }
            4 => {
                bytes.push(0);
                fs::write(&path, bytes).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            GroupDirectory::open(&root, identity, MetadataLimits::default()).is_err(),
            "damage {damage}"
        );
    }
}

#[test]
fn evidence_remains_binding_across_metadata_changes_and_rejects_future_scope() {
    let (_temporary, mut journal) = journal_mode(true);
    let first = append(&mut journal, 1);
    journal.sync_through(first).unwrap();
    journal.publish_durable_progress().unwrap();
    let bytes = fs::read(journal.directory.root.join(NAME)).unwrap()[..RECORD_BYTES].to_vec();
    for change_volume in [false, true] {
        let mut other = journal.directory.manifest.clone();
        if change_volume {
            other.identity.volume_id = ozzy_proto::VolumeId::from_bytes([99; 16]);
        } else {
            other.identity.replica_node_id = ozzy_proto::NodeId::from_bytes([99; 16]);
        }
        assert!(decode(&bytes, &other).is_err());
    }
    let mut next = journal.directory.manifest.clone();
    next.generation += 1;
    next.parent_generation = journal.directory.manifest.generation;
    next.promised_view += 1;
    assert_eq!(decode(&bytes, &next).unwrap().op_number, 1);
    let mut earlier = next.clone();
    earlier.generation = 1;
    assert!(decode(&bytes, &earlier).is_err());
    next.segments.last_mut().unwrap().segment_id += 1;
    assert_eq!(decode(&bytes, &next).unwrap(), next.accepted);
    next.generation -= 1;
    assert!(decode(&bytes, &next).is_err());
}

#[test]
fn failed_evidence_publication_fences_and_never_erases_prior_evidence() {
    let (_temporary, mut journal) = journal_mode(true);
    let first = append(&mut journal, 1);
    journal.sync_through(first).unwrap();
    journal.publish_durable_progress().unwrap();
    let second = append(&mut journal, 2);
    journal.sync_through(second).unwrap();
    assert!(
        journal
            .publish_durable_progress_observing(&mut FailAt(PersistencePhase::EvidenceCopiesSynced))
            .is_err()
    );
    assert!(journal.writer.is_faulted());
    assert!(journal.prepare_durable_progress().is_err());
    assert!(
        protected(&journal.directory.root, &journal.directory.manifest)
            .unwrap()
            .op_number
            >= 1
    );
}

#[test]
fn cached_evidence_matches_the_file_across_publications_rolls_and_reopen() {
    let (_temporary, mut journal) = journal_mode(true);
    let path = journal.directory.root.join(NAME);
    let file = |path: &std::path::Path| read_records(&File::open(path).unwrap()).unwrap();
    assert_eq!(journal.evidence.as_ref().unwrap().records, file(&path));
    for id in 1..=6 {
        let position = append(&mut journal, id);
        journal.sync_through(position).unwrap();
        journal.publish_durable_progress().unwrap();
        assert_eq!(journal.evidence.as_ref().unwrap().records, file(&path));
        if id == 3 {
            journal = journal.roll_active(1024 * 1024).unwrap();
            assert_eq!(journal.evidence.as_ref().unwrap().records, file(&path));
        }
    }
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    drop(journal);
    let journal = GroupDirectory::open(&root, identity, MetadataLimits::default())
        .unwrap()
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let copies = Copies::new(
        &journal.evidence.as_ref().unwrap().records,
        &journal.directory.manifest,
    )
    .unwrap();
    assert!(copies.mirrored());
    assert_eq!(copies.sequences[copies.newest], Some(6));
}

#[test]
fn one_damaged_copy_is_tolerated_and_rewritten_at_recovery() {
    for damaged in 0..2 {
        let (_temporary, mut journal) = journal_mode(true);
        for id in 1..=3 {
            let position = append(&mut journal, id);
            journal.sync_through(position).unwrap();
            journal.publish_durable_progress().unwrap();
        }
        let root = journal.directory.root.clone();
        let identity = journal.directory.identity;
        drop(journal);
        let path = root.join(NAME);
        let intact = fs::read(&path).unwrap();
        assert_eq!(
            intact[..RECORD_BYTES],
            intact[COPY_STRIDE..][..RECORD_BYTES]
        );
        let mut bytes = intact.clone();
        bytes[damaged * COPY_STRIDE + 80] ^= 1;
        fs::write(&path, bytes).unwrap();
        let directory = GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
        assert_eq!(
            protected(&root, &directory.manifest).unwrap().op_number,
            3,
            "damaged copy {damaged}"
        );
        let journal = directory
            .recover(
                JournalGeneration(2),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        drop(journal);
        assert_eq!(fs::read(&path).unwrap(), intact, "damaged copy {damaged}");
    }
}

#[test]
fn publication_alternates_the_first_copy_and_never_starts_with_the_newest() {
    let (_temporary, mut journal) = journal_mode(true);
    let path = journal.directory.root.join(NAME);
    for id in 1..=4 {
        let position = append(&mut journal, id);
        journal.sync_through(position).unwrap();
        let before = fs::read(&path).unwrap();
        let copies = Copies::decode_file(&before, &journal.directory.manifest).unwrap();
        let prepared = journal.prepare_durable_progress().unwrap().unwrap();
        // Tear the first write: the other copy must still decode the prior prefix.
        let mut torn = before.clone();
        torn[prepared.first * COPY_STRIDE..][..RECORD_BYTES / 2]
            .copy_from_slice(&prepared.bytes[..RECORD_BYTES / 2]);
        assert_eq!(
            Copies::decode_file(&torn, &journal.directory.manifest)
                .unwrap()
                .protected(&journal.directory.manifest)
                .unwrap(),
            copies.protected(&journal.directory.manifest).unwrap()
        );
        let completed = prepared.publish();
        journal.complete_durable_progress(completed).unwrap();
        let after =
            Copies::decode_file(&fs::read(&path).unwrap(), &journal.directory.manifest).unwrap();
        assert!(after.mirrored());
        assert_eq!(
            after
                .protected(&journal.directory.manifest)
                .unwrap()
                .op_number,
            u64::from(id)
        );
    }
}
