use super::*;
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};

#[test]
fn allocated_segments_keep_their_full_size_across_append_roll_and_restart() {
    for mode in [SegmentWriteMode::Buffered, SegmentWriteMode::DataSync] {
        let (_temporary, mut journal) = journal();
        journal.set_write_mode(mode).unwrap();
        let root = journal.directory.root.clone();
        let identity = journal.directory.identity;
        let capacity = journal.writer.header().capacity();
        for _ in 0..2 {
            let path = root.join(segment_name(journal.writer.header().segment_id()));
            assert_eq!(fs::metadata(&path).unwrap().len(), capacity);
            for id in 1..=3 {
                let written = append(&mut journal, id);
                journal.sync_through(written).unwrap();
                assert_eq!(fs::metadata(&path).unwrap().len(), capacity);
                assert!(written.end_offset() < capacity);
            }
            journal = if mode == SegmentWriteMode::Buffered {
                let (next, publication) = journal.begin_buffered_roll(capacity).unwrap();
                next.complete_buffered_roll(publication.publish().unwrap())
                    .unwrap()
            } else {
                journal.roll_active(capacity).unwrap()
            };
        }
        let expected = journal.writer.written_position().next_chain();
        drop(journal);
        let mut restarted = GroupDirectory::open(&root, identity, MetadataLimits::default())
            .unwrap()
            .recover(
                JournalGeneration(2),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        assert_eq!(restarted.writer.written_position().next_chain(), expected);
        restarted.set_write_mode(mode).unwrap();
        let path = root.join(segment_name(restarted.writer.header().segment_id()));
        let written = append(&mut restarted, 4);
        restarted.sync_through(written).unwrap();
        assert_eq!(fs::metadata(path).unwrap().len(), capacity);
    }
}

#[test]
fn zeroing_the_active_remainder_changes_no_recovered_state() {
    let (_temporary, mut journal) = journal();
    journal.set_write_mode(SegmentWriteMode::DataSync).unwrap();
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    let written = append(&mut journal, 1);
    journal.sync_through(written).unwrap();
    let unsynced = append(&mut journal, 2);
    // Never zero past bytes that are written but not yet synchronized.
    assert!(journal.zero_active_remainder(&[0; 4096]).is_err());
    journal.sync_through(unsynced).unwrap();
    journal.zero_active_remainder(&[0; 4096]).unwrap();
    let path = root.join(segment_name(journal.writer.header().segment_id()));
    let bytes = fs::read(&path).unwrap();
    assert_eq!(bytes.len() as u64, journal.writer.header().capacity());
    assert!(
        bytes[unsynced.end_offset() as usize..]
            .iter()
            .all(|&byte| byte == 0)
    );
    // Writes continue on the zeroed blocks and recover exactly.
    let later = append(&mut journal, 3);
    journal.sync_through(later).unwrap();
    drop(journal);
    let restarted = GroupDirectory::open(&root, identity, MetadataLimits::default())
        .unwrap()
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(
        restarted.writer.written_position().next_chain(),
        later.next_chain()
    );
    assert_eq!(
        restarted.writer.written_position().end_offset(),
        later.end_offset()
    );
}

#[test]
fn recovery_restores_full_allocation_after_a_proven_eof_crash_tail() {
    let (_temporary, mut journal) = journal();
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    let capacity = journal.writer.header().capacity();
    let first = append(&mut journal, 1);
    journal.sync_through(first).unwrap();
    journal.publish_durable_progress().unwrap();
    append(&mut journal, 2);
    drop(journal);
    let path = root.join("segments/1.log");
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(first.end_offset() + 37).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let mut recovered = GroupDirectory::open(&root, identity, MetadataLimits::default())
        .unwrap()
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(
        recovered.writer.written_position().next_chain(),
        first.next_chain()
    );
    assert_eq!(fs::metadata(&path).unwrap().len(), capacity);
    let written = append(&mut recovered, 3);
    recovered.sync_through(written).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().len(), capacity);
}

#[test]
fn partial_group_after_durable_position_is_zeroed() {
    let (_temporary, mut journal) = journal();
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    let first = append(&mut journal, 1);
    journal.sync_through(first).unwrap();
    journal.publish_durable_progress().unwrap();
    append(&mut journal, 2);
    drop(journal);
    let path = root.join("segments/1.log");
    let mut bytes = fs::read(&path).unwrap();
    bytes[first.end_offset() as usize + 37..].fill(0);
    fs::write(&path, &bytes).unwrap();
    let mut recovered = GroupDirectory::open(&root, identity, MetadataLimits::default())
        .unwrap()
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(
        recovered.writer.written_position().next_chain(),
        first.next_chain()
    );
    let repaired = fs::read(&path).unwrap();
    assert_eq!(repaired.len(), bytes.len());
    assert!(
        repaired[first.end_offset() as usize..]
            .iter()
            .all(|&byte| byte == 0)
    );
    let next = append(&mut recovered, 3);
    recovered.sync_through(next).unwrap();
}

#[test]
fn hole_after_durable_position_discards_later_valid_group() {
    let (_temporary, mut journal) = journal();
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    let first = append(&mut journal, 1);
    journal.sync_through(first).unwrap();
    journal.publish_durable_progress().unwrap();
    let second = append(&mut journal, 2);
    let third = append(&mut journal, 3);
    drop(journal);
    let path = root.join("segments/1.log");
    let mut bytes = fs::read(&path).unwrap();
    bytes[first.end_offset() as usize..second.end_offset() as usize].fill(0);
    fs::write(&path, &bytes).unwrap();

    let mut recovered = GroupDirectory::open(&root, identity, MetadataLimits::default())
        .unwrap()
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(
        recovered.writer.written_position().next_chain(),
        first.next_chain()
    );
    let repaired = fs::read(&path).unwrap();
    assert!(
        repaired[first.end_offset() as usize..]
            .iter()
            .all(|&byte| byte == 0)
    );
    assert_ne!(
        bytes[second.end_offset() as usize..third.end_offset() as usize],
        repaired[second.end_offset() as usize..third.end_offset() as usize]
    );
    let next = append(&mut recovered, 4);
    recovered.sync_through(next).unwrap();
}

#[test]
fn hole_before_durable_position_refuses_without_mutation() {
    let (_temporary, mut journal) = journal();
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    let first = append(&mut journal, 1);
    let second = append(&mut journal, 2);
    journal.sync_through(second).unwrap();
    journal.publish_durable_progress().unwrap();
    append(&mut journal, 3);
    drop(journal);
    let path = root.join("segments/1.log");
    let mut bytes = fs::read(&path).unwrap();
    bytes[first.end_offset() as usize..second.end_offset() as usize].fill(0);
    fs::write(&path, &bytes).unwrap();

    assert!(matches!(
        GroupDirectory::open(&root, identity, MetadataLimits::default())
            .unwrap()
            .recover(
                JournalGeneration(2),
                DecodeLimits::default(),
                OperationLimits::default(),
            ),
        Err(DirectoryError::Writer(
            WriterError::ProtectedPrefixMismatch(2)
        ))
    ));
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

fn journal() -> (tempfile::TempDir, OpenGroupJournal) {
    journal_mode(false)
}

pub(super) fn journal_mode(evidence: bool) -> (tempfile::TempDir, OpenGroupJournal) {
    journal_mode_capacity(evidence, 1024 * 1024)
}

pub(super) fn journal_mode_capacity(
    evidence: bool,
    capacity: u64,
) -> (tempfile::TempDir, OpenGroupJournal) {
    let root = tempfile::tempdir().unwrap();
    let identity = GroupIdentity {
        group_id: GroupId::from_bytes([1; 16]),
        replica_node_id: NodeId::from_bytes([2; 16]),
        volume_id: VolumeId::from_bytes([3; 16]),
        store_id: StoreId::from_bytes([4; 16]),
        store_generation: 1,
    };
    let header =
        SegmentHeader::new(identity.group_id, 1, None, crate::Digest::ZERO, capacity).unwrap();
    let directory = if evidence {
        GroupDirectory::format_new_with_durable_evidence(
            root.path().join("group"),
            identity,
            1,
            &header,
            b"test configuration",
        )
    } else {
        GroupDirectory::format_new_with_commit_mode(
            root.path().join("group"),
            identity,
            1,
            CommitMode::External,
            &header,
        )
    }
    .unwrap();
    let journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    (root, journal)
}

pub(super) fn append(journal: &mut OpenGroupJournal, id: u8) -> WriterPosition {
    let chain = journal.writer().written_position().next_chain();
    journal
        .append(&[crate::CanonicalOperation {
            group_id: journal.directory.identity.group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: chain.next_op_number(),
            previous_digest: chain.previous_digest(),
            kind: crate::OperationKind::Barrier,
            body: &[id; 16],
        }])
        .unwrap()
}

#[test]
fn accepted_publication_never_claims_later_writes() {
    let (_root, mut journal) = journal();
    let first = append(&mut journal, 1);
    append(&mut journal, 2);
    journal.sync_through(first).unwrap();
    journal.publish_durable_progress().unwrap();
    assert_eq!(
        journal.directory.manifest.accepted,
        position_before(first.next_chain()).unwrap()
    );
    assert_eq!(journal.directory.manifest.committed, LogPosition::GENESIS);
    assert_eq!(
        journal
            .writer()
            .written_position()
            .next_chain()
            .next_op_number(),
        3
    );
}

pub(super) struct FailAt(pub(super) PersistencePhase);

impl PersistenceObserver for FailAt {
    fn completed(&mut self, phase: PersistencePhase) -> io::Result<()> {
        if phase == self.0 {
            Err(io::Error::other("injected publication failure"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn every_ambiguous_progress_publication_fences_without_losing_prior_evidence() {
    for phase in [
        PersistencePhase::ManifestTemporarySynced,
        PersistencePhase::ManifestLinked,
        PersistencePhase::ManifestDirectorySynced,
        PersistencePhase::CurrentTemporarySynced,
        PersistencePhase::CurrentRenamed,
        PersistencePhase::CurrentDirectorySynced,
    ] {
        let (_root, mut journal) = journal();
        let first = append(&mut journal, 1);
        journal.sync_through(first).unwrap();
        journal.publish_durable_progress().unwrap();
        let prior = journal.directory.manifest.accepted;
        let second = append(&mut journal, 2);
        journal.sync_through(second).unwrap();
        assert!(
            journal
                .publish_durable_progress_observing(&mut FailAt(phase))
                .is_err()
        );
        assert!(journal.writer.is_faulted());
        assert!(journal.sync_through(second).is_err());
        let root = journal.directory.root.clone();
        let identity = journal.directory.identity;
        drop(journal);
        let reopened = GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
        assert!(
            reopened.manifest.accepted.op_number >= prior.op_number,
            "{phase:?}"
        );
        assert_eq!(reopened.manifest.committed, LogPosition::GENESIS);
        let recovered = reopened
            .recover(
                JournalGeneration(2),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        assert_eq!(recovered.accepted_position().unwrap().op_number, 2);
    }
}
