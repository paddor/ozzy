use super::*;
use crate::{GroupDirectory, GroupIdentity, MetadataLimits};
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};

fn flags(journal: &crate::OpenGroupJournal, dsync: bool) {
    let actual = i32::try_from(
        rustix::fs::fcntl_getfl(&journal.writer().io)
            .unwrap()
            .bits(),
    )
    .unwrap();
    assert_eq!(actual & libc::O_DSYNC != 0, dsync);
    assert!(
        actual & (libc::O_SYNC & !libc::O_DSYNC) == 0,
        "must not request full metadata sync"
    );
    assert_eq!(journal.writer().data_sync(), dsync);
}

fn append(journal: &mut crate::OpenGroupJournal) -> WriterPosition {
    let chain = journal.writer().written_position().next_chain();
    journal
        .append(&[CanonicalOperation {
            group_id: journal.directory().identity().group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: chain.next_op_number(),
            previous_digest: chain.previous_digest(),
            kind: crate::OperationKind::Barrier,
            body: &[7; 16],
        }])
        .unwrap()
}

#[test]
fn data_sync_is_default_across_roll_and_reopen_with_exact_prefix_completion() {
    let temporary = tempfile::tempdir().unwrap();
    let identity = GroupIdentity {
        group_id: GroupId::new(),
        replica_node_id: NodeId::new(),
        volume_id: VolumeId::new(),
        store_id: StoreId::new(),
        store_generation: 1,
    };
    let root = temporary.path().join("group");
    let header = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 32768).unwrap();
    let mut journal = GroupDirectory::format_new(&root, identity, 1, &header)
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    for _ in 0..3 {
        flags(&journal, true);
        let previous = journal.writer().durable_position();
        let first = append(&mut journal);
        let second = append(&mut journal);
        assert_eq!(journal.writer().durable_position(), previous);
        journal.sync_through(first).unwrap();
        assert_eq!(journal.writer().durable_position(), first);
        journal.sync_through(second).unwrap();
        journal = journal.roll_active(32768).unwrap();
    }
    let expected = journal.accepted_position().unwrap();
    drop(journal);
    let mut journal = GroupDirectory::open(&root, identity, MetadataLimits::default())
        .unwrap()
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    flags(&journal, true);
    assert_eq!(journal.accepted_position().unwrap(), expected);
    journal.set_write_mode(SegmentWriteMode::Buffered).unwrap();
    flags(&journal, false);
    let written = append(&mut journal);
    journal.sync_through(written).unwrap();
    journal = journal.roll_active(32768).unwrap();
    flags(&journal, false);
    journal.set_write_mode(SegmentWriteMode::DataSync).unwrap();
    flags(&journal, true);
}

#[test]
fn failed_write_mode_change_fences_existing_descriptor() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("segment");
    let header = SegmentHeader::new(GroupId::new(), 1, None, Digest::ZERO, 32768).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let mut writer =
        SegmentWriter::initialize(std::sync::Arc::new(file), header, JournalGeneration(1)).unwrap();
    std::fs::rename(&path, temporary.path().join("moved")).unwrap();
    assert!(
        writer
            .set_write_mode(&path, SegmentWriteMode::DataSync)
            .is_err()
    );
    assert!(writer.is_faulted());
    assert!(matches!(
        writer.sync_through(writer.begin_sync()),
        Err(WriterError::Faulted)
    ));
}
