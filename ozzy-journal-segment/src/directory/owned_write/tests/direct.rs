use super::*;
use bytes::Bytes;
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};

const CAPACITY: u64 = 1024 * 1024;

fn identity() -> crate::GroupIdentity {
    crate::GroupIdentity {
        group_id: GroupId::from_bytes([1; 16]),
        replica_node_id: NodeId::from_bytes([2; 16]),
        volume_id: VolumeId::from_bytes([3; 16]),
        store_id: StoreId::from_bytes([4; 16]),
        store_generation: 1,
    }
}

/// Open a journal writing through `O_DIRECT` when `direct`. `None` when the
/// temporary file system rejects direct I/O (tmpfs before 6.6, overlayfs).
fn open(root: &std::path::Path, direct: bool, reopen: bool) -> Option<OpenGroupJournal> {
    let directory = if reopen {
        crate::GroupDirectory::open(root, identity(), crate::MetadataLimits::default()).unwrap()
    } else {
        let header =
            crate::SegmentHeader::new(identity().group_id, 1, None, Digest::ZERO, CAPACITY)
                .unwrap();
        crate::GroupDirectory::format_new_with_durable_evidence(
            root,
            identity(),
            1,
            &header,
            b"test configuration",
        )
        .unwrap()
    };
    match directory.with_direct_io(direct).recover(
        ozzy_journal::progress::JournalGeneration(1 + u128::from(reopen)),
        crate::DecodeLimits::default(),
        OperationLimits::default(),
    ) {
        Ok(journal) => Some(journal),
        Err(DirectoryError::Io(error)) if direct && error.raw_os_error() == Some(libc::EINVAL) => {
            eprintln!("skipped: file system rejects O_DIRECT: {error}");
            None
        }
        Err(error) => panic!("{error}"),
    }
}

fn flags(journal: &OpenGroupJournal) -> i32 {
    i32::try_from(
        rustix::fs::fcntl_getfl(journal.writer().write_handle())
            .unwrap()
            .bits(),
    )
    .unwrap()
}

/// Write `sizes` as one group each through the owned-write pipeline.
fn write_groups(journal: OpenGroupJournal, sizes: &[usize]) -> OpenGroupJournal {
    write_groups_with(journal, sizes, None)
}

/// Like `write_groups`, submitting through kernel AIO when `aio` is given.
fn write_groups_with(
    journal: OpenGroupJournal,
    sizes: &[usize],
    mut aio: Option<&mut crate::JournalAio<()>>,
) -> OpenGroupJournal {
    let mut journal = journal;
    for (index, size) in sizes.iter().enumerate() {
        let chain = journal.writer().written_position().next_chain();
        let number = chain.next_op_number();
        let body = Bytes::from(body_with_payload(number, &vec![index as u8 + 1; *size]));
        let op = operation(&journal, number, chain.previous_digest(), &body);
        let shared = vec![SharedJournalOperation {
            header: op.header(),
            body: body.clone(),
            body_digest: canonical_body_digest(&body),
        }];
        let mut pipeline = journal.begin_write_pipeline(4).unwrap();
        let group = pipeline.preencode_shared_raw(shared).unwrap();
        let write = pipeline.prepare(group).unwrap();
        let completed = match aio.as_deref_mut() {
            Some(aio) => {
                aio.submit(vec![write], ()).unwrap();
                let mut completed = Vec::new();
                while completed.is_empty() {
                    aio.wait(|(), batch| completed.extend(batch)).unwrap();
                }
                assert_eq!(aio.in_flight(), 0);
                completed.pop().unwrap()
            }
            None => write.write(),
        };
        pipeline.complete(completed).unwrap();
        journal = pipeline.finish().unwrap();
        journal.sync_through(journal.begin_sync()).unwrap();
    }
    journal
}

#[test]
fn direct_writes_match_buffered_bytes_and_recover() {
    let sizes = [0, 1, 7, 4095, 4096, 65537, 300_000];
    let mut images = Vec::new();
    // Buffered through a thread, direct through a thread, direct through AIO.
    for (direct, aio) in [(false, false), (true, false), (true, true)] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("group");
        let Some(journal) = open(&root, direct, false) else {
            return;
        };
        assert_eq!(journal.writer().is_direct(), direct);
        let mut context = aio.then(|| crate::JournalAio::new(1).unwrap());
        let journal = write_groups_with(journal, &sizes, context.as_mut());
        let end = journal.accepted_position().unwrap();
        let path = journal.active_segment_path();
        drop(journal);
        images.push(std::fs::read(&path).unwrap());
        let journal = open(&root, direct, true).unwrap();
        assert_eq!(journal.accepted_position().unwrap(), end);
    }
    assert_eq!(images[0], images[1]);
    assert_eq!(images[0], images[2]);
}

#[test]
fn aio_refuses_buffered_or_nonconsecutive_writes_with_failed_completions() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("group");
    let journal = open(&root, false, false).unwrap();
    let body = Bytes::from(body_with_payload(1, &[1; 100]));
    let chain = journal.writer().written_position().next_chain();
    let op = operation(
        &journal,
        chain.next_op_number(),
        chain.previous_digest(),
        &body,
    );
    let shared = vec![SharedJournalOperation {
        header: op.header(),
        body: body.clone(),
        body_digest: canonical_body_digest(&body),
    }];
    let mut journal = journal;
    let mut pipeline = journal.begin_write_pipeline(4).unwrap();
    let group = pipeline.preencode_shared_raw(shared).unwrap();
    let write = pipeline.prepare(group).unwrap();
    let mut aio = crate::JournalAio::<()>::new(1).unwrap();
    let ((), failed) = aio.submit(vec![write], ()).unwrap_err();
    assert_eq!(failed.len(), 1);
    assert!(
        pipeline
            .complete(failed.into_iter().next().unwrap())
            .is_err()
    );
    assert_eq!(aio.in_flight(), 0);
    journal = match pipeline.finish() {
        Ok(journal) => journal,
        Err(_) => return,
    };
    assert!(journal.writer().is_faulted());
}

#[test]
fn direct_descriptor_follows_write_mode_roll_and_reopen() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("group");
    let Some(journal) = open(&root, true, false) else {
        return;
    };
    let direct = |journal: &OpenGroupJournal, dsync: bool| {
        let flags = flags(journal);
        assert_ne!(flags & libc::O_DIRECT, 0);
        assert_eq!(flags & libc::O_DSYNC != 0, dsync);
        // Headers, zeroing and recovery keep the buffered descriptor.
        let io = i32::try_from(
            rustix::fs::fcntl_getfl(journal.writer().buffered_io())
                .unwrap()
                .bits(),
        )
        .unwrap();
        assert_eq!(io & libc::O_DIRECT, 0);
    };
    direct(&journal, true);
    let mut journal = write_groups(journal, &[100, 5000]);
    journal
        .set_write_mode(crate::SegmentWriteMode::Buffered)
        .unwrap();
    direct(&journal, false);
    let journal = write_groups(journal, &[9000]);
    let mut journal = journal.roll_active(CAPACITY).unwrap();
    direct(&journal, false);
    journal
        .set_write_mode(crate::SegmentWriteMode::DataSync)
        .unwrap();
    direct(&journal, true);
    let mut journal = write_groups(journal, &[1, 70_000]);
    // The buffered descriptor still zeroes the unused remainder.
    journal.writer.zero_remainder(&vec![0; 64 * 1024]).unwrap();
    let end = journal.accepted_position().unwrap();
    drop(journal);
    let journal = open(&root, true, true).unwrap();
    direct(&journal, true);
    assert_eq!(journal.accepted_position().unwrap(), end);
}
