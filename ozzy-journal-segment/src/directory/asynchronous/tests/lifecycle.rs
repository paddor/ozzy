use super::*;

async fn create_index(journal: &Journal, id: u64, digest: char) -> PathBuf {
    let path = journal.root().join(format!(
        "indexes/{id}-{}.idx",
        digest.to_string().repeat(64)
    ));
    let handle = journal
        .access
        .open(path.clone(), ozzy_io::OpenMode::CreateNew, false, false)
        .await
        .unwrap();
    journal
        .access
        .write_all(&handle, 0, b"derived")
        .await
        .unwrap();
    journal.access.sync(&handle).await.unwrap();
    journal
        .access
        .done(Operation::Close { handle })
        .await
        .unwrap();
    journal
        .access
        .sync_directory(journal.root().join("indexes"))
        .await
        .unwrap();
    path
}

#[test]
fn async_index_cleanup_preserves_selected_and_captured_segment_ids() {
    let (mut controller, mut journal) = empty_journal();
    drive(&mut controller, append_confirmed(&mut journal));
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    let selected = drive(&mut controller, create_index(&journal, 2, 'a'));
    let captured = drive(&mut controller, create_index(&journal, 1, 'b'));
    let orphan = drive(&mut controller, create_index(&journal, 99, 'c'));
    let another = drive(&mut controller, create_index(&journal, 98, 'd'));
    let mut history = journal.freeze_history(32768).unwrap();
    let target = journal.accepted_position().unwrap();
    // Public suffix installation removes segment 1 from selection but its
    // captured history still protects both payload and derived index.
    let replacement = crate::SuffixReplacement {
        expected_current: journal.current(),
        protected_committed: LogPosition::GENESIS,
        retained: LogPosition::GENESIS,
        promised_view: 2,
        last_normal_view: 2,
        committed: LogPosition::GENESIS,
        writer_generation: JournalGeneration(9),
        segment_capacity: 32768,
        body_encoding: BodyEncoding::Raw,
    };
    let installer = drive(
        &mut controller,
        journal.begin_suffix_replacement(crate::SuffixRequest {
            replacement,
            accepted: LogPosition::GENESIS,
            limits: crate::SuffixStreamLimits {
                max_segments: 16,
                ..crate::SuffixStreamLimits::default()
            },
        }),
    )
    .unwrap();
    // No suffix is needed: old accepted history was not confirmed.
    let mut journal = drive(&mut controller, installer.finish()).unwrap();
    let current = drive(
        &mut controller,
        create_index(&journal, journal.manifest.segments[0].segment_id, 'e'),
    );
    let result = drive(&mut controller, journal.reclaim_unreferenced_indexes(1)).unwrap();
    assert_eq!(result.removed_files, 1);
    assert!(!result.complete);
    assert!(controller.image().bytes(&captured, false).is_ok());
    assert!(
        controller.image().bytes(&selected, false).is_ok(),
        "captured empty active segment is protected too"
    );
    assert!(controller.image().bytes(&current, false).is_ok());
    assert_eq!(
        drive(&mut controller, history.position(1)).unwrap(),
        Some(target)
    );
    let result = drive(&mut controller, journal.reclaim_unreferenced_indexes(8)).unwrap();
    assert_eq!(result.removed_files, 1);
    assert!(controller.image().bytes(&orphan, false).is_err());
    assert!(controller.image().bytes(&another, false).is_err());
    drop(history);
    let result = drive(&mut controller, journal.reclaim_unreferenced_indexes(8)).unwrap();
    assert_eq!(result.removed_files, 2);
    assert!(result.complete);
    assert!(controller.image().bytes(&current, false).is_ok());
}

#[test]
fn failed_async_index_cleanup_fences_owner_until_reopen() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io.clone(), 2)).unwrap();
    let path = drive(&mut controller, create_index(&journal, 99, 'a'));
    let mut removed = false;
    assert!(
        drive_with(
            &mut controller,
            journal.reclaim_unreferenced_indexes(4),
            |operation| {
                if matches!(operation, Operation::RemoveFile { .. }) {
                    removed = true;
                }
                if removed && matches!(operation, Operation::Sync { .. }) {
                    Effect::FailBefore(io::ErrorKind::Other)
                } else {
                    Effect::Normal
                }
            }
        )
        .is_err()
    );
    assert!(removed);
    assert!(journal.is_faulted());
    assert!(drive(&mut controller, journal.close()).is_err());
    let journal = drive(&mut controller, open(io, 3)).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 1);
    assert!(controller.image().bytes(&path, false).is_err());
}

#[test]
fn async_close_keeps_captured_history_and_lock_alive_without_publishing_evidence() {
    let (mut controller, io) = setup(baseline());
    let journal = drive(&mut controller, open(io.clone(), 2)).unwrap();
    let current = journal.current();
    let durable = controller
        .image()
        .bytes(Path::new("/group/DURABLE"), false)
        .unwrap()
        .to_vec();
    let mut history = journal.freeze_history(32768).unwrap();
    drive(&mut controller, journal.close()).unwrap();
    assert!(
        drive(&mut controller, open(io.clone(), 3)).is_err(),
        "reader still owns group lock"
    );
    assert!(
        drive(&mut controller, history.position(1))
            .unwrap()
            .is_some()
    );
    assert_eq!(
        controller
            .image()
            .bytes(Path::new("/group/DURABLE"), false)
            .unwrap(),
        durable
    );
    drop(history);
    let journal = drive(&mut controller, open(io, 4)).unwrap();
    assert_eq!(journal.current(), current);
    assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
    drive(&mut controller, journal.close()).unwrap();
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn recovery_reserves_full_capacity_after_a_proven_eof_crash_tail() {
    use std::os::unix::fs::MetadataExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("group");
    let segment = path.join("segments/1.log");
    // Segments reserve capacity without extending EOF, so count blocks.
    let reserved = || std::fs::metadata(&segment).unwrap().blocks() * 512;
    let config = ozzy_io_pool::Config {
        threads: 1,
        max_inflight: 1,
        handles: 32,
        limits: io_limits(),
    };
    let mut chosen = limits();
    chosen.io.write_mode = SegmentWriteMode::Buffered;
    let (pool, mut clients) = ozzy_io_pool::Pool::new(config).unwrap();
    let mut journal = Journal::format(
        path.clone(),
        Local::new(clients.remove(0)),
        spec(CommitMode::External),
        JournalGeneration(1),
        chosen,
    )
    .await
    .unwrap();
    let capacity = journal.writer.header().capacity();
    assert!(reserved() >= capacity);
    let first = append_confirmed(&mut journal).await;
    let unsynced = operation(&journal);
    journal
        .append(&[unsynced], BodyEncoding::Raw)
        .await
        .unwrap();
    drop(journal);
    pool.shutdown().await;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap();
    file.set_len(first.end_offset() + 37).unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(reserved() < capacity);
    let (pool, mut clients) = ozzy_io_pool::Pool::new(config).unwrap();
    let mut recovered = Journal::open(
        path,
        Local::new(clients.remove(0)),
        crate::ExpectedStore {
            identity: identity(),
            configuration: Some(b"test deployment"),
        },
        JournalGeneration(2),
        chosen,
    )
    .await
    .unwrap();
    assert_eq!(
        recovered.writer.written_position().next_chain(),
        first.next_chain()
    );
    assert!(reserved() >= capacity);
    let written = append_confirmed(&mut recovered).await;
    assert!(written.end_offset() < capacity);
    assert!(reserved() >= capacity);
    drop(recovered);
    pool.shutdown().await;
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn allocated_segments_keep_their_full_size_across_append_roll_and_restart() {
    use std::os::unix::fs::MetadataExt;
    for mode in [SegmentWriteMode::Buffered, SegmentWriteMode::DataSync] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("group");
        // Segments reserve capacity without extending EOF, so count blocks.
        let reserved = |journal: &Journal| {
            let name = crate::directory::segment_name(journal.writer.header().segment_id());
            std::fs::metadata(path.join(name)).unwrap().blocks() * 512
        };
        let mut chosen = limits();
        chosen.io.write_mode = mode;
        let (backend, io) = real_backend(false);
        let mut journal = Journal::format(
            path.clone(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            chosen,
        )
        .await
        .unwrap();
        let capacity = journal.writer.header().capacity();
        for _ in 0..2 {
            assert!(reserved(&journal) >= capacity);
            for _ in 0..3 {
                let written = append_confirmed(&mut journal).await;
                assert!(written.end_offset() < capacity);
                assert!(reserved(&journal) >= capacity);
            }
            journal.roll_active(capacity, 4).await.unwrap();
        }
        let expected = journal.writer.written_position().next_chain();
        drop(journal);
        backend.shutdown().await;
        let (backend, io) = real_backend(false);
        let mut restarted = open_real(&path, io, 2, chosen).await;
        assert_eq!(restarted.writer.written_position().next_chain(), expected);
        append_confirmed(&mut restarted).await;
        assert!(reserved(&restarted) >= capacity);
        drop(restarted);
        backend.shutdown().await;
    }
}
