use super::orphans::put_file;
use super::*;

async fn commit(journal: &mut Journal) {
    let mut next = journal.next_manifest().unwrap();
    next.accepted = journal.accepted_position().unwrap();
    next.committed = next.accepted;
    journal.install_metadata(next).await.unwrap();
}

#[test]
fn a_concurrent_pass_keeps_everything_newer_than_its_capture() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, commit(&mut journal));
    let old = journal.current().generation;
    drive(&mut controller, commit(&mut journal));
    let captured = journal.current().generation;
    let active = journal.manifest().segments.last().unwrap().segment_id;
    let root = journal.root().to_path_buf();
    let stale = root.join(format!("segments/{}.log", active - 1));
    let future = root.join(format!("segments/{}.log", active + 5));
    let stale_temporary = root.join(format!(".MANIFEST.{captured}.tmp"));
    for path in [&stale, &future, &stale_temporary] {
        drive(
            &mut controller,
            put_file(&journal, path.clone(), b"leftover"),
        );
    }
    let mut files = journal.capture_unselected().unwrap();
    assert!(matches!(
        journal.capture_unselected(),
        Err(DirectoryError::CleanupBusy)
    ));
    // The owner publishes beside the pass, and a new publication's
    // temporary appears after the capture.
    drive(&mut controller, commit(&mut journal));
    let newer = journal.current().generation;
    let newer_temporary = root.join(format!(".MANIFEST.{}.tmp", newer + 1));
    drive(
        &mut controller,
        put_file(&journal, newer_temporary.clone(), b"in flight"),
    );
    drive(&mut controller, files.validate()).unwrap();
    let metadata = drive(&mut controller, files.reclaim_metadata(128)).unwrap();
    assert!(metadata.removed_manifest_generations.contains(&old));
    assert!(!metadata.removed_manifest_generations.contains(&captured));
    assert!(!metadata.removed_manifest_generations.contains(&newer));
    assert!(!controller.image().exists(&stale_temporary, false));
    assert!(controller.image().exists(&newer_temporary, false));
    let segments = drive(&mut controller, files.reclaim_segments(128)).unwrap();
    assert_eq!(segments.removed_segment_ids, [active - 1]);
    assert!(controller.image().exists(&future, false));
    assert!(!journal.is_faulted());
    drop(files);
    // An exclusive pass, as after opening, removes what a concurrent one keeps.
    let segments = drive(&mut controller, journal.reclaim_unreferenced_segments(128)).unwrap();
    assert_eq!(segments.removed_segment_ids, [active + 5]);
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn a_concurrent_pass_refuses_an_older_selection() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, commit(&mut journal));
    let mut files = journal.capture_unselected().unwrap();
    let current_path = journal.root().join("CURRENT");
    let file = drive(
        &mut controller,
        journal
            .access
            .open(current_path, ozzy_io::OpenMode::ReadWrite, false, false),
    )
    .unwrap();
    drive(&mut controller, journal.access.write_all(&file, 0, &[255])).unwrap();
    drive(
        &mut controller,
        journal.access.done(Operation::Close { handle: file }),
    )
    .unwrap();
    assert!(drive(&mut controller, files.validate()).is_err());
    drop(files);
    // Dropping the capture frees the slot.
    drop(journal.capture_unselected().unwrap());
}
