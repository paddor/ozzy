//! A retired segment file kept as the spare and reused by the next roll.

use super::orphans::put_file;
use super::*;

const SPARE: &str = "/group/segments/spare.log";

/// Segment 1 holds three groups, and the spare holds a copy of its bytes,
/// as cleanup leaves a retired segment.
fn spare_image() -> Image {
    let (mut controller, io) = setup(Image::default());
    let mut journal = drive(
        &mut controller,
        Journal::format(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        ),
    )
    .unwrap();
    for _ in 0..3 {
        drive(&mut controller, append_confirmed(&mut journal));
    }
    let retired = controller
        .image()
        .bytes(Path::new("/group/segments/1.log"), false)
        .unwrap()
        .to_vec();
    drive(&mut controller, put_file(&journal, SPARE.into(), &retired));
    drive(
        &mut controller,
        journal.access.sync_directory("/group/segments".into()),
    )
    .unwrap();
    drop(journal);
    controller.crash(true).unwrap().0
}

fn roll(controller: &mut Controller, journal: Journal) -> Journal {
    let (pending, work) = journal.begin_owned_roll(32768, 4).unwrap();
    pending.complete(drive(controller, work.publish())).unwrap()
}

#[test]
fn a_roll_takes_the_spare_and_its_old_groups_stay_unused_capacity() {
    let (mut controller, io) = setup(spare_image());
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    let retired = controller.image().bytes(Path::new(SPARE), false).unwrap();
    let retired = retired.len() as u64;
    let mut journal = roll(&mut controller, journal);
    assert_eq!(journal.writer.header().segment_id(), 2);
    assert!(!controller.image().exists(Path::new(SPARE), false));
    let reused = Path::new("/group/segments/2.log");
    assert_eq!(
        controller.image().bytes(reused, false).unwrap().len() as u64,
        retired,
        "the file keeps its written blocks"
    );
    drive(&mut controller, append_confirmed(&mut journal));
    // Recovery ends the log at the first group of the earlier use.
    drop(journal);
    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let journal = drive(&mut controller, open(io, 3)).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 4);
    assert_eq!(journal.writer.written_position().end_offset(), 8192);
    // Sealed, the reused file keeps old bytes past its sealed length.
    let journal = roll(&mut controller, journal);
    let sealed = journal.manifest().segments[1].sealed.unwrap();
    assert!(sealed.valid_bytes < retired);
    drop(journal);
    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let journal = drive(&mut controller, open(io, 4)).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 4);
    assert_eq!(journal.writer.header().segment_id(), 3);
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn cleanup_keeps_one_unselected_segment_as_the_spare() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    for (id, bytes) in [(2, &b"two"[..]), (3, b"three")] {
        let path = format!("/group/segments/{id}.log");
        drive(&mut controller, put_file(&journal, path.into(), bytes));
    }
    let cleanup = drive(&mut controller, journal.reclaim_unreferenced_segments(8)).unwrap();
    assert_eq!(cleanup.removed_segment_ids, [2, 3]);
    assert_eq!(
        cleanup.reclaimed_bytes, 5,
        "only the second file is unlinked"
    );
    assert_eq!(
        controller.image().bytes(Path::new(SPARE), false).unwrap(),
        b"two"
    );
    for id in [2, 3] {
        let path = format!("/group/segments/{id}.log");
        assert!(!controller.image().exists(Path::new(&path), false));
    }
    // With a spare in place, cleanup unlinks.
    drive(
        &mut controller,
        put_file(&journal, "/group/segments/4.log".into(), b"four"),
    );
    let cleanup = drive(&mut controller, journal.reclaim_unreferenced_segments(8)).unwrap();
    assert_eq!(cleanup.removed_segment_ids, [4]);
    assert_eq!(cleanup.reclaimed_bytes, 4);
    assert_eq!(
        controller.image().bytes(Path::new(SPARE), false).unwrap(),
        b"two"
    );
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn roll_from_the_spare_crash_cuts_keep_confirmed_history() {
    let image = spare_image();
    for immediate in [false, true] {
        let mut finished = false;
        for cut in 0..600 {
            let (mut controller, io) = setup(image.clone());
            let journal = drive(&mut controller, open(io, 2)).unwrap();
            let done = super::recovery::run_cut(
                &mut controller,
                async {
                    let (pending, work) = journal.begin_owned_roll(32768, 4)?;
                    let mut journal = pending.complete(work.publish().await)?;
                    append_confirmed(&mut journal).await;
                    Ok::<_, DirectoryError>(journal)
                },
                cut,
                immediate,
            );
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let mut journal = drive(&mut controller, open(io, 3)).unwrap();
            let accepted = journal.accepted_position().unwrap().op_number;
            assert!(matches!(accepted, 3 | 4), "cut {cut}: {accepted}");
            assert!(matches!(journal.writer.header().segment_id(), 1 | 2));
            // Whatever the cut left, cleanup, appends, and the next roll work.
            drive(&mut controller, journal.reclaim_unreferenced_segments(8)).unwrap();
            drive(&mut controller, append_confirmed(&mut journal));
            let journal = roll(&mut controller, journal);
            drop(journal);
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let journal = drive(&mut controller, open(io, 4)).unwrap();
            assert_eq!(journal.accepted_position().unwrap().op_number, accepted + 1);
            if done {
                assert_eq!(accepted, 4);
                finished = true;
                break;
            }
        }
        assert!(finished);
    }
}

#[test]
fn repair_inspection_finds_no_damage_in_reused_files() {
    let (mut controller, io) = setup(spare_image());
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    let mut journal = roll(&mut controller, journal);
    drive(&mut controller, append_confirmed(&mut journal));
    // First the reused file is active, then sealed with old bytes past its end.
    for sealed in [false, true] {
        if sealed {
            journal = roll(&mut controller, journal);
            drive(&mut controller, append_confirmed(&mut journal));
        }
        drop(journal);
        let (mut next, io) = setup(controller.crash(true).unwrap().0);
        let mut directory = drive(
            &mut next,
            RecoveryDirectory::open_for_repair(
                "/group".into(),
                io.clone(),
                identity(),
                b"test deployment",
                limits(),
            ),
        )
        .unwrap();
        let damage = drive(&mut next, directory.sealed_damage(32768)).unwrap();
        assert_eq!(damage.map(|ranges| ranges.len()), Some(0));
        drop(directory);
        journal = drive(&mut next, open(io, 3 + u128::from(sealed))).unwrap();
        controller = next;
    }
    drive(&mut controller, journal.close()).unwrap();
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn real_files_roll_into_the_spare_with_either_backend() {
    for aio in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("group");
        let (backend, io) = super::real_backend(aio);
        let mut limits = limits();
        limits.io.direct = aio;
        let mut journal = Journal::format(
            path.clone(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits,
        )
        .await
        .unwrap();
        for _ in 0..3 {
            append_confirmed(&mut journal).await;
        }
        let spare = path.join("segments/spare.log");
        std::fs::copy(path.join("segments/1.log"), &spare).unwrap();
        let (pending, work) = journal.begin_owned_roll(32768, 4).unwrap();
        let mut journal = pending.complete(work.publish().await).unwrap();
        assert!(!spare.exists());
        append_confirmed(&mut journal).await;
        // Seal the reused file with old groups past its end.
        let (pending, work) = journal.begin_owned_roll(32768, 4).unwrap();
        let journal = pending.complete(work.publish().await).unwrap();
        drop(journal);
        backend.shutdown().await;
        let (backend, io) = super::real_backend(aio);
        let journal = super::open_real(&path, io, 2, limits).await;
        assert_eq!(journal.accepted_position().unwrap().op_number, 4);
        assert_eq!(journal.writer.header().segment_id(), 3);
        drop(journal);
        backend.shutdown().await;
    }
}

#[test]
fn an_installed_suffix_takes_the_spare() {
    let (mut controller, io) = setup(spare_image());
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    let retired = controller.image().bytes(Path::new(SPARE), false).unwrap();
    let retired = retired.len();
    // A new view replaces the log after the committed prefix.
    let protected = journal.committed_position().unwrap();
    let replacement = crate::SuffixReplacement {
        expected_current: journal.current(),
        protected_committed: protected,
        promised_view: 2,
        last_normal_view: 2,
        committed: protected,
        writer_generation: JournalGeneration(9),
        segment_capacity: 32768,
        body_encoding: BodyEncoding::Raw,
    };
    let selected = CanonicalOperation {
        op_number: protected.op_number + 1,
        previous_digest: protected.digest,
        body: &[7; 16],
        ..operation(&journal)
    };
    let accepted = LogPosition {
        op_number: selected.op_number,
        digest: crate::logical_operation_digest(&selected),
    };
    let request = crate::SuffixRequest {
        replacement,
        accepted,
        limits: crate::SuffixStreamLimits {
            max_group_operations: 2,
            max_group_body_bytes: 128,
            max_segments: 16,
            max_staged_bytes: 128 * 1024,
            max_source_segment_bytes: 32768,
            max_orphan_probes: 8,
        },
    };
    let mut installer = drive(&mut controller, journal.begin_suffix_replacement(request)).unwrap();
    assert!(!controller.image().exists(Path::new(SPARE), false));
    drive(&mut controller, installer.append_chunk(&[selected])).unwrap();
    let journal = drive(&mut controller, Box::pin(installer.finish())).unwrap();
    assert_eq!(journal.writer.header().segment_id(), 2);
    let segment = Path::new("/group/segments/2.log");
    assert_eq!(
        controller.image().bytes(segment, false).unwrap().len(),
        retired
    );
    drop(journal);
    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let journal = drive(&mut controller, open(io, 3)).unwrap();
    assert_eq!(journal.accepted_position().unwrap(), accepted);
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn a_zeroed_spare_is_the_next_segment() {
    let (mut controller, mut journal) = empty_journal();
    drive(&mut controller, journal.zero_spare(4096)).unwrap();
    let spare = controller.image().bytes(Path::new(SPARE), false).unwrap();
    assert_eq!(spare, vec![0; 4096]);
    drive(&mut controller, append_confirmed(&mut journal));
    let journal = roll(&mut controller, journal);
    assert!(!controller.image().exists(Path::new(SPARE), false));
    // Capacity past the zeroed prefix stays reserved, not written.
    let reused = Path::new("/group/segments/2.log");
    assert_eq!(controller.image().bytes(reused, false).unwrap().len(), 4096);
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn zeroing_ahead_stops_at_its_bound_and_at_capacity() {
    let (mut controller, mut journal) = empty_journal();
    let segment = Path::new("/group/segments/1.log");
    let written = journal.writer().written_position().end_offset();
    drive(&mut controller, journal.zero_ahead(4096)).unwrap();
    let bytes = controller.image().bytes(segment, false).unwrap();
    assert_eq!(bytes.len() as u64, written + 4096);
    assert!(bytes[written as usize..].iter().all(|&byte| byte == 0));
    drive(&mut controller, journal.zero_ahead(u64::MAX)).unwrap();
    let bytes = controller.image().bytes(segment, false).unwrap();
    assert_eq!(bytes.len(), 32768);
    drive(&mut controller, journal.close()).unwrap();
}
