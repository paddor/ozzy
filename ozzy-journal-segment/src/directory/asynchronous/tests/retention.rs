use super::*;
use crate::{AsyncRetirementBudget, RetentionFloors};

mod scan;

fn budget() -> AsyncRetirementBudget {
    AsyncRetirementBudget {
        max_segments: 2,
        max_read_bytes: 65536,
    }
}

pub(super) fn retained_baseline() -> Image {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    drive(&mut controller, append_confirmed(&mut journal));
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    let mut next = journal.next_manifest().unwrap();
    next.accepted = journal.accepted_position().unwrap();
    next.committed = next.accepted;
    drive(&mut controller, journal.install_metadata(next)).unwrap();
    let id = ozzy_proto::CheckpointId::from_bytes([0x66; 16]);
    drive(
        &mut controller,
        journal.build_checkpoint(id, Digest::from_bytes([0x67; 32]), 8, b"retained state"),
    )
    .unwrap();
    drive(&mut controller, journal.install_checkpoint(id)).unwrap();
    drop(journal);
    controller.crash(true).unwrap().0
}

#[test]
fn captured_generations_stay_readable_after_retirement_and_delay_deletion() {
    let (mut controller, io) = setup(retained_baseline());
    let mut journal = drive(&mut controller, open(io.clone(), 3)).unwrap();
    let captured = journal.capture_sealed_segments(&[2, 1]).unwrap();
    assert_eq!(
        captured
            .references()
            .iter()
            .map(|r| r.segment_id)
            .collect::<Vec<_>>(),
        [2, 1]
    );
    assert!(journal.capture_sealed_segments(&[3]).is_err());
    assert!(journal.capture_sealed_segments(&[1, 1]).is_err());
    let floors = RetentionFloors::new(vec![]).unwrap();
    let retired = drive(
        &mut controller,
        journal.retire_sealed_prefix(&floors, budget()),
    )
    .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [1, 2]);
    assert_eq!(retired.scanned_segments, 2);
    let cleaned = drive(&mut controller, journal.reclaim_unreferenced_segments(2)).unwrap();
    assert_eq!(cleaned.removed_segment_ids.len(), 0);
    assert_eq!(cleaned.pinned_segment_ids, [1, 2]);
    let bytes = drive(&mut controller, captured.read_segment(1)).unwrap();
    let reference = captured.references()[1];
    assert!(bytes.len() as u64 >= reference.sealed.unwrap().valid_bytes);
    assert!(bytes.len() as u64 <= reference.capacity);
    assert_eq!(
        crate::decode_segment_header(&bytes).unwrap().segment_id(),
        1
    );
    drop(journal);
    assert!(drive(&mut controller, open(io.clone(), 4)).is_err());
    drop(captured);
    let mut journal = drive(&mut controller, open(io, 5)).unwrap();
    assert_eq!(
        drive(&mut controller, journal.reclaim_unreferenced_segments(1))
            .unwrap()
            .removed_segment_ids,
        [1]
    );
    assert_eq!(
        drive(&mut controller, journal.reclaim_unreferenced_segments(1))
            .unwrap()
            .removed_segment_ids,
        [2]
    );
    assert_eq!(journal.manifest.segments.len(), 1);
    assert_eq!(journal.committed_position().unwrap().op_number, 2);
}

#[test]
fn retirement_scan_bounds_and_damaged_authority_prevent_unlinking() {
    let (mut controller, io) = setup(retained_baseline());
    let mut journal = drive(&mut controller, open(io, 3)).unwrap();
    let floors = RetentionFloors::new(vec![]).unwrap();
    assert!(matches!(
        drive(
            &mut controller,
            journal.retire_sealed_prefix(
                &floors,
                AsyncRetirementBudget {
                    max_segments: 1,
                    max_read_bytes: 32767
                }
            )
        ),
        Err(DirectoryError::RetentionScanBudget)
    ));
    assert_eq!(journal.manifest.segments.len(), 3);
    let retired = drive(
        &mut controller,
        journal.retire_sealed_prefix(
            &floors,
            AsyncRetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [1]);
    let file = drive(
        &mut controller,
        journal.access.open(
            journal.root().join("CURRENT"),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(&mut controller, journal.access.write_all(&file, 0, &[99])).unwrap();
    assert!(drive(&mut controller, journal.reclaim_unreferenced_segments(2)).is_err());
    assert!(
        controller
            .image()
            .bytes(Path::new("/group/segments/1.log"), false)
            .is_ok()
    );
}

#[test]
fn failed_index_deletion_barrier_never_unlinks_its_payload_segment() {
    let (mut controller, io) = setup(retained_baseline());
    let mut journal = drive(&mut controller, open(io, 3)).unwrap();
    let floors = RetentionFloors::new(vec![]).unwrap();
    drive(
        &mut controller,
        journal.retire_sealed_prefix(&floors, budget()),
    )
    .unwrap();
    let path = journal
        .root()
        .join("indexes")
        .join(format!("1-{}.idx", "a".repeat(64)));
    let file = drive(
        &mut controller,
        journal
            .access
            .open(path, ozzy_io::OpenMode::CreateNew, false, false),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(&file, 0, b"derived bytes"),
    )
    .unwrap();
    drive(&mut controller, journal.access.sync(&file)).unwrap();
    drive(
        &mut controller,
        journal
            .access
            .sync_directory(journal.root().join("indexes")),
    )
    .unwrap();
    let mut removed_index = false;
    let result = drive_with(
        &mut controller,
        journal.reclaim_unreferenced_segments(2),
        |operation| {
            if let Operation::RemoveFile { path } = operation
                && path.extension().is_some_and(|extension| extension == "idx")
            {
                removed_index = true;
            }
            if removed_index && matches!(operation, Operation::Sync { .. }) {
                Effect::FailBefore(io::ErrorKind::Other)
            } else {
                Effect::Normal
            }
        },
    );
    assert!(removed_index);
    assert!(result.is_err());
    assert!(journal.is_faulted());
    assert!(
        controller
            .image()
            .bytes(Path::new("/group/segments/1.log"), false)
            .is_ok()
    );
}

#[test]
fn retirement_and_deletion_crash_cuts_preserve_the_selected_checkpoint() {
    let image = retained_baseline();
    let floors = RetentionFloors::new(vec![]).unwrap();
    for immediate in [false, true] {
        let mut finished = false;
        for cut in 0..500 {
            let (mut controller, io) = setup(image.clone());
            let mut journal = drive(&mut controller, open(io, 3)).unwrap();
            let done = {
                let mut future = std::pin::pin!(async {
                    journal.retire_sealed_prefix(&floors, budget()).await?;
                    journal.reclaim_unreferenced_segments(2).await
                });
                let mut done = false;
                for _ in 0..cut {
                    if let Poll::Ready(result) = poll(future.as_mut()) {
                        result.unwrap();
                        done = true;
                        break;
                    }
                    let (id, stage) = controller.jobs()[0];
                    match stage {
                        Stage::Queued => {
                            controller.execute(id, Effect::Normal).unwrap();
                            if immediate {
                                controller.deliver(id).unwrap();
                            }
                        }
                        Stage::Executed => controller.deliver(id).unwrap(),
                    }
                }
                done
            };
            drop(journal);
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let journal = drive(&mut controller, open(io, 4)).unwrap();
            assert_eq!(journal.committed_position().unwrap().op_number, 2);
            assert_eq!(
                drive(&mut controller, journal.read_checkpoint_state())
                    .unwrap()
                    .unwrap(),
                b"retained state"
            );
            if done {
                finished = true;
                break;
            }
        }
        assert!(finished);
    }
}
