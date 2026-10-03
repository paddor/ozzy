use super::*;
use crate::checkpoint_name;
use ozzy_proto::CheckpointId;

fn id(byte: u8) -> CheckpointId {
    CheckpointId::from_bytes([byte; 16])
}
fn schema() -> Digest {
    Digest::from_bytes([0x98; 32])
}

#[test]
fn metadata_cleanup_refuses_damaged_authority_before_releasing_abandoned_source() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, commit(&mut journal));
    let built = drive(
        &mut controller,
        journal.build_checkpoint(id(12), schema(), 8, b"abandoned state"),
    )
    .unwrap();
    let source = built.manifest().source_manifest_generation;
    let source_path = journal.root().join(format!("MANIFEST.{source}"));
    let mut next = journal.next_manifest().unwrap();
    next.promised_view += 1;
    drive(&mut controller, journal.install_metadata(next)).unwrap();
    assert!(
        !drive(&mut controller, journal.reclaim_unreferenced_metadata(128))
            .unwrap()
            .removed_manifest_generations
            .contains(&source)
    );
    drop(built);
    let current_path = journal.root().join("CURRENT");
    let current = controller
        .image()
        .bytes(&current_path, false)
        .unwrap()
        .to_vec();
    let file = drive(
        &mut controller,
        journal
            .access
            .open(current_path, ozzy_io::OpenMode::ReadWrite, false, false),
    )
    .unwrap();
    drive(&mut controller, journal.access.write_all(&file, 0, &[255])).unwrap();
    assert!(drive(&mut controller, journal.reclaim_unreferenced_metadata(128)).is_err());
    assert!(controller.image().exists(&source_path, false));
    drive(
        &mut controller,
        journal.access.write_all(&file, 0, &current),
    )
    .unwrap();
    drive(&mut controller, journal.access.sync(&file)).unwrap();
    drive(
        &mut controller,
        journal.access.done(Operation::Close { handle: file }),
    )
    .unwrap();
    assert!(
        drive(&mut controller, journal.reclaim_unreferenced_metadata(128))
            .unwrap()
            .removed_manifest_generations
            .contains(&source)
    );
    drive(&mut controller, journal.close()).unwrap();
}

async fn commit(journal: &mut Journal) {
    let mut next = journal.next_manifest().unwrap();
    next.accepted = journal.accepted_position().unwrap();
    next.committed = next.accepted;
    journal.install_metadata(next).await.unwrap();
}

#[test]
fn build_results_and_readers_protect_checkpoint_files_and_source_manifests() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io.clone(), 2)).unwrap();
    drive(&mut controller, commit(&mut journal));
    let built = drive(
        &mut controller,
        journal.build_checkpoint(id(10), schema(), 8, b"first state"),
    )
    .unwrap();
    let source = built.manifest().source_manifest_generation;
    let mut next = journal.next_manifest().unwrap();
    next.promised_view += 1;
    drive(&mut controller, journal.install_metadata(next)).unwrap();
    let cleaned = drive(&mut controller, journal.reclaim_unreferenced_metadata(128)).unwrap();
    assert!(!cleaned.removed_manifest_generations.contains(&source));
    let cleaned = drive(
        &mut controller,
        journal.reclaim_unreferenced_checkpoints(128),
    )
    .unwrap();
    assert_eq!(cleaned.pinned_checkpoint_ids, [id(10)]);
    drive(&mut controller, journal.install_checkpoint(id(10))).unwrap();
    let captured = journal.capture_checkpoint().unwrap();
    drop(built);
    drive(&mut controller, append_confirmed(&mut journal));
    drive(&mut controller, commit(&mut journal));
    drive(
        &mut controller,
        journal.build_checkpoint(id(11), schema(), 8, b"second state"),
    )
    .unwrap();
    drive(&mut controller, journal.install_checkpoint(id(11))).unwrap();
    assert_eq!(
        drive(&mut controller, captured.read_state()).unwrap(),
        b"first state"
    );
    let cleaned = drive(&mut controller, journal.reclaim_unreferenced_metadata(128)).unwrap();
    assert!(!cleaned.removed_manifest_generations.contains(&source));
    assert_eq!(
        drive(
            &mut controller,
            journal.reclaim_unreferenced_checkpoints(128)
        )
        .unwrap()
        .pinned_checkpoint_ids,
        [id(10)]
    );
    drop(journal);
    assert!(drive(&mut controller, open(io.clone(), 3)).is_err());
    drop(captured);
    let mut journal = drive(&mut controller, open(io, 4)).unwrap();
    assert_eq!(
        drive(&mut controller, journal.reclaim_unreferenced_checkpoints(1))
            .unwrap()
            .removed_checkpoint_ids,
        [id(10)]
    );
    let cleaned = drive(&mut controller, journal.reclaim_unreferenced_metadata(128)).unwrap();
    assert!(cleaned.removed_manifest_generations.contains(&source));
    assert_eq!(
        drive(&mut controller, journal.read_checkpoint_state())
            .unwrap()
            .unwrap(),
        b"second state"
    );
    assert_eq!(journal.committed_position().unwrap().op_number, 2);
}

fn obsolete_checkpoint_image() -> Image {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, commit(&mut journal));
    drive(
        &mut controller,
        journal.build_checkpoint(id(10), schema(), 8, b"first state"),
    )
    .unwrap();
    drive(&mut controller, journal.install_checkpoint(id(10))).unwrap();
    drive(&mut controller, append_confirmed(&mut journal));
    drive(&mut controller, commit(&mut journal));
    drive(
        &mut controller,
        journal.build_checkpoint(id(11), schema(), 8, b"second state"),
    )
    .unwrap();
    drive(&mut controller, journal.install_checkpoint(id(11))).unwrap();
    drop(journal);
    controller.crash(true).unwrap().0
}

#[test]
fn unexpected_nested_checkpoint_directory_refuses_bounded_cleanup() {
    let (mut controller, io) = setup(obsolete_checkpoint_image());
    let mut journal = drive(&mut controller, open(io, 3)).unwrap();
    let root = journal
        .root()
        .join("checkpoints")
        .join(checkpoint_name(id(10)));
    drive(
        &mut controller,
        journal.access.done(Operation::CreateDirectory {
            path: root.join("nested"),
        }),
    )
    .unwrap();
    assert!(drive(&mut controller, journal.reclaim_unreferenced_checkpoints(1)).is_err());
    assert!(
        controller
            .image()
            .bytes(&root.join("manifest"), false)
            .is_ok()
    );
    assert_eq!(
        drive(&mut controller, journal.read_checkpoint_state())
            .unwrap()
            .unwrap(),
        b"second state"
    );
}

#[test]
fn checkpoint_and_manifest_cleanup_crash_cuts_keep_selected_sources() {
    let image = obsolete_checkpoint_image();
    for immediate in [false, true] {
        let mut finished = false;
        for cut in 0..500 {
            let (mut controller, io) = setup(image.clone());
            let mut journal = drive(&mut controller, open(io, 3)).unwrap();
            let done = {
                let mut future = std::pin::pin!(async {
                    journal.reclaim_unreferenced_checkpoints(1).await?;
                    journal.reclaim_unreferenced_metadata(128).await
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
            let mut journal = drive(&mut controller, open(io, 4)).unwrap();
            assert_eq!(journal.committed_position().unwrap().op_number, 2);
            assert_eq!(
                drive(&mut controller, journal.read_checkpoint_state())
                    .unwrap()
                    .unwrap(),
                b"second state"
            );
            drive(&mut controller, journal.reclaim_unreferenced_checkpoints(1)).unwrap();
            drive(&mut controller, journal.reclaim_unreferenced_metadata(128)).unwrap();
            if done {
                finished = true;
                break;
            }
        }
        assert!(finished);
    }
}
