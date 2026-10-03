use super::*;

pub(super) async fn put_file(journal: &Journal, path: PathBuf, bytes: &[u8]) {
    let file = journal
        .access
        .open(path, ozzy_io::OpenMode::CreateNew, false, false)
        .await
        .unwrap();
    journal.access.write_all(&file, 0, bytes).await.unwrap();
    journal.access.sync(&file).await.unwrap();
    journal
        .access
        .done(Operation::Close { handle: file })
        .await
        .unwrap();
}

fn orphan_image() -> Image {
    let (mut controller, io) = setup(baseline());
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    let checkpoint = journal.root().join("checkpoints").join("77".repeat(16));
    drive(
        &mut controller,
        journal.access.done(Operation::CreateDirectory {
            path: checkpoint.clone(),
        }),
    )
    .unwrap();
    drive(
        &mut controller,
        put_file(
            &journal,
            journal.root().join("segments/9.log"),
            b"orphan payload",
        ),
    );
    for id in [9, 10] {
        drive(
            &mut controller,
            put_file(
                &journal,
                journal
                    .root()
                    .join(format!("indexes/{id}-{}.idx", "ab".repeat(32))),
                b"orphan index",
            ),
        );
    }
    for chunk in 0..8 {
        drive(
            &mut controller,
            put_file(
                &journal,
                checkpoint.join(format!("CHUNK.{chunk:08}")),
                b"partial checkpoint",
            ),
        );
    }
    for relative in ["segments/unknown", "indexes/unknown", "MANIFEST.01"] {
        drive(
            &mut controller,
            put_file(&journal, journal.root().join(relative), b"preserve"),
        );
    }
    for path in [
        journal.root().join("segments"),
        journal.root().join("indexes"),
        checkpoint,
        journal.root().join("checkpoints"),
        journal.root().to_path_buf(),
    ] {
        drive(&mut controller, journal.access.sync_directory(path)).unwrap();
    }
    drop(journal);
    controller.crash(true).unwrap().0
}

async fn reclaim(journal: &mut Journal) -> Result<(), DirectoryError> {
    journal.reclaim_unreferenced_segments(1).await?;
    journal.reclaim_unreferenced_indexes(128).await?;
    journal.reclaim_unreferenced_checkpoints(1).await?;
    journal.reclaim_unreferenced_metadata(128).await?;
    Ok(())
}

fn advance(controller: &mut Controller, immediate: bool) {
    let (id, stage) = controller.jobs()[0];
    if stage == Stage::Queued {
        let removes_payload = matches!(controller.operation(id).unwrap().unprotected(),
            Operation::RemoveFile { path } if path == Path::new("/group/segments/9.log"));
        if removes_payload {
            let index = PathBuf::from(format!("/group/indexes/9-{}.idx", "ab".repeat(32)));
            for durable in [false, true] {
                assert_eq!(
                    controller
                        .image()
                        .bytes(&index, durable)
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::NotFound,
                    "index deletion must precede payload deletion"
                );
            }
        }
        controller.execute(id, Effect::Normal).unwrap();
        if immediate {
            controller.deliver(id).unwrap();
        }
    } else {
        controller.deliver(id).unwrap();
    }
}

#[test]
fn orphan_cleanup_crash_cuts_preserve_selected_bytes_and_unknown_names() {
    let image = orphan_image();
    let selected = image
        .bytes(Path::new("/group/segments/1.log"), true)
        .unwrap()
        .to_vec();
    for immediate in [false, true] {
        let mut finished = false;
        for cut in 0..500 {
            let (mut controller, io) = setup(image.clone());
            let mut journal = drive(&mut controller, open(io, 3)).unwrap();
            let done = {
                let mut future = std::pin::pin!(reclaim(&mut journal));
                let mut done = false;
                for _ in 0..cut {
                    if let Poll::Ready(result) = poll(future.as_mut()) {
                        result.unwrap();
                        done = true;
                        break;
                    }
                    advance(&mut controller, immediate);
                }
                done
            };
            drop(journal);
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let mut journal = drive(&mut controller, open(io, 4)).unwrap();
            drive(&mut controller, reclaim(&mut journal)).unwrap();
            let after = controller.image();
            assert!(!after.exists(
                &journal.root().join("checkpoints").join("77".repeat(16)),
                true
            ));
            assert_eq!(
                after
                    .bytes(Path::new("/group/segments/1.log"), true)
                    .unwrap(),
                selected
            );
            for relative in ["segments/unknown", "indexes/unknown", "MANIFEST.01"] {
                assert_eq!(
                    after.bytes(&journal.root().join(relative), true).unwrap(),
                    b"preserve"
                );
            }
            for relative in [
                "segments/9.log".to_owned(),
                format!("indexes/9-{}.idx", "ab".repeat(32)),
                format!("indexes/10-{}.idx", "ab".repeat(32)),
            ] {
                assert_eq!(
                    after
                        .bytes(&journal.root().join(relative), true)
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::NotFound
                );
            }
            assert_eq!(
                drive(&mut controller, journal.reclaim_unreferenced_checkpoints(1))
                    .unwrap()
                    .removed_checkpoint_ids,
                []
            );
            if done {
                finished = true;
                break;
            }
        }
        assert!(
            finished,
            "all physical execution and completion cuts must finish"
        );
    }
}
