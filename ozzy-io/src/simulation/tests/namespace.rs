//! Namespace changes and descriptor lifetimes: links, renames, unlinks, locks,
//! and inode reclamation.

use super::*;

#[tokio::test]
async fn hard_links_rename_unlink_and_open_directory_identity_survive_namespace_changes() {
    let mut harness = Harness::new();
    harness
        .run(Operation::CreateDirectory {
            path: "/part".into(),
        })
        .await;
    let directory = harness.directory("/part").await;
    let file = harness.open("/part/old", OpenMode::CreateNew).await;
    harness.run(write(&file, 0, b"first")).await;
    harness
        .run(Operation::HardLink {
            source: "/part/old".into(),
            destination: "/part/link".into(),
        })
        .await;
    harness
        .run(Operation::Rename {
            source: "/part".into(),
            destination: "/moved".into(),
        })
        .await;
    harness
        .run(Operation::RemoveFile {
            path: "/moved/old".into(),
        })
        .await;
    harness.run(write(&file, 0, b"later")).await;
    harness.sync(&file).await;
    harness.sync(&directory).await;
    let root = harness.directory("/").await;
    harness.sync(&root).await;
    let (image, _) = harness.controller.crash(true).unwrap();
    assert!(!image.exists(Path::new("/part"), false));
    assert!(!image.exists(Path::new("/moved/old"), false));
    assert_eq!(
        image.bytes(Path::new("/moved/link"), false).unwrap(),
        b"later"
    );
}

#[tokio::test]
async fn rename_and_deletion_need_parent_barriers() {
    for barrier in [false, true] {
        let mut harness = Harness::new();
        let _file = harness.established(b"data").await;
        harness
            .run(Operation::Rename {
                source: "/data".into(),
                destination: "/new".into(),
            })
            .await;
        if barrier {
            let root = harness.directory("/").await;
            harness.sync(&root).await;
        }
        let (image, _) = harness.controller.crash(true).unwrap();
        assert_eq!(image.exists(Path::new("/new"), false), barrier);
        assert_eq!(image.exists(Path::new("/data"), false), !barrier);
        let mut harness = Harness::from_image(image);
        let name = if barrier { "/new" } else { "/data" };
        harness
            .run(Operation::RemoveFile { path: name.into() })
            .await;
        if barrier {
            let root = harness.directory("/").await;
            harness.sync(&root).await;
        }
        let (image, _) = harness.controller.crash(true).unwrap();
        assert_eq!(image.exists(Path::new(name), false), !barrier);
    }
}

#[tokio::test]
async fn locks_close_and_handle_drop_follow_descriptor_lifetimes() {
    let mut harness = Harness::new();
    let first = harness.established(b"data").await;
    let second = harness.open("/data", OpenMode::ReadWrite).await;
    harness
        .run(Operation::LockExclusive {
            handle: first.clone(),
        })
        .await;
    let error = harness
        .perform(
            Operation::LockExclusive {
                handle: second.clone(),
            },
            Effect::Normal,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    drop(first);
    harness
        .run(Operation::LockExclusive {
            handle: second.clone(),
        })
        .await;
    harness
        .run(Operation::Close {
            handle: second.clone(),
        })
        .await;
    assert_eq!(
        harness
            .perform(write(&second, 0, b"no"), Effect::Normal)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
}

#[tokio::test]
async fn retired_inodes_are_reclaimed_but_unlinked_open_files_stay_readable() {
    let mut configuration = config();
    configuration.image.nodes = 2; // root plus one file
    let (controller, clients) = Controller::new(configuration, Image::default()).unwrap();
    let mut harness = Harness {
        controller,
        clients,
    };
    for _ in 0..16 {
        let file = harness.open("/transient", OpenMode::CreateNew).await;
        harness.run(write(&file, 0, b"retained")).await;
        harness
            .run(Operation::RemoveFile {
                path: "/transient".into(),
            })
            .await;
        let read = harness
            .run(Operation::Read {
                handle: file.clone(),
                offset: 0,
                length: 8,
            })
            .await;
        let Outcome::Read(bytes) = &*read else {
            panic!("read response")
        };
        assert_eq!(bytes.as_slice(), b"retained");
        drop(read);
        drop(file);
    }
}

#[tokio::test]
async fn unlinked_open_directory_preserves_durable_children_until_its_last_handle_closes() {
    let mut configuration = config();
    configuration.image.nodes = 3; // root, directory, file
    let (controller, clients) = Controller::new(configuration, Image::default()).unwrap();
    let mut harness = Harness {
        controller,
        clients,
    };
    harness
        .run(Operation::CreateDirectory {
            path: "/dir".into(),
        })
        .await;
    let directory = harness.directory("/dir").await;
    let file = harness.open("/dir/file", OpenMode::CreateNew).await;
    harness.run(write(&file, 0, b"retained")).await;
    harness.sync(&file).await;
    harness.sync(&directory).await;
    let root = harness.directory("/").await;
    harness.sync(&root).await;
    drop(file);
    harness
        .run(Operation::RemoveFile {
            path: "/dir/file".into(),
        })
        .await;
    harness
        .run(Operation::RemoveDirectory {
            path: "/dir".into(),
        })
        .await;
    harness.sync(&root).await;
    harness.controller.jobs();
    assert_eq!(harness.controller.image().stored_bytes(), 16);
    let error = harness
        .perform(
            Operation::Open {
                path: "/other".into(),
                mode: OpenMode::CreateNew,
                direct: false,
                data_sync: false,
            },
            Effect::Normal,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::StorageFull);
    drop(directory);
    let _other = harness.open("/other", OpenMode::CreateNew).await;
    assert_eq!(harness.controller.image().stored_bytes(), 0);
}
