//! Descriptors on demand: a spent budget parks idle handles, and a parked
//! handle opens its file again at its next use.

use super::*;

/// One shard with room for `handles` descriptors.
fn pool(handles: usize) -> (Pool, Client) {
    let mut config = config();
    config.handles = handles;
    config.limits.shards = 1;
    config.limits.data.operations = 8;
    let (pool, mut clients) = Pool::new(config).unwrap();
    (pool, clients.remove(0))
}

/// Create `name` holding its own name, and keep the handle.
async fn file(client: &mut Client, root: &Path, name: &str) -> Handle {
    let handle = open(client, &root.join(name), OpenMode::CreateNew).await;
    perform(client, write(&handle, 0, name.as_bytes()))
        .await
        .unwrap();
    handle
}

async fn read(client: &mut Client, handle: &Handle) -> io::Result<Vec<u8>> {
    let operation = Operation::Read {
        handle: handle.clone(),
        offset: 0,
        length: 16,
    };
    let read = perform(client, operation).await?;
    let Outcome::Read(bytes) = &*read else {
        panic!("read response")
    };
    Ok(bytes.as_slice().to_vec())
}

fn held(pool: &Pool) -> usize {
    pool.shared.files().held(0)
}

#[tokio::test]
async fn a_spent_budget_parks_the_handle_used_longest_ago_and_use_opens_it_again() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut client) = pool(2);
    let a = file(&mut client, temp.path(), "a").await;
    let b = file(&mut client, temp.path(), "b").await;
    // The third open takes the descriptor of `a`, the fourth use that of `b`.
    let c = file(&mut client, temp.path(), "c").await;
    assert_eq!(held(&pool), 2);
    assert_eq!(read(&mut client, &a).await.unwrap(), b"a");
    assert_eq!(held(&pool), 2);
    // A parked handle that was created anew still writes.
    perform(&mut client, write(&b, 1, b"!")).await.unwrap();
    assert_eq!(read(&mut client, &b).await.unwrap(), b"b!");
    assert_eq!(read(&mut client, &c).await.unwrap(), b"c");
    assert_eq!(held(&pool), 2);
    drop((a, b, c));
    pool.shutdown().await;
}

#[tokio::test]
async fn a_locked_handle_keeps_its_descriptor() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut client) = pool(1);
    let lock = file(&mut client, temp.path(), "lock").await;
    let locked = Operation::LockExclusive {
        handle: lock.clone(),
    };
    perform(&mut client, locked).await.unwrap();
    let error = perform(
        &mut client,
        Operation::Open {
            path: temp.path().join("other"),
            mode: OpenMode::CreateNew,
            direct: false,
            data_sync: false,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(read(&mut client, &lock).await.unwrap(), b"lock");
    pool.shutdown().await;
}

#[tokio::test]
async fn a_parked_handle_follows_renames_of_its_file_and_of_its_directory() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut client) = pool(1);
    let root = temp.path();
    std::fs::create_dir(root.join("old")).unwrap();
    let a = file(&mut client, &root.join("old"), "a").await;
    // Opening `b` parks `a`.
    let b = file(&mut client, root, "b").await;
    let renames = [
        (root.join("old/a"), root.join("old/moved")),
        (root.join("old"), root.join("new")),
    ];
    for (source, destination) in renames {
        let rename = Operation::Rename {
            source,
            destination,
        };
        perform(&mut client, rename).await.unwrap();
    }
    assert_eq!(read(&mut client, &a).await.unwrap(), b"a");
    assert_eq!(read(&mut client, &b).await.unwrap(), b"b");
    assert_eq!(std::fs::read(root.join("new/moved")).unwrap(), b"a");
    pool.shutdown().await;
}

#[tokio::test]
async fn a_file_that_loses_its_name_keeps_its_descriptor() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut client) = pool(2);
    let root = temp.path();
    let removed = file(&mut client, root, "removed").await;
    let replaced = file(&mut client, root, "replaced").await;
    // Both are parked in turn, then lose their names while parked or open.
    let other = file(&mut client, root, "other").await;
    let remove = Operation::RemoveFile {
        path: root.join("removed"),
    };
    perform(&mut client, remove).await.unwrap();
    let rename = Operation::Rename {
        source: root.join("other"),
        destination: root.join("replaced"),
    };
    perform(&mut client, rename).await.unwrap();
    // Two nameless files hold the whole budget now, so `other` is parked
    // and nothing else can open.
    assert_eq!(read(&mut client, &removed).await.unwrap(), b"removed");
    assert_eq!(read(&mut client, &replaced).await.unwrap(), b"replaced");
    assert_eq!(
        read(&mut client, &other).await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(removed);
    assert_eq!(read(&mut client, &other).await.unwrap(), b"other");
    assert_eq!(std::fs::read(root.join("replaced")).unwrap(), b"other");
    pool.shutdown().await;
}

#[tokio::test]
async fn a_failed_rename_leaves_the_destination_handle_its_name() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut client) = pool(1);
    let root = temp.path();
    let claimed = file(&mut client, root, "claimed").await;
    let rename = Operation::Rename {
        source: root.join("missing"),
        destination: root.join("claimed"),
    };
    let error = perform(&mut client, rename).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    // The handle can still give up its descriptor and take it back.
    let other = file(&mut client, root, "other").await;
    assert_eq!(read(&mut client, &claimed).await.unwrap(), b"claimed");
    assert_eq!(read(&mut client, &other).await.unwrap(), b"other");
    pool.shutdown().await;
}

#[tokio::test]
async fn another_file_at_a_parked_handles_path_is_an_error() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut client) = pool(1);
    let root = temp.path();
    let a = file(&mut client, root, "a").await;
    let b = file(&mut client, root, "b").await;
    // Behind the pool's back. The old file keeps its inode under another
    // name, so the new one cannot be given the same number.
    std::fs::rename(root.join("a"), root.join("moved")).unwrap();
    std::fs::write(root.join("a"), b"impostor").unwrap();
    let error = read(&mut client, &a).await.unwrap_err();
    assert!(error.to_string().contains("another file"), "{error}");
    assert_eq!(read(&mut client, &b).await.unwrap(), b"b");
    pool.shutdown().await;
}

#[tokio::test]
async fn a_dropped_handle_frees_its_slot_without_a_scan() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut client) = pool(4);
    let kept = file(&mut client, temp.path(), "kept").await;
    let dropped = file(&mut client, temp.path(), "dropped").await;
    assert_eq!(held(&pool), 2);
    drop(dropped);
    // Any later job runs the reclamation the drop asked for.
    assert_eq!(read(&mut client, &kept).await.unwrap(), b"kept");
    let other = file(&mut client, temp.path(), "other").await;
    assert_eq!(held(&pool), 2);
    assert_eq!(pool.shared.files().open(), 2);
    drop((kept, other));
    pool.shutdown().await;
}
