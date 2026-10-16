//! Direct-write driver: allocation and barriers through the direct worker,
//! canceled handoffs, and driver failure during startup.

use super::*;

#[cfg(target_os = "linux")]
#[tokio::test]
async fn direct_writes_allocation_and_explicit_barriers() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut clients) = Pool::new(config()).unwrap();
    let path = temp.path().join("direct");
    let result = perform(
        &mut clients[0],
        Operation::Open {
            path: path.clone(),
            mode: OpenMode::CreateNew,
            direct: true,
            data_sync: true,
        },
    )
    .await
    .unwrap();
    let Outcome::Opened(handle) = &*result else {
        panic!("open response")
    };
    let handle = handle.clone();
    drop(result);
    perform(
        &mut clients[0],
        Operation::SetLength {
            handle: handle.clone(),
            length: 8192,
        },
    )
    .await
    .unwrap();
    perform(
        &mut clients[0],
        Operation::Allocate {
            handle: handle.clone(),
            offset: 0,
            length: 8192,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        *perform(&mut clients[0], write(&handle, 4096, &[42; 4096]))
            .await
            .unwrap(),
        Outcome::Written(4096)
    ));
    assert_eq!(
        perform(&mut clients[0], write(&handle, 1, &[42; 4096]))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    perform(
        &mut clients[0],
        Operation::Sync {
            handle,
            mode: SyncMode::Data,
        },
    )
    .await
    .unwrap();
    let buffered = open(&mut clients[0], &path, OpenMode::Read).await;
    let read = perform(
        &mut clients[0],
        Operation::Read {
            handle: buffered,
            offset: 4096,
            length: 4096,
        },
    )
    .await
    .unwrap();
    let Outcome::Read(bytes) = &*read else {
        panic!("read response")
    };
    assert_eq!(bytes.as_slice(), &[42; 4096]);
    pool.shutdown().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn direct_handoff_retains_canceled_buffers_and_physical_handle_capacity() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("data");
    let gate = Gate::new(|_| true);
    let (pool, mut clients) = Pool::with_direct_worker(
        Config {
            handles: 2,
            ..config()
        },
        HeldDirect(gate.clone()),
    )
    .unwrap();
    let opening = perform(
        &mut clients[0],
        Operation::Open {
            path: path.clone(),
            mode: OpenMode::CreateNew,
            direct: true,
            data_sync: false,
        },
    )
    .await
    .unwrap();
    let Outcome::Opened(handle) = &*opening else {
        panic!("handle")
    };
    let handle = handle.clone();
    drop(opening);
    perform(
        &mut clients[0],
        Operation::LockExclusive {
            handle: handle.clone(),
        },
    )
    .await
    .unwrap();
    let written = clients[0]
        .submit(Class::Data, write(&handle, 0, &[3; 4096]))
        .unwrap();
    gate.wait().await;
    drop(written);
    assert_eq!(pool.admission().used(0, Class::Data).operations, 1);
    // Progress helpers remain runnable even while the direct driver is held.
    clients[0]
        .submit(Class::Progress, Operation::Close { handle })
        .unwrap()
        .await
        .unwrap();
    assert!(matches!(
        std::fs::File::open(&path).unwrap().try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    let error = clients[0]
        .submit(
            Class::Progress,
            Operation::Open {
                path: path.clone(),
                mode: OpenMode::Read,
                direct: false,
                data_sync: false,
            },
        )
        .unwrap()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    let shutdown = pool.shutdown();
    tokio::pin!(shutdown);
    assert!(futures::poll!(&mut shutdown).is_pending());
    gate.release();
    shutdown.await;
    assert_eq!(pool.admission().used(0, Class::Data), Quota::default());
    std::fs::File::open(path).unwrap().try_lock().unwrap();
    assert_eq!(gate.state.lock().unwrap().threads, ["ozzy/disk/aio"]);
}

#[tokio::test]
async fn direct_driver_failure_during_startup_fences_and_drains_all_helpers() {
    for kind in 0..3 {
        for _ in 0..8 {
            let (pool, mut clients) =
                Pool::with_direct_worker(config(), FailedDirect(kind)).unwrap();
            pool.shutdown().await;
            assert_eq!(
                clients[0]
                    .submit(
                        Class::Data,
                        Operation::ReadDirectory {
                            path: "/".into(),
                            max_entries: 0,
                            max_name_bytes: 0,
                        }
                    )
                    .unwrap_err()
                    .error
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
        }
    }
}
