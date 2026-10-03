#![cfg(target_os = "linux")]

use ozzy_io::{
    Backend, Class, Completed, Handle, Limits, OpenMode, Operation, Outcome, Quota, SyncMode,
    WriteBuffer,
};
use ozzy_io_aio::{Aio, Client, Config, PoolConfig};
use std::{io, path::Path};

fn config(depth: usize) -> Config {
    Config {
        depth,
        pool: PoolConfig {
            threads: 2,
            max_inflight: 2,
            handles: 16,
            limits: Limits {
                shards: 2,
                data: Quota {
                    operations: 16,
                    bytes: 1024 * 1024,
                },
                progress: Quota {
                    operations: 8,
                    bytes: 256 * 1024,
                },
            },
        },
    }
}

#[test]
fn invalid_device_depth_is_rejected_before_starting_workers() {
    for depth in [0, 65, usize::MAX] {
        assert_eq!(
            Aio::new(config(depth)).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

async fn run(client: &mut Client, operation: Operation) -> io::Result<Completed> {
    client.submit(Class::Data, operation).unwrap().await
}

async fn open(
    client: &mut Client,
    path: &Path,
    mode: OpenMode,
    direct: bool,
    data_sync: bool,
) -> Handle {
    let result = run(
        client,
        Operation::Open {
            path: path.into(),
            mode,
            direct,
            data_sync,
        },
    )
    .await
    .unwrap();
    let Outcome::Opened(handle) = &*result else {
        panic!("open result")
    };
    handle.clone()
}

fn write(handle: &Handle, offset: u64, byte: u8) -> Operation {
    Operation::Write {
        handle: handle.clone(),
        offset,
        data: WriteBuffer::from_vec(vec![byte; 4096]),
    }
}

#[tokio::test]
async fn direct_writes_shared_with_helpers_preserve_bytes_and_barriers() {
    for depth in [1, 4, 64] {
        let root = tempfile::tempdir().unwrap();
        let (backend, mut clients) = Aio::new(config(depth)).unwrap();
        let path = root.path().join("data");
        let handle = open(&mut clients[0], &path, OpenMode::CreateNew, true, false).await;
        run(
            &mut clients[0],
            Operation::Allocate {
                handle: handle.clone(),
                offset: 0,
                length: 8 * 4096,
            },
        )
        .await
        .unwrap();
        // Interleave progress/data and two shards. Reverse observation order;
        // physical completion is independent of awaiting the returned future.
        let mut writes = Vec::new();
        for block in 0..8 {
            let class = if block % 3 == 0 {
                Class::Progress
            } else {
                Class::Data
            };
            writes.push(
                clients[block % 2]
                    .submit(
                        class,
                        write(&handle, (block * 4096) as u64, block as u8 + 1),
                    )
                    .unwrap(),
            );
        }
        for result in writes.into_iter().rev() {
            assert!(matches!(*result.await.unwrap(), Outcome::Written(4096)));
        }
        run(
            &mut clients[0],
            Operation::Sync {
                handle: handle.clone(),
                mode: SyncMode::Data,
            },
        )
        .await
        .unwrap();
        run(&mut clients[0], Operation::Close { handle })
            .await
            .unwrap();
        let buffered = open(&mut clients[1], &path, OpenMode::Read, false, false).await;
        let result = run(
            &mut clients[1],
            Operation::Read {
                handle: buffered,
                offset: 0,
                length: 8 * 4096,
            },
        )
        .await
        .unwrap();
        let Outcome::Read(bytes) = &*result else {
            panic!("read result")
        };
        for block in 0..8 {
            assert_eq!(
                &bytes.as_slice()[block * 4096..][..4096],
                &[block as u8 + 1; 4096]
            );
        }
        // Results may outlive shutdown; physical handles and kernel state may not.
        backend.shutdown().await;
        assert_eq!(backend.admission().used(1, Class::Data).operations, 1);
        drop(result);
        assert_eq!(backend.admission().used(1, Class::Data), Quota::default());
    }
}

#[tokio::test]
async fn canceled_writes_still_finish_before_shutdown_and_release_file_locks() {
    let root = tempfile::tempdir().unwrap();
    let (backend, mut clients) = Aio::new(config(1)).unwrap();
    let path = root.path().join("data");
    let handle = open(&mut clients[0], &path, OpenMode::CreateNew, true, true).await;
    let lock_path = root.path().join("group.lock");
    let lock = open(
        &mut clients[0],
        &lock_path,
        OpenMode::CreateNew,
        false,
        false,
    )
    .await;
    run(
        &mut clients[0],
        Operation::LockExclusive {
            handle: lock.clone(),
        },
    )
    .await
    .unwrap();
    for block in 0..8 {
        drop(
            clients[block % 2]
                .submit(
                    Class::Data,
                    Operation::Protected {
                        operation: Box::new(write(&handle, (block * 4096) as u64, block as u8 + 1)),
                        handles: vec![lock.clone()],
                    },
                )
                .unwrap(),
        );
    }
    drop(lock);
    // Keep the application's opaque handle alive across shutdown.
    backend.shutdown().await;
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 8 * 4096);
    let bytes = std::fs::read(&path).unwrap();
    for block in 0..8 {
        assert_eq!(&bytes[block * 4096..][..4096], &[block as u8 + 1; 4096]);
    }
    std::fs::File::open(lock_path).unwrap().try_lock().unwrap();
    for shard in 0..2 {
        assert_eq!(
            backend.admission().used(shard, Class::Data),
            Quota::default()
        );
    }
    assert_eq!(
        clients[0]
            .submit(Class::Data, write(&handle, 0, 0))
            .unwrap_err()
            .error
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn idle_wakeups_rejection_and_buffered_fallback_keep_driver_live() {
    let root = tempfile::tempdir().unwrap();
    let (backend, mut clients) = Aio::new(config(1)).unwrap();
    let path = root.path().join("data");
    let handle = open(&mut clients[0], &path, OpenMode::CreateNew, true, true).await;
    let empty = Operation::Write {
        handle: handle.clone(),
        offset: 0,
        data: WriteBuffer::from_vec(Vec::new()),
    };
    assert!(matches!(
        *run(&mut clients[0], empty).await.unwrap(),
        Outcome::Written(0)
    ));
    for _ in 0..32 {
        assert_eq!(
            run(&mut clients[0], write(&handle, 1, 2))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(matches!(
            *run(&mut clients[0], write(&handle, 0, 3)).await.unwrap(),
            Outcome::Written(4096)
        ));
    }
    let buffered = open(&mut clients[1], &path, OpenMode::ReadWrite, false, false).await;
    let result = run(
        &mut clients[1],
        Operation::Write {
            handle: buffered,
            offset: 4096,
            data: WriteBuffer::from_vec(b"abc".to_vec()),
        },
    )
    .await
    .unwrap();
    assert!(matches!(*result, Outcome::Written(3)));
    let readonly = open(&mut clients[0], &path, OpenMode::Read, true, false).await;
    assert_eq!(
        run(&mut clients[0], write(&readonly, 0, 7))
            .await
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EBADF)
    );
    backend.shutdown().await;
    assert_eq!(&std::fs::read(path).unwrap()[4096..], b"abc");
}
