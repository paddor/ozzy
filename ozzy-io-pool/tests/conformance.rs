//! The same ordered file workload through real workers and controlled storage.
//! These are backend-contract checks, not broker power-loss qualification.

use ozzy_io::simulation::{self, Controller, Effect, Image, ImageLimits, Stage};
use ozzy_io::{
    Backend, Class, Completed, Handle, Limits, OpenMode, Operation, Outcome, Quota, SyncMode,
    WriteBuffer,
};
use ozzy_io_pool::{Config, Pool};
use std::{io, path::Path, task::Poll};

fn limits() -> Limits {
    Limits {
        shards: 1,
        data: Quota {
            operations: 8,
            bytes: 128 * 1024,
        },
        progress: Quota {
            operations: 4,
            bytes: 128 * 1024,
        },
    }
}

async fn run(backend: &mut impl Backend, operation: Operation) -> io::Result<Completed> {
    backend.submit(Class::Data, operation).unwrap().await
}

async fn open(backend: &mut impl Backend, path: &Path, mode: OpenMode) -> Handle {
    let result = run(
        backend,
        Operation::Open {
            path: path.into(),
            mode,
            direct: false,
            data_sync: false,
        },
    )
    .await
    .unwrap();
    let Outcome::Opened(handle) = &*result else {
        panic!("open response")
    };
    handle.clone()
}

#[derive(Debug, PartialEq, Eq)]
struct Observed {
    bytes: Vec<u8>,
    length: u64,
    names: Vec<std::ffi::OsString>,
    lock_conflict: io::ErrorKind,
    closed_handle: io::ErrorKind,
}

#[allow(clippy::too_many_lines)] // Keep the shared lifecycle sequence together.
async fn file_workload(backend: &mut impl Backend, root: &Path) -> Observed {
    let root = root.join("partition");
    run(backend, Operation::CreateDirectory { path: root.clone() })
        .await
        .unwrap();
    let opened = run(backend, Operation::OpenDirectory { path: root.clone() })
        .await
        .unwrap();
    let Outcome::Opened(directory) = &*opened else {
        panic!("directory handle")
    };
    let directory = directory.clone();
    drop(opened);
    let path = root.join("pending");
    let file = open(backend, &path, OpenMode::CreateNew).await;
    let alias = open(backend, &path, OpenMode::Read).await;
    run(
        backend,
        Operation::LockExclusive {
            handle: file.clone(),
        },
    )
    .await
    .unwrap();
    let lock_conflict = run(
        backend,
        Operation::LockExclusive {
            handle: alias.clone(),
        },
    )
    .await
    .unwrap_err()
    .kind();
    let data = WriteBuffer::shared(
        vec![
            bytes::Bytes::from_static(b"abc"),
            bytes::Bytes::from_static(b"def"),
        ],
        6,
    )
    .unwrap();
    let written = run(
        backend,
        Operation::Write {
            handle: file.clone(),
            offset: 0,
            data,
        },
    )
    .await
    .unwrap();
    assert!(matches!(*written, Outcome::Written(6)));
    drop(written);
    run(
        backend,
        Operation::SetLength {
            handle: file.clone(),
            length: 10,
        },
    )
    .await
    .unwrap();
    run(
        backend,
        Operation::Sync {
            handle: file.clone(),
            mode: SyncMode::Data,
        },
    )
    .await
    .unwrap();
    run(
        backend,
        Operation::HardLink {
            source: path.clone(),
            destination: root.join("backup"),
        },
    )
    .await
    .unwrap();
    run(
        backend,
        Operation::Rename {
            source: path,
            destination: root.join("current"),
        },
    )
    .await
    .unwrap();
    run(
        backend,
        Operation::Sync {
            handle: directory.clone(),
            mode: SyncMode::All,
        },
    )
    .await
    .unwrap();
    run(
        backend,
        Operation::Close {
            handle: file.clone(),
        },
    )
    .await
    .unwrap();
    let closed_handle = run(backend, Operation::Metadata { handle: file })
        .await
        .unwrap_err()
        .kind();
    run(
        backend,
        Operation::LockExclusive {
            handle: alias.clone(),
        },
    )
    .await
    .unwrap();
    let result = run(
        backend,
        Operation::Read {
            handle: alias.clone(),
            offset: 1,
            length: 64,
        },
    )
    .await
    .unwrap();
    let Outcome::Read(buffer) = &*result else {
        panic!("read response")
    };
    let bytes = buffer.as_slice().to_vec();
    drop(result);
    let result = run(
        backend,
        Operation::Metadata {
            handle: alias.clone(),
        },
    )
    .await
    .unwrap();
    let Outcome::Metadata(metadata) = &*result else {
        panic!("metadata")
    };
    let length = metadata.length;
    drop(result);
    let result = run(
        backend,
        Operation::ReadDirectory {
            path: root.clone(),
            max_entries: 4,
            max_name_bytes: 128,
        },
    )
    .await
    .unwrap();
    let Outcome::Directory(entries) = &*result else {
        panic!("directory")
    };
    let names = entries.iter().map(|entry| entry.name.clone()).collect();
    drop(result);
    for name in ["backup", "current"] {
        run(
            backend,
            Operation::RemoveFile {
                path: root.join(name),
            },
        )
        .await
        .unwrap();
    }
    run(backend, Operation::RemoveDirectory { path: root })
        .await
        .unwrap();
    run(backend, Operation::Close { handle: directory })
        .await
        .unwrap();
    run(backend, Operation::Close { handle: alias })
        .await
        .unwrap();
    Observed {
        bytes,
        length,
        names,
        lock_conflict,
        closed_handle,
    }
}

#[tokio::test]
async fn controlled_pool_and_aio_backends_obey_the_same_file_contract() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut clients) = Pool::new(Config {
        threads: 2,
        max_inflight: 2,
        handles: 8,
        limits: limits(),
    })
    .unwrap();
    let real = file_workload(&mut clients[0], temp.path()).await;
    pool.shutdown().await;

    #[cfg(target_os = "linux")]
    {
        let (aio, mut clients) = ozzy_io_aio::Aio::new(ozzy_io_aio::Config {
            depth: 1,
            pool: Config {
                threads: 2,
                max_inflight: 2,
                handles: 8,
                limits: limits(),
            },
        })
        .unwrap();
        assert_eq!(file_workload(&mut clients[0], temp.path()).await, real);
        aio.shutdown().await;
    }

    let (mut controller, mut clients) = Controller::new(
        simulation::Config {
            limits: limits(),
            handles: 8,
            image: ImageLimits {
                nodes: 64,
                directory_entries: 128,
                file_bytes: 1024 * 1024,
                total_bytes: 4 * 1024 * 1024,
            },
            trace_events: 512,
        },
        Image::default(),
    )
    .unwrap();
    let workload = file_workload(&mut clients[0], Path::new("/"));
    tokio::pin!(workload);
    let modeled = loop {
        if let Poll::Ready(result) = futures::poll!(&mut workload) {
            break result;
        }
        let jobs = controller.jobs();
        assert!(
            !jobs.is_empty(),
            "file workload stalled without pending I/O"
        );
        for (id, stage) in jobs {
            assert_eq!(stage, Stage::Queued);
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    };
    controller.begin_shutdown().unwrap().wait().await;
    assert_eq!(modeled, real);
    assert_eq!(real.bytes, b"bcdef\0\0\0\0");
    assert_eq!(real.length, 10);
    assert_eq!(real.lock_conflict, io::ErrorKind::WouldBlock);
    assert_eq!(real.closed_handle, io::ErrorKind::NotFound);
}
