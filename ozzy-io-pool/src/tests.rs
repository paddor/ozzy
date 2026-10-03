// Detailed lifetime and execution-order tests below use real files on explicit
// workers. Gates control execution rather than relying on disk timing.

use super::*;
use ozzy_io::{Completed, FileKind, Handle, OpenMode, Outcome, Quota, SyncMode, WriteBuffer};
use std::path::Path;
use std::sync::Condvar;

mod queues;
mod startup;

#[derive(Debug)]
pub(super) struct Gate {
    filter: fn(&Operation) -> bool,
    state: Mutex<GateState>,
    wake: Condvar,
    entered: Notify,
}

#[derive(Debug, Default)]
struct GateState {
    blocked: usize,
    released: bool,
    threads: Vec<String>,
}

impl Gate {
    fn new(filter: fn(&Operation) -> bool) -> Arc<Self> {
        Arc::new(Self {
            filter,
            state: Mutex::default(),
            wake: Condvar::new(),
            entered: Notify::new(),
        })
    }

    pub(super) fn enter(&self, operation: &Operation) {
        let mut state = self.state.lock().unwrap();
        state
            .threads
            .push(std::thread::current().name().unwrap_or("unnamed").into());
        if !(self.filter)(operation) {
            return;
        }
        state.blocked += 1;
        self.entered.notify_waiters();
        while !state.released {
            state = self.wake.wait(state).unwrap();
        }
    }

    async fn wait(&self) {
        loop {
            let entered = self.entered.notified();
            tokio::pin!(entered);
            entered.as_mut().enable();
            if self.state.lock().unwrap().blocked > 0 {
                return;
            }
            entered.await;
        }
    }

    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.wake.notify_all();
    }
}

fn config() -> Config {
    Config {
        threads: 2,
        max_inflight: 2,
        handles: 8,
        limits: Limits {
            shards: 2,
            data: Quota {
                operations: 4,
                bytes: 128 * 1024,
            },
            progress: Quota {
                operations: 4,
                bytes: 128 * 1024,
            },
        },
    }
}

async fn perform(client: &mut Client, operation: Operation) -> io::Result<Completed> {
    client.submit(Class::Data, operation).unwrap().await
}

async fn open(client: &mut Client, path: &Path, mode: OpenMode) -> Handle {
    let result = perform(
        client,
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

fn write(handle: &Handle, offset: u64, bytes: &[u8]) -> Operation {
    Operation::Write {
        handle: handle.clone(),
        offset,
        data: WriteBuffer::from_vec(bytes.to_vec()),
    }
}

fn install(pool: &Pool, filter: fn(&Operation) -> bool) -> Arc<Gate> {
    let gate = Gate::new(filter);
    *pool.shared.gate.lock().unwrap() = Some(gate.clone());
    gate
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One ordered file/directory publication sequence.
async fn file_and_directory_operations_run_on_explicit_workers() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut clients) = Pool::new(config()).unwrap();
    let gate = install(&pool, |_| false);
    let client = &mut clients[0];
    let root = temp.path().join("partition");
    perform(client, Operation::CreateDirectory { path: root.clone() })
        .await
        .unwrap();
    let directory = perform(client, Operation::OpenDirectory { path: root.clone() })
        .await
        .unwrap();
    let Outcome::Opened(directory) = &*directory else {
        panic!("directory handle")
    };
    let handle = open(client, &root.join("new"), OpenMode::CreateNew).await;
    assert!(matches!(
        *perform(client, write(&handle, 0, b"abc")).await.unwrap(),
        Outcome::Written(3)
    ));
    perform(
        client,
        Operation::SetLength {
            handle: handle.clone(),
            length: 10,
        },
    )
    .await
    .unwrap();
    let read = perform(
        client,
        Operation::Read {
            handle: handle.clone(),
            offset: 1,
            length: 64,
        },
    )
    .await
    .unwrap();
    let Outcome::Read(bytes) = &*read else {
        panic!("read response")
    };
    assert_eq!(bytes.as_slice(), b"bc\0\0\0\0\0\0\0");
    drop(read);
    let metadata = perform(
        client,
        Operation::Metadata {
            handle: handle.clone(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        *metadata,
        Outcome::Metadata(ozzy_io::Metadata {
            kind: FileKind::File,
            length: 10
        })
    ));
    drop(metadata);
    perform(
        client,
        Operation::Sync {
            handle: handle.clone(),
            mode: SyncMode::All,
        },
    )
    .await
    .unwrap();
    perform(
        client,
        Operation::HardLink {
            source: root.join("new"),
            destination: root.join("copy"),
        },
    )
    .await
    .unwrap();
    perform(
        client,
        Operation::Rename {
            source: root.join("new"),
            destination: root.join("current"),
        },
    )
    .await
    .unwrap();
    perform(
        client,
        Operation::Sync {
            handle: directory.clone(),
            mode: SyncMode::All,
        },
    )
    .await
    .unwrap();
    let listing = perform(
        client,
        Operation::ReadDirectory {
            path: root.clone(),
            max_entries: 4,
            max_name_bytes: 128,
        },
    )
    .await
    .unwrap();
    let Outcome::Directory(entries) = &*listing else {
        panic!("directory response")
    };
    assert_eq!(
        entries
            .iter()
            .map(|e| e.name.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["copy", "current"]
    );
    drop(listing);
    perform(
        client,
        Operation::Close {
            handle: handle.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        perform(client, Operation::Metadata { handle })
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    for name in ["copy", "current"] {
        perform(
            client,
            Operation::RemoveFile {
                path: root.join(name),
            },
        )
        .await
        .unwrap();
    }
    perform(client, Operation::RemoveDirectory { path: root })
        .await
        .unwrap();
    pool.shutdown().await;
    assert!(
        gate.state
            .lock()
            .unwrap()
            .threads
            .iter()
            .all(|name| name.starts_with("ozzy_io-"))
    );
}

#[tokio::test]
async fn blocked_write_keeps_other_shard_and_progress_worker_runnable() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut clients) = Pool::new(config()).unwrap();
    let first = open(
        &mut clients[0],
        &temp.path().join("first"),
        OpenMode::CreateNew,
    )
    .await;
    let second = open(
        &mut clients[1],
        &temp.path().join("second"),
        OpenMode::CreateNew,
    )
    .await;
    let gate = install(&pool, |op| matches!(op, Operation::Write { offset: 0, .. }));
    let blocked = clients[0]
        .submit(Class::Data, write(&first, 0, b"blocked"))
        .unwrap();
    gate.wait().await;
    let healthy = clients[1]
        .submit(Class::Data, write(&second, 1, b"healthy"))
        .unwrap();
    assert!(matches!(*healthy.await.unwrap(), Outcome::Written(7)));
    let progress = clients[0]
        .submit(
            Class::Progress,
            Operation::Metadata {
                handle: first.clone(),
            },
        )
        .unwrap();
    assert!(matches!(*progress.await.unwrap(), Outcome::Metadata(_)));
    gate.release();
    assert!(matches!(*blocked.await.unwrap(), Outcome::Written(7)));
    pool.shutdown().await;
}

#[tokio::test]
async fn canceled_running_and_queued_jobs_still_write_and_shutdown_drains_them() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("data");
    let (pool, mut clients) = Pool::new(Config {
        threads: 1,
        ..config()
    })
    .unwrap();
    let handle = open(&mut clients[0], &path, OpenMode::CreateNew).await;
    let gate = install(&pool, |op| matches!(op, Operation::Write { offset: 0, .. }));
    let first = clients[0]
        .submit(Class::Data, write(&handle, 0, b"abc"))
        .unwrap();
    gate.wait().await;
    let second = clients[0]
        .submit(Class::Data, write(&handle, 3, b"def"))
        .unwrap();
    drop((first, second));
    assert_eq!(pool.admission().used(0, Class::Data).operations, 2);
    assert_eq!(
        clients[0]
            .submit(Class::Data, write(&handle, 6, b"no"))
            .unwrap_err()
            .error
            .kind(),
        io::ErrorKind::WouldBlock
    );
    let shutdown = pool.shutdown();
    tokio::pin!(shutdown);
    assert!(futures::poll!(&mut shutdown).is_pending());
    assert_eq!(
        clients[0]
            .submit(
                Class::Progress,
                Operation::Metadata {
                    handle: handle.clone()
                }
            )
            .unwrap_err()
            .error
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    drop(handle);
    gate.release();
    shutdown.await;
    assert_eq!(std::fs::read(path).unwrap(), b"abcdef");
    assert_eq!(pool.admission().used(0, Class::Data), Quota::default());
}

#[tokio::test]
async fn canceled_open_cannot_leak_a_handle_slot() {
    let temp = tempfile::tempdir().unwrap();
    let mut limits = config().limits;
    limits.data.operations = 2;
    let (pool, mut clients) = Pool::new(Config {
        handles: 2,
        limits,
        ..config()
    })
    .unwrap();
    let gate = install(&pool, |op| matches!(op, Operation::Open { .. }));
    let opening = clients[0]
        .submit(
            Class::Data,
            Operation::Open {
                path: temp.path().join("first"),
                mode: OpenMode::CreateNew,
                direct: false,
                data_sync: false,
            },
        )
        .unwrap();
    gate.wait().await;
    drop(opening);
    gate.release();
    pool.admission().ready(0, Class::Data, 0).await.unwrap();
    let next = open(
        &mut clients[0],
        &temp.path().join("next"),
        OpenMode::CreateNew,
    )
    .await;
    drop(next);
    pool.shutdown().await;
    assert_eq!(pool.admission().used(0, Class::Data), Quota::default());
    assert_eq!(
        std::fs::metadata(temp.path().join("first")).unwrap().len(),
        0
    );
}

#[tokio::test]
async fn completed_reads_keep_capacity_until_result_drop() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut clients) = Pool::new(config()).unwrap();
    let handle = open(
        &mut clients[0],
        &temp.path().join("data"),
        OpenMode::CreateNew,
    )
    .await;
    let one = perform(
        &mut clients[0],
        Operation::Read {
            handle: handle.clone(),
            offset: 0,
            length: 64,
        },
    )
    .await
    .unwrap();
    let two = clients[0]
        .submit(
            Class::Data,
            Operation::Read {
                handle: handle.clone(),
                offset: 0,
                length: 32,
            },
        )
        .unwrap();
    assert_eq!(
        clients[0]
            .submit(
                Class::Data,
                Operation::Metadata {
                    handle: handle.clone()
                }
            )
            .unwrap_err()
            .error
            .kind(),
        io::ErrorKind::WouldBlock
    );
    let two = two.await.unwrap();
    assert_eq!(
        pool.admission().used(0, Class::Data),
        Quota {
            operations: 2,
            bytes: 96
        }
    );
    // Shutdown waits for physical work, not for application payload lifetimes.
    pool.shutdown().await;
    drop((one, two));
    assert_eq!(pool.admission().used(0, Class::Data), Quota::default());
}

#[tokio::test]
async fn backend_identity_prevents_aliasing_and_close_invalidates_all_clones() {
    let temp = tempfile::tempdir().unwrap();
    let (a, mut first) = Pool::new(config()).unwrap();
    let (b, mut second) = Pool::new(config()).unwrap();
    let handle = open(
        &mut first[0],
        &temp.path().join("first"),
        OpenMode::CreateNew,
    )
    .await;
    let _other = open(
        &mut second[0],
        &temp.path().join("second"),
        OpenMode::CreateNew,
    )
    .await;
    let error = perform(
        &mut second[0],
        Operation::Metadata {
            handle: handle.clone(),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    perform(
        &mut first[0],
        Operation::Close {
            handle: handle.clone(),
        },
    )
    .await
    .unwrap();
    let error = perform(&mut first[0], write(&handle, 0, b"no"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn failed_open_and_last_handle_drop_release_capacity_and_file_lock() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("data");
    let (pool, mut clients) = Pool::new(Config {
        handles: 2,
        ..config()
    })
    .unwrap();
    let error = perform(
        &mut clients[0],
        Operation::Open {
            path: path.clone(),
            mode: OpenMode::Read,
            direct: false,
            data_sync: false,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    let mut handle = open(&mut clients[0], &path, OpenMode::CreateNew).await;
    perform(
        &mut clients[0],
        Operation::LockExclusive {
            handle: handle.clone(),
        },
    )
    .await
    .unwrap();
    let error = perform(
        &mut clients[0],
        Operation::Open {
            path: path.clone(),
            mode: OpenMode::ReadWrite,
            direct: false,
            data_sync: false,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    for _ in 0..32 {
        drop(handle);
        handle = open(&mut clients[0], &path, OpenMode::ReadWrite).await;
        perform(
            &mut clients[0],
            Operation::LockExclusive {
                handle: handle.clone(),
            },
        )
        .await
        .unwrap();
    }
    // Shutdown closes even handles retained by application code.
    pool.shutdown().await;
    let file = std::fs::File::open(path).unwrap();
    file.try_lock().unwrap();
    drop(handle);
}

#[tokio::test]
async fn oversized_listings_and_offset_overflow_are_refused() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut clients) = Pool::new(config()).unwrap();
    let handle = open(
        &mut clients[0],
        &temp.path().join("data"),
        OpenMode::CreateNew,
    )
    .await;
    let error = perform(
        &mut clients[0],
        Operation::ReadDirectory {
            path: temp.path().into(),
            max_entries: 0,
            max_name_bytes: 0,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    let error = clients[0]
        .submit(Class::Data, write(&handle, u64::MAX, b"xx"))
        .unwrap_err();
    assert_eq!(error.error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(pool.admission().used(0, Class::Data), Quota::default());
    pool.shutdown().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn special_files_and_symlinks_are_rejected_without_blocking() {
    let temp = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink("/dev/null", temp.path().join("link")).unwrap();
    let (pool, mut clients) = Pool::new(config()).unwrap();
    for path in [temp.path().join("link"), "/dev/null".into()] {
        assert!(
            perform(
                &mut clients[0],
                Operation::Open {
                    path,
                    mode: OpenMode::Read,
                    direct: false,
                    data_sync: false
                }
            )
            .await
            .is_err()
        );
    }
    pool.shutdown().await;
}

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

#[tokio::test]
async fn dropping_owner_requests_drain_without_waiting_on_blocked_io() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("data");
    let (pool, mut clients) = Pool::new(config()).unwrap();
    let handle = open(&mut clients[0], &path, OpenMode::CreateNew).await;
    let gate = install(&pool, |op| matches!(op, Operation::Write { .. }));
    let written = clients[0]
        .submit(Class::Data, write(&handle, 0, b"still runs"))
        .unwrap();
    gate.wait().await;
    drop(pool);
    assert_eq!(
        clients[0]
            .submit(Class::Progress, Operation::Metadata { handle })
            .unwrap_err()
            .error
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    gate.release();
    assert!(matches!(*written.await.unwrap(), Outcome::Written(10)));
    // Wait for the detached drain before the temporary directory is removed.
    let monitor = Pool {
        shared: clients[0].shared.clone(),
        next_thread: AtomicUsize::new(0),
        threads: Vec::new(),
    };
    monitor.shutdown().await;
    assert_eq!(std::fs::read(path).unwrap(), b"still runs");
}

#[tokio::test]
async fn execution_panic_fences_device_and_still_closes_held_files() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("data");
    let (pool, mut clients) = Pool::new(config()).unwrap();
    let handle = open(&mut clients[0], &path, OpenMode::CreateNew).await;
    perform(
        &mut clients[0],
        Operation::LockExclusive {
            handle: handle.clone(),
        },
    )
    .await
    .unwrap();
    install(&pool, |_| panic!("injected execution panic"));
    let response = clients[0]
        .submit(
            Class::Data,
            Operation::Metadata {
                handle: handle.clone(),
            },
        )
        .unwrap();
    assert_eq!(response.await.unwrap_err().kind(), io::ErrorKind::Other);
    assert_eq!(
        clients[0]
            .submit(Class::Data, Operation::Metadata { handle })
            .unwrap_err()
            .error
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    pool.shutdown().await;
    std::fs::File::open(path).unwrap().try_lock().unwrap();
}

#[tokio::test]
async fn protected_file_job_keeps_separate_group_lock_after_owner_and_observer_drop() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut clients) = Pool::new(config()).unwrap();
    let lock_path = temp.path().join("group.lock");
    let lock = open(&mut clients[0], &lock_path, OpenMode::CreateNew).await;
    perform(
        &mut clients[0],
        Operation::LockExclusive {
            handle: lock.clone(),
        },
    )
    .await
    .unwrap();
    let data = open(
        &mut clients[0],
        &temp.path().join("data"),
        OpenMode::CreateNew,
    )
    .await;
    let gate = install(&pool, |operation| {
        matches!(operation.unprotected(), Operation::Write { .. })
    });
    let pending = clients[0]
        .submit(
            Class::Data,
            Operation::Protected {
                operation: Box::new(write(&data, 0, b"metadata")),
                handles: vec![lock.clone()],
            },
        )
        .unwrap();
    gate.wait().await;
    drop((pending, lock, data));
    assert!(matches!(
        std::fs::File::open(&lock_path).unwrap().try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    gate.release();
    pool.shutdown().await;
    std::fs::File::open(lock_path).unwrap().try_lock().unwrap();
    assert_eq!(
        std::fs::read(temp.path().join("data")).unwrap(),
        b"metadata"
    );
}

#[cfg(target_os = "linux")]
struct HeldDirect(Arc<Gate>);

#[cfg(target_os = "linux")]
struct Unpark(std::thread::Thread);

#[cfg(target_os = "linux")]
impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

#[cfg(target_os = "linux")]
impl direct::Worker for HeldDirect {
    fn run(self: Box<Self>, queue: &mut direct::Queue) -> io::Result<()> {
        assert_eq!(std::thread::current().name(), Some("ozzy_io-direct"));
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        loop {
            queue.register(&waker);
            for class in [Class::Progress, Class::Data] {
                while let Some(write) = queue.try_recv(class) {
                    self.0.enter(write.job.operation.as_ref().unwrap());
                    let length = write.data().len();
                    write.finish(Ok(length));
                }
            }
            if queue.drained() {
                return Ok(());
            }
            std::thread::park();
        }
    }
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
    assert_eq!(gate.state.lock().unwrap().threads, ["ozzy_io-direct"]);
}

struct FailedDirect(u8);

impl direct::Worker for FailedDirect {
    fn run(self: Box<Self>, _: &mut direct::Queue) -> io::Result<()> {
        match self.0 {
            0 => Err(io::Error::other("injected setup failure")),
            1 => panic!("injected driver panic"),
            _ => Ok(()), // Premature successful return is still a failure.
        }
    }
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

/// Runs when the last worker releases the initializer, after that worker
/// reported the end of the drain.
struct Lingering(Arc<std::sync::atomic::AtomicBool>);

impl Drop for Lingering {
    fn drop(&mut self) {
        std::thread::sleep(std::time::Duration::from_millis(50));
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

#[tokio::test]
async fn join_waits_for_workers_that_outlive_their_completion_report() {
    let ended = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lingering = Lingering(ended.clone());
    let (pool, clients) = Pool::with_initializer(
        config(),
        Arc::new(move |_| {
            let _held = &lingering;
            Ok(())
        }),
    )
    .unwrap();
    drop(clients);
    pool.shutdown().await;
    pool.join();
    assert!(ended.load(std::sync::atomic::Ordering::Acquire));
    // A second call finds no thread left to wait for.
    pool.join();
}
