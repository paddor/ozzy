use super::{FailedDirect, config, install, open, write};
#[cfg(target_os = "linux")]
use super::{Gate, HeldDirect};
use crate::{Class, Config, Initializer, Pool, Worker};
use ozzy_io::{Backend, OpenMode, Operation, Outcome};
use std::{
    io,
    sync::{Arc, Mutex, atomic::Ordering},
};

#[tokio::test]
async fn placement_runs_on_every_worker_before_startup_returns() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let caller = std::thread::current().id();
    let initialize: Initializer = {
        let seen = seen.clone();
        Arc::new(move |role| {
            assert_ne!(std::thread::current().id(), caller);
            seen.lock().unwrap().push(role);
            Ok(())
        })
    };
    let (pool, _) =
        Pool::with_direct_worker_and_initializer(config(), FailedDirect(0), initialize).unwrap();
    let mut roles = seen.lock().unwrap().clone();
    roles.sort();
    assert_eq!(
        roles,
        [
            Worker::Data(0),
            Worker::Data(1),
            Worker::Progress,
            Worker::Progress,
            Worker::Direct
        ]
    );
    pool.shutdown().await;
}

#[tokio::test]
async fn independent_progress_jobs_run_while_another_is_blocked() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut clients) = Pool::new(config()).unwrap();
    let handle = open(
        &mut clients[0],
        &temp.path().join("metadata"),
        OpenMode::CreateNew,
    )
    .await;
    let gate = install(&pool, |op| matches!(op, Operation::Metadata { .. }));
    let first = clients[0]
        .submit(
            Class::Progress,
            Operation::Metadata {
                handle: handle.clone(),
            },
        )
        .unwrap();
    gate.wait().await;
    let second = clients[1]
        .submit(Class::Progress, Operation::Metadata { handle })
        .unwrap();
    let second_ready = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let entered = gate.entered.notified();
            tokio::pin!(entered);
            entered.as_mut().enable();
            if gate.state.lock().unwrap().blocked == 2 {
                break;
            }
            entered.await;
        }
    })
    .await;
    gate.release();
    second_ready.expect("second progress worker was blocked by the first");
    first.await.unwrap();
    second.await.unwrap();
    pool.shutdown().await;
}

#[test]
fn initializer_errors_and_panics_are_startup_failures_for_every_role() {
    for failed in [
        Worker::Data(0),
        Worker::Data(1),
        Worker::Progress,
        Worker::Direct,
    ] {
        for panic in [false, true] {
            let initialize = Arc::new(move |role| {
                if role != failed {
                    return Ok(());
                }
                assert!(!panic, "injected placement panic");
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected affinity failure",
                ))
            });
            let error =
                Pool::with_direct_worker_and_initializer(config(), FailedDirect(0), initialize)
                    .unwrap_err();
            assert_eq!(
                error.kind(),
                if panic {
                    io::ErrorKind::Other
                } else {
                    io::ErrorKind::PermissionDenied
                }
            );
        }
    }
}

#[tokio::test]
async fn physical_job_limit_leaves_reserved_progress_runnable() {
    let temp = tempfile::tempdir().unwrap();
    let (pool, mut clients) = Pool::new(Config {
        max_inflight: 1,
        ..config()
    })
    .unwrap();
    let handle = open(
        &mut clients[0],
        &temp.path().join("data"),
        OpenMode::CreateNew,
    )
    .await;
    let gate = install(&pool, |op| matches!(op, Operation::Write { .. }));
    let blocked = clients[0]
        .submit(Class::Data, write(&handle, 0, b"first"))
        .unwrap();
    gate.wait().await;
    let queued = clients[1]
        .submit(Class::Data, write(&handle, 5, b"second"))
        .unwrap();
    let metadata = clients[0]
        .submit(Class::Progress, Operation::Metadata { handle })
        .unwrap();
    assert!(matches!(*metadata.await.unwrap(), Outcome::Metadata(_)));
    assert_eq!(gate.state.lock().unwrap().blocked, 1);
    assert_eq!(pool.shared.executing.load(Ordering::Acquire), 1);
    drop(blocked); // Losing the observer cannot release physical capacity.
    assert_eq!(pool.shared.executing.load(Ordering::Acquire), 1);
    gate.release();
    queued.await.unwrap();
    pool.shutdown().await;
    assert_eq!(
        std::fs::read(temp.path().join("data")).unwrap(),
        b"firstsecond"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn physical_limit_survives_direct_driver_handoff() {
    let temp = tempfile::tempdir().unwrap();
    let gate = Gate::new(|_| true);
    let (pool, mut clients) = Pool::with_direct_worker(
        Config {
            max_inflight: 1,
            ..config()
        },
        HeldDirect(gate.clone()),
    )
    .unwrap();
    let opening = clients[0]
        .submit(
            Class::Data,
            Operation::Open {
                path: temp.path().join("direct"),
                mode: OpenMode::CreateNew,
                direct: true,
                data_sync: false,
            },
        )
        .unwrap()
        .await
        .unwrap();
    let Outcome::Opened(handle) = &*opening else {
        panic!("handle")
    };
    let handle = handle.clone();
    drop(opening);
    let blocked = clients[0]
        .submit(Class::Data, write(&handle, 0, &[1; 4096]))
        .unwrap();
    gate.wait().await;
    drop(blocked);
    let mut queued = clients[1]
        .submit(
            Class::Data,
            Operation::Metadata {
                handle: handle.clone(),
            },
        )
        .unwrap();
    clients[0]
        .submit(Class::Progress, Operation::Metadata { handle })
        .unwrap()
        .await
        .unwrap();
    assert_eq!(pool.shared.executing.load(Ordering::Acquire), 1);
    assert!(futures::poll!(&mut queued).is_pending());
    gate.release();
    queued.await.unwrap();
    pool.shutdown().await;
}
