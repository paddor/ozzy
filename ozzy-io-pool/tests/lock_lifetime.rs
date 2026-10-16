//! A child process between fork and exec must not extend a closed file's lock.
#![cfg(target_os = "linux")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "test code"
)]

use ozzy_io::{Backend, Class, OpenMode, Operation, Outcome};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::thread::JoinHandle;
use std::time::Duration;

/// An unrelated child that inherited every descriptor and has not exec'd yet.
struct HeldFork {
    gate: UnixStream,
    task: Option<JoinHandle<()>>,
}

impl HeldFork {
    fn start() -> Self {
        let (gate, child_gate) = UnixStream::pair().unwrap();
        gate.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.arg("--list").stdout(Stdio::null());
        // SAFETY: The post-fork closure only invokes async-signal-safe read/write
        // syscalls through rustix, on a socket allocated before fork. It neither
        // allocates nor locks. Parent release or EOF always releases the child.
        unsafe {
            command.pre_exec(move || {
                rustix::io::write(&child_gate, b"R")?;
                let mut release = [0];
                loop {
                    match rustix::io::read(&child_gate, &mut release) {
                        Err(rustix::io::Errno::INTR) => {}
                        result => return result.map(|_| ()).map_err(Into::into),
                    }
                }
            });
        }
        let task = std::thread::spawn(move || {
            let mut child = command.spawn().unwrap();
            assert!(child.wait().unwrap().success());
        });
        let mut held = Self {
            gate,
            task: Some(task),
        };
        let mut ready = [0];
        held.gate.read_exact(&mut ready).unwrap();
        assert_eq!(ready, *b"R");
        held
    }
}

impl Drop for HeldFork {
    fn drop(&mut self) {
        let _ = self.gate.write_all(b"G");
        if let Some(task) = self.task.take() {
            let result = task.join();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}

mod pool {
    mod stress {
        use super::super::*;
        use ozzy_io::{Limits, Quota};
        use ozzy_io_pool::{Config, Pool};

        #[tokio::test]
        async fn closed_lock_is_released_while_a_forked_child_holds_its_descriptor() {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("group.lock");
            let quota = Quota {
                operations: 4,
                bytes: 64 * 1024,
            };
            let (pool, mut clients) = Pool::new(Config {
                threads: 1,
                max_inflight: 1,
                handles: 2,
                limits: Limits {
                    shards: 1,
                    data: quota,
                    progress: quota,
                },
            })
            .unwrap();
            let client = &mut clients[0];
            let opened = client
                .submit(
                    Class::Data,
                    Operation::Open {
                        path: path.clone(),
                        mode: OpenMode::CreateNew,
                        direct: false,
                        data_sync: false,
                    },
                )
                .unwrap()
                .await
                .unwrap();
            let Outcome::Opened(handle) = &*opened else {
                panic!("open response")
            };
            let handle = handle.clone();
            drop(opened);
            let lock = Operation::LockExclusive {
                handle: handle.clone(),
            };
            client.submit(Class::Data, lock).unwrap().await.unwrap();
            let child = HeldFork::start();
            let close = Operation::Close { handle };
            client.submit(Class::Data, close).unwrap().await.unwrap();
            std::fs::File::open(&path).unwrap().try_lock().unwrap();
            drop(child);
            pool.shutdown().await;
        }
    }
}
