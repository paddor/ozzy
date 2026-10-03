// Fault schedules are driven explicitly, with no wall-clock sleeps.

use super::*;
use crate::{Completed, Handle, OpenMode, Quota, SyncMode, WriteBuffer};
use std::future::Future;

fn config() -> Config {
    Config {
        limits: Limits {
            shards: 2,
            data: Quota {
                operations: 16,
                bytes: 512 * 1024,
            },
            progress: Quota {
                operations: 8,
                bytes: 512 * 1024,
            },
        },
        handles: 16,
        image: ImageLimits {
            nodes: 128,
            directory_entries: 512,
            file_bytes: 1024 * 1024,
            total_bytes: 4 * 1024 * 1024,
        },
        trace_events: 10_000,
    }
}

#[test]
fn local_partition_tasks_share_one_lane_without_holding_it_across_awaits() {
    let (mut controller, mut clients) = Controller::new(config(), Image::default()).unwrap();
    let lane = crate::Local::new(clients.remove(0));
    let other = lane.clone();
    let mut first = std::pin::pin!(lane.execute(
        Class::Data,
        Operation::CreateDirectory {
            path: "/first".into()
        }
    ));
    let mut second = std::pin::pin!(other.execute(
        Class::Data,
        Operation::CreateDirectory {
            path: "/second".into()
        }
    ));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    let jobs = controller.jobs();
    assert_eq!(jobs.len(), 2);
    assert_eq!(lane.admission().used(0, Class::Data).operations, 2);
    controller.execute(jobs[1].0, Effect::Normal).unwrap();
    controller.deliver(jobs[1].0).unwrap();
    assert!(matches!(
        second.as_mut().poll(&mut cx),
        std::task::Poll::Ready(Ok(_))
    ));
    assert!(first.as_mut().poll(&mut cx).is_pending());
    controller.execute(jobs[0].0, Effect::Normal).unwrap();
    controller.deliver(jobs[0].0).unwrap();
    assert!(matches!(
        first.as_mut().poll(&mut cx),
        std::task::Poll::Ready(Ok(_))
    ));
    assert!(
        controller
            .image()
            .exists(std::path::Path::new("/first"), false)
    );
    assert!(
        controller
            .image()
            .exists(std::path::Path::new("/second"), false)
    );
}

struct Harness {
    controller: Controller,
    clients: Vec<Client>,
}

impl Harness {
    fn new() -> Self {
        Self::from_image(Image::default())
    }
    fn from_image(image: Image) -> Self {
        let (controller, clients) = Controller::new(config(), image).unwrap();
        Self {
            controller,
            clients,
        }
    }

    fn submit(&mut self, operation: Operation) -> (JobId, Completion) {
        let completion = self.clients[0].submit(Class::Data, operation).unwrap();
        let id = self.controller.jobs().last().unwrap().0;
        (id, completion)
    }

    async fn perform(&mut self, operation: Operation, effect: Effect) -> io::Result<Completed> {
        let (id, completion) = self.submit(operation);
        self.controller.execute(id, effect).unwrap();
        self.controller.deliver(id).unwrap();
        completion.await
    }

    async fn run(&mut self, operation: Operation) -> Completed {
        self.perform(operation, Effect::Normal).await.unwrap()
    }

    async fn open(&mut self, path: &str, mode: OpenMode) -> Handle {
        let value = self
            .run(Operation::Open {
                path: path.into(),
                mode,
                direct: false,
                data_sync: false,
            })
            .await;
        let Outcome::Opened(handle) = &*value else {
            panic!("opened file")
        };
        handle.clone()
    }

    async fn directory(&mut self, path: &str) -> Handle {
        let value = self
            .run(Operation::OpenDirectory { path: path.into() })
            .await;
        let Outcome::Opened(handle) = &*value else {
            panic!("opened directory")
        };
        handle.clone()
    }

    async fn sync(&mut self, handle: &Handle) {
        self.run(Operation::Sync {
            handle: handle.clone(),
            mode: SyncMode::All,
        })
        .await;
    }

    async fn established(&mut self, bytes: &[u8]) -> Handle {
        let file = self.open("/data", OpenMode::CreateNew).await;
        self.run(write(&file, 0, bytes)).await;
        self.sync(&file).await;
        let root = self.directory("/").await;
        self.sync(&root).await;
        file
    }
}

fn write(handle: &Handle, offset: u64, bytes: &[u8]) -> Operation {
    Operation::Write {
        handle: handle.clone(),
        offset,
        data: WriteBuffer::from_vec(bytes.to_vec()),
    }
}

#[tokio::test]
async fn file_and_directory_barriers_are_independent() {
    for sync_file in [false, true] {
        for sync_directory in [false, true] {
            let mut harness = Harness::new();
            let file = harness.open("/data", OpenMode::CreateNew).await;
            harness.run(write(&file, 0, b"payload")).await;
            if sync_file {
                harness.sync(&file).await;
            }
            if sync_directory {
                let root = harness.directory("/").await;
                harness.sync(&root).await;
            }
            let (image, _) = harness.controller.crash(true).unwrap();
            assert_eq!(image.exists(Path::new("/data"), false), sync_directory);
            if sync_directory {
                assert_eq!(
                    image.bytes(Path::new("/data"), false).unwrap(),
                    if sync_file {
                        b"payload".as_slice()
                    } else {
                        &[]
                    }
                );
            }
        }
    }
}

#[tokio::test]
async fn crash_between_physical_durability_and_observation_keeps_bytes_not_success() {
    let mut harness = Harness::new();
    let file = harness.established(b"old").await;
    let (write_id, mut written) = harness.submit(write(&file, 0, b"new"));
    harness
        .controller
        .execute(write_id, Effect::Normal)
        .unwrap();
    let (sync_id, mut synced) = harness.submit(Operation::Sync {
        handle: file,
        mode: SyncMode::Data,
    });
    harness.controller.execute(sync_id, Effect::Normal).unwrap();
    assert!(futures::poll!(&mut written).is_pending());
    assert!(futures::poll!(&mut synced).is_pending());
    let (image, trace) = harness.controller.crash(true).unwrap();
    assert_eq!(image.bytes(Path::new("/data"), false).unwrap(), b"new");
    assert_eq!(written.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(synced.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert!(!trace.contains(&Event::Deliver(sync_id)));
}

#[tokio::test]
async fn execution_order_and_delivery_order_are_independent() {
    let mut harness = Harness::new();
    let file = harness.established(b"old").await;
    let (first_id, mut first) = harness.submit(write(&file, 0, b"one"));
    let (read_id, read) = harness.submit(Operation::Read {
        handle: file.clone(),
        offset: 0,
        length: 3,
    });
    let (second_id, second) = harness.submit(write(&file, 0, b"two"));
    harness
        .controller
        .execute(first_id, Effect::Normal)
        .unwrap();
    harness.controller.execute(read_id, Effect::Normal).unwrap();
    harness
        .controller
        .execute(second_id, Effect::Normal)
        .unwrap();
    harness.controller.deliver(second_id).unwrap();
    assert!(matches!(*second.await.unwrap(), Outcome::Written(3)));
    assert!(futures::poll!(&mut first).is_pending());
    harness.controller.deliver(read_id).unwrap();
    let value = read.await.unwrap();
    let Outcome::Read(bytes) = &*value else {
        panic!("read response")
    };
    assert_eq!(bytes.as_slice(), b"one");
    harness.controller.deliver(first_id).unwrap();
    assert!(matches!(*first.await.unwrap(), Outcome::Written(3)));
    assert_eq!(
        harness
            .controller
            .image()
            .bytes(Path::new("/data"), false)
            .unwrap(),
        b"two"
    );
    assert_eq!(
        harness
            .controller
            .image()
            .bytes(Path::new("/data"), true)
            .unwrap(),
        b"old"
    );
}

#[tokio::test]
async fn later_write_can_reach_media_while_an_earlier_group_is_a_hole() {
    let mut harness = Harness::new();
    let file = harness.established(b"protect!").await;
    harness
        .run(Operation::SetLength {
            handle: file.clone(),
            length: 24,
        })
        .await;
    harness.sync(&file).await;
    let (_missing_id, missing) = harness.submit(write(&file, 8, b"BBBBBBBB"));
    let (late_id, late) = harness.submit(write(&file, 16, b"CCCCCCCC"));
    harness.controller.execute(late_id, Effect::Normal).unwrap();
    harness
        .controller
        .persist_range(Path::new("/data"), 16..24)
        .unwrap();
    let (image, _) = harness.controller.crash(true).unwrap();
    assert_eq!(
        image.bytes(Path::new("/data"), false).unwrap(),
        b"protect!\0\0\0\0\0\0\0\0CCCCCCCC"
    );
    assert_eq!(missing.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(late.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
}

#[tokio::test]
async fn short_writes_errors_and_torn_persistence_are_explicit() {
    let mut harness = Harness::new();
    let file = harness.established(b"original").await;
    let result = harness
        .perform(write(&file, 0, b"01234567"), Effect::Short(3))
        .await
        .unwrap();
    assert!(matches!(*result, Outcome::Written(3)));
    drop(result);
    assert_eq!(
        harness
            .controller
            .image()
            .bytes(Path::new("/data"), false)
            .unwrap(),
        b"012ginal"
    );
    let error = harness
        .perform(
            write(&file, 3, b"ABCDE"),
            Effect::WriteThenError {
                bytes: 2,
                error: io::ErrorKind::Other,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    harness
        .controller
        .persist_range(Path::new("/data"), 1..4)
        .unwrap();
    let (image, _) = harness.controller.crash(true).unwrap();
    assert_eq!(image.bytes(Path::new("/data"), false).unwrap(), b"o12Ainal");
}

#[tokio::test]
async fn failed_barrier_may_have_persisted_data_but_never_returns_success() {
    for effect in [
        Effect::FailBefore(io::ErrorKind::Other),
        Effect::FailAfter(io::ErrorKind::Other),
    ] {
        let mut harness = Harness::new();
        let file = harness.established(b"old").await;
        harness.run(write(&file, 0, b"new")).await;
        let error = harness
            .perform(
                Operation::Sync {
                    handle: file,
                    mode: SyncMode::All,
                },
                effect,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        let (image, _) = harness.controller.crash(true).unwrap();
        assert_eq!(
            image.bytes(Path::new("/data"), false).unwrap(),
            if matches!(effect, Effect::FailBefore(_)) {
                b"old"
            } else {
                b"new"
            }
        );
    }
}

#[tokio::test]
async fn process_death_is_not_power_loss_and_old_handles_never_reactivate() {
    for power_loss in [false, true] {
        let mut harness = Harness::new();
        let file = harness.established(b"old").await;
        harness.run(write(&file, 0, b"new")).await;
        let (old_id, pending) = harness.submit(write(&file, 0, b"bad"));
        let (image, _) = harness.controller.crash(power_loss).unwrap();
        assert_eq!(pending.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(
            harness.clients[0]
                .submit(
                    Class::Data,
                    Operation::Metadata {
                        handle: file.clone()
                    }
                )
                .unwrap_err()
                .error
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        let mut restarted = Harness::from_image(image);
        let error = restarted
            .perform(Operation::Metadata { handle: file }, Effect::Normal)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            restarted
                .controller
                .image()
                .bytes(Path::new("/data"), false)
                .unwrap(),
            if power_loss { b"old" } else { b"new" }
        );
        let (_id, _completion) = restarted.submit(Operation::OpenDirectory { path: "/".into() });
        assert_eq!(
            restarted
                .controller
                .execute(old_id, Effect::Normal)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

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
async fn canceled_observation_keeps_admitted_work_and_charges_until_release() {
    let mut harness = Harness::new();
    let file = harness.established(b"old").await;
    let (id, completion) = harness.submit(write(&file, 0, b"new"));
    drop(completion);
    assert_eq!(
        harness
            .controller
            .admission()
            .used(0, Class::Data)
            .operations,
        1
    );
    harness.controller.execute(id, Effect::Normal).unwrap();
    assert_eq!(
        harness
            .controller
            .admission()
            .used(0, Class::Data)
            .operations,
        1
    );
    harness.controller.deliver(id).unwrap();
    assert_eq!(
        harness.controller.admission().used(0, Class::Data),
        Quota::default()
    );
    assert_eq!(
        harness
            .controller
            .image()
            .bytes(Path::new("/data"), false)
            .unwrap(),
        b"new"
    );
}

#[tokio::test]
async fn shutdown_waits_for_execution_not_for_result_observation() {
    let mut harness = Harness::new();
    let file = harness.established(b"old").await;
    let (id, mut completion) = harness.submit(write(&file, 0, b"new"));
    let drain = harness.controller.begin_shutdown().unwrap().wait();
    tokio::pin!(drain);
    assert!(futures::poll!(&mut drain).is_pending());
    assert_eq!(
        harness.clients[0]
            .submit(Class::Progress, Operation::Metadata { handle: file })
            .unwrap_err()
            .error
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    harness.controller.execute(id, Effect::Normal).unwrap();
    drain.await;
    assert!(futures::poll!(&mut completion).is_pending());
    harness.controller.deliver(id).unwrap();
    assert!(matches!(*completion.await.unwrap(), Outcome::Written(3)));
}

#[tokio::test]
async fn file_length_can_persist_without_its_new_bytes() {
    let mut harness = Harness::new();
    let file = harness.established(b"safe").await;
    harness.run(write(&file, 4, b"lost")).await;
    harness
        .controller
        .persist_length(Path::new("/data"))
        .unwrap();
    let (image, _) = harness.controller.crash(true).unwrap();
    assert_eq!(
        image.bytes(Path::new("/data"), false).unwrap(),
        b"safe\0\0\0\0"
    );
}

#[tokio::test]
async fn data_sync_write_does_not_flush_other_dirty_ranges() {
    let mut harness = Harness::new();
    let buffered = harness.established(b"old-old").await;
    let value = harness
        .run(Operation::Open {
            path: "/data".into(),
            mode: OpenMode::ReadWrite,
            direct: false,
            data_sync: true,
        })
        .await;
    let Outcome::Opened(handle) = &*value else {
        panic!("open response")
    };
    let synced = handle.clone();
    drop(value);
    harness.run(write(&buffered, 0, b"bad")).await;
    harness.run(write(&synced, 4, b"new")).await;
    let (image, _) = harness.controller.crash(true).unwrap();
    assert_eq!(image.bytes(Path::new("/data"), false).unwrap(), b"old-new");
}

#[tokio::test]
async fn invalid_schedule_and_storage_limits_leave_operation_or_image_intact() {
    let mut harness = Harness::new();
    let file = harness.established(b"old").await;
    let (id, completion) = harness.submit(Operation::Sync {
        handle: file.clone(),
        mode: SyncMode::Data,
    });
    assert_eq!(
        harness
            .controller
            .execute(id, Effect::Short(1))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        harness.controller.deliver(id).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    harness.controller.execute(id, Effect::Normal).unwrap();
    harness.controller.deliver(id).unwrap();
    completion.await.unwrap();
    let error = harness
        .perform(
            Operation::SetLength {
                handle: file,
                length: 2 * 1024 * 1024,
            },
            Effect::Normal,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::StorageFull);
    assert_eq!(
        harness
            .controller
            .image()
            .bytes(Path::new("/data"), false)
            .unwrap(),
        b"old"
    );
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
async fn trace_limit_refuses_the_next_event_without_losing_the_pending_reply() {
    let mut configuration = config();
    configuration.trace_events = 1;
    let (controller, clients) = Controller::new(configuration, Image::default()).unwrap();
    let mut harness = Harness {
        controller,
        clients,
    };
    let (id, mut completion) = harness.submit(Operation::OpenDirectory { path: "/".into() });
    harness.controller.execute(id, Effect::Normal).unwrap();
    assert_eq!(
        harness.controller.deliver(id).unwrap_err().kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(harness.controller.jobs(), [(id, Stage::Executed)]);
    assert!(futures::poll!(&mut completion).is_pending());
    drop(harness.controller);
    assert_eq!(
        completion.await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

async fn seeded_schedule(mut seed: u64) -> (Vec<Event>, Vec<u8>) {
    let mut random = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let mut harness = Harness::new();
    let file = harness.established(&[0; 32]).await;
    let mut dirty = [0_u8; 32];
    let mut stable = dirty;
    for group in 0..8 {
        let mut jobs = Vec::new();
        for index in group * 4..group * 4 + 4 {
            let byte = (random() % 255 + 1) as u8;
            let (id, completion) = harness.submit(write(&file, index as u64, &[byte]));
            jobs.push((id, Some(completion), index, byte, Effect::Normal));
        }
        if random() & 1 != 0 {
            jobs.reverse();
        }
        for (id, completion, index, byte, expected) in &mut jobs {
            let effect = match random() % 3 {
                0 => Effect::Normal,
                1 => Effect::Short(0),
                _ => Effect::WriteThenError {
                    bytes: 1,
                    error: io::ErrorKind::Other,
                },
            };
            harness.controller.execute(*id, effect).unwrap();
            *expected = effect;
            if effect != Effect::Short(0) {
                dirty[*index] = *byte;
            }
            if random() & 1 != 0 {
                harness
                    .controller
                    .persist_range(Path::new("/data"), *index..*index + 1)
                    .unwrap();
                stable[*index] = dirty[*index];
            }
            if random() & 1 != 0 {
                drop(completion.take());
            }
        }
        if random() & 1 != 0 {
            jobs.reverse();
        }
        for (id, completion, _, _, expected) in jobs {
            harness.controller.deliver(id).unwrap();
            if let Some(completion) = completion {
                let result = completion.await;
                match expected {
                    Effect::WriteThenError { error, .. } => {
                        assert_eq!(result.unwrap_err().kind(), error);
                    }
                    Effect::Short(count) => assert!(
                        matches!(*result.unwrap(), Outcome::Written(actual) if actual == count)
                    ),
                    Effect::Normal => assert!(matches!(*result.unwrap(), Outcome::Written(1))),
                    _ => unreachable!(),
                }
            }
        }
        if random() % 4 == 0 {
            harness.sync(&file).await;
            stable = dirty;
        }
        assert_eq!(
            harness
                .controller
                .image()
                .bytes(Path::new("/data"), false)
                .unwrap(),
            dirty
        );
        assert_eq!(
            harness
                .controller
                .image()
                .bytes(Path::new("/data"), true)
                .unwrap(),
            stable
        );
    }
    let (image, trace) = harness.controller.crash(true).unwrap();
    let bytes = image.bytes(Path::new("/data"), false).unwrap().to_vec();
    assert_eq!(bytes, stable);
    (trace, bytes)
}

#[tokio::test]
async fn seeded_schedules_reproduce_exact_events_and_independent_byte_oracle() {
    for seed in 1..=16 {
        assert_eq!(
            seeded_schedule(seed).await,
            seeded_schedule(seed).await,
            "seed {seed}"
        );
    }
}
