use super::{Domain, Limits};
mod capacity;
use std::{
    io,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
};

fn limits(bytes: usize, buffers: usize) -> Limits {
    Limits {
        bytes,
        buffers,
        cache_bytes: bytes,
    }
}

#[test]
fn arena_capability_expires_with_owner_while_shared_bytes_remain_valid() {
    let domain = Domain::new(None, 128).unwrap();
    let owner = domain.owner(limits(128, 2)).unwrap();
    let allocator = owner.allocator();
    assert_eq!(owner.allocated_bytes(), 0);
    let mut arena = allocator.try_arena(64).unwrap();
    arena.bytes_mut().extend_from_slice(&[7; 16]);
    let bytes = super::Arena::share(&Arc::new(arena));
    drop(owner);
    assert_eq!(
        allocator.try_arena(1).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(domain.reserved_bytes(), 128);
    assert_eq!(bytes.as_ref(), &[7; 16]);
    std::thread::spawn(move || drop(bytes)).join().unwrap();
    assert_eq!(domain.reserved_bytes(), 0);
}

#[test]
fn unused_owner_grants_reserve_node_capacity_and_control_stays_separate() {
    let domain = Domain::new(Some(3), 1024).unwrap();
    assert_eq!(domain.node(), Some(3));
    let data = domain.owner(limits(768, 2)).unwrap();
    let control = domain.owner(limits(256, 2)).unwrap();
    assert_eq!(domain.reserved_bytes(), 1024);
    assert_eq!(
        domain.owner(limits(1, 1)).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let full = data.try_lease(768).unwrap();
    assert_eq!(
        data.try_lease(1).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let progress = control.try_lease(256).unwrap();
    drop((full, progress));
    data.trim_cache();
    control.trim_cache();
    assert_eq!(data.allocated_bytes(), 0);
    assert_eq!(domain.reserved_bytes(), 1024);
    drop((data, control));
    assert_eq!(domain.reserved_bytes(), 0);
}

#[test]
fn immutable_aliases_keep_full_capacity_and_return_once_after_last_drop() {
    let domain = Domain::new(None, 1024).unwrap();
    let owner = domain.owner(limits(1024, 1)).unwrap();
    let mut buffer = owner.try_lease(1024).unwrap();
    buffer.fill(7);
    buffer.resize(16).unwrap();
    let bytes = buffer.freeze();
    let slice = bytes.slice(3..5);
    let pointer = bytes.as_ptr();
    drop(bytes);
    assert_eq!(slice.as_ref(), &[7, 7]);
    assert_eq!(owner.allocated_bytes(), 1024);
    assert!(owner.try_lease(1).is_err());
    std::thread::spawn(move || drop(slice)).join().unwrap();
    let recycled = owner.try_lease(256).unwrap();
    assert_eq!(recycled.as_ptr(), pointer);
    assert!(recycled.iter().all(|&byte| byte == 0));
    assert_eq!(recycled.capacity(), 1024);
    assert_eq!(owner.allocated_bytes(), 1024);
    drop(recycled);
    owner.trim_cache();
    assert_eq!(owner.allocated_bytes(), 0);
}

#[test]
fn remote_payload_outlives_owner_and_retains_its_domain_reservation() {
    let domain = Domain::new(None, 32).unwrap();
    let owner = domain.owner(limits(32, 1)).unwrap();
    let bytes = owner.try_lease(32).unwrap().freeze();
    drop(owner);
    assert_eq!(domain.reserved_bytes(), 32);
    assert!(domain.owner(limits(32, 1)).is_err());
    std::thread::spawn(move || drop(bytes)).join().unwrap();
    assert_eq!(domain.reserved_bytes(), 0);
    assert!(domain.owner(limits(32, 1)).is_ok());
}

#[test]
fn cache_eviction_respects_byte_and_buffer_limits_without_stale_contents() {
    let domain = Domain::new(None, 256).unwrap();
    let owner = domain.owner(limits(256, 2)).unwrap();
    let mut first = owner.try_lease(64).unwrap();
    first.fill(9);
    let second = owner.try_lease(64).unwrap();
    assert!(owner.try_lease(1).is_err());
    drop((first, second));
    let large = owner.try_lease(256).unwrap();
    assert!(large.iter().all(|&byte| byte == 0));
    assert_eq!(owner.allocated_bytes(), 256);
    assert!(owner.try_lease(1).is_err());
    drop(large);
    assert_eq!(
        owner.try_lease(257).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        owner.try_lease(0).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn canceled_waiters_do_not_reserve_or_steal_returned_capacity() {
    let domain = Domain::new(None, 128).unwrap();
    let owner = domain.owner(limits(128, 1)).unwrap();
    let bytes = owner.try_lease(128).unwrap().freeze();
    let mut canceled = Box::pin(owner.lease(128));
    assert!(futures::poll!(&mut canceled).is_pending());
    drop(canceled);
    let mut waiting = Box::pin(owner.lease(128));
    assert!(futures::poll!(&mut waiting).is_pending());
    std::thread::spawn(move || drop(bytes)).join().unwrap();
    let received = waiting.await.unwrap();
    assert_eq!(received.len(), 128);
    assert_eq!(owner.allocated_bytes(), 128);
}

#[test]
fn cross_thread_returns_are_bounded_by_physical_buffer_count() {
    let domain = Domain::new(None, 4096).unwrap();
    let owner = domain.owner(limits(4096, 64)).unwrap();
    let mut buffers: Vec<_> = (0..64)
        .map(|_| owner.try_lease(64).unwrap().freeze())
        .collect();
    assert_eq!(owner.allocated_bytes(), 4096);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let remote: Vec<_> = buffers.drain(..8).collect();
            scope.spawn(move || drop(remote));
        }
    });
    let reused: Vec<_> = (0..64).map(|_| owner.try_lease(64).unwrap()).collect();
    assert!(owner.try_lease(1).is_err());
    assert_eq!(owner.allocated_bytes(), 4096);
    drop(reused);
    owner.trim_cache();
    assert_eq!(owner.allocated_bytes(), 0);
}

#[test]
fn concurrent_owner_startup_cannot_multiply_node_budget() {
    let domain = Domain::new(Some(0), 4096).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let winners = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let (domain, barrier, winners) = (domain.clone(), barrier.clone(), winners.clone());
            scope.spawn(move || {
                barrier.wait();
                let owner = domain.owner(limits(4096, 1));
                if owner.is_ok() {
                    winners.fetch_add(1, Ordering::Relaxed);
                }
                barrier.wait(); // Keep the winning reservation until all tried.
                drop(owner);
            });
        }
    });
    assert_eq!(winners.load(Ordering::Relaxed), 1);
    assert_eq!(domain.reserved_bytes(), 0);
}

#[tokio::test]
async fn canceled_file_wait_keeps_payload_charged_until_physical_completion() {
    use ozzy_io::simulation::{Config, Controller, Effect, Image, ImageLimits};
    use ozzy_io::{
        Backend, Class, Limits as IoLimits, OpenMode, Operation, Outcome, Quota, WriteBuffer,
    };
    let (mut device, mut clients) = Controller::new(
        Config {
            limits: IoLimits {
                shards: 1,
                data: Quota {
                    operations: 4,
                    bytes: 8192,
                },
                progress: Quota {
                    operations: 2,
                    bytes: 8192,
                },
            },
            handles: 4,
            image: ImageLimits {
                nodes: 8,
                directory_entries: 16,
                file_bytes: 4096,
                total_bytes: 8192,
            },
            trace_events: 32,
        },
        Image::default(),
    )
    .unwrap();
    let open = clients[0]
        .submit(
            Class::Data,
            Operation::Open {
                path: "/payload".into(),
                mode: OpenMode::CreateNew,
                direct: false,
                data_sync: false,
            },
        )
        .unwrap();
    let id = device.jobs()[0].0;
    device.execute(id, Effect::Normal).unwrap();
    device.deliver(id).unwrap();
    let opened = open.await.unwrap();
    let Outcome::Opened(handle) = &*opened else {
        panic!("handle")
    };
    let handle = handle.clone();
    drop(opened);
    let domain = Domain::new(None, 1024).unwrap();
    let owner = domain.owner(limits(1024, 1)).unwrap();
    let mut buffer = owner.try_lease(1024).unwrap();
    buffer.fill(42);
    buffer.resize(128).unwrap();
    let write = clients[0]
        .submit(
            Class::Data,
            Operation::Write {
                handle,
                offset: 0,
                data: WriteBuffer::shared(vec![buffer.freeze()], 1024).unwrap(),
            },
        )
        .unwrap();
    drop(write);
    drop(owner);
    assert_eq!(domain.reserved_bytes(), 1024);
    let id = device.jobs()[0].0;
    device.execute(id, Effect::Normal).unwrap();
    assert_eq!(domain.reserved_bytes(), 0);
    device.deliver(id).unwrap();
    assert_eq!(
        device
            .image()
            .bytes(std::path::Path::new("/payload"), false)
            .unwrap(),
        &[42; 128]
    );
    device.begin_shutdown().unwrap().wait().await;
}
