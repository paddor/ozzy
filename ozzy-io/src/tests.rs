use crate::*;
use std::cell::RefCell;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Wake, Waker};

struct TestAdmission {
    view: Admission,
    lanes: Vec<RefCell<Lane>>,
}

impl TestAdmission {
    fn try_charge(&self, shard: usize, class: Class, bytes: usize) -> io::Result<Charge> {
        self.lanes[shard].borrow_mut().try_charge(class, bytes)
    }
    fn used(&self, shard: usize, class: Class) -> Quota {
        self.view.used(shard, class)
    }
    async fn ready(&self, shard: usize, class: Class, bytes: usize) -> io::Result<()> {
        self.view.ready(shard, class, bytes).await
    }
    fn close(&self) {
        self.view.close();
    }
    fn limits(&self) -> Limits {
        self.view.limits()
    }
}

fn admission() -> TestAdmission {
    let view = Admission::new(Limits {
        shards: 2,
        data: Quota {
            operations: 4,
            bytes: 100,
        },
        progress: Quota {
            operations: 2,
            bytes: 20,
        },
    })
    .unwrap();
    let lanes = (0..view.limits().shards)
        .map(|shard| RefCell::new(view.lane(shard).unwrap()))
        .collect();
    TestAdmission { view, lanes }
}

#[test]
fn operation_protection_is_bounded_and_charged() {
    let owner = HandleOwner::default();
    let (handle, _) = owner.create(1, Waker::noop().clone());
    let operation = || Operation::Metadata {
        handle: handle.clone(),
    };
    let protected = Operation::Protected {
        operation: Box::new(operation()),
        handles: vec![handle.clone()],
    };
    assert!(protected.retained_bytes().unwrap() >= size_of::<Operation>() + size_of::<Handle>());
    let nested = Operation::Protected {
        operation: Box::new(protected),
        handles: vec![handle.clone()],
    };
    assert_eq!(
        nested.retained_bytes().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let too_many = Operation::Protected {
        operation: Box::new(operation()),
        handles: vec![handle; 65],
    };
    assert_eq!(
        too_many.retained_bytes().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn fixed_shard_shares_preserve_device_and_progress_capacity() {
    let admission = admission();
    let first = admission.try_charge(0, Class::Data, 40).unwrap();
    assert_eq!(
        admission.try_charge(0, Class::Data, 11).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let second = admission.try_charge(0, Class::Data, 10).unwrap();
    assert_eq!(
        admission.try_charge(0, Class::Data, 0).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let other = admission.try_charge(1, Class::Data, 50).unwrap();
    let progress = admission.try_charge(0, Class::Progress, 10).unwrap();
    assert_eq!(
        admission.try_charge(1, Class::Data, 51).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    drop((first, second, other, progress));
    assert_eq!(admission.used(0, Class::Data), Quota::default());
}

#[test]
fn concurrent_final_drops_return_full_shard_capacity() {
    let admission = Admission::new(Limits {
        shards: 1,
        data: Quota {
            operations: 64,
            bytes: 640,
        },
        progress: Quota {
            operations: 1,
            bytes: 1,
        },
    })
    .unwrap();
    let mut lane = admission.lane(0).unwrap();
    assert_eq!(
        admission.lane(0).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    let charges: Vec<_> = (0..64)
        .map(|_| lane.try_charge(Class::Data, 10).unwrap())
        .collect();
    assert_eq!(
        lane.try_charge(Class::Data, 1).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let mut groups = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for (index, charge) in charges.into_iter().enumerate() {
        groups[index % 4].push(charge);
    }
    std::thread::scope(|scope| {
        for group in groups {
            scope.spawn(move || drop(group));
        }
    });
    assert_eq!(admission.used(0, Class::Data), Quota::default());
    for _ in 0..64 {
        drop(lane.try_charge(Class::Data, 10).unwrap());
    }
    assert_eq!(admission.used(0, Class::Data), Quota::default());
}

#[test]
fn split_remainders_do_not_create_capacity() {
    let limits = Limits {
        shards: 3,
        data: Quota {
            operations: 8,
            bytes: 101,
        },
        progress: Quota {
            operations: 4,
            bytes: 17,
        },
    };
    for class in [Class::Data, Class::Progress] {
        let shares: Vec<_> = (0..3).map(|shard| limits.share(shard, class)).collect();
        let total = match class {
            Class::Data => limits.data,
            Class::Progress => limits.progress,
        };
        assert_eq!(
            shares.iter().map(|q| q.operations).sum::<usize>(),
            total.operations
        );
        assert_eq!(shares.iter().map(|q| q.bytes).sum::<usize>(), total.bytes);
    }
}

#[tokio::test]
async fn cancellation_keeps_charge_until_physical_completion() {
    let admission = admission();
    let (reply, completion) = completion(admission.try_charge(0, Class::Data, 50).unwrap());
    drop(completion);
    assert_eq!(admission.used(0, Class::Data).bytes, 50);
    let ready = admission.ready(0, Class::Data, 1);
    tokio::pin!(ready);
    assert!(futures::poll!(&mut ready).is_pending());
    reply.finish(Ok(Outcome::Done));
    ready.await.unwrap();
    assert_eq!(admission.used(0, Class::Data), Quota::default());
}

#[tokio::test]
async fn completion_observation_and_payload_release_are_distinct() {
    let admission = admission();
    let (reply, completion) = completion(admission.try_charge(0, Class::Data, 50).unwrap());
    reply.finish(Ok(Outcome::Read(
        ReadBuffer::new(vec![42; 50].into_boxed_slice(), 3).unwrap(),
    )));
    assert_eq!(admission.used(0, Class::Data).bytes, 50);
    let value = completion.await.unwrap();
    let Outcome::Read(buffer) = &*value else {
        panic!("read response")
    };
    assert_eq!(buffer.as_slice(), &[42; 3]);
    assert_eq!(admission.used(0, Class::Data).bytes, 50);
    drop(value);
    assert_eq!(admission.used(0, Class::Data), Quota::default());
}

#[tokio::test]
async fn directory_transfer_keeps_charge_until_result_release() {
    let admission = admission();
    let (reply, completion) = completion(admission.try_charge(0, Class::Data, 50).unwrap());
    reply.finish(Ok(Outcome::Directory(vec![Entry {
        name: "segment".into(),
        kind: FileKind::File,
    }])));
    let mut result = completion.await.unwrap();
    let entries = result.take_directory().unwrap();
    assert_eq!(entries[0].name, "segment");
    assert!(result.take_directory().unwrap().is_empty());
    assert_eq!(admission.used(0, Class::Data).bytes, 50);
    drop(result);
    assert_eq!(admission.used(0, Class::Data), Quota::default());
    assert_eq!(entries.len(), 1);
}

#[tokio::test]
async fn completions_can_arrive_out_of_order_without_exchanging_results() {
    let admission = admission();
    let (a, mut first) = completion(admission.try_charge(0, Class::Data, 10).unwrap());
    let (b, second) = completion(admission.try_charge(0, Class::Data, 10).unwrap());
    b.finish(Ok(Outcome::Written(2)));
    assert!(matches!(*second.await.unwrap(), Outcome::Written(2)));
    assert!(futures::poll!(&mut first).is_pending());
    a.finish(Ok(Outcome::Written(1)));
    assert!(matches!(*first.await.unwrap(), Outcome::Written(1)));
}

#[tokio::test]
async fn errors_and_abandoned_replies_wake_observers() {
    let admission = admission();
    for explicit in [false, true] {
        let (reply, mut completion) = completion(admission.try_charge(0, Class::Data, 10).unwrap());
        assert!(futures::poll!(&mut completion).is_pending());
        if explicit {
            reply.finish(Err(io::ErrorKind::PermissionDenied.into()));
        } else {
            drop(reply);
        }
        let error = completion.await.unwrap_err();
        assert_eq!(
            error.kind(),
            if explicit {
                io::ErrorKind::PermissionDenied
            } else {
                io::ErrorKind::BrokenPipe
            }
        );
        assert_eq!(admission.used(0, Class::Data), Quota::default());
    }
}

#[tokio::test]
async fn shutdown_wakes_capacity_waiters_and_canceled_waits_do_not_steal_wakes() {
    let admission = admission();
    let held = admission.try_charge(0, Class::Data, 50).unwrap();
    {
        let canceled = admission.ready(0, Class::Data, 1);
        tokio::pin!(canceled);
        assert!(futures::poll!(&mut canceled).is_pending());
    }
    let wait = admission.ready(0, Class::Data, 1);
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    admission.close();
    assert_eq!(wait.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    drop(held);
    assert_eq!(
        admission
            .try_charge(0, Class::Progress, 0)
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn handles_are_backend_scoped_and_last_drop_only_signals_reclamation() {
    #[derive(Default)]
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let wake = Arc::new(Counter::default());
    let owner = HandleOwner::default();
    let (handle, token) = owner.create(7, Waker::from(wake.clone()));
    assert_eq!(owner.key(&handle), Some(7));
    assert_eq!(HandleOwner::default().key(&handle), None);
    let copy = handle.clone();
    drop(handle);
    assert!(token.is_alive());
    assert_eq!(wake.0.load(Ordering::Relaxed), 0);
    drop(copy);
    assert!(!token.is_alive());
    assert_eq!(wake.0.load(Ordering::Relaxed), 1);
}

#[test]
fn invalid_or_overflowed_budgets_fail_before_allocation() {
    let valid = admission().limits();
    for limits in [
        Limits { shards: 0, ..valid },
        Limits { shards: 5, ..valid },
        Limits {
            data: Quota {
                operations: usize::MAX,
                bytes: 50,
            },
            ..valid
        },
    ] {
        assert_eq!(
            Admission::new(limits).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    let operation = Operation::ReadDirectory {
        path: "x".into(),
        max_entries: usize::MAX,
        max_name_bytes: 0,
    };
    assert_eq!(
        operation.retained_bytes().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(WriteBuffer::shared(vec![bytes::Bytes::from_static(b"abc")], 2).is_err());
}

#[test]
fn positions_outside_the_signed_file_api_are_rejected_in_every_backend() {
    let (handle, _) = HandleOwner::default().create(0, futures::task::noop_waker());
    for operation in [
        Operation::Read {
            handle: handle.clone(),
            offset: u64::MAX,
            length: 0,
        },
        Operation::Write {
            handle: handle.clone(),
            offset: i64::MAX as u64,
            data: WriteBuffer::from_vec(vec![1]),
        },
        Operation::SetLength {
            handle: handle.clone(),
            length: u64::MAX,
        },
        Operation::Allocate {
            handle,
            offset: i64::MAX as u64,
            length: 1,
        },
    ] {
        assert_eq!(
            operation.retained_bytes().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
