//! Explicit physical execution and result delivery for async storage tests.

use ozzy_io::{
    Operation,
    simulation::{Controller, Effect, Stage},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

pub(crate) fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[derive(Default)]
pub(crate) struct Scheduling(AtomicBool);

impl Wake for Scheduling {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl Scheduling {
    pub(crate) fn poll<T>(self: &Arc<Self>, future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
        self.0.store(false, Ordering::Relaxed);
        future.poll(&mut Context::from_waker(&Waker::from(self.clone())))
    }

    pub(crate) fn woken(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

pub(crate) fn drive<T>(controller: &mut Controller, future: impl Future<Output = T>) -> T {
    drive_with(controller, future, |_| Effect::Normal)
}

pub(crate) fn drive_with<T>(
    controller: &mut Controller,
    future: impl Future<Output = T>,
    mut effect: impl FnMut(&Operation) -> Effect,
) -> T {
    let mut future = std::pin::pin!(future);
    let scheduling = Arc::new(Scheduling::default());
    for _ in 0..100_000 {
        if let Poll::Ready(result) = scheduling.poll(future.as_mut()) {
            return result;
        }
        let jobs = controller.jobs();
        assert!(
            !jobs.is_empty() || scheduling.woken(),
            "storage future stalled without I/O or a wake"
        );
        for (id, stage) in jobs {
            assert_eq!(stage, Stage::Queued);
            let fault = effect(controller.operation(id).unwrap().unprotected());
            controller.execute(id, fault).unwrap();
            controller.deliver(id).unwrap();
        }
    }
    panic!("storage future exceeded test step bound")
}
