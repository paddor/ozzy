//! The worker loop: take queued jobs, execute them under the device fence,
//! and settle their replies.

use super::{Job, Receiver, Shared, Worker, files};
use crate::parking::Seat;
use ozzy_io::{Operation, Outcome};
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(super) fn run(mut receiver: Receiver, shared: &Arc<Shared>, role: Worker, seat: Seat) {
    if !shared.parking.register(seat) {
        shared.abandon();
        return;
    }
    let class = usize::from(role == Worker::Progress);
    let mut announced = false;
    loop {
        if shared.reclaim.swap(false, Ordering::AcqRel) {
            shared.reclaim_files();
        }
        if let Some((job, executing)) = take(&mut receiver, shared, role) {
            shared.queued[class].fetch_sub(1, Ordering::AcqRel);
            if announced {
                shared.parking.busy(seat);
                announced = false;
            }
            perform(job, executing, shared);
            continue;
        }
        // The ring reports empty while a producer finishes publishing or while
        // bounded maintenance retires sender lanes. Retry until a counted job
        // becomes visible; no later wake may come for it.
        if shared.queued[class].load(Ordering::Acquire) != 0
            && (role == Worker::Progress
                || shared.executing.load(Ordering::Acquire) < shared.max_inflight)
        {
            std::thread::yield_now();
            continue;
        }
        if !announced {
            // A job published after this announcement wakes this seat; one
            // published before it is found by the next take.
            shared.parking.idle(seat);
            announced = true;
            continue;
        }
        if shared.drained() && !shared.state().starting {
            shared.parking.busy(seat);
            shared.worker_finished();
            return;
        }
        std::thread::park();
        shared.parking.busy(seat);
        announced = false;
    }
}

/// Take one queued job. Ordinary jobs also need an execution slot.
fn take(receiver: &mut Receiver, shared: &Shared, role: Worker) -> Option<(Job, bool)> {
    let executing = role != Worker::Progress;
    if executing && !shared.reserve_execution() {
        return None;
    }
    let job = receiver.try_recv().ok();
    // A data worker refused by this probe may park, but this worker checks the
    // queue again after the release. The direct writer does not: wake it.
    if job.is_none() && executing && shared.release_execution() {
        shared.wake_direct();
    }
    Some((job?, executing))
}

fn perform(job: Job, executing: bool, shared: &Arc<Shared>) {
    let Job {
        operation,
        reply,
        mut slot,
    } = job;
    slot.executing = executing;
    slot.observe_start(&operation);
    let result = execute(operation, shared, slot.shard);
    slot.settle(reply, result);
}

/// Execute unless the device is fenced. A panicking execution fences it.
fn execute(operation: Operation, shared: &Arc<Shared>, origin: usize) -> io::Result<Outcome> {
    if shared.failed.load(Ordering::Acquire) {
        return Err(io::ErrorKind::BrokenPipe.into());
    }
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        #[cfg(test)]
        super::tests::Gate::enter_installed(shared, &operation);
        files::execute(operation, shared, origin)
    }))
    .unwrap_or_else(|_| {
        shared.fail();
        Err(io::Error::other("file worker panicked"))
    })
}
