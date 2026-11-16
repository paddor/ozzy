//! The order in which the waiting jobs of one lane try for capacity.
//!
//! Capacity changes are broadcast. If every waiting job listened, each
//! change would wake them all for one to win. So the jobs of a class queue
//! up, and only the first waits for the broadcast. The others sleep. One
//! of them is woken to try when the job in front of the queue left, when
//! that job lacks only bytes and the sleeper fits, or when it fits in what
//! an admitted job left. A woken job that does not get in sleeps again.
//!
//! The queue only decides whom a change wakes. A sleeping job that is
//! polled for another reason while it fits goes ahead, so a job in front
//! that nobody polls holds no one up who is polled.

use crate::Class;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
    task::{Poll, Waker},
};

#[derive(Debug)]
struct Waiting {
    id: u64,
    /// Bytes the job retains once admitted.
    bytes: usize,
    /// Whether the job may try for capacity now.
    listens: bool,
    waker: Option<Waker>,
}

impl Waiting {
    fn listen(&mut self) {
        self.listens = true;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// The waiting jobs of one lane, by class.
#[derive(Debug, Default)]
pub(super) struct Turns {
    waiting: [RefCell<VecDeque<Waiting>>; 2],
    next: Cell<u64>,
}

/// One job's place in the queue. Dropping it leaves the queue.
#[derive(Debug)]
pub(super) struct Turn {
    turns: Rc<Turns>,
    class: usize,
    id: u64,
}

/// Where job `id` waits. Jobs queue up in the order of their IDs.
fn position(waiting: &VecDeque<Waiting>, id: u64) -> Option<usize> {
    waiting.binary_search_by_key(&id, |job| job.id).ok()
}

impl Turns {
    /// Queue up behind the waiting jobs of `class` with a job of `bytes`.
    /// It tries at once if it fits while the first job does not: the room
    /// that is free is not the first job's to take.
    pub(super) fn join(
        self: &Rc<Self>,
        class: Class,
        bytes: usize,
        fit: impl Fn(usize) -> bool,
    ) -> Turn {
        let id = self.next.get();
        self.next.set(id.wrapping_add(1));
        let mut waiting = self.waiting[class.index()].borrow_mut();
        let listens = waiting
            .front()
            .is_none_or(|first| !fit(first.bytes) && fit(bytes));
        waiting.push_back(Waiting {
            id,
            bytes,
            listens,
            waker: None,
        });
        Turn {
            turns: self.clone(),
            class: class.index(),
            id,
        }
    }
}

impl Turn {
    /// Wait until this job may try for capacity, or until a poll finds
    /// that it `fits`.
    pub(super) async fn taken(&self, fits: impl Fn() -> bool) {
        std::future::poll_fn(|cx| {
            let mut waiting = self.turns.waiting[self.class].borrow_mut();
            match position(&waiting, self.id).and_then(|index| waiting.get_mut(index)) {
                Some(job) if !job.listens && !fits() => {
                    job.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
                Some(job) => {
                    job.listens = true;
                    Poll::Ready(())
                }
                None => Poll::Ready(()),
            }
        })
        .await;
    }

    /// Whether this job is the first of the queue, the one that waits for
    /// the broadcast.
    pub(super) fn first(&self) -> bool {
        let waiting = self.turns.waiting[self.class].borrow();
        waiting.front().is_none_or(|first| first.id == self.id)
    }

    /// Sleep again after a try that found no room.
    pub(super) fn rest(&self) {
        let mut waiting = self.turns.waiting[self.class].borrow_mut();
        let index = position(&waiting, self.id).filter(|index| *index != 0);
        if let Some(job) = index.and_then(|index| waiting.get_mut(index)) {
            job.listens = false;
        }
    }

    /// Let the first sleeping job whose bytes `fit` now try. A job calls
    /// this when it lacks only bytes, and for what it leaves once admitted.
    pub(super) fn promote(&self, fit: impl Fn(usize) -> bool) {
        let mut waiting = self.turns.waiting[self.class].borrow_mut();
        let sleeping = waiting.iter_mut().filter(|job| !job.listens);
        if let Some(next) = { sleeping }.find(|job| fit(job.bytes)) {
            next.listen();
        }
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        let mut waiting = self.turns.waiting[self.class].borrow_mut();
        if let Some(index) = position(&waiting, self.id) {
            waiting.remove(index);
        }
        // Someone must listen while jobs wait.
        if let Some(first) = waiting.front_mut().filter(|first| !first.listens) {
            first.listen();
        }
    }
}
