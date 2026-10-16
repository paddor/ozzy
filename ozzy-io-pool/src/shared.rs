//! State shared by the pool owner, its clients, and every worker: admission
//! and queue counters, parking, the file table, and drain progress.

use super::{CLOSED, Config, direct, files};
use crate::parking::{DATA, PROGRESS, Parking};
use ozzy_io::signal::CloseSignal;
use ozzy_io::{Admission, HandleOwner, Limits, lock_unpoisoned};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Wake;

#[derive(Debug)]
pub(super) struct Shared {
    pub(super) admission: Admission,
    pub(super) state: Mutex<State>,
    pub(super) active: AtomicUsize,
    pub(super) queued: [AtomicUsize; 2],
    pub(super) executing: AtomicUsize,
    pub(super) failed: AtomicBool,
    pub(super) reclaim: AtomicBool,
    pub(super) parking: Parking,
    pub(super) finished: CloseSignal,
    pub(super) handles: HandleOwner,
    pub(super) files: Mutex<files::Files>,
    pub(super) reclamation: Mutex<()>,
    pub(super) direct: Option<Arc<direct::Shared>>,
    pub(super) max_inflight: usize,
    #[cfg(test)]
    pub(super) gate: Mutex<Option<Arc<super::tests::Gate>>>,
}

#[derive(Debug, Default)]
pub(super) struct State {
    pub(super) workers: usize,
    pub(super) starting: bool,
}

impl Shared {
    /// Pool state before any worker starts.
    pub(super) fn new(
        config: Config,
        limits: Limits,
        direct: Option<Arc<direct::Shared>>,
    ) -> io::Result<Self> {
        let handles = HandleOwner::default();
        Ok(Self {
            admission: Admission::new(limits)?,
            state: Mutex::new(State {
                starting: true,
                ..State::default()
            }),
            active: AtomicUsize::new(0),
            queued: [AtomicUsize::new(0), AtomicUsize::new(0)],
            executing: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
            reclaim: AtomicBool::new(false),
            parking: Parking::new(config.threads, config.threads.min(2)),
            finished: CloseSignal::default(),
            handles: handles.clone(),
            files: Mutex::new(files::Files::new(config.handles, limits.shards, handles)),
            reclamation: Mutex::new(()),
            direct,
            max_inflight: config.max_inflight,
            #[cfg(test)]
            gate: Mutex::new(None),
        })
    }

    #[allow(
        deprecated,
        reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
    )]
    pub(super) fn reserve_job(&self) -> Result<(), ()> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active & CLOSED == 0)
                    .then(|| active.checked_add(1))
                    .flatten()
            })
            .map(|_| ())
            .map_err(|_| ())
    }

    pub(super) fn drained(&self) -> bool {
        let active = self.active.load(Ordering::Acquire);
        active & CLOSED != 0 && active & !CLOSED == 0
    }

    /// Wake the worker that takes a job just published to `class`.
    pub(super) fn published(&self, class: usize, direct: bool) {
        if direct {
            self.wake_direct();
        } else {
            self.parking.wake_one(class);
        }
    }

    /// Take one execution slot for an ordinary job.
    #[allow(
        deprecated,
        reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
    )]
    pub(super) fn reserve_execution(&self) -> bool {
        self.executing
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                (used < self.max_inflight).then_some(used + 1)
            })
            .is_ok()
    }

    /// Return an execution slot. Returns whether the pool was full, so that
    /// a worker refused a slot may be waiting for this one.
    pub(super) fn release_execution(&self) -> bool {
        self.executing.fetch_sub(1, Ordering::AcqRel) == self.max_inflight
    }

    pub(super) fn wake_direct(&self) {
        if let Some(direct) = &self.direct {
            direct.wake.wake();
        }
    }

    /// Wake every worker for a lifecycle change: stop, startup end, or drain.
    pub(super) fn wake_all(&self) {
        self.parking.wake_all();
        self.wake_direct();
    }

    pub(super) fn fail(&self) {
        self.failed.store(true, Ordering::Release);
        self.stop();
    }

    /// A worker that cannot run fences the pool and leaves it.
    pub(super) fn abandon(&self) {
        self.fail();
        self.worker_finished();
    }
    pub(super) fn files(&self) -> MutexGuard<'_, files::Files> {
        lock_unpoisoned(&self.files)
    }

    pub(super) fn state(&self) -> MutexGuard<'_, State> {
        lock_unpoisoned(&self.state)
    }

    pub(super) fn reclaim_files(&self) {
        self.with_reclaimed_files(|_| ());
    }

    pub(super) fn with_reclaimed_files<T>(&self, action: impl FnOnce(&mut files::Files) -> T) -> T {
        // Another worker must not reserve between registry removal and physical
        // close. This lock belongs only to storage workers, never shard handles.
        let _reclamation = lock_unpoisoned(&self.reclamation);
        let retired = self.files().reclaim();
        // Closing a descriptor may block. Never hold the lookup lock for it.
        drop(retired);
        action(&mut self.files())
    }

    pub(super) fn stop(&self) {
        self.active.fetch_or(CLOSED, Ordering::AcqRel);
        self.admission.close();
        self.wake_all();
    }

    pub(super) fn worker_finished(&self) {
        let last = {
            let mut state = self.state();
            state.workers -= 1;
            state.workers == 0
        };
        if last {
            // Only worker threads have ever inserted actual descriptors.
            self.files().clear();
            self.finished.close();
        }
    }
}

impl Wake for Shared {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.reclaim.store(true, Ordering::Release);
        if !self.parking.wake_one(DATA) {
            self.parking.wake_one(PROGRESS);
        }
    }
}
