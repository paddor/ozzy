//! One simulated device per broker. Each accepted job completes after a
//! seeded delay in 1 ms steps; jobs due in one step complete in seeded order.

use crate::seeds::{Rng, Trace};
use ozzy_io::{
    Limits, Local, Operation, Quota,
    simulation::{Config, Controller, Effect, Image, ImageLimits, JobId, Stage},
};
use std::{
    collections::BTreeMap,
    future::Future,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

/// The unit a power loss persists whole or not at all, as a drive's atomic
/// sector write does.
const SECTOR: usize = 512;

#[derive(Debug)]
pub(crate) struct Disk {
    controller: Controller,
    rng: Rng,
    mean: f64,
    due: BTreeMap<JobId, u64>,
    /// No job completes before this step.
    stalled_until: u64,
    /// Every write and sync fails, as on a failing device.
    failing: bool,
    /// An injected failure was delivered.
    failed: bool,
}

impl Disk {
    pub(crate) fn new(seed: u64, mean: f64) -> (Self, Local) {
        Self::with_image(seed, mean, Image::default())
    }

    /// Lose power, then start a fresh device over what reached media. Each
    /// queued write may have reached the cache unreported, even an `O_DSYNC`
    /// one. Each cached sector reaches media at one seeded chance per loss,
    /// so writes tear and land out of order.
    pub(crate) fn power_loss(mut self) -> (Self, Local) {
        self.run_queued_writes(Effect::SkipBarrier);
        let chance = (1 + self.rng.below(3)) as f64 / 4.0;
        for (path, run) in self.controller.image().unsynced_runs(SECTOR) {
            for start in run.clone().step_by(SECTOR) {
                if self.rng.chance(chance) {
                    let sector = start..run.end.min(start + SECTOR);
                    self.controller
                        .persist_range(&path, sector)
                        .expect("persist sector");
                }
            }
        }
        let seed = self.rng.next();
        let (image, _) = self.controller.crash(true).expect("simulated power loss");
        Self::with_image(seed, self.mean, image)
    }

    /// Kill the process, then start a fresh device incarnation over the same
    /// cache. Each queued write may still run, as the kernel finishes it,
    /// unless the device is failing. Unsynchronized bytes stay cached.
    pub(crate) fn process_crash(mut self) -> (Self, Local) {
        if !self.failing {
            self.run_queued_writes(Effect::Normal);
        }
        let seed = self.rng.next();
        let (image, _) = self
            .controller
            .crash(false)
            .expect("simulated process crash");
        Self::with_image(seed, self.mean, image)
    }

    /// Run each queued write at an even chance, unreported.
    fn run_queued_writes(&mut self, effect: Effect) {
        for (id, stage) in self.controller.jobs() {
            if stage == Stage::Queued
                && self.controller.operation(id).is_some_and(|operation| {
                    matches!(unprotected(operation), Operation::Write { .. })
                })
                && self.rng.chance(0.5)
            {
                self.controller.execute(id, effect).expect("execute");
            }
        }
    }

    fn with_image(seed: u64, mean: f64, image: Image) -> (Self, Local) {
        let (controller, mut clients) = Controller::new(
            Config {
                limits: Limits {
                    shards: 1,
                    data: Quota {
                        operations: 8,
                        bytes: 8 * 1024 * 1024,
                    },
                    progress: Quota {
                        operations: 8,
                        bytes: 8 * 1024 * 1024,
                    },
                },
                handles: 96,
                // Room for every segment the journal may select and its index.
                image: ImageLimits {
                    nodes: 2048,
                    directory_entries: 4096,
                    file_bytes: 1024 * 1024,
                    total_bytes: 256 * 1024 * 1024,
                },
                // Drained every tick; one tick schedules far fewer events.
                trace_events: 100_000,
            },
            image,
        )
        .expect("simulated device");
        let disk = Self {
            controller,
            rng: Rng::new(seed),
            mean,
            due: BTreeMap::new(),
            stalled_until: 0,
            failing: false,
            failed: false,
        };
        (disk, Local::new(clients.remove(0)))
    }

    /// Complete no job before step `until`, as a stalled device does. Jobs
    /// due meanwhile complete once it ends.
    pub(crate) fn stall(&mut self, until: u64) {
        self.stalled_until = until;
    }

    /// Fail every later write and sync, as a failing device does: each one
    /// before or after its physical effect, or after a write prefix. A
    /// restart gets a healthy device.
    pub(crate) fn fail_writes(&mut self) {
        self.failing = true;
    }

    /// Whether an injected failure was delivered.
    pub(crate) fn failed(&self) -> bool {
        self.failed
    }

    /// Complete jobs due by step `at`. New jobs draw their completion step
    /// first.
    pub(crate) fn step(&mut self, at: u64, broker: usize, trace: &mut Trace) {
        let jobs = self.controller.jobs();
        for (id, _) in &jobs {
            let rng = &mut self.rng;
            let mean = self.mean;
            self.due
                .entry(*id)
                .or_insert_with(|| at + rng.exponential(mean));
        }
        if at < self.stalled_until {
            self.controller.take_trace();
            return;
        }
        let mut ready: Vec<_> = jobs
            .into_iter()
            .filter(|(id, _)| self.due[id] <= at)
            .collect();
        self.rng.shuffle(&mut ready);
        for (id, stage) in ready {
            self.due.remove(&id);
            self.finish(id, stage);
            trace.word(at);
            trace.word(broker as u64);
            trace.word(id.number);
        }
        self.controller.take_trace();
    }

    /// Complete everything accepted so far. Startup and shutdown only.
    pub(crate) fn settle(&mut self) {
        for (id, stage) in self.controller.jobs() {
            self.due.remove(&id);
            self.finish(id, stage);
        }
        self.controller.take_trace();
    }

    fn finish(&mut self, id: JobId, stage: Stage) {
        if stage == Stage::Queued {
            let effect = self.effect(id);
            self.controller.execute(id, effect).expect("execute");
        }
        self.controller.deliver(id).expect("deliver");
    }

    /// Normal, or an injected failure of a write or sync.
    fn effect(&mut self, id: JobId) -> Effect {
        if !self.failing {
            return Effect::Normal;
        }
        let length = match self.controller.operation(id).map(unprotected) {
            Some(Operation::Write { data, .. }) => data.len(),
            Some(Operation::Sync { .. }) => 0,
            _ => return Effect::Normal,
        };
        self.failed = true;
        let error = io::ErrorKind::Other;
        match self.rng.below(3) {
            0 => Effect::FailBefore(error),
            1 if length > 0 => Effect::WriteThenError {
                bytes: self.rng.below(length as u64) as usize,
                error,
            },
            _ => Effect::FailAfter(error),
        }
    }

    /// Drive a startup future, completing its storage jobs as it waits.
    pub(crate) fn drive<T>(&mut self, future: impl Future<Output = T>) -> T {
        let flag = Arc::new(Flag::default());
        let waker = Waker::from(flag.clone());
        let mut future = std::pin::pin!(future);
        for _ in 0..10_000 {
            flag.take();
            if let Poll::Ready(value) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
                return value;
            }
            if !flag.take() {
                self.settle();
            }
        }
        panic!("startup storage stalled");
    }
}

/// The operation inside any handles held until it runs.
fn unprotected(operation: &Operation) -> &Operation {
    match operation {
        Operation::Protected { operation, .. } => unprotected(operation),
        operation => operation,
    }
}

/// A waker that only records that it fired.
#[derive(Debug, Default)]
pub(crate) struct Flag(AtomicBool);

impl Flag {
    /// Whether it fired since the last call.
    pub(crate) fn take(&self) -> bool {
        self.0.swap(false, Ordering::Relaxed)
    }

    pub(crate) fn is_set(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}
