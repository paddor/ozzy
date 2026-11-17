//! The order in which waiting jobs take a full lane.

use super::*;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

type Job<'a> = Pin<Box<dyn Future<Output = io::Result<Completed>> + 'a>>;

/// One job on the lane and the wakes it got.
struct Task<'a> {
    job: Job<'a>,
    wakes: Arc<Wakes>,
}

impl<'a> Task<'a> {
    fn new(lane: &'a crate::Local, operation: Operation) -> Self {
        Self {
            job: Box::pin(lane.execute(Class::Progress, operation)),
            wakes: Arc::default(),
        }
    }

    fn poll(&mut self) -> Poll<io::Result<Completed>> {
        let waker = Waker::from(self.wakes.clone());
        self.job.as_mut().poll(&mut Context::from_waker(&waker))
    }

    fn wakes(&self) -> usize {
        self.wakes.0.load(Ordering::Relaxed)
    }
}

fn directory(name: &str) -> Operation {
    Operation::CreateDirectory {
        path: format!("/{name}").into(),
    }
}

/// A job that retains about `bytes`.
fn listing(bytes: usize) -> Operation {
    Operation::ReadDirectory {
        path: "/".into(),
        max_entries: 0,
        max_name_bytes: bytes,
    }
}

/// A lane of four operations and 256 KiB for the class the tests use.
fn lane() -> (Controller, crate::Local) {
    let (controller, mut clients) = Controller::new(config(), Image::default()).unwrap();
    (controller, crate::Local::new(clients.remove(0)))
}

/// Finish the queued job `index` and drop its result, good or bad, which
/// frees its share of the lane.
fn finish(controller: &mut Controller, index: usize, task: &mut Task<'_>) {
    let id = controller.jobs()[index].0;
    controller.execute(id, Effect::Normal).unwrap();
    controller.deliver(id).unwrap();
    assert!(task.poll().is_ready());
}

#[test]
fn waiting_jobs_take_a_full_lane_in_the_order_they_came() {
    let (mut controller, lane) = lane();
    let names = ["a", "b", "c", "d", "e", "f", "g"];
    let mut tasks: Vec<_> = (names.iter())
        .map(|name| Task::new(&lane, directory(name)))
        .collect();
    for task in &mut tasks {
        assert!(task.poll().is_pending());
    }
    assert_eq!(controller.jobs().len(), 4, "the lane is full");
    assert!(!lane.admission().operation_free(0, Class::Progress));
    let (running, waiting) = tasks.split_at_mut(4);
    assert!(waiting.iter().all(|task| task.wakes() == 0));

    finish(&mut controller, 0, &mut running[0]);
    let wakes: Vec<_> = waiting.iter().map(Task::wakes).collect();
    assert_eq!(wakes, [1, 0, 0], "only the first waiting job hears of it");
    assert!(waiting[0].poll().is_pending());
    assert_eq!(controller.jobs().len(), 4, "it took the free operation");
    let wakes: Vec<_> = waiting.iter().map(Task::wakes).collect();
    assert_eq!(wakes, [1, 1, 0], "the next one listens from now on");

    // A waiting job that gives up lets the one behind it listen.
    assert!(waiting[1].poll().is_pending());
    let (gone, last) = waiting.split_at_mut(2);
    gone[1].job = Box::pin(std::future::pending());
    assert_eq!(last[0].wakes(), 1);
    assert!(last[0].poll().is_pending());
    finish(&mut controller, 0, &mut running[1]);
    assert_eq!(last[0].wakes(), 2);
    assert!(last[0].poll().is_pending());
    assert_eq!(controller.jobs().len(), 4);
}

#[test]
fn a_smaller_job_passes_one_that_lacks_only_bytes() {
    let (mut controller, lane) = lane();
    let mut held = Task::new(&lane, listing(100 * 1024));
    let mut large = Task::new(&lane, listing(200 * 1024));
    let mut small = Task::new(&lane, directory("small"));
    assert!(held.poll().is_pending());
    assert!(large.poll().is_pending());
    assert_eq!(controller.jobs().len(), 1, "the large job does not fit");
    // An operation is free, so the small job tries at once.
    assert!(small.poll().is_pending());
    assert_eq!(controller.jobs().len(), 2);

    // With no operation free a new job sleeps behind the large one.
    let mut fill = [
        Task::new(&lane, directory("a")),
        Task::new(&lane, directory("b")),
    ];
    for task in &mut fill {
        assert!(task.poll().is_pending());
    }
    assert!(!lane.admission().operation_free(0, Class::Progress));
    let mut sleeper = Task::new(&lane, directory("late"));
    assert!(sleeper.poll().is_pending());
    assert_eq!((controller.jobs().len(), sleeper.wakes()), (4, 0));
    // An operation comes free. The large job still lacks bytes and lets
    // the late one pass.
    finish(&mut controller, 1, &mut small);
    assert_eq!((large.wakes(), sleeper.wakes()), (1, 0));
    assert!(large.poll().is_pending());
    assert_eq!(sleeper.wakes(), 1);
    assert!(sleeper.poll().is_pending());
    assert_eq!(controller.jobs().len(), 4, "the late job runs");
    // The bytes come free, and the large job runs.
    finish(&mut controller, 0, &mut held);
    assert!(large.poll().is_pending());
    assert_eq!(controller.jobs().len(), 4);
}

#[test]
fn a_waiting_job_nobody_polls_holds_no_one_up() {
    let (mut controller, lane) = lane();
    let names = ["a", "b", "c", "d", "e", "f"];
    let mut tasks: Vec<_> = (names.iter())
        .map(|name| Task::new(&lane, directory(name)))
        .collect();
    for task in &mut tasks {
        assert!(task.poll().is_pending());
    }
    let (running, waiting) = tasks.split_at_mut(4);
    finish(&mut controller, 0, &mut running[0]);
    // The first waiting job heard of the free operation and does nothing.
    assert_eq!((waiting[0].wakes(), waiting[1].wakes()), (1, 0));
    // The one behind it is polled anyway and takes it.
    assert!(waiting[1].poll().is_pending());
    assert_eq!(controller.jobs().len(), 4);
}
