//! Loom model of std OnceLock's acquire/release publication, not its internals.
//! The standard cell owns immutable storage; Loom tracks when it becomes visible.
use loom::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub(crate) struct OnceLock<T> {
    value: std::sync::OnceLock<T>,
    ready: AtomicBool,
}

impl<T> OnceLock<T> {
    pub(crate) fn new() -> Self {
        Self {
            value: std::sync::OnceLock::new(),
            ready: AtomicBool::new(false),
        }
    }

    pub(crate) fn get(&self) -> Option<&T> {
        self.ready
            .load(Ordering::Acquire)
            .then(|| self.value.get().expect("published failure"))
    }

    pub(crate) fn get_or_init(&self, initialize: impl FnOnce() -> T) -> &T {
        let value = self.value.get_or_init(initialize);
        self.ready.store(true, Ordering::Release);
        value
    }
}
