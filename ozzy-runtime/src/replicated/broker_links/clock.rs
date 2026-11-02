use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::signal::StateSignal;
use tokio::runtime::Handle;

/// Shared SDK timer observations. Manual clocks let tests advance retries and
/// deadlines without waiting for wall time or changing record identity.
#[derive(Clone, Debug)]
pub struct SdkClock {
    state: Arc<State>,
}

/// An SDK clock whose real-time timers belong to the SDK owner thread.
#[derive(Clone, Debug)]
pub(in crate::replicated) struct OwnerClock {
    clock: SdkClock,
    owner: Handle,
}

#[derive(Debug)]
struct State {
    start: Instant,
    manual: bool,
    manual_nanos: AtomicU64,
    changed: StateSignal,
    /// Deadlines that manual waiters wait for, each with its waiter count.
    deadlines: Mutex<BTreeMap<Duration, usize>>,
}

/// Clock mode or monotonic observation was invalid.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("invalid SDK clock observation")]
pub struct ClockError;

impl Default for SdkClock {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SdkClock {
    fn new(manual: bool) -> Self {
        Self {
            state: Arc::new(State {
                start: Instant::now(),
                manual,
                manual_nanos: AtomicU64::new(0),
                changed: StateSignal::default(),
                deadlines: Mutex::default(),
            }),
        }
    }

    pub(super) fn bind(&self, owner: Handle) -> OwnerClock {
        OwnerClock {
            clock: self.clone(),
            owner,
        }
    }

    /// Start deterministic observations at zero. All clones share advancement.
    pub fn manual() -> Self {
        Self::new(true)
    }

    /// Monotonic time since this SDK clock started.
    pub fn now(&self) -> Duration {
        if self.state.manual {
            Duration::from_nanos(self.state.manual_nanos.load(Ordering::Acquire))
        } else {
            self.state.start.elapsed()
        }
    }

    /// Advance a manual clock. Equal observations are harmless; backward time
    /// and advancing a real clock fail without modifying its current observation.
    #[allow(
        deprecated,
        reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
    )]
    pub fn advance(&self, now: Duration) -> Result<(), ClockError> {
        if !self.state.manual {
            return Err(ClockError);
        }
        let nanos = u64::try_from(now.as_nanos()).map_err(|_| ClockError)?;
        let current = self
            .state
            .manual_nanos
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (nanos >= current).then_some(nanos)
            })
            .map_err(|_| ClockError)?;
        if nanos != current {
            self.state.changed.notify_changed();
        }
        Ok(())
    }

    /// The earliest deadline a waiter of this manual clock still waits for.
    /// Nothing that waits on this clock can happen before it, so simulated
    /// time can step straight there.
    pub fn next_deadline(&self) -> Option<Duration> {
        let now = self.now();
        let deadlines = self.state.deadlines();
        deadlines
            .range((std::ops::Bound::Excluded(now), std::ops::Bound::Unbounded))
            .next()
            .map(|(&deadline, _)| deadline)
    }
}

impl State {
    fn deadlines(&self) -> std::sync::MutexGuard<'_, BTreeMap<Duration, usize>> {
        self.deadlines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// One manual waiter's deadline, listed while it waits.
struct Waiting<'a> {
    state: &'a State,
    deadline: Duration,
}

impl<'a> Waiting<'a> {
    fn new(state: &'a State, deadline: Duration) -> Self {
        *state.deadlines().entry(deadline).or_default() += 1;
        Self { state, deadline }
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let mut deadlines = self.state.deadlines();
        if let Some(waiters) = deadlines.get_mut(&self.deadline) {
            *waiters -= 1;
            if *waiters == 0 {
                deadlines.remove(&self.deadline);
            }
        }
    }
}

impl SdkClock {
    /// Wait until this clock reaches `deadline`. A manual clock wakes when it
    /// is advanced past it; a real one sleeps on the current Tokio runtime.
    pub async fn until(&self, deadline: Duration) {
        if self.state.manual {
            let _waiting = Waiting::new(&self.state, deadline);
            self.state
                .changed
                .wait_for(|| (self.now() >= deadline).then_some(()))
                .await;
        } else {
            tokio::time::sleep(deadline.saturating_sub(self.now())).await;
        }
    }
}

impl OwnerClock {
    /// Monotonic time since the underlying SDK clock started.
    pub(in crate::replicated) fn now(&self) -> Duration {
        self.clock.now()
    }

    pub(in crate::replicated) async fn until(&self, deadline: Duration) {
        if self.clock.state.manual {
            self.clock.until(deadline).await;
        } else {
            let sleep = {
                // Callers can poll on any executor. Only timer construction
                // enters the SDK owner; its guard must end before suspension.
                let _owner = self.owner.enter();
                tokio::time::sleep(deadline.saturating_sub(self.now()))
            };
            sleep.await;
        }
    }

    /// [`Self::until`] `deadline`, or never without one.
    pub(in crate::replicated) async fn until_due(&self, deadline: Option<Duration>) {
        match deadline {
            Some(deadline) => self.until(deadline).await,
            None => std::future::pending().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn manual_deadline_survives_cancellation_and_wakes_after_advancement() {
        let manual = SdkClock::manual();
        let clock = manual.bind(Handle::current());
        let mut first = Box::pin(clock.until(Duration::from_secs(2)));
        assert!(futures::poll!(first.as_mut()).is_pending());
        manual.advance(Duration::from_secs(1)).unwrap();
        assert!(manual.advance(Duration::ZERO).is_err());
        assert_eq!(clock.now(), Duration::from_secs(1));
        drop(first);
        let mut replacement = Box::pin(clock.until(Duration::from_secs(2)));
        assert!(futures::poll!(replacement.as_mut()).is_pending());
        manual.clone().advance(Duration::from_secs(2)).unwrap();
        replacement.await;
        assert!(SdkClock::default().advance(Duration::ZERO).is_err());
    }

    #[tokio::test]
    async fn manual_waiters_list_their_deadlines_until_they_end() {
        let clock = SdkClock::manual();
        assert_eq!(clock.next_deadline(), None);
        let mut late = Box::pin(clock.until(Duration::from_secs(5)));
        let mut early = Box::pin(clock.until(Duration::from_secs(2)));
        let mut again = Box::pin(clock.until(Duration::from_secs(2)));
        for waiter in [&mut late, &mut early, &mut again] {
            assert!(futures::poll!(waiter.as_mut()).is_pending());
        }
        assert_eq!(clock.next_deadline(), Some(Duration::from_secs(2)));
        drop(early);
        assert_eq!(clock.next_deadline(), Some(Duration::from_secs(2)));
        clock.advance(Duration::from_secs(2)).unwrap();
        again.await;
        assert_eq!(clock.next_deadline(), Some(Duration::from_secs(5)));
        drop(late);
        assert_eq!(clock.next_deadline(), None);
    }
}
