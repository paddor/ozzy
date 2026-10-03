use std::{
    sync::{
        Arc,
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
    owner: Option<Handle>,
}

#[derive(Debug)]
struct State {
    start: Instant,
    manual: bool,
    manual_nanos: AtomicU64,
    changed: StateSignal,
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
            }),
            owner: None,
        }
    }

    pub(super) fn bind_owner(&mut self, owner: Handle) {
        self.owner = Some(owner);
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

    pub(in crate::replicated) async fn until(&self, deadline: Duration) {
        if self.state.manual {
            self.state
                .changed
                .wait_for(|| (self.now() >= deadline).then_some(()))
                .await;
        } else {
            let sleep = {
                // Callers can poll on any executor. Only timer construction
                // enters the SDK owner; its guard must end before suspension.
                let _owner = self
                    .owner
                    .as_ref()
                    .expect("SDK clock owner missing")
                    .enter();
                tokio::time::sleep(deadline.saturating_sub(self.now()))
            };
            sleep.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn manual_deadline_survives_cancellation_and_wakes_after_advancement() {
        let clock = SdkClock::manual();
        let mut first = Box::pin(clock.until(Duration::from_secs(2)));
        assert!(futures::poll!(first.as_mut()).is_pending());
        clock.advance(Duration::from_secs(1)).unwrap();
        assert!(clock.advance(Duration::ZERO).is_err());
        assert_eq!(clock.now(), Duration::from_secs(1));
        drop(first);
        let mut replacement = Box::pin(clock.until(Duration::from_secs(2)));
        assert!(futures::poll!(replacement.as_mut()).is_pending());
        clock.clone().advance(Duration::from_secs(2)).unwrap();
        replacement.await;
        assert!(SdkClock::default().advance(Duration::ZERO).is_err());
    }
}
