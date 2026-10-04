//! Time advances while the full workload, startup, and physical schedules run.
use ozzy_runtime::replicated::SdkClock;
use serde::Serialize;
use std::{future::Future, time::Duration};

/// Optional virtual duration with separate real-time advancement pacing.
#[derive(Clone, Debug, Serialize)]
pub struct Time {
    /// Stop generating at this virtual duration, or use ordinary elapsed time.
    pub duration: Option<Duration>,
    /// Real interval between manual observations.
    pub tick: Duration,
    /// Virtual advancement per observation. Transport and disk still run normally.
    pub step: Duration,
}

impl Default for Time {
    fn default() -> Self {
        Self {
            duration: None,
            tick: Duration::from_millis(2),
            step: Duration::from_millis(10),
        }
    }
}

impl Time {
    pub(super) fn validate(&self) -> Result<(), String> {
        if self.duration.is_some_and(|duration| duration.is_zero())
            || self.tick.is_zero()
            || self.step.is_zero()
        {
            return Err("virtual duration, clock tick and clock step must be positive".into());
        }
        Ok(())
    }

    pub(super) async fn drive<T>(&self, clock: &SdkClock, operation: impl Future<Output = T>) -> T {
        if self.duration.is_none() {
            return operation.await;
        }
        let mut operation = std::pin::pin!(operation);
        let mut tick = tokio::time::interval(self.tick);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                result = &mut operation => return result,
                _ = tick.tick() => clock.advance(clock.now().saturating_add(self.step)).unwrap(),
            }
        }
    }
}
