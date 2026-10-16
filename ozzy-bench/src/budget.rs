//! Time limits of one timed run, derived from its phases.
//!
//! The controller, its workers, remote backstops, and the comparison driver
//! share these limits, so a longer run never meets a fixed cap.

use std::time::Duration;

/// One control step: a worker connects, a broker starts serving, or a
/// worker waits for its next command.
pub const CONTROL_STEP: Duration = Duration::from_secs(60);
/// Workers encode and transfer their reports after the drain deadline.
pub const REPORTS: Duration = Duration::from_secs(5);
/// A stopping process exits, or a remote one gets between TERM and KILL.
pub const EXIT_GRACE: Duration = Duration::from_secs(5);
/// Default final confirmation and delivery drain, in seconds.
pub const DEFAULT_DRAIN_SECS: u64 = 10;
/// A broker formats its stores and zero-fills where each partition's first
/// appends land. Three brokers on one busy disk can take minutes.
pub const INITIALIZE: Duration = Duration::from_secs(180);
/// Brokers prepare, initialize, and start serving before workers connect.
pub const SETUP: Duration = Duration::from_secs(2 * CONTROL_STEP.as_secs() + INITIALIZE.as_secs());

/// Steps a process takes outside the measured phases: it connects, starts
/// serving, and waits for the start command.
const SETUP_STEPS: u32 = 3;
/// A broker drains once for confirmations, then once to observe its threads.
const DRAINS: u32 = 2;

/// The measured phases of one timed run.
#[derive(Clone, Copy, Debug)]
pub struct RunBudget {
    warmup: Duration,
    duration: Duration,
    drain: Duration,
}

impl RunBudget {
    /// Phases from the run's arguments, in seconds.
    #[must_use]
    pub fn new(warmup: f64, duration: f64, drain: u64) -> Self {
        Self {
            warmup: Duration::from_secs_f64(warmup),
            duration: Duration::from_secs_f64(duration),
            drain: Duration::from_secs(drain),
        }
    }

    /// From the start command until the last report arrives.
    #[must_use]
    pub fn measured(self) -> Duration {
        self.warmup + self.duration + self.drain + REPORTS
    }

    /// The controller's limit once every worker started, setup included,
    /// and how long a broker waits for its drain command.
    #[must_use]
    pub fn watchdog(self) -> Duration {
        SETUP + self.measured()
    }

    /// The life of one process of the run, controller or worker: setup,
    /// the watchdog, the broker's drains, and its exit.
    #[must_use]
    pub fn lifetime(self) -> Duration {
        CONTROL_STEP * SETUP_STEPS + self.watchdog() + self.drain * DRAINS + EXIT_GRACE
    }

    /// The life of an external server that starts before one case and stops
    /// after it.
    #[must_use]
    pub fn serving(self) -> Duration {
        CONTROL_STEP + self.lifetime()
    }
}

/// Render a duration as a `timeout(1)` argument in whole seconds, rounded up.
#[must_use]
pub fn timeout_arg(limit: Duration) -> String {
    format!("{}s", limit.as_secs() + u64::from(limit.subsec_nanos() > 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_remote_lifetime_outlasts_the_watchdog_of_any_run_length() {
        for duration in [0.0, 30.0, 1200.0, 86_400.0] {
            let budget = RunBudget::new(2.0, duration, DEFAULT_DRAIN_SECS);
            assert!(budget.measured() >= Duration::from_secs_f64(duration));
            assert!(budget.watchdog() > budget.measured());
            assert!(budget.lifetime() > budget.watchdog());
            assert!(budget.serving() > budget.lifetime());
        }
    }

    #[test]
    fn timeout_arguments_round_up_to_whole_seconds() {
        assert_eq!(timeout_arg(Duration::from_secs(5)), "5s");
        assert_eq!(timeout_arg(Duration::from_millis(5_001)), "6s");
    }
}
