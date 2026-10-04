//! Transport context and independently controlled protocol/time observations.

use omq_tokio::Context;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Shared OMQ transport with ordinary or explicitly simulated broker time.
#[derive(Clone, Debug)]
pub struct ServingContext {
    pub(super) omq: Context,
    pub(super) time: Time,
}

impl From<Context> for ServingContext {
    fn from(omq: Context) -> Self {
        Self {
            omq,
            time: Time::default(),
        }
    }
}

impl ServingContext {
    /// Use the existing shared manual clock for protocol deadlines and append
    /// timestamps. Physical I/O execution and completion remain independent.
    #[cfg(feature = "simulation")]
    pub fn simulated(
        omq: Context,
        clock: ozzy_runtime::replicated::SdkClock,
        epoch_millis: u64,
    ) -> Self {
        Self {
            omq,
            time: Time::Manual {
                clock,
                epoch_millis,
            },
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) enum Time {
    #[default]
    System,
    #[cfg(feature = "simulation")]
    Manual {
        clock: ozzy_runtime::replicated::SdkClock,
        epoch_millis: u64,
    },
}

pub(super) enum Timer {
    System(Instant),
    #[cfg(feature = "simulation")]
    Manual {
        clock: ozzy_runtime::replicated::SdkClock,
        origin: Duration,
    },
}

impl Time {
    pub(super) fn start(&self) -> Timer {
        match self {
            Self::System => Timer::System(Instant::now()),
            #[cfg(feature = "simulation")]
            Self::Manual { clock, .. } => Timer::Manual {
                clock: clock.clone(),
                origin: clock.now(),
            },
        }
    }

    pub(super) fn timestamp(&self) -> u64 {
        match self {
            Self::System => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            #[cfg(feature = "simulation")]
            Self::Manual {
                clock,
                epoch_millis,
            } => epoch_millis
                .saturating_add(u64::try_from(clock.now().as_millis()).unwrap_or(u64::MAX)),
        }
    }
}

impl Timer {
    pub(super) fn elapsed(&self) -> Duration {
        match self {
            Self::System(start) => start.elapsed(),
            #[cfg(feature = "simulation")]
            Self::Manual { clock, origin } => clock.now().saturating_sub(*origin),
        }
    }
}
