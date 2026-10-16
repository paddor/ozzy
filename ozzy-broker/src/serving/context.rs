//! Transport context and independently controlled protocol/time observations.

use omq_tokio::Context;
use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
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
    /// The clock of an SDK owner inside this broker.
    pub(super) fn sdk_clock(&self) -> ozzy_runtime::replicated::SdkClock {
        match self {
            Self::System => ozzy_runtime::replicated::SdkClock::default(),
            #[cfg(feature = "simulation")]
            Self::Manual { clock, .. } => clock.clone(),
        }
    }

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

    /// One alarm on this timer's clock, re-armed in place for each deadline.
    pub(super) fn alarm(&self) -> Alarm {
        let wait = match self {
            Self::System(start) => Wait::System {
                start: *start,
                sleep: Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
                    *start,
                ))),
            },
            #[cfg(feature = "simulation")]
            Self::Manual { clock, origin } => Wait::Manual {
                clock: clock.clone(),
                origin: *origin,
                until: None,
            },
        };
        Alarm { wait, armed: None }
    }
}

/// Fires once the clock reaches the deadline it was last polled with. Polling
/// with a new deadline moves the same registration instead of making another.
pub(super) struct Alarm {
    wait: Wait,
    armed: Option<Duration>,
}

enum Wait {
    System {
        start: Instant,
        sleep: Pin<Box<tokio::time::Sleep>>,
    },
    #[cfg(feature = "simulation")]
    Manual {
        clock: ozzy_runtime::replicated::SdkClock,
        origin: Duration,
        until: Option<Pin<Box<dyn Future<Output = ()>>>>,
    },
}

impl Alarm {
    /// Ready once `deadline` since the start has passed. Without a deadline,
    /// never ready.
    pub(super) fn poll_at(
        &mut self,
        cx: &mut task::Context<'_>,
        deadline: Option<Duration>,
    ) -> Poll<()> {
        let Some(deadline) = deadline else {
            self.armed = None;
            return Poll::Pending;
        };
        // Moving the registration costs more than waking early once, so a
        // later deadline keeps the earlier wakeup and re-arms after it.
        if self.armed.is_none_or(|armed| deadline < armed) {
            self.armed = Some(deadline);
            self.wait.arm(deadline);
        }
        let fired = self.wait.poll(cx);
        if fired.is_ready() {
            self.armed = None;
        }
        fired
    }
}

impl Wait {
    fn arm(&mut self, deadline: Duration) {
        match self {
            Self::System { start, sleep } => {
                if let Some(at) = start.checked_add(deadline) {
                    sleep.as_mut().reset(at.into());
                }
            }
            #[cfg(feature = "simulation")]
            Self::Manual {
                clock,
                origin,
                until,
            } => {
                let clock = clock.clone();
                let at = origin.saturating_add(deadline);
                *until = Some(Box::pin(async move { clock.until(at).await }));
            }
        }
    }

    fn poll(&mut self, cx: &mut task::Context<'_>) -> Poll<()> {
        match self {
            Self::System { sleep, .. } => sleep.as_mut().poll(cx),
            #[cfg(feature = "simulation")]
            Self::Manual { until, .. } => until
                .as_mut()
                .map_or(Poll::Pending, |until| until.as_mut().poll(cx)),
        }
    }
}
