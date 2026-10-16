//! SDK retry deadlines and pacing.

use std::time::Duration;

use super::{BrokerLinkError, Error, TopicRoutes};
use ozzy_proto::{directory::RouteState, nack::RetryClass};

/// Shared classification for membership and cumulative progress. A selected
/// route also validates authority hints before they influence another attempt.
pub(crate) fn classify(
    error: &BrokerLinkError,
    route: Option<&RouteState>,
) -> Result<Option<RetryClass>, BrokerLinkError> {
    let class = match error {
        BrokerLinkError::Timeout | BrokerLinkError::Session | BrokerLinkError::Transport(_) => {
            RetryClass::UnknownOutcome
        }
        BrokerLinkError::Rejected { retry, .. } if *retry != RetryClass::Permanent => *retry,
        _ => return Ok(None),
    };
    if class != RetryClass::AfterBackoff
        && let (BrokerLinkError::Rejected { hint, .. }, Some(route)) = (error, route)
    {
        TopicRoutes::validate_hint(*hint, route)?;
    }
    Ok(Some(class))
}

/// Client failure-detection and retry pacing, independent of replica election timers.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Deadline for one peer's HELLO/WELCOME attempt.
    pub handshake_timeout: Duration,
    /// Deadline before an unanswered APPEND is treated as an unknown outcome.
    /// A slow disk is not packet loss; choose this above expected commit tails.
    pub response_timeout: Duration,
    /// Delay after every configured voter has been tried without success.
    pub initial_backoff: Duration,
    /// Cap on exponential full-round backoff. Payload retries never bypass it.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(1),
            response_timeout: Duration::from_secs(2),
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(1),
        }
    }
}

impl RetryPolicy {
    pub(super) fn validate(self) -> Result<(), Error> {
        let now = tokio::time::Instant::now();
        if [
            self.handshake_timeout,
            self.response_timeout,
            self.initial_backoff,
            self.max_backoff,
        ]
        .iter()
        .any(|&duration| duration.is_zero() || now.checked_add(duration).is_none())
            || self.initial_backoff > self.max_backoff
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
