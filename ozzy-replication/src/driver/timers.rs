//! The deadlines and timestamps the driver's timeouts count from.

use super::Timing;
use std::time::Duration;

/// Every instant the driver schedules by, on the adapter's clock.
#[derive(Debug)]
pub(super) struct Timers {
    /// When a backup stops waiting for its primary.
    pub(super) contact_deadline: Duration,
    /// When a backup of this view last acknowledged, or when the role began.
    pub(super) backup_heard_at: Duration,
    /// When outstanding commit or activation work counts as stalled.
    pub(super) progress_deadline: Duration,
    /// When accepted but not durable operations count as stalled.
    pub(super) persistence_deadline: Duration,
    /// When busy local storage last completed work; `None` while idle.
    pub(super) storage_busy_since: Option<Duration>,
    /// When the current election, installation, or activation gives up.
    pub(super) election_deadline: Duration,
    /// The election timeout now in force. It grows after failed elections.
    pub(super) election_delay: Duration,
    /// The next heartbeat of a primary or election resend.
    pub(super) retry_at: Duration,
}

impl Timers {
    /// Start every timeout at `now`, with `progress` until the first
    /// progress deadline and `retry` until the first send.
    pub(super) fn new(now: Duration, timing: Timing, progress: Duration, retry: Duration) -> Self {
        let mut timers = Self {
            contact_deadline: now,
            backup_heard_at: now,
            progress_deadline: now,
            persistence_deadline: now,
            storage_busy_since: None,
            election_deadline: now,
            election_delay: timing.election_timeout,
            retry_at: now.saturating_add(retry),
        };
        timers.restart(now, timing, progress);
        timers
    }

    /// Restart every timeout at `now`, with `progress` until the progress
    /// deadline. The next send and the storage watch keep their own time.
    pub(super) fn restart(&mut self, now: Duration, timing: Timing, progress: Duration) {
        self.contact_deadline = now.saturating_add(timing.primary_timeout);
        self.backup_heard_at = now;
        self.persistence_deadline = now.saturating_add(timing.primary_timeout);
        self.progress_deadline = now.saturating_add(progress);
        self.election_deadline = now.saturating_add(self.election_delay);
    }
}
