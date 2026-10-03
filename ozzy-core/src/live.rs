//! Repair pacing for a loss-tolerant live stream beside an ordered replay path.

use std::time::Duration;

mod cursor;
pub use cursor::{CursorError, LiveCursor, Publication, Replay, Replayed};

/// Distinguish an observed gap from a live stream merely behind the publisher.
///
/// Applies to operation numbers or record offsets. The adapter owns payloads,
/// validates history, and resets this state when replacing the stream's scope.
/// A repair bound supplies neither receive credit nor confirmation evidence.
#[derive(Debug)]
pub struct LiveProgress {
    quiet: Duration,
    last: Option<(u64, Duration)>,
}

impl LiveProgress {
    /// Silence required before probing repairs a possibly lost final message.
    pub const fn new(quiet: Duration) -> Self {
        Self { quiet, last: None }
    }

    /// Record newly retained live data, never duplicate or replay-only traffic.
    pub fn advanced(&mut self, through: u64, now: Duration) {
        if self.last.is_none_or(|(previous, _)| through > previous) {
            self.last = Some((through, now));
        }
    }

    /// Time from which the stream counts as silent; none before any live progress.
    pub fn silent_at(&self) -> Option<Duration> {
        self.last.map(|(_, at)| at.saturating_add(self.quiet))
    }

    /// Forget old live progress after an authority change or receive retraction.
    pub fn reset(&mut self) {
        self.last = None;
    }

    /// Cap repair at a held message's predecessor. Otherwise suppress speculative
    /// repair while live data advances; silence permits repair through the probe.
    /// The received cursor is the contiguous prefix across both delivery paths.
    pub fn repair_limit(
        &self,
        received: u64,
        held_predecessor: Option<u64>,
        now: Duration,
    ) -> Option<u64> {
        held_predecessor.or_else(|| {
            self.last
                .filter(|(_, at)| now.saturating_sub(*at) < self.quiet)
                .map(|_| received)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_progress_defers_speculative_repair_but_never_an_observed_gap() {
        let mut live = LiveProgress::new(Duration::from_secs(1));
        assert_eq!(live.repair_limit(0, None, Duration::ZERO), None);
        live.advanced(5, Duration::ZERO);
        assert_eq!(
            live.repair_limit(5, None, Duration::from_millis(999)),
            Some(5)
        );
        assert_eq!(live.repair_limit(5, Some(9), Duration::ZERO), Some(9));
        assert_eq!(live.repair_limit(5, Some(5), Duration::ZERO), Some(5));
        // Duplicate/reordered messages cannot postpone a missing final message.
        live.advanced(5, Duration::from_millis(900));
        live.advanced(4, Duration::from_millis(950));
        assert_eq!(live.repair_limit(5, None, Duration::from_secs(1)), None);
        live.advanced(6, Duration::from_secs(1));
        assert_eq!(live.repair_limit(6, None, Duration::from_secs(1)), Some(6));
        live.reset();
        assert_eq!(live.repair_limit(2, None, Duration::from_secs(1)), None);
        live.advanced(3, Duration::from_secs(1));
        assert_eq!(live.repair_limit(3, None, Duration::from_secs(1)), Some(3));
    }
}
