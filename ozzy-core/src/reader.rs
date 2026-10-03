//! Deterministic scheduling for one subscription's owned read operation.
//!
//! Events are serialized by the owner. I/O can finish after a newer
//! source event, so a completion may describe an obsolete reason to wait.
//! Transport/session validation and the single pending future stay with the
//! adapter. Replacing a subscription must discard both its future and scheduler.

/// Result of one bounded read, including any records already selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    /// The captured visible history ended. Wait for a source change.
    CaughtUp,
    /// More bounded work is available immediately.
    More,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Phase {
    #[default]
    Runnable,
    Reading {
        source_changed: bool,
    },
    WaitingForSource,
}

/// Read readiness without clocks, callbacks, queues, or I/O.
///
/// Exactly one owned read may be pending. Repeated polls retain its intervening
/// events. A stale completion retries once; unchanged waits remain parked.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReadScheduler {
    phase: Phase,
}

impl ReadScheduler {
    /// New subscriptions need their initial source inspection.
    pub const fn new() -> Self {
        Self {
            phase: Phase::Runnable,
        }
    }

    /// Readiness is necessary but does not reserve an output buffer.
    pub const fn is_runnable(self) -> bool {
        matches!(self.phase, Phase::Runnable | Phase::Reading { .. })
    }

    /// A commit, retention, or authority change requires another source check.
    pub fn source_changed(&mut self) {
        match &mut self.phase {
            Phase::Reading { source_changed, .. } => *source_changed = true,
            _ => self.phase = Phase::Runnable,
        }
    }

    /// Start or repoll the same owned read after reserving its external resources.
    /// Returns false while parked; repeated pending polls never erase events.
    pub fn begin_poll(&mut self) -> bool {
        match self.phase {
            Phase::Runnable => {
                self.phase = Phase::Reading {
                    source_changed: false,
                };
                true
            }
            Phase::Reading { .. } => true,
            Phase::WaitingForSource => false,
        }
    }

    /// Consume the pending read's outcome. True requests another bounded turn.
    /// A completion without a pending read is rejected without changing state.
    pub fn complete(&mut self, outcome: ReadOutcome) -> Result<bool, NoPendingRead> {
        let Phase::Reading { source_changed } = self.phase else {
            return Err(NoPendingRead);
        };
        self.phase = match outcome {
            ReadOutcome::More => Phase::Runnable,
            ReadOutcome::CaughtUp if source_changed => Phase::Runnable,
            ReadOutcome::CaughtUp => Phase::WaitingForSource,
        };
        Ok(self.is_runnable())
    }
}

/// The owner attempted to install a read result without a pending read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("no pending subscription read")]
pub struct NoPendingRead;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_change_survives_pending_repolls_and_caught_up_completion() {
        for before_completion in [false, true] {
            let mut scheduler = ReadScheduler::new();
            assert!(scheduler.begin_poll());
            if before_completion {
                scheduler.source_changed();
            }
            for _ in 0..8 {
                assert!(scheduler.begin_poll());
            }
            assert_eq!(
                scheduler.complete(ReadOutcome::CaughtUp),
                Ok(before_completion)
            );
            if !before_completion {
                scheduler.source_changed();
            }
            assert!(scheduler.begin_poll());
            assert_eq!(scheduler.complete(ReadOutcome::CaughtUp), Ok(false));
            assert!(!scheduler.begin_poll());
            scheduler.source_changed();
            assert!(scheduler.begin_poll());
            assert_eq!(scheduler.complete(ReadOutcome::More), Ok(true));
        }
    }

    #[test]
    fn duplicate_or_unsolicited_completion_does_not_change_the_schedule() {
        let mut scheduler = ReadScheduler::new();
        let before = scheduler;
        assert_eq!(scheduler.complete(ReadOutcome::More), Err(NoPendingRead));
        assert_eq!(scheduler, before);
        assert!(scheduler.begin_poll());
        assert_eq!(scheduler.complete(ReadOutcome::CaughtUp), Ok(false));
        let before = scheduler;
        assert_eq!(scheduler.complete(ReadOutcome::More), Err(NoPendingRead));
        assert_eq!(scheduler, before);
    }
}
