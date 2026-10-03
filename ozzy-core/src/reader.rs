//! Deterministic scheduling for one subscription's owned read operation.
//!
//! Events are serialized by the owner. I/O can finish after a newer source or
//! credit event, so a completion may describe an obsolete reason to wait.
//! Transport/session validation and the single pending future stay with the
//! adapter. Replacing a subscription must discard both its future and scheduler.

/// Result of one bounded read, including any records already selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    /// The captured visible history ended. Wait for a source change.
    CaughtUp,
    /// More bounded work is available immediately.
    More,
    /// The next record did not fit captured credit. Wait for more credit.
    Credit,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Phase {
    #[default]
    Runnable,
    Reading {
        source_changed: bool,
        credit_changed: bool,
    },
    WaitingForSource,
    WaitingForCredit,
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

    /// Readiness is necessary but does not reserve credit or an output buffer.
    pub const fn is_runnable(self) -> bool {
        matches!(self.phase, Phase::Runnable | Phase::Reading { .. })
    }

    /// A commit, retention, or authority change requires another source check.
    /// Even a credit-blocked reader must observe retention or authority errors.
    pub fn source_changed(&mut self) {
        match &mut self.phase {
            Phase::Reading { source_changed, .. } => *source_changed = true,
            _ => self.phase = Phase::Runnable,
        }
    }

    /// Publish a strictly advancing, validated cumulative credit grant.
    /// Duplicate or stale credit is not an event and must be filtered by the owner.
    pub fn credit_advanced(&mut self) {
        match &mut self.phase {
            Phase::Reading { credit_changed, .. } => *credit_changed = true,
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
                    credit_changed: false,
                };
                true
            }
            Phase::Reading { .. } => true,
            Phase::WaitingForSource | Phase::WaitingForCredit => false,
        }
    }

    /// Consume the pending read's outcome. True requests another bounded turn.
    /// A completion without a pending read is rejected without changing state.
    pub fn complete(&mut self, outcome: ReadOutcome) -> Result<bool, NoPendingRead> {
        let Phase::Reading {
            source_changed,
            credit_changed,
        } = self.phase
        else {
            return Err(NoPendingRead);
        };
        self.phase = match outcome {
            ReadOutcome::More => Phase::Runnable,
            ReadOutcome::CaughtUp if source_changed => Phase::Runnable,
            ReadOutcome::Credit if credit_changed => Phase::Runnable,
            ReadOutcome::CaughtUp => Phase::WaitingForSource,
            ReadOutcome::Credit => Phase::WaitingForCredit,
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
    fn credit_return_cannot_be_lost_before_or_after_an_old_credit_completion() {
        for before_completion in [false, true] {
            let mut scheduler = ReadScheduler::new();
            assert!(scheduler.begin_poll());
            if before_completion {
                scheduler.credit_advanced();
            }
            assert_eq!(
                scheduler.complete(ReadOutcome::Credit),
                Ok(before_completion)
            );
            if !before_completion {
                scheduler.credit_advanced();
            }
            assert!(
                scheduler.begin_poll(),
                "credit must make the next read runnable"
            );
            assert_eq!(scheduler.complete(ReadOutcome::Credit), Ok(false));
            assert!(!scheduler.begin_poll(), "unchanged credit must park again");
        }
    }

    #[test]
    fn pending_repolls_preserve_both_events_in_either_order() {
        for source_first in [false, true] {
            for outcome in [ReadOutcome::CaughtUp, ReadOutcome::Credit] {
                let mut scheduler = ReadScheduler::new();
                assert!(scheduler.begin_poll());
                if source_first {
                    scheduler.source_changed();
                } else {
                    scheduler.credit_advanced();
                }
                for _ in 0..8 {
                    assert!(scheduler.begin_poll());
                }
                if source_first {
                    scheduler.credit_advanced();
                } else {
                    scheduler.source_changed();
                }
                assert_eq!(scheduler.complete(outcome), Ok(true));
                assert!(scheduler.begin_poll());
                assert_eq!(scheduler.complete(outcome), Ok(false));
            }
        }
    }

    #[test]
    fn only_the_relevant_change_retries_a_completed_wait() {
        for outcome in [ReadOutcome::CaughtUp, ReadOutcome::Credit] {
            let mut scheduler = ReadScheduler::new();
            assert!(scheduler.begin_poll());
            match outcome {
                ReadOutcome::CaughtUp => scheduler.credit_advanced(),
                ReadOutcome::Credit => scheduler.source_changed(),
                ReadOutcome::More => unreachable!(),
            }
            assert_eq!(scheduler.complete(outcome), Ok(false));
            assert!(!scheduler.begin_poll());
            scheduler.source_changed(); // Retention/authority must also wake credit waits.
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
