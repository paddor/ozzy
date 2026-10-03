//! One reader's position across a shared live stream and an ordered replay path.
//!
//! The live stream is loss-tolerant and shared by every reader of a partition.
//! The replay path is the reader's own exact-offset subscription. This cursor
//! decides, per message, what is delivered and which path is active. The adapter
//! owns payloads, sockets, sources, and local capacity; it retains one held publication,
//! pauses live reads while [`LiveCursor::is_paused`], and keeps its replay
//! subscription equal to [`LiveCursor::replay`].

use std::time::Duration;

use super::LiveProgress;

/// The replay subscription the adapter must hold open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Replay {
    /// Exact start offset of a fresh subscription generation.
    pub from: u64,
    /// Records the replay path may still deliver; none while no publication is held.
    pub limit: Option<u64>,
}

/// Decision for one publication read from the live stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Publication {
    /// Every record was delivered before. Drop the message.
    Drop,
    /// Deliver the records after the first `skip`.
    Deliver {
        /// Already delivered records to omit from this publication.
        skip: u64,
    },
    /// Records are missing before this message. Retain it and pause live reads.
    Hold,
}

/// Decision for one contiguous message from the replay path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replayed {
    /// Live delivery already resumed. Drop the message.
    Ignore,
    /// Deliver every record. Replay continues.
    Deliver,
    /// Deliver every record, then the held publication after its first `skip`
    /// records, or release it when none remain. Resume live reads.
    DeliverThenHeld {
        /// Prefix to omit from the held publication; none means it is obsolete.
        skip: Option<u64>,
    },
}

/// The adapter violated this cursor's contract. No state changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CursorError {
    /// A publication was read while another one is held.
    #[error("live reads must pause while a publication is held")]
    Paused,
    /// A message was empty or its offsets overflow.
    #[error("empty or overflowing record range")]
    Range,
    /// Replay records do not start at the cursor.
    #[error("replay records are not contiguous with the cursor")]
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// The replay path delivers. A held publication bounds it.
    Replay { held: Option<(u64, u64)> },
    /// Only the live stream delivers. No replay subscription exists.
    Live,
}

/// Exact-offset delivery over both paths. No record is skipped or repeated.
#[derive(Debug)]
pub struct LiveCursor {
    next: u64,
    state: State,
    progress: LiveProgress,
}

impl LiveCursor {
    /// Start in replay at an exact offset. Subscribe to the live stream first.
    /// After `quiet` without live progress, replay probes for a lost final message.
    pub const fn new(start: u64, quiet: Duration) -> Self {
        Self {
            next: start,
            state: State::Replay { held: None },
            progress: LiveProgress::new(quiet),
        }
    }

    /// First offset not yet delivered.
    pub const fn next(&self) -> u64 {
        self.next
    }

    /// Live reads stay paused while one publication is held.
    pub const fn is_paused(&self) -> bool {
        matches!(self.state, State::Replay { held: Some(_) })
    }

    /// Required replay subscription. None means it must be canceled.
    pub fn replay(&self) -> Option<Replay> {
        match self.state {
            State::Replay { held } => Some(Replay {
                from: self.next,
                limit: held.map(|(first, _)| first.saturating_sub(self.next)),
            }),
            State::Live => None,
        }
    }

    /// One publication of `records` records starting at offset `first`.
    /// Any publication without a gap proves that the live stream covers the
    /// cursor from here on, so replay ends even when every record is a duplicate.
    pub fn publication(
        &mut self,
        first: u64,
        records: u64,
        now: Duration,
    ) -> Result<Publication, CursorError> {
        let end = end(first, records)?;
        if self.is_paused() {
            return Err(CursorError::Paused);
        }
        if first > self.next {
            self.state = State::Replay {
                held: Some((first, records)),
            };
            return Ok(Publication::Hold);
        }
        if self.state != State::Live {
            self.go_live(now);
        }
        if end <= self.next {
            return Ok(Publication::Drop);
        }
        let skip = self.next - first;
        self.next = end;
        self.progress.advanced(end, now);
        Ok(Publication::Deliver { skip })
    }

    /// One replay message of `records` records starting at offset `first`.
    pub fn replayed(
        &mut self,
        first: u64,
        records: u64,
        now: Duration,
    ) -> Result<Replayed, CursorError> {
        let end = end(first, records)?;
        let State::Replay { held } = self.state else {
            return Ok(Replayed::Ignore);
        };
        if first != self.next {
            return Err(CursorError::Replay);
        }
        self.next = end;
        let Some((held_first, held_records)) = held else {
            return Ok(Replayed::Deliver);
        };
        if end < held_first {
            return Ok(Replayed::Deliver);
        }
        let skip = end - held_first;
        self.next = end.max(held_first + held_records);
        self.go_live(now);
        Ok(Replayed::DeliverThenHeld {
            skip: (skip < held_records).then_some(skip),
        })
    }

    /// When live delivery has been silent for the quiet interval, return to
    /// replay. True means [`LiveCursor::replay`] changed.
    pub fn tick(&mut self, now: Duration) -> bool {
        if self.deadline().is_none_or(|deadline| now < deadline) {
            return false;
        }
        self.state = State::Replay { held: None };
        true
    }

    /// Earliest time at which [`LiveCursor::tick`] can return to replay.
    pub fn deadline(&self) -> Option<Duration> {
        match self.state {
            State::Live => self.progress.silent_at(),
            State::Replay { .. } => None,
        }
    }

    /// The log's authority changed. Release any held publication and replay
    /// from the cursor under the new source.
    pub fn source_changed(&mut self) {
        self.state = State::Replay { held: None };
        self.progress.reset();
    }

    /// Entering live delivery restarts the silence clock: a publication arrived.
    fn go_live(&mut self, now: Duration) {
        self.state = State::Live;
        self.progress.reset();
        self.progress.advanced(self.next, now);
    }
}

fn end(first: u64, records: u64) -> Result<u64, CursorError> {
    match first.checked_add(records) {
        Some(end) if records != 0 => Ok(end),
        _ => Err(CursorError::Range),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUIET: Duration = Duration::from_secs(1);
    const T0: Duration = Duration::ZERO;

    const fn replay(from: u64, limit: Option<u64>) -> Replay {
        Replay { from, limit }
    }

    /// A cursor that replayed `[start, next)` and then went live.
    fn live(start: u64, next: u64) -> LiveCursor {
        let mut cursor = LiveCursor::new(start, QUIET);
        if next > start {
            assert_eq!(
                cursor.replayed(start, next - start, T0),
                Ok(Replayed::Deliver)
            );
        }
        assert_eq!(
            cursor.publication(next, 1, T0),
            Ok(Publication::Deliver { skip: 0 })
        );
        assert_eq!(cursor.replay(), None);
        cursor
    }

    #[test]
    fn a_new_cursor_replays_from_its_exact_start_without_a_limit() {
        let cursor = LiveCursor::new(7, QUIET);
        assert_eq!(cursor.next(), 7);
        assert_eq!(cursor.replay(), Some(replay(7, None)));
        assert!(!cursor.is_paused());
        assert_eq!(cursor.deadline(), None);
    }

    #[test]
    fn replay_ends_at_the_first_publication_that_leaves_no_gap() {
        // Contiguous, overlapping, and fully delivered publications all prove
        // that the live stream covers the cursor from here on.
        for (first, records, decision, next) in [
            (10, 4, Publication::Deliver { skip: 0 }, 14),
            (8, 4, Publication::Deliver { skip: 2 }, 12),
            (3, 4, Publication::Drop, 10),
            (6, 4, Publication::Drop, 10),
        ] {
            let mut cursor = LiveCursor::new(0, QUIET);
            assert_eq!(cursor.replayed(0, 10, T0), Ok(Replayed::Deliver));
            assert_eq!(cursor.replay(), Some(replay(10, None)));
            assert_eq!(cursor.publication(first, records, T0), Ok(decision));
            assert_eq!(cursor.next(), next);
            assert_eq!(cursor.replay(), None, "live delivery cancels replay");
            assert_eq!(cursor.replayed(next, 1, T0), Ok(Replayed::Ignore));
            assert_eq!(cursor.next(), next);
        }
    }

    #[test]
    fn a_gap_holds_one_publication_and_bounds_replay_at_its_first_record() {
        let mut cursor = LiveCursor::new(0, QUIET);
        assert_eq!(cursor.publication(100, 5, T0), Ok(Publication::Hold));
        assert!(cursor.is_paused());
        assert_eq!(cursor.replay(), Some(replay(0, Some(100))));
        assert_eq!(cursor.publication(105, 5, T0), Err(CursorError::Paused));

        assert_eq!(cursor.replayed(0, 60, T0), Ok(Replayed::Deliver));
        assert_eq!(cursor.replay(), Some(replay(60, Some(40))));
        assert_eq!(
            cursor.replayed(60, 40, T0),
            Ok(Replayed::DeliverThenHeld { skip: Some(0) })
        );
        assert_eq!(cursor.next(), 105);
        assert!(!cursor.is_paused());
        assert_eq!(cursor.replay(), None);
    }

    #[test]
    fn replay_beyond_its_limit_trims_or_releases_the_held_publication() {
        for (records, held, next) in [(103, Some(3), 105), (105, None, 105), (120, None, 120)] {
            let mut cursor = LiveCursor::new(0, QUIET);
            assert_eq!(cursor.publication(100, 5, T0), Ok(Publication::Hold));
            assert_eq!(
                cursor.replayed(0, records, T0),
                Ok(Replayed::DeliverThenHeld { skip: held })
            );
            assert_eq!(cursor.next(), next);
            assert_eq!(cursor.replay(), None);
        }
    }

    #[test]
    fn live_delivery_is_contiguous_and_every_loss_returns_to_bounded_replay() {
        let mut cursor = live(0, 10);
        assert_eq!(cursor.next(), 11);
        assert_eq!(
            cursor.publication(11, 3, T0),
            Ok(Publication::Deliver { skip: 0 })
        );
        assert_eq!(cursor.publication(11, 3, T0), Ok(Publication::Drop));
        assert_eq!(
            cursor.publication(12, 4, T0),
            Ok(Publication::Deliver { skip: 2 })
        );
        assert_eq!(cursor.next(), 16);

        // A full queue lost publications. Repair, resume, then lose more.
        for (first, records) in [(20, 2), (30, 1)] {
            let from = cursor.next();
            assert_eq!(
                cursor.publication(first, records, T0),
                Ok(Publication::Hold)
            );
            assert_eq!(cursor.replay(), Some(replay(from, Some(first - from))));
            assert_eq!(
                cursor.replayed(from, first - from, T0),
                Ok(Replayed::DeliverThenHeld { skip: Some(0) })
            );
            assert_eq!(cursor.next(), first + records);
            assert_eq!(cursor.replay(), None);
        }
    }

    #[test]
    fn silence_probes_for_a_lost_final_publication_and_duplicates_cannot_delay_it() {
        let mut cursor = live(0, 10);
        assert_eq!(cursor.deadline(), Some(QUIET));
        assert!(!cursor.tick(QUIET.checked_sub(Duration::from_nanos(1)).unwrap()));
        assert_eq!(cursor.replay(), None);

        let later = Duration::from_millis(900);
        assert_eq!(cursor.publication(10, 1, later), Ok(Publication::Drop));
        assert_eq!(cursor.deadline(), Some(QUIET));
        assert_eq!(
            cursor.publication(11, 1, later),
            Ok(Publication::Deliver { skip: 0 })
        );
        assert_eq!(cursor.deadline(), Some(later + QUIET));
        assert!(!cursor.tick(QUIET));

        assert!(cursor.tick(later + QUIET));
        assert_eq!(cursor.replay(), Some(replay(12, None)));
        assert_eq!(cursor.deadline(), None);
        assert!(!cursor.tick(later + 2 * QUIET), "already replaying");

        // The lost final record arrives by replay. The cursor stays parked in
        // replay until the live stream proves itself again.
        assert_eq!(cursor.replayed(12, 1, later + QUIET), Ok(Replayed::Deliver));
        assert_eq!(cursor.replay(), Some(replay(13, None)));
        assert_eq!(
            cursor.publication(13, 1, later + 2 * QUIET),
            Ok(Publication::Deliver { skip: 0 })
        );
        assert_eq!(cursor.replay(), None);
        assert_eq!(cursor.deadline(), Some(later + 3 * QUIET));
    }

    #[test]
    fn a_source_change_releases_held_state_and_replays_from_the_cursor() {
        let mut held = LiveCursor::new(0, QUIET);
        assert_eq!(held.replayed(0, 4, T0), Ok(Replayed::Deliver));
        assert_eq!(held.publication(9, 1, T0), Ok(Publication::Hold));
        held.source_changed();
        assert!(!held.is_paused());
        assert_eq!(held.replay(), Some(replay(4, None)));

        let mut cursor = live(0, 4);
        cursor.source_changed();
        assert_eq!(cursor.replay(), Some(replay(5, None)));
        assert_eq!(cursor.deadline(), None, "old live progress is forgotten");
        assert_eq!(cursor.replayed(5, 2, T0), Ok(Replayed::Deliver));
        assert_eq!(cursor.next(), 7);
    }

    #[test]
    fn contract_violations_change_nothing() {
        let mut cursor = LiveCursor::new(5, QUIET);
        for (first, records) in [(4, 2), (6, 1)] {
            assert_eq!(
                cursor.replayed(first, records, T0),
                Err(CursorError::Replay)
            );
        }
        assert_eq!(cursor.replayed(5, 0, T0), Err(CursorError::Range));
        assert_eq!(cursor.replayed(5, u64::MAX, T0), Err(CursorError::Range));
        assert_eq!(cursor.publication(5, 0, T0), Err(CursorError::Range));
        assert_eq!(cursor.publication(u64::MAX, 2, T0), Err(CursorError::Range));
        assert_eq!(cursor.next(), 5);
        assert_eq!(cursor.replay(), Some(replay(5, None)));
    }
}
