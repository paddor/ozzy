//! Small status retries only. No timer here schedules payloads or supplies votes.

use std::time::Duration;

use ozzy_proto::RequestId;

use super::prefix_valid;
use crate::{Digest, OpNumber, Prefix, Scope};

/// One immutable status request, including the tail needed to detect a lost last batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Probe {
    /// Exact normal scope; this request never changes a view or authorizes recovery.
    pub scope: Scope,
    /// Unique exchange identity, frozen across retries and retired after completion.
    pub request_id: RequestId,
    /// Primary's outstanding tail at request creation, not a commit assertion.
    pub tail: Prefix,
    /// Highest locally available operation, including unsent work awaiting
    /// receive credit. Admission hint only, never a repair or commit boundary.
    pub available: OpNumber,
    /// Bytes needed by the next bounded data packet. Admission hint only.
    pub minimum_body_bytes: u64,
}

/// Independent status-query pacing, not election or durable-progress timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeTiming {
    /// Delay before the first retry and between unchanged completed exchanges.
    pub initial: Duration,
    /// Maximum retry delay after repeated unanswered status requests.
    pub maximum: Duration,
}

/// Rejected probe transition. Errors preserve the pending request and deadlines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProbeError {
    /// Zero identity, malformed scope/tail, or invalid initial/maximum delay.
    #[error("invalid replica probe parameters")]
    Invalid,
    /// Retire old correlation explicitly before polling another view/group.
    #[error("replica probe scope changed without invalidation")]
    Scope,
    /// Adapter-supplied monotonic time moved backward.
    #[error("replica probe clock regressed")]
    Clock,
    /// A retry/completion deadline cannot be represented; no wraparound is allowed.
    #[error("replica probe deadline exhausted")]
    TimeExhausted,
    /// The local request-identity sequence cannot advance; never reuse retired IDs.
    #[error("replica probe request identities exhausted")]
    IdsExhausted,
}

/// Allocation-free, one-outstanding-request scheduler for one authenticated peer.
///
/// Poll only when the adapter needs receipt/credit status. Opening is immediate;
/// unanswered small requests back off to an explicit ceiling. Repeated calls at
/// one instant emit at most one pending request. New work after a completed
/// exchange opens immediately; unchanged status retains its pacing delay.
/// A fresh valid response resets pacing.
/// No observation changes durable ACKs, quorum state, or commit-progress deadlines.
#[derive(Debug)]
pub struct ProbeScheduler {
    timing: ProbeTiming,
    next_id: u128,
    now: Duration,
    due: Duration,
    delay: Duration,
    pending: Option<Probe>,
    advertised: OpNumber,
}

impl ProbeScheduler {
    /// Start a peer-local request sequence and clock without allocating or reading time.
    ///
    /// Supply a fresh process/session-specific starting ID, never a reused constant
    /// across actor lifetimes. Keep this scheduler across view/receive-epoch changes;
    /// `invalidate` retires correlation without resetting the identity sequence.
    pub fn new(first: RequestId, timing: ProbeTiming, now: Duration) -> Result<Self, ProbeError> {
        let next_id = u128::from_be_bytes(*first.as_bytes());
        if next_id == 0 || timing.initial.is_zero() || timing.maximum < timing.initial {
            return Err(ProbeError::Invalid);
        }
        now.checked_add(timing.maximum)
            .ok_or(ProbeError::TimeExhausted)?;
        Ok(Self {
            timing,
            next_id,
            now,
            due: now,
            delay: timing.initial,
            pending: None,
            advertised: OpNumber(0),
        })
    }

    /// Currently outstanding frozen request, for exact response/history-work matching.
    pub const fn pending(&self) -> Option<Probe> {
        self.pending
    }

    /// Emit a new small probe when due, or retry the exact pending request.
    ///
    /// Changed `tail` or `available` does not mutate a live exchange. After
    /// completion new availability bypasses the idle delay and captures both.
    /// A returned request remains owned
    /// here even if transport admission fails; retry keeps its request identity.
    pub fn poll(
        &mut self,
        scope: Scope,
        tail: Prefix,
        available: OpNumber,
        minimum_body_bytes: u64,
        now: Duration,
    ) -> Result<Option<Probe>, ProbeError> {
        self.check_time(now)?;
        if scope.group_id.as_bytes() == &[0; 16]
            || scope.configuration_digest == Digest::ZERO
            || !prefix_valid(tail)
            || tail.op.0 == u64::MAX
            || available < tail.op
            || available.0 == u64::MAX
            || minimum_body_bytes == 0
        {
            return Err(ProbeError::Invalid);
        }
        if self.pending.is_some_and(|request| request.scope != scope) {
            return Err(ProbeError::Scope);
        }
        let new_work = self.pending.is_none() && available > self.advertised;
        if now < self.due && !new_work {
            self.now = now;
            return Ok(None);
        }
        let (request, next_id) = if let Some(request) = self.pending {
            (request, self.next_id)
        } else {
            let next_id = self
                .next_id
                .checked_add(1)
                .ok_or(ProbeError::IdsExhausted)?;
            (
                Probe {
                    scope,
                    tail,
                    available,
                    minimum_body_bytes,
                    request_id: RequestId::from_bytes(self.next_id.to_be_bytes()),
                },
                next_id,
            )
        };
        let due = now
            .checked_add(self.delay)
            .ok_or(ProbeError::TimeExhausted)?;
        self.now = now;
        self.due = due;
        self.delay = self.delay.saturating_mul(2).min(self.timing.maximum);
        self.next_id = next_id;
        self.pending = Some(request);
        self.advertised = request.available;
        Ok(Some(request))
    }

    /// Retire a validated response only if its scope and ID name the live request.
    ///
    /// Call after authenticated decoding and required report/history checks, not
    /// on arbitrary network activity. Stale replies do not extend retry deadlines.
    /// This method does not decide whether a newly reported receive epoch is valid.
    pub fn complete(
        &mut self,
        scope: Scope,
        id: RequestId,
        now: Duration,
    ) -> Result<bool, ProbeError> {
        let Some(due) = self.completion_deadline(scope, id, now)? else {
            self.now = now;
            return Ok(false);
        };
        self.now = now;
        self.due = due;
        self.delay = self.timing.initial;
        self.pending = None;
        Ok(true)
    }

    pub(super) fn completion_deadline(
        &self,
        scope: Scope,
        id: RequestId,
        now: Duration,
    ) -> Result<Option<Duration>, ProbeError> {
        self.check_time(now)?;
        if self
            .pending
            .is_none_or(|request| request.scope != scope || request.request_id != id)
        {
            return Ok(None);
        }
        now.checked_add(self.timing.initial)
            .map(Some)
            .ok_or(ProbeError::TimeExhausted)
    }

    /// Retire correlation after role/session replacement, keeping identities monotonic.
    /// The next poll can open immediately; old responses cannot complete that request.
    pub fn invalidate(&mut self, now: Duration) -> Result<(), ProbeError> {
        self.check_time(now)?;
        self.now = now;
        self.due = now;
        self.delay = self.timing.initial;
        self.pending = None;
        Ok(())
    }

    /// Retry the current immutable exchange sooner after a new receiver hint.
    /// Preserve its identity, frozen tail, and any correlated history lookup.
    pub(super) fn expedite(&mut self, now: Duration) -> Result<(), ProbeError> {
        self.check_time(now)?;
        self.now = now;
        self.due = self.due.min(now);
        self.delay = self.timing.initial;
        Ok(())
    }

    fn check_time(&self, now: Duration) -> Result<(), ProbeError> {
        if now < self.now {
            Err(ProbeError::Clock)
        } else {
            Ok(())
        }
    }
}
