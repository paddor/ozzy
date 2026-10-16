//! Shared normal/catch-up send policy. Timers produce queries, never data copies.

use std::time::Duration;

use crate::OpNumber;
use ozzy_proto::RequestId;

use super::{
    Channel, FlowError, Operation, PipelineLimits, Prefix, Probe, ProbeError, ProbeScheduler,
    ProbeTiming, Report, Scope, Sender, VecDeque, ledger,
};

/// Exact report awaiting independently verified local history.
/// New receiver epochs require correlation; known-epoch PUB progress does not.
///
/// A disk callback must additionally match the actor's current journal generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenRequest {
    request_id: Option<RequestId>,
    report: Report,
}

impl OpenRequest {
    /// Candidate base/receipt prefixes and receive incarnation, not trusted history.
    pub const fn report(self) -> Report {
        self.report
    }
}

/// One bounded repair range using existing reservations. Never new send capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Repair {
    /// Exact receiver incarnation to put on each packet.
    pub channel: Channel,
    /// Last known retained or already retried prefix; start strictly after it.
    pub after: Prefix,
    /// Frozen outstanding tail covered by the answered probe, inclusive.
    pub through: Prefix,
}

/// Nonvoting result of an authenticated receipt/status observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusOutcome {
    /// Stale correlation, another scope/epoch notification, or unchanged status.
    Ignored,
    /// Same-channel accounting or the current probe exchange advanced.
    Observed,
    /// New channel or unreserved PUB progress needs a local-history check.
    Verify,
}

/// Rejected send-policy transition. No error supplies durability or quorum evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TransmitError {
    /// Receipt or exact-history validation failed.
    #[error(transparent)]
    Flow(#[from] FlowError),
    /// Monotonic time or probe identity cannot advance safely.
    #[error(transparent)]
    Probe(#[from] ProbeError),
}

/// One peer's preallocated local ledger, correlated probes, and bounded repair.
///
/// The actor and simulator share this policy. Neither timer expiry nor an
/// unsolicited partial receipt schedules a retransmission. Only a valid response
/// to a live probe can identify an outstanding range for repair. Payload ownership,
/// authenticated binding, role checks, journal generations, and quorum stay outside.
#[derive(Debug)]
pub struct Transmitter {
    scope: Scope,
    limits: PipelineLimits,
    probes: ProbeScheduler,
    sender: Option<Sender>,
    announced_channel: Option<Channel>,
    // Reserved before the first open; moved into Sender without allocating.
    spare: VecDeque<Operation>,
    active: bool,
    candidate: Option<OpenRequest>,
    candidate_limit: Option<OpNumber>,
    repair: Option<Repair>,
    broadcast: bool,
    catch_up: Option<OpNumber>,
    receipt_progress: ozzy_core::live::LiveProgress,
}

impl Transmitter {
    /// Reserve this sender's local metadata window and start fresh probe correlation.
    /// Remote receipt reports cannot increase this allocation.
    pub fn new(
        scope: Scope,
        limits: PipelineLimits,
        first: RequestId,
        timing: ProbeTiming,
        now: Duration,
    ) -> Result<Self, TransmitError> {
        // The initial probe validates scope before any network or ledger action.
        let probes = ProbeScheduler::new(first, timing, now)?;
        Ok(Self {
            scope,
            limits,
            probes,
            sender: None,
            announced_channel: None,
            spare: ledger(limits)?,
            active: false,
            candidate: None,
            candidate_limit: None,
            repair: None,
            broadcast: false,
            catch_up: None,
            receipt_progress: ozzy_core::live::LiveProgress::new(timing.maximum),
        })
    }

    /// Enable loss-tolerant PUB delivery. PEER reservations cover repair only.
    pub fn enable_broadcast(&mut self) {
        self.broadcast = true;
    }

    /// A correlated probe found missing publications beyond existing reservations.
    /// Fresh PEER sends are allowed only while the frozen target remains unsent.
    pub fn needs_catch_up(&self) -> bool {
        self.catch_up.is_some_and(|target| {
            self.sender()
                .is_some_and(|sender| sender.sent().op < target)
        })
    }

    /// Frozen upper bound for fresh PEER catch-up in broadcast mode.
    pub const fn catch_up_through(&self) -> Option<OpNumber> {
        self.catch_up
    }

    /// Active peer ledger, or none until exact history verifies a correlated open.
    pub fn sender(&self) -> Option<&Sender> {
        self.sender.as_ref().filter(|_| self.active)
    }

    /// Pending history check, for matching asynchronous callbacks before adoption.
    pub const fn candidate(&self) -> Option<OpenRequest> {
        self.candidate
    }

    /// Current immutable exchange, for adapter correlation and diagnostics.
    pub const fn pending_probe(&self) -> Option<Probe> {
        self.probes.pending()
    }

    /// Fence link-session correlation without granting fresh receive capacity.
    /// Retain the channel ledger, outstanding sends, and any established repair.
    /// A new probe verifies peer state; an old asynchronous history lookup cannot
    /// install its retired candidate after the session changes.
    pub fn replace_session(&mut self, now: Duration) -> Result<(), TransmitError> {
        self.probes.invalidate(now)?;
        self.announced_channel = None;
        self.candidate = None;
        self.candidate_limit = None;
        Ok(())
    }

    /// Fence a changed normal scope while retaining all startup allocations and IDs.
    /// Reconnection alone does not call this method or reset local accounting.
    pub fn change_scope(&mut self, scope: Scope, now: Duration) -> Result<(), TransmitError> {
        if scope == self.scope {
            return Ok(());
        }
        self.probes.invalidate(now)?;
        self.scope = scope;
        self.active = false;
        self.announced_channel = None;
        self.candidate = None;
        self.candidate_limit = None;
        self.repair = None;
        self.catch_up = None;
        self.receipt_progress.reset();
        Ok(())
    }

    /// When `poll_probe` next sends or retries on time alone. `None` while a
    /// repair runs, which probes never overtake.
    pub fn next_probe(&self) -> Option<Duration> {
        self.repair.is_none().then(|| self.probes.due())
    }

    /// Poll small status traffic independently of durable ACK or election timers.
    ///
    /// `transport_pending` includes this peer's locally retained data transmissions.
    /// Do not let a probe overtake that queue and mistake unsent bytes for a gap.
    /// Poll while durability/receipt remains unresolved, including received
    /// but not durably acknowledged bytes: a receiver can reset after volatile receipt.
    pub fn poll_probe(
        &mut self,
        available: Prefix,
        transport_pending: bool,
        now: Duration,
    ) -> Result<Option<Probe>, TransmitError> {
        if transport_pending || self.repair.is_some() {
            return Ok(None);
        }
        let tail = if self.broadcast {
            available
        } else {
            self.sender().map_or(available, Sender::sent)
        };
        Ok(self
            .probes
            .poll(self.scope, tail, available.op.max(tail.op), now)?)
    }

    /// Reserve a fresh suffix once after ensuring the adapter can retain its packet.
    /// Known repair takes precedence over sending more data beyond that gap.
    pub fn record_send(&mut self, operations: &[Operation]) -> Result<(), FlowError> {
        let Some(sender) = self.sender.as_mut().filter(|_| self.active) else {
            return Err(FlowError::Channel);
        };
        if self.repair.is_some()
            || self.broadcast
                && self.catch_up.is_some_and(|through| {
                    operations.last().is_some_and(|op| op.prefix.op > through)
                })
        {
            return Err(FlowError::Capacity);
        }
        sender.record_send(sender.channel(), operations)
    }

    /// Observe authenticated status. Unknown epochs need correlation plus history.
    /// Same-epoch unsolicited receipts may retire reservations but never request repair.
    pub fn observe(
        &mut self,
        report: Report,
        request_id: Option<RequestId>,
        now: Duration,
    ) -> Result<StatusOutcome, TransmitError> {
        self.observe_with_repair_limit(report, request_id, None, now)
    }

    /// A correlated response may stop repair before data already held on SUB.
    /// The bound grants no capacity or history authority and cannot exceed the probe.
    pub fn observe_with_repair_limit(
        &mut self,
        report: Report,
        request_id: Option<RequestId>,
        repair_limit: Option<OpNumber>,
        now: Duration,
    ) -> Result<StatusOutcome, TransmitError> {
        if report.channel.scope != self.scope {
            return Ok(StatusOutcome::Ignored);
        }
        let correlated = if let Some(id) = request_id {
            let Some(probe) = self.probes.pending().filter(|probe| probe.request_id == id) else {
                return Ok(StatusOutcome::Ignored);
            };
            self.probes.completion_deadline(probe.scope, id, now)?;
            Some(probe)
        } else {
            None
        };
        report.validate_shape()?;
        let request = OpenRequest { request_id, report };
        if self
            .sender()
            .is_none_or(|sender| sender.channel() != report.channel)
        {
            if correlated.is_none() {
                // A changed receive epoch announces reset volatile history between
                // periodic probes. Ask immediately, but keep any live correlation
                // and history lookup. The hint itself installs no receive capacity.
                if self.sender().is_some() && self.announced_channel != Some(report.channel) {
                    self.probes.expedite(now)?;
                    self.announced_channel = Some(report.channel);
                }
                return Ok(StatusOutcome::Ignored);
            }
            return Ok(self.hold(request, repair_limit));
        }
        if self.superseded(report) {
            return Ok(StatusOutcome::Ignored);
        }
        if self.broadcast
            && self
                .sender()
                .is_some_and(|sender| report.received.op > sender.sent().op)
        {
            return Ok(self.hold(request, repair_limit));
        }
        let changed = self.observe_receipt(report, now)?;
        if let Some(probe) = correlated {
            self.answer_probe(probe, repair_limit, now)?;
        }
        self.trim_repair();
        Ok(if changed || correlated.is_some() {
            StatusOutcome::Observed
        } else {
            StatusOutcome::Ignored
        })
    }

    /// Hold a report for local-history verification, unless the pending
    /// candidate already carries a newer revision of the same channel.
    fn hold(&mut self, request: OpenRequest, repair_limit: Option<OpNumber>) -> StatusOutcome {
        if self.superseded(request.report) {
            return StatusOutcome::Ignored;
        }
        self.candidate = Some(request);
        self.candidate_limit = repair_limit;
        StatusOutcome::Verify
    }

    fn superseded(&self, report: Report) -> bool {
        self.candidate.is_some_and(|old| {
            old.report.channel == report.channel && old.report.revision > report.revision
        })
    }

    fn clear_candidate(&mut self) {
        self.candidate = None;
        self.candidate_limit = None;
    }

    /// Retire reservations the current channel reports received.
    fn observe_receipt(&mut self, report: Report, now: Duration) -> Result<bool, FlowError> {
        let Some(sender) = self.sender.as_mut() else {
            return Ok(false);
        };
        let before = sender.received();
        let changed = sender.observe(report)?;
        if sender.received().op > before.op {
            self.receipt_progress.advanced(sender.received().op.0, now);
        }
        Ok(changed)
    }

    /// Complete a live probe on the current channel and plan the repair of
    /// what it shows outstanding through its frozen tail.
    fn answer_probe(
        &mut self,
        probe: Probe,
        repair_limit: Option<OpNumber>,
        now: Duration,
    ) -> Result<(), TransmitError> {
        if let Some(sender) = self.sender.as_ref() {
            let target = if self.broadcast {
                repair_limit.unwrap_or(probe.tail.op).min(probe.tail.op)
            } else {
                probe.tail.op
            };
            if self.broadcast {
                self.catch_up = (sender.received().op < target).then_some(target);
            }
            let through = sender
                .outstanding()
                .take_while(|op| op.prefix.op <= target)
                .last()
                .map_or(sender.received(), |op| op.prefix);
            // A status reply can overtake the data connection. Retrying a
            // moving window duplicates the stream and competes with repair.
            let quiet = !self.broadcast
                || self
                    .receipt_progress
                    .repair_limit(sender.received().op.0, None, now)
                    .is_none();
            self.repair = if quiet && sender.received().op < through.op {
                Some(Repair {
                    channel: sender.channel(),
                    after: sender.received(),
                    through,
                })
            } else {
                None
            };
        }
        self.probes.complete(probe.scope, probe.request_id, now)?;
        self.clear_candidate();
        Ok(())
    }

    /// Adopt only the still-current candidate after exact local-history verification.
    ///
    /// Prefix arguments must come from current-generation local history, not copied
    /// report fields. Stale callbacks return false. Old pending payloads must be
    /// fenced by their epochs; they cannot become reservations in the new channel.
    pub fn open_verified(
        &mut self,
        request: OpenRequest,
        base: Prefix,
        received: Prefix,
        now: Duration,
    ) -> Result<bool, TransmitError> {
        if self.candidate != Some(request) {
            return Ok(false);
        }
        let probe = if let Some(id) = request.request_id {
            let Some(probe) = self.probes.pending().filter(|probe| {
                probe.scope == request.report.channel.scope && probe.request_id == id
            }) else {
                return Ok(false);
            };
            self.probes.completion_deadline(probe.scope, id, now)?;
            Some(probe)
        } else {
            if !self.broadcast
                || self
                    .sender()
                    .is_none_or(|sender| sender.channel() != request.report.channel)
            {
                return Ok(false);
            }
            None
        };
        let report = request.report;
        if report.base != base || report.received != received {
            return Err(FlowError::History.into());
        }
        report.validate_shape()?;
        let same_channel = self
            .sender()
            .is_some_and(|sender| sender.channel() == report.channel);
        let previous = self.sender().map(|sender| sender.received().op);
        if !self.adopt(report, received, same_channel)? {
            self.clear_candidate();
            return Ok(false);
        }
        if let Some(probe) = probe {
            self.probes.complete(probe.scope, probe.request_id, now)?;
            if self.broadcast {
                let target = self
                    .candidate_limit
                    .unwrap_or(probe.tail.op)
                    .min(probe.tail.op);
                self.catch_up = (report.received.op < target).then_some(target);
            }
        }
        self.active = true;
        self.clear_candidate();
        if !same_channel {
            self.repair = None;
            self.receipt_progress.reset();
        }
        if !same_channel || previous.is_none_or(|op| received.op > op) {
            self.receipt_progress.advanced(received.op.0, now);
        }
        self.trim_repair();
        Ok(true)
    }

    /// Move the ledger to a verified report: accept PUB progress on the same
    /// channel, reopen on a new one, or install the reserved ledger. Returns
    /// false when same-channel progress no longer applies.
    fn adopt(
        &mut self,
        report: Report,
        received: Prefix,
        same_channel: bool,
    ) -> Result<bool, FlowError> {
        match &mut self.sender {
            Some(sender) if self.broadcast && same_channel => {
                sender.observe_publication(report, received)
            }
            Some(sender) => sender.reopen(report, received).map(|()| true),
            None => {
                self.sender = Some(Sender::from_checked_report(
                    report,
                    self.limits,
                    std::mem::take(&mut self.spare),
                ));
                Ok(true)
            }
        }
    }

    /// Current bounded retry range, suppressed while this peer has queued data.
    pub fn repair(&self, transport_pending: bool) -> Option<Repair> {
        if transport_pending { None } else { self.repair }
    }

    /// Record one retained retry chunk without charging unique-send metadata again.
    /// `through` must name an exact reserved operation inside the current repair.
    pub fn record_repair(&mut self, repair: Repair, through: Prefix) -> Result<(), FlowError> {
        if self.repair != Some(repair) {
            return Err(FlowError::Channel);
        }
        if through.op <= repair.after.op
            || through.op > repair.through.op
            || self
                .sender()
                .is_none_or(|sender| !sender.outstanding().any(|op| op.prefix == through))
        {
            return Err(FlowError::History);
        }
        self.repair = if through == repair.through {
            None
        } else {
            Some(Repair {
                after: through,
                ..repair
            })
        };
        Ok(())
    }

    fn trim_repair(&mut self) {
        if self.catch_up.is_some_and(|target| {
            self.sender()
                .is_some_and(|sender| sender.received().op >= target)
        }) {
            self.catch_up = None;
        }
        let (Some(repair), Some(received)) = (self.repair, self.sender().map(Sender::received))
        else {
            return;
        };
        self.repair = if received.op >= repair.through.op {
            None
        } else if received.op > repair.after.op {
            Some(Repair {
                after: received,
                ..repair
            })
        } else {
            Some(repair)
        };
    }
}
