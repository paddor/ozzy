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

/// One bounded repair range using existing reservations. Never new send credit.
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
    /// Receipt/credit or exact-history validation failed.
    #[error(transparent)]
    Flow(#[from] FlowError),
    /// Monotonic time or probe identity cannot advance safely.
    #[error(transparent)]
    Probe(#[from] ProbeError),
}

/// One peer's preallocated credit ledger, correlated probes, and bounded repair.
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
    /// Remote receive credits are independent and cannot increase this allocation.
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
    /// Reconnection alone does not call this method or reset credit accounting.
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

    /// Poll small status traffic independently of durable ACK or election timers.
    ///
    /// `transport_pending` includes this peer's locally retained data transmissions.
    /// Do not let a probe overtake that queue and mistake unsent bytes for a gap.
    /// Poll while durability/receipt/credit remains unresolved, including received
    /// but not durably acknowledged bytes: a receiver can reset after volatile receipt.
    pub fn poll_probe(
        &mut self,
        available: Prefix,
        minimum_body_bytes: u64,
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
        Ok(self.probes.poll(
            self.scope,
            tail,
            available.op.max(tail.op),
            minimum_body_bytes,
            now,
        )?)
    }

    /// Reserve a fresh suffix once after ensuring the adapter can retain its packet.
    /// Known repair takes precedence over sending more data beyond that gap.
    pub fn record_send(&mut self, operations: &[Operation]) -> Result<(), FlowError> {
        if !self.active {
            return Err(FlowError::Channel);
        }
        if self.repair.is_some()
            || self.broadcast
                && self.catch_up.is_some_and(|through| {
                    operations.last().is_some_and(|op| op.prefix.op > through)
                })
        {
            return Err(FlowError::Capacity);
        }
        let sender = self.sender.as_mut().expect("active ledger");
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
    /// The bound grants no credit or history authority and cannot exceed the probe.
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
        if self
            .sender()
            .is_some_and(|sender| sender.channel() == report.channel)
        {
            if self.candidate.is_some_and(|old| {
                old.report.channel == report.channel && old.report.revision > report.revision
            }) {
                return Ok(StatusOutcome::Ignored);
            }
            if self.broadcast && report.received.op > self.sender().expect("active").sent().op {
                self.candidate = Some(OpenRequest { request_id, report });
                self.candidate_limit = repair_limit;
                return Ok(StatusOutcome::Verify);
            }
            let sender = self.sender.as_mut().expect("active ledger");
            let before = sender.received();
            let changed = sender.observe(report)?;
            if sender.received().op > before.op {
                self.receipt_progress.advanced(sender.received().op.0, now);
            }
            if let Some(probe) = correlated {
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
                self.probes.complete(probe.scope, probe.request_id, now)?;
                self.candidate = None;
                self.candidate_limit = None;
            }
            self.trim_repair();
            return Ok(if changed || correlated.is_some() {
                StatusOutcome::Observed
            } else {
                StatusOutcome::Ignored
            });
        }
        let Some(probe) = correlated else {
            // A changed receive epoch can announce renewed capacity between
            // periodic probes. Ask immediately, but keep any live correlation
            // and history lookup. The hint itself installs no receive credit.
            if self.sender().is_some() && self.announced_channel != Some(report.channel) {
                self.probes.expedite(now)?;
                self.announced_channel = Some(report.channel);
            }
            return Ok(StatusOutcome::Ignored);
        };
        if self.candidate.is_some_and(|old| {
            old.report.channel == report.channel && old.report.revision > report.revision
        }) {
            return Ok(StatusOutcome::Ignored);
        }
        let candidate = OpenRequest {
            request_id: Some(probe.request_id),
            report,
        };
        self.candidate = Some(candidate);
        self.candidate_limit = repair_limit;
        Ok(StatusOutcome::Verify)
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
        if let Some(sender) = &mut self.sender {
            if self.broadcast && same_channel {
                if !sender.observe_publication(report, received)? {
                    self.candidate = None;
                    self.candidate_limit = None;
                    return Ok(false);
                }
            } else {
                sender.reopen(report, received)?;
            }
        } else {
            self.sender = Some(Sender::from_checked_report(
                report,
                self.limits,
                std::mem::take(&mut self.spare),
            ));
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
        self.candidate = None;
        self.candidate_limit = None;
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

    /// Current bounded retry range, suppressed while this peer has queued data.
    pub fn repair(&self, transport_pending: bool) -> Option<Repair> {
        if transport_pending { None } else { self.repair }
    }

    /// Record one retained retry chunk without charging unique-send credits again.
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
        let Some(repair) = self.repair else {
            return;
        };
        let received = self
            .sender()
            .expect("repair has an active ledger")
            .received();
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
