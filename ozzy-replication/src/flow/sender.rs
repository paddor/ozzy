use super::{
    Channel, FlowError, Operation, PipelineLimits, Prefix, Report, VecDeque, batch, ledger,
};

#[cfg(test)]
mod tests;

/// One peer's absolute send reservations and bounded sent-but-unreceipted ledger.
///
/// No payload ownership or quorum state lives here. Keep one instance per peer;
/// a slow peer cannot consume another peer's credits. Updates allocate no memory.
#[derive(Debug)]
pub struct Sender {
    report: Report,
    sent: Prefix,
    sent_bytes: u64,
    pending: VecDeque<Operation>,
    limits: PipelineLimits,
}

impl Sender {
    /// Open after a fresh correlated exchange with the bound peer and epoch.
    ///
    /// `verified_received` must come from exact local-history lookup, not a copied
    /// untrusted report field. This comparison grants no election or commit authority.
    /// Opening never implicitly replaces another instance; the adapter must fence
    /// its old packets and status correlation before adopting a new epoch.
    /// `limits` bounds local sent-but-unreceipted metadata/body ownership, not
    /// the remote receive window. A larger remote window never enlarges it.
    pub fn open(
        report: Report,
        limits: PipelineLimits,
        verified_received: Prefix,
    ) -> Result<Self, FlowError> {
        report.validate_shape()?;
        if report.received != verified_received {
            return Err(FlowError::History);
        }
        Ok(Self::from_checked_report(report, limits, ledger(limits)?))
    }

    pub(super) fn from_checked_report(
        report: Report,
        limits: PipelineLimits,
        pending: VecDeque<Operation>,
    ) -> Self {
        Self {
            report,
            sent: report.received,
            sent_bytes: report.received_bytes,
            pending,
            limits,
        }
    }

    /// Replace an explicitly retired channel after a fresh, history-verified open.
    ///
    /// Preserves startup metadata capacity instead of allocating another ledger.
    /// The adapter must fence old payload ownership, validate probe/session/scope
    /// correlation, and independently verify both reported base and receipt history.
    /// `verified_received` has the same requirements as in `open`.
    /// Same-channel status belongs in `observe`; it must never reset send charges.
    pub fn reopen(&mut self, report: Report, verified_received: Prefix) -> Result<(), FlowError> {
        if report.channel == self.report.channel {
            return Err(FlowError::Channel);
        }
        report.validate_shape()?;
        if report.received != verified_received {
            return Err(FlowError::History);
        }
        self.pending.clear();
        self.report = report;
        self.sent = report.received;
        self.sent_bytes = report.received_bytes;
        Ok(())
    }

    /// Advance beyond PEER reservations after independently checking a PUB receipt
    /// against local history. Counters are receiver-owned, just as at channel open.
    /// Existing reservations are retired only when the verified prefix covers them.
    pub(super) fn observe_publication(
        &mut self,
        report: Report,
        verified: Prefix,
    ) -> Result<bool, FlowError> {
        if report.received != verified {
            return Err(FlowError::History);
        }
        if report.received.op <= self.sent.op {
            return self.observe(report);
        }
        report.validate_shape()?;
        if report.channel != self.report.channel {
            return Err(FlowError::Channel);
        }
        if report.base != self.report.base {
            return Err(FlowError::History);
        }
        if report.revision < self.report.revision {
            return Ok(false);
        }
        if report.revision == self.report.revision
            || report.operation_limit < self.report.operation_limit
            || report.byte_limit < self.report.byte_limit
            || report.received_bytes <= self.sent_bytes
        {
            return Err(FlowError::Report);
        }
        self.pending.clear();
        self.report = report;
        self.sent = report.received;
        self.sent_bytes = report.received_bytes;
        Ok(true)
    }

    /// Latest verified receiver accounting, including the channel's fixed base.
    pub const fn report(&self) -> Report {
        self.report
    }

    /// Exact channel to attach to payloads and check on incoming reports.
    pub const fn channel(&self) -> Channel {
        self.report.channel
    }

    /// Highest PEER reservation or independently verified PUB receipt.
    /// Not evidence of transport submission, persistence, or a quorum.
    pub const fn sent(&self) -> Prefix {
        self.sent
    }

    /// Latest verified volatile receipt. Never use as a durable quorum ACK.
    pub const fn received(&self) -> Prefix {
        self.report.received
    }

    /// Intersection of unreserved remote credits and local outstanding capacity.
    /// May be zero; not a reservation. Remote credit cannot grow the local ledger.
    pub fn available(&self) -> PipelineLimits {
        let remote_count = self.report.operation_limit - (self.sent.op.0 - self.report.base.op.0);
        let remote_bytes = self.report.byte_limit - self.sent_bytes;
        let local_count = self.limits.max_operations - self.pending.len();
        let local_bytes =
            self.limits.max_body_bytes as u64 - (self.sent_bytes - self.report.received_bytes);
        PipelineLimits {
            max_operations: remote_count.min(local_count as u64) as usize,
            max_body_bytes: remote_bytes.min(local_bytes) as usize,
        }
    }

    /// Bounded outstanding metadata, in canonical order, for probe/repair planning.
    /// Retransmission reuses these existing reservations rather than calling `record_send`.
    pub fn outstanding(&self) -> impl ExactSizeIterator<Item = &Operation> {
        self.pending.iter()
    }

    /// Reserve credit once for a new contiguous suffix retained in the local send path.
    ///
    /// Reserve local payload/outbox capacity first. After success the adapter must
    /// retain the transmission across local backpressure or fence the whole epoch.
    /// Do not call again for a retry, duplicate, or packet still queued locally.
    pub fn record_send(
        &mut self,
        channel: Channel,
        operations: &[Operation],
    ) -> Result<(), FlowError> {
        if channel != self.report.channel {
            return Err(FlowError::Channel);
        }
        if operations.len() > self.limits.max_operations - self.pending.len() {
            return Err(FlowError::Capacity);
        }
        let (end, bytes) = batch(operations, self.sent)?;
        let total = self
            .sent_bytes
            .checked_add(bytes)
            .ok_or(FlowError::Exhausted)?;
        if end.op.0 - self.report.base.op.0 > self.report.operation_limit
            || total > self.report.byte_limit
            || total - self.report.received_bytes > self.limits.max_body_bytes as u64
        {
            return Err(FlowError::Capacity);
        }
        self.pending.extend(operations.iter().copied());
        self.sent = end;
        self.sent_bytes = total;
        Ok(())
    }

    /// Incorporate same-epoch receipt/credit progress. Return false for stale or exact repeats.
    ///
    /// Check the exact receipt boundary and cumulative bytes against reserved sends
    /// before freeing metadata. Conflicting same-revision reports and all retractions
    /// fail without changing state. Epoch changes require a separate correlated open.
    pub fn observe(&mut self, report: Report) -> Result<bool, FlowError> {
        if report.channel != self.report.channel {
            return Err(FlowError::Channel);
        }
        report.validate_shape()?;
        if report.base != self.report.base {
            return Err(FlowError::History);
        }
        if report.revision < self.report.revision {
            return Ok(false);
        }
        if report.revision == self.report.revision {
            return if report == self.report {
                Ok(false)
            } else {
                Err(FlowError::Report)
            };
        }
        if report.received.op < self.report.received.op
            || report.operation_limit < self.report.operation_limit
            || report.byte_limit < self.report.byte_limit
        {
            return Err(FlowError::Report);
        }
        let mut end = self.report.received;
        let mut bytes = self.report.received_bytes;
        let mut count = 0;
        for operation in &self.pending {
            if operation.prefix.op > report.received.op {
                break;
            }
            end = operation.prefix;
            bytes = bytes
                .checked_add(operation.body_bytes)
                .ok_or(FlowError::Exhausted)?;
            count += 1;
        }
        if end != report.received || bytes != report.received_bytes {
            return Err(FlowError::History);
        }
        self.pending.drain(..count);
        self.report = report;
        Ok(true)
    }
}
