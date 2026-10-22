//! Adopting a verified report: publication progress, a reopen, or the first ledger.

use std::time::Duration;

use super::{FlowError, OpenRequest, Prefix, Report, Sender, TransmitError, Transmitter};

impl Transmitter {
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
}
