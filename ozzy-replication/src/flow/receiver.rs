#[cfg(test)]
mod tests;

use super::{
    Channel, FlowError, Operation, PipelineLimits, Prefix, ReceiveEpoch, Report, VecDeque, batch,
    capacities, ledger,
};

/// Local retained-body accounting. Capacity is never advertised to a sender.
/// The adapter reserves actual payload backing before retaining a suffix.
#[derive(Debug)]
pub struct Receiver {
    report: Report,
    released: Prefix,
    released_bytes: u64,
    retained: VecDeque<Operation>,
    limits: PipelineLimits,
}

impl Receiver {
    /// Start at an exact applied prefix with a bounded local metadata window.
    pub fn new(channel: Channel, base: Prefix, limits: PipelineLimits) -> Result<Self, FlowError> {
        capacities(limits)?;
        let report = Report {
            channel,
            revision: 1,
            base,
            received: base,
            received_bytes: 0,
        };
        report.validate_shape()?;
        Ok(Self {
            report,
            released: base,
            released_bytes: 0,
            retained: ledger(limits)?,
            limits,
        })
    }

    /// Exact contiguous receipt. This supplies no persistence or vote evidence.
    pub const fn report(&self) -> Report {
        self.report
    }

    /// Local room after every still-retained body, independently of queue slots.
    pub fn available(&self) -> PipelineLimits {
        PipelineLimits {
            max_operations: self.limits.max_operations - self.retained.len(),
            max_body_bytes: self.limits.max_body_bytes
                - (self.report.received_bytes - self.released_bytes) as usize,
        }
    }

    /// Install a fresh incarnation after all old payload ownership is fenced.
    pub fn reinitialize(&mut self, channel: Channel, applied: Prefix) -> Result<(), FlowError> {
        if channel.epoch == self.report.channel.epoch {
            return Err(FlowError::Channel);
        }
        let report = Report {
            channel,
            revision: 1,
            base: applied,
            received: applied,
            received_bytes: 0,
        };
        report.validate_shape()?;
        self.retained.clear();
        self.released = applied;
        self.released_bytes = 0;
        self.report = report;
        Ok(())
    }

    /// Retract only unadmitted history. Accepted bodies remain charged.
    /// Caller fences old packets and supplies a never-reused receive epoch.
    pub fn retract(&mut self, epoch: ReceiveEpoch, through: Prefix) -> Result<(), FlowError> {
        if epoch == self.report.channel.epoch {
            return Err(FlowError::Channel);
        }
        let count = if through == self.released {
            0
        } else {
            self.retained
                .iter()
                .position(|op| op.prefix == through)
                .ok_or(FlowError::History)?
                + 1
        };
        let received_bytes = self
            .retained
            .iter()
            .take(count)
            .try_fold(0u64, |sum, op| sum.checked_add(op.body_bytes))
            .ok_or(FlowError::Exhausted)?;
        let report = Report {
            channel: Channel {
                epoch,
                ..self.report.channel
            },
            revision: 1,
            base: self.released,
            received: through,
            received_bytes,
        };
        report.validate_shape()?;
        self.retained.truncate(count);
        self.released_bytes = 0;
        self.report = report;
        Ok(())
    }

    /// Charge a complete contiguous suffix after physical backing is reserved.
    /// Duplicates are filtered by the adapter before this transition.
    pub fn retain(&mut self, channel: Channel, operations: &[Operation]) -> Result<(), FlowError> {
        if channel != self.report.channel {
            return Err(FlowError::Channel);
        }
        let (end, bytes) = batch(operations, self.report.received)?;
        let available = self.available();
        if operations.len() > available.max_operations || bytes > available.max_body_bytes as u64 {
            return Err(FlowError::Capacity);
        }
        let total = self
            .report
            .received_bytes
            .checked_add(bytes)
            .ok_or(FlowError::Exhausted)?;
        let revision = self
            .report
            .revision
            .checked_add(1)
            .ok_or(FlowError::Exhausted)?;
        self.retained.extend(operations.iter().copied());
        self.report.received = end;
        self.report.received_bytes = total;
        self.report.revision = revision;
        Ok(())
    }

    /// Release after application and policy-specific physical persistence.
    /// This changes local room only. It emits no receipt or admission grant.
    pub fn release(&mut self, through: Prefix) -> Result<(), FlowError> {
        if through == self.released {
            return Ok(());
        }
        let count = self
            .retained
            .iter()
            .position(|op| op.prefix == through)
            .ok_or(FlowError::History)?
            + 1;
        let bytes: u64 = self
            .retained
            .iter()
            .take(count)
            .map(|op| op.body_bytes)
            .sum();
        let released_bytes = self
            .released_bytes
            .checked_add(bytes)
            .ok_or(FlowError::Exhausted)?;
        self.retained.drain(..count);
        self.released = through;
        self.released_bytes = released_bytes;
        Ok(())
    }
}
