#[cfg(test)]
mod tests;

use super::{
    Channel, FlowError, Operation, PipelineLimits, Prefix, ReceiveEpoch, Report, VecDeque, batch,
    capacities, ledger,
};

/// Preallocated accounting for retained-but-not-released canonical bodies.
///
/// Payloads stay owned by the adapter. Retraction requires a fresh epoch and
/// preserves charges for every operation the adapter still retains above application.
#[derive(Debug)]
pub struct Receiver {
    report: Report,
    released: Prefix,
    released_bytes: u64,
    retained: VecDeque<Operation>,
    limits: PipelineLimits,
    automatic_credit: bool,
}

impl Receiver {
    /// Start at an exact applied prefix, with no outstanding retention yet charged.
    /// Allocates metadata once from the configured operation bound.
    pub fn new(channel: Channel, base: Prefix, limits: PipelineLimits) -> Result<Self, FlowError> {
        Self::initialize(channel, base, limits, true)
    }

    /// Start with no advertised capacity. The shard owner must reserve shared
    /// destination and dispatch capacity before calling `grant`. Releasing data,
    /// changing epochs, and reinitializing never grant capacity automatically.
    pub fn new_reserved(
        channel: Channel,
        base: Prefix,
        limits: PipelineLimits,
    ) -> Result<Self, FlowError> {
        Self::initialize(channel, base, limits, false)
    }

    fn initialize(
        channel: Channel,
        base: Prefix,
        limits: PipelineLimits,
        automatic_credit: bool,
    ) -> Result<Self, FlowError> {
        let capacity = capacities(limits)?;
        let (operation_limit, byte_limit) = if automatic_credit { capacity } else { (0, 0) };
        let report = Report {
            channel,
            revision: 1,
            base,
            received: base,
            received_bytes: 0,
            operation_limit,
            byte_limit,
        };
        report.validate(limits)?;
        Ok(Self {
            report,
            released: base,
            released_bytes: 0,
            retained: ledger(limits)?,
            limits,
            automatic_credit,
        })
    }

    /// Current cumulative state. Repeated reads do not advance its revision.
    pub const fn report(&self) -> Report {
        self.report
    }

    /// Whether only external shard reservations can advertise new capacity.
    pub const fn is_reserved(&self) -> bool {
        !self.automatic_credit
    }

    /// Fence unused grants in a fresh epoch without discarding any retained
    /// operation. Staged, validating, and accepted bodies keep their charges.
    /// The caller fences old packets before reclaiming corresponding unused
    /// destination reservations. This is valid only for external-credit mode.
    pub fn revoke_unused(&mut self, epoch: ReceiveEpoch) -> Result<Report, FlowError> {
        if !self.is_reserved() {
            return Err(FlowError::Invalid);
        }
        self.retract(epoch, self.report.received)?;
        Ok(self.report)
    }

    /// Free capacity excludes both unused grants and previously retained bodies.
    /// It observes this partition's bound, not aggregate shard backing.
    pub fn available(&self) -> PipelineLimits {
        PipelineLimits {
            max_operations: self.limits.max_operations
                - (self.report.operation_limit - (self.released.op.0 - self.report.base.op.0))
                    as usize,
            max_body_bytes: self.limits.max_body_bytes
                - (self.report.byte_limit - self.released_bytes) as usize,
        }
    }

    /// Add externally reserved credit to this exact receive epoch. Count and
    /// byte reservations may advance independently. Unused grants plus retained
    /// operations must still fit the partition bound; aggregate shard admission
    /// is the caller's responsibility. A failed grant changes no counters.
    pub fn grant(
        &mut self,
        channel: Channel,
        operations: u64,
        bytes: u64,
    ) -> Result<(), FlowError> {
        if self.automatic_credit || (operations == 0 && bytes == 0) {
            return Err(FlowError::Invalid);
        }
        if channel != self.report.channel {
            return Err(FlowError::Channel);
        }
        let operation_limit = self
            .report
            .operation_limit
            .checked_add(operations)
            .ok_or(FlowError::Exhausted)?;
        let byte_limit = self
            .report
            .byte_limit
            .checked_add(bytes)
            .ok_or(FlowError::Exhausted)?;
        let (count, capacity) = capacities(self.limits)?;
        if operation_limit - (self.released.op.0 - self.report.base.op.0) > count
            || byte_limit - self.released_bytes > capacity
        {
            return Err(FlowError::Capacity);
        }
        let revision = self
            .report
            .revision
            .checked_add(1)
            .ok_or(FlowError::Exhausted)?;
        self.report.operation_limit = operation_limit;
        self.report.byte_limit = byte_limit;
        self.report.revision = revision;
        Ok(())
    }

    /// Reinitialize after selected-history installation has applied its whole tail.
    ///
    /// The adapter must fence the old scope/generation and retire its payloads first.
    /// This does not authorize discarding accepted history or bypass application.
    /// Use `retract` for same-image staging rejection with accepted bodies retained.
    /// A fresh epoch is mandatory; the existing metadata allocation is reused.
    pub fn reinitialize(&mut self, channel: Channel, applied: Prefix) -> Result<(), FlowError> {
        if channel.epoch == self.report.channel.epoch {
            return Err(FlowError::Channel);
        }
        let (operation_limit, byte_limit) = if self.automatic_credit {
            capacities(self.limits)?
        } else {
            (0, 0)
        };
        let report = Report {
            channel,
            revision: 1,
            base: applied,
            received: applied,
            received_bytes: 0,
            operation_limit,
            byte_limit,
        };
        report.validate(self.limits)?;
        self.retained.clear();
        self.released = applied;
        self.released_bytes = 0;
        self.report = report;
        Ok(())
    }

    /// Retract an unadmitted suffix in a fresh epoch, keeping accepted bodies charged.
    ///
    /// `through` must be the exact last retained operation, or the current applied
    /// release floor to discard everything above it. The adapter must keep every
    /// accepted-but-unapplied operation; this ledger cannot establish acceptance.
    /// Fence old packets and update actual payload ownership before publishing.
    ///
    /// Scope and applied floor stay unchanged. Counters restart at that floor;
    /// retained bytes consume the new window before any credit is advertised.
    /// No allocation occurs, even after cumulative counter/revision exhaustion.
    /// The caller must supply an epoch never used before, not merely one different
    /// from the current epoch. View installation/restart need separate initialization.
    pub fn retract(&mut self, epoch: ReceiveEpoch, through: Prefix) -> Result<(), FlowError> {
        if epoch == self.report.channel.epoch {
            return Err(FlowError::Channel);
        }
        let count = if through == self.released {
            0
        } else {
            self.retained
                .iter()
                .position(|operation| operation.prefix == through)
                .ok_or(FlowError::History)?
                + 1
        };
        let received_bytes = self
            .retained
            .iter()
            .take(count)
            .try_fold(0u64, |sum, operation| sum.checked_add(operation.body_bytes))
            .ok_or(FlowError::Exhausted)?;
        let (operation_limit, byte_limit) = if self.automatic_credit {
            capacities(self.limits)?
        } else {
            (count as u64, received_bytes)
        };
        let report = Report {
            channel: Channel {
                epoch,
                ..self.report.channel
            },
            revision: 1,
            base: self.released,
            received: through,
            received_bytes,
            operation_limit,
            byte_limit,
        };
        report.validate(self.limits)?;
        self.retained.truncate(count);
        self.released_bytes = 0;
        self.report = report;
        Ok(())
    }

    /// Charge a fresh contiguous suffix whose complete bodies the adapter now retains.
    ///
    /// Filter already-received duplicates before calling; this method rejects
    /// noncontiguous inputs without charging them. Reserve payload capacity before
    /// this transition. Publish its report only after actual retention succeeds.
    pub fn retain(&mut self, channel: Channel, operations: &[Operation]) -> Result<(), FlowError> {
        if channel != self.report.channel {
            return Err(FlowError::Channel);
        }
        if operations.len() > self.limits.max_operations - self.retained.len() {
            return Err(FlowError::Capacity);
        }
        let (end, bytes) = batch(operations, self.report.received)?;
        let total = self
            .report
            .received_bytes
            .checked_add(bytes)
            .ok_or(FlowError::Exhausted)?;
        if end.op.0 - self.report.base.op.0 > self.report.operation_limit
            || total > self.report.byte_limit
        {
            return Err(FlowError::Capacity);
        }
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

    /// Release an exact retained prefix after application relinquishes it.
    ///
    /// The caller must supply real application completion; this bookkeeping method
    /// cannot establish commit or durability. Repeating the current floor is a no-op.
    /// Reserved mode releases bookkeeping only. The shard decides whether actual
    /// buffer release permits another grant; this method never makes that decision.
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
        // These bytes are a bounded subset of already checked cumulative retention.
        let bytes: u64 = self
            .retained
            .iter()
            .take(count)
            .map(|op| op.body_bytes)
            .sum();
        let operation_limit = self
            .report
            .operation_limit
            .checked_add(if self.automatic_credit {
                count as u64
            } else {
                0
            })
            .ok_or(FlowError::Exhausted)?;
        let byte_limit = self
            .report
            .byte_limit
            .checked_add(if self.automatic_credit { bytes } else { 0 })
            .ok_or(FlowError::Exhausted)?;
        let released_bytes = self
            .released_bytes
            .checked_add(bytes)
            .ok_or(FlowError::Exhausted)?;
        let revision = self
            .report
            .revision
            .checked_add(1)
            .ok_or(FlowError::Exhausted)?;
        self.retained.drain(..count);
        self.released = through;
        self.released_bytes = released_bytes;
        self.report.operation_limit = operation_limit;
        self.report.byte_limit = byte_limit;
        self.report.revision = revision;
        Ok(())
    }
}
