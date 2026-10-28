//! A release proposal holds one broker-built `Release`, and a carry proposal
//! one broker-built `Carry`. Validation writes the position, like an APPEND
//! position: a `Release` takes offsets and a timestamp, a `Carry` only a
//! timestamp.

use super::{AppendBuffer, JournalError, PARTITION, POSITION};
use ozzy_journal::operation::OperationKind;
use ozzy_proto::PartitionIncarnation;

/// Trailing release key: millis, then schedule sequence.
const THROUGH: usize = 16;

impl AppendBuffer {
    /// Whether this buffer is one broker-built `Release`.
    pub(in crate::replica_journal) fn is_release(&self) -> bool {
        self.is_one(OperationKind::Release)
    }

    /// Whether this buffer is one broker-built `Carry`.
    pub(in crate::replica_journal) fn is_carry(&self) -> bool {
        self.is_one(OperationKind::Carry)
    }

    fn is_one(&self, kind: OperationKind) -> bool {
        !self.is_producer() && self.entries.len() == 1 && self.entries[0].envelope.kind == kind
    }

    /// The partition of the one `Release`, and the release key millis of its
    /// last record.
    pub(in crate::replica_journal) fn release_target(
        &self,
    ) -> Result<(PartitionIncarnation, u64), JournalError> {
        let body = self.body(OperationKind::Release, THROUGH)?;
        let millis = body[body.len() - THROUGH..body.len() - 8]
            .try_into()
            .map_err(|_| JournalError::AppendMismatch)?;
        Ok((partition(body)?, u64::from_be_bytes(millis)))
    }

    /// The partition of the one `Carry`.
    pub(in crate::replica_journal) fn carry_partition(
        &self,
    ) -> Result<PartitionIncarnation, JournalError> {
        partition(self.body(OperationKind::Carry, 0)?)
    }

    /// The body of the one `kind`, with its position and `trailer` more
    /// bytes.
    fn body(&self, kind: OperationKind, trailer: usize) -> Result<&[u8], JournalError> {
        self.is_one(kind)
            .then(|| self.bodies.get(self.entries[0].body.clone()))
            .flatten()
            .filter(|body| body.len() >= POSITION + 16 + trailer)
            .ok_or(JournalError::AppendMismatch)
    }

    /// Write the first offset and timestamp of the one `Release`.
    pub(in crate::replica_journal) fn place_release(
        &mut self,
        first_offset: u64,
        timestamp: u64,
    ) -> Result<(), JournalError> {
        self.place(OperationKind::Release, first_offset, timestamp)
    }

    /// Write the timestamp of the one `Carry`. It takes no offsets.
    pub(in crate::replica_journal) fn place_carry(
        &mut self,
        timestamp: u64,
    ) -> Result<(), JournalError> {
        self.place(OperationKind::Carry, 0, timestamp)
    }

    fn place(
        &mut self,
        kind: OperationKind,
        first_offset: u64,
        timestamp: u64,
    ) -> Result<(), JournalError> {
        if !self.is_one(kind) {
            return Err(JournalError::AppendMismatch);
        }
        let mut position = [0; 16];
        position[..8].copy_from_slice(&first_offset.to_be_bytes());
        position[8..].copy_from_slice(&timestamp.to_be_bytes());
        self.write_position(0, position).map(drop)
    }
}

fn partition(body: &[u8]) -> Result<PartitionIncarnation, JournalError> {
    let partition = body[PARTITION..PARTITION + 16]
        .try_into()
        .map_err(|_| JournalError::AppendMismatch)?;
    Ok(PartitionIncarnation::from_bytes(partition))
}
