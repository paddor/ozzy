//! A release proposal holds one broker-built `Release`. Validation writes its
//! offset and timestamp, like an APPEND position.

use super::{AppendBuffer, JournalError};
use ozzy_journal::operation::OperationKind;
use ozzy_proto::PartitionIncarnation;

/// Byte offset of the partition in a one-batch body, after the batch count.
const PARTITION: usize = 4;

/// Byte offset of the position: batch count, partition, owner epoch,
/// producer, producer epoch, first sequence.
const POSITION: usize = 4 + 16 + 8 + 16 + 8 + 8;

/// Trailing release key: millis, then schedule sequence.
const THROUGH: usize = 16;

impl AppendBuffer {
    /// Whether this buffer is one broker-built `Release`.
    pub(in crate::replica_journal) fn is_release(&self) -> bool {
        !self.is_producer()
            && self.entries.len() == 1
            && self.entries[0].envelope.kind == OperationKind::Release
    }

    /// The partition of the one `Release`, and the release key millis of its
    /// last record.
    pub(in crate::replica_journal) fn release_target(
        &self,
    ) -> Result<(PartitionIncarnation, u64), JournalError> {
        let body = self
            .is_release()
            .then(|| self.bodies.get(self.entries[0].body.clone()))
            .flatten()
            .filter(|body| body.len() >= POSITION + 16 + THROUGH)
            .ok_or(JournalError::AppendMismatch)?;
        let partition = body[PARTITION..PARTITION + 16]
            .try_into()
            .map_err(|_| JournalError::AppendMismatch)?;
        let millis = body[body.len() - THROUGH..body.len() - 8]
            .try_into()
            .map_err(|_| JournalError::AppendMismatch)?;
        Ok((
            PartitionIncarnation::from_bytes(partition),
            u64::from_be_bytes(millis),
        ))
    }

    /// Write the first offset and timestamp of the one `Release`.
    pub(in crate::replica_journal) fn place_release(
        &mut self,
        first_offset: u64,
        timestamp: u64,
    ) -> Result<(), JournalError> {
        if !self.is_release() {
            return Err(JournalError::AppendMismatch);
        }
        let start = self.entries[0].body.start + POSITION;
        let target = self
            .bodies
            .mutable()?
            .get_mut(start..start + 16)
            .ok_or(JournalError::AppendMismatch)?;
        target[..8].copy_from_slice(&first_offset.to_be_bytes());
        target[8..].copy_from_slice(&timestamp.to_be_bytes());
        self.validated = false;
        Ok(())
    }
}
