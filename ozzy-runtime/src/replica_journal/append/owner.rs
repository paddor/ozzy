//! An owner append is one producer-less APPEND batch at the offset its one
//! writer expects. Validation checks that offset and stamps the owner epoch
//! and timestamp.

use super::{AppendBuffer, JournalError, PARTITION, POSITION, RequestKind};
use ozzy_journal::operation::{Append, AppendBatch, AppendRecord, OperationBody};
use ozzy_proto::{
    Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence,
};

/// The owner epoch field follows the partition.
const OWNER_EPOCH: usize = PARTITION + 16;

impl AppendBuffer {
    /// Whether this buffer holds one owner batch.
    pub(in crate::replica_journal) fn is_owner(&self) -> bool {
        self.request_kind == RequestKind::Owner
    }

    /// Encode one owner batch into an empty buffer. The owner epoch and the
    /// timestamp are placeholders until validation.
    pub(in crate::replica_journal) fn prepare_owner_batch(
        &mut self,
        partition: PartitionIncarnation,
        first_offset: Offset,
        records: Vec<AppendRecord<'_>>,
    ) -> Result<(), JournalError> {
        if !self.is_empty() || records.is_empty() {
            return Err(JournalError::AppendMismatch);
        }
        self.push_typed_body(&OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition,
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([0; 16]),
                producer_epoch: ProducerEpoch::new(0),
                first_sequence: ProducerSequence::ZERO,
                first_offset,
                append_timestamp_millis: 0,
                records: records.into(),
            }],
        }))?;
        self.request_kind = RequestKind::Owner;
        Ok(())
    }

    /// The partition and first offset of the one owner batch.
    pub(in crate::replica_journal) fn owner_target(
        &self,
    ) -> Result<(PartitionIncarnation, Offset), JournalError> {
        let body = self.owner_body()?;
        let partition = body[PARTITION..PARTITION + 16]
            .try_into()
            .map_err(|_| JournalError::AppendMismatch)?;
        let offset = body[POSITION..POSITION + 8]
            .try_into()
            .map_err(|_| JournalError::AppendMismatch)?;
        Ok((
            PartitionIncarnation::from_bytes(partition),
            Offset::new(u64::from_be_bytes(offset)),
        ))
    }

    /// Write the owner epoch and the timestamp of the one owner batch. The
    /// first offset stays as the writer set it.
    pub(in crate::replica_journal) fn place_owner(
        &mut self,
        owner_epoch: OwnerEpoch,
        timestamp: u64,
    ) -> Result<(), JournalError> {
        let (_, first_offset) = self.owner_target()?;
        let start = self.entries[0].body.start;
        let epoch: &mut [u8; 8] = self
            .bodies
            .mutable()?
            .get_mut(start + OWNER_EPOCH..start + OWNER_EPOCH + 8)
            .and_then(|field| field.try_into().ok())
            .ok_or(JournalError::AppendMismatch)?;
        *epoch = owner_epoch.get().to_be_bytes();
        let mut position = [0; 16];
        position[..8].copy_from_slice(&first_offset.get().to_be_bytes());
        position[8..].copy_from_slice(&timestamp.to_be_bytes());
        self.write_position(0, position).map(drop)
    }

    /// The timestamp validation stamped on a one-batch APPEND body.
    pub(in crate::replica_journal) fn owner_timestamp(&self) -> Option<u64> {
        let entry = self.entries.first().filter(|_| self.entries.len() == 1)?;
        let body = self.bodies.get(entry.body.clone())?;
        body.get(POSITION + 8..POSITION + 16)?
            .try_into()
            .ok()
            .map(u64::from_be_bytes)
    }

    fn owner_body(&self) -> Result<&[u8], JournalError> {
        self.is_owner()
            .then(|| self.bodies.get(self.entries.first()?.body.clone()))
            .flatten()
            .filter(|body| body.len() >= POSITION + 16)
            .ok_or(JournalError::AppendMismatch)
    }
}
