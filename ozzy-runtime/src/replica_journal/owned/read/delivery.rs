//! Reuse native reader delivery for the new journal placement. Resident SDK
//! batches retain their encoded backing; cold reads keep the bounded copy path.

use super::{
    JournalError, OwnedJournal, PartitionReadCursor, PartitionReadError, PartitionReadLimits,
    PreparedRead, Rc, Source, read_fault,
};
use crate::replica_journal::read::delivery::{ReadDelivery, ResidentPartition, ResidentSource};

/// Read selection awaiting the same owner checks as copied historical reads.
#[derive(Debug)]
pub struct CompletedDelivery {
    key: Rc<()>,
    cursor: PartitionReadCursor,
    limits: PartitionReadLimits,
    result: Result<ReadDelivery, JournalError>,
}

/// Owner-checked records retaining the subscriber's output admission. Encoding
/// shares whole resident SDK batches when credit permits, including LZ4 bytes.
#[derive(Debug)]
pub struct PartitionDelivery {
    read: ReadDelivery,
    limits: PartitionReadLimits,
}

impl PartitionDelivery {
    pub(in crate::replica_journal) fn into_native(self) -> ReadDelivery {
        self.read
    }

    /// Encode through the existing reader contract. `maximum` is the reader's
    /// whole credit window; a fitting compressed batch waits for enough credit
    /// instead of being decompressed merely because current credit is smaller.
    /// The encoder's remaining capacity must fit the captured read limits.
    pub fn encode(
        self,
        output: &mut ozzy_proto::reader::RecordsEncoder<'_>,
        maximum: ozzy_proto::data::DataLimits,
    ) -> Result<PartitionReadCursor, JournalError> {
        let remaining = output.remaining();
        if remaining.max_records > self.limits.max_records
            || remaining.max_parts > self.limits.max_parts
            || remaining.envelope.max_payload_bytes > self.limits.max_payload_bytes
        {
            return Err(PartitionReadError::Limits.into());
        }
        self.read.encode(output, maximum)
    }
}

impl PreparedRead {
    /// Resolve resident captures without file work or payload copying; execute
    /// cold captures through the shared backend. Neither path advances a reader
    /// until the owner checks the result and transport encoding succeeds.
    pub async fn read_delivery(self) -> CompletedDelivery {
        let key = self.key.clone();
        let cursor = self.cursor;
        let limits = self.limits;
        let result = match self.into_resident() {
            Ok(delivery) => Ok(ReadDelivery::Resident(delivery)),
            Err(read) => read.read().await.result.map(ReadDelivery::Copied),
        };
        CompletedDelivery {
            key,
            cursor,
            limits,
            result,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "retain captured cold work without allocating"
    )]
    fn into_resident(mut self) -> Result<ResidentPartition, Self> {
        let records = match std::mem::replace(&mut self.source, Source::Empty) {
            Source::Empty => None,
            Source::Memory(records) => Some(ResidentSource::Pending(records)),
            Source::Stored(records) => match records.into_resident() {
                Ok(records) => Some(ResidentSource::Stored(records)),
                Err(records) => {
                    self.source = Source::Stored(records);
                    return Err(self);
                }
            },
        };
        Ok(ResidentPartition {
            cursor: self.cursor,
            _lease: self.buffer,
            records,
        })
    }
}

impl OwnedJournal {
    /// Recheck scope, generation, retained range and owner before giving native
    /// output encoding access to a detached result. Stale results grant nothing.
    pub fn complete_delivery(
        &mut self,
        done: CompletedDelivery,
    ) -> Result<PartitionDelivery, JournalError> {
        self.healthy()?;
        if !Rc::ptr_eq(&self.read_key, &done.key) {
            return Err(PartitionReadError::Fenced.into());
        }
        self.check_reader(done.cursor)?;
        self.faulted |= done.result.as_ref().err().is_some_and(read_fault);
        done.result.map(|read| PartitionDelivery {
            read,
            limits: done.limits,
        })
    }
}
