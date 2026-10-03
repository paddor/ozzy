//! Encoded and decoded record aliases retain the same bounded input reservation.

use super::Decoder;
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::data::{DataLimits, OwnedRecord, OwnedRecordIter, OwnedRecords};

pub(super) struct FrameRecords {
    records: OwnedRecords,
    owner: Bytes,
}

pub(super) struct Batch {
    records: OwnedRecordIter,
    owner: Bytes,
}

impl FrameRecords {
    pub(super) fn new(records: OwnedRecords, message: &Message) -> Self {
        Self {
            records,
            owner: message.part_bytes(1).expect("validated envelope frame"),
        }
    }

    pub(super) fn into_batch(self, skip: u64) -> Batch {
        let mut records = self.records.into_records();
        for _ in 0..skip {
            records.next().expect("validated skipped prefix");
        }
        Batch {
            records,
            owner: self.owner,
        }
    }
}

impl Batch {
    pub(super) fn next(
        &mut self,
        decoder: &mut Decoder,
        limits: DataLimits,
    ) -> Result<Option<OwnedRecord>, crate::replicated::ReaderError> {
        let Some(record) = self.records.next() else {
            return Ok(None);
        };
        let mut record = decoder.decode(record, limits)?;
        for bytes in &mut record.payload {
            *bytes = Bytes::from_owner(RecordFrame {
                bytes: std::mem::take(bytes),
                _owner: self.owner.clone(),
            });
        }
        Ok(Some(record))
    }
}

struct RecordFrame {
    bytes: Bytes,
    _owner: Bytes,
}

impl AsRef<[u8]> for RecordFrame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests;
