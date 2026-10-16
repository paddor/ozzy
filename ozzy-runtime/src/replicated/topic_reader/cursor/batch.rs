//! Encoded and decoded record aliases retain the same bounded input reservation.

use super::Decoder;
use bytes::Bytes;
use ozzy_proto::{
    append::PayloadEncoding,
    data::{DataLimits, Encoding, OwnedRecord, OwnedRecordIter, OwnedRecords},
};

pub(super) struct FrameRecords {
    records: OwnedRecords,
    owner: Bytes,
}

pub(super) struct Batch {
    records: OwnedRecordIter,
    owner: Bytes,
}

impl FrameRecords {
    /// Records decoded from a delivery, kept alive with its header frame.
    /// Every received frame carries the same input reservation: raw payloads
    /// alias the payload frame, and a decompressed delivery shares one owner.
    pub(super) fn new(records: OwnedRecords, encoding: PayloadEncoding, owner: Bytes) -> Self {
        let records = if encoding == PayloadEncoding::Raw {
            records
        } else {
            records.with_payload_owner(|bytes| retained(bytes, owner.clone()))
        };
        Self { records, owner }
    }

    pub(super) fn into_batch(self, skip: u64) -> Batch {
        let mut records = self.records.into_records();
        for _ in 0..skip {
            if records.next().is_none() {
                break;
            }
        }
        Batch {
            records,
            owner: self.owner,
        }
    }
}

impl Batch {
    #[inline]
    pub(super) fn next(
        &mut self,
        decoder: &mut Decoder,
        limits: DataLimits,
    ) -> Result<Option<OwnedRecord>, crate::replicated::ReaderError> {
        let Some(record) = self.records.next() else {
            return Ok(None);
        };
        // Only a compressed record decodes into new storage. A raw record
        // skips the decoder so it is not moved through another call.
        if record.encoding == Encoding::Raw {
            return Ok(Some(record));
        }
        let mut record = decoder.decode(record, limits)?;
        for bytes in &mut record.payload {
            *bytes = retained(std::mem::take(bytes), self.owner.clone());
        }
        Ok(Some(record))
    }
}

/// `bytes` that also keep the header frame, and with it the reservation.
fn retained(bytes: Bytes, owner: Bytes) -> Bytes {
    Bytes::from_owner(RecordFrame {
        bytes,
        _owner: owner,
    })
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
