//! Validated record selectors shared by pending and resident async reads.

use std::{
    ops::Range,
    sync::{Arc, OnceLock},
};

use bytes::Bytes;
use ozzy_journal::operation::{
    AppendBatchSummary, AppendBatchView, PreparedAppendPayload, decode_append_batches,
};
use ozzy_proto::{MessageId, Offset, ProducerSequence};
use smallvec::SmallVec;

use super::{IndexedReadError, IndexedRecord, shared_range};
use crate::{OffsetIndexEntry, OperationLimits};

mod records;
mod span;
pub use span::RecordSpan;
mod resident;
use records::{CachedRecords, RecordRef};
pub use resident::DEFAULT_RESIDENT_BYTES;
pub(crate) use resident::ResidentOperations;
mod prepared;
pub use prepared::PreparedOperationRecords;

#[derive(Debug)]
pub(crate) struct CachedOperation {
    location: crate::OperationLocation,
    body: Bytes,
    shared_backing_bytes: Option<usize>,
    batches: Arc<[CachedBatch]>,
}

impl CachedOperation {
    pub(crate) fn from_body(
        entry: OffsetIndexEntry,
        body: Bytes,
        limits: OperationLimits,
    ) -> Result<Self, IndexedReadError> {
        let append = decode_append_batches(&body, limits)?;
        let batches = append
            .iter()
            .map(|batch| cache_batch(&body, batch))
            .collect::<Result<Vec<_>, _>>()?;
        drop(append);
        Ok(Self {
            location: entry.location.operation,
            body,
            shared_backing_bytes: None,
            batches: batches.into(),
        })
    }

    pub(crate) fn select(
        &self,
        entry: OffsetIndexEntry,
    ) -> Result<SelectedBatch<'_>, IndexedReadError> {
        if self.location != entry.location.operation {
            return Err(IndexedReadError::InvalidLocation);
        }
        let batch = self
            .batches
            .get(entry.location.batch_index as usize)
            .ok_or(IndexedReadError::InvalidSelector)?;
        Ok(SelectedBatch {
            location: &self.location,
            body: &self.body,
            shared_backing_bytes: self.shared_backing_bytes,
            batch,
            batch_index: entry.location.batch_index,
            decoded: DecodedCell::default(),
        })
    }
    /// Body and selector bytes. Decompressed producer blocks belong to single
    /// reads, never to cached or resident entries.
    pub(crate) fn resident_bytes(&self) -> usize {
        self.shared_backing_bytes.unwrap_or(self.body.len())
            + self.batches.len() * size_of::<CachedBatch>()
            + self
                .batches
                .iter()
                .map(|batch| batch.records.retained_bytes())
                .sum::<usize>()
    }
}

#[derive(Debug)]
struct CachedBatch {
    summary: AppendBatchSummary,
    timestamp_millis: u64,
    records: CachedRecords,
    descriptors_end: Option<usize>,
    prepared: Option<CachedPrepared>,
}

#[derive(Debug)]
struct CachedPrepared {
    descriptors: Range<usize>,
    encoded_payload: Range<usize>,
    decoded_bytes: usize,
}

/// Decompressed producer block of one batch, owned by a single read. Shared
/// resident and pending records keep only the compressed block, so dropping
/// the read releases these bytes.
#[derive(Debug, Default)]
pub(crate) struct DecodedCell(OnceLock<Bytes>);

impl DecodedCell {
    fn get<'a>(&'a self, batch: &CachedBatch, body: &'a Bytes) -> &'a Bytes {
        let Some(prepared) = &batch.prepared else {
            return body;
        };
        self.0.get_or_init(|| {
            Bytes::from(
                lz4rip::block::decompress(
                    &body[prepared.encoded_payload.clone()],
                    prepared.decoded_bytes,
                )
                .expect("validated canonical LZ4 block"),
            )
        })
    }
}

/// Read-owned decompression cells for every batch of one operation.
#[derive(Debug, Default)]
pub struct DecodedBatches {
    owner: usize,
    cells: Box<[DecodedCell]>,
}

impl DecodedBatches {
    fn for_batches(batches: &Arc<[CachedBatch]>) -> Self {
        Self {
            owner: batches.as_ptr() as usize,
            cells: batches.iter().map(|_| DecodedCell::default()).collect(),
        }
    }

    fn cell(&self, batches: &Arc<[CachedBatch]>, index: usize) -> &DecodedCell {
        assert_eq!(
            self.owner,
            batches.as_ptr() as usize,
            "decoded cells belong to another operation"
        );
        &self.cells[index]
    }
}

#[derive(Debug)]
struct CachedRecord {
    descriptor_start: usize,
    encoding: ozzy_proto::data::Encoding,
    message_id: MessageId,
    parts: SmallVec<[Range<usize>; 2]>,
}

/// One validated batch borrowed for consecutive indexed records. Keeping this
/// view avoids repeating source checks and cache lookup for every record.
#[derive(Debug)]
pub(crate) struct SelectedBatch<'a> {
    location: &'a crate::OperationLocation,
    body: &'a Bytes,
    shared_backing_bytes: Option<usize>,
    batch: &'a CachedBatch,
    batch_index: u32,
    decoded: DecodedCell,
}

impl SelectedBatch<'_> {
    pub(crate) fn span(
        &self,
        entry: OffsetIndexEntry,
        count: usize,
    ) -> Result<RecordSpan<'_>, IndexedReadError> {
        self.record(entry)?
            .ok_or(IndexedReadError::InvalidSelector)?;
        let start = entry.location.record_index as usize;
        let end = start
            .checked_add(count)
            .filter(|&end| count != 0 && end <= self.batch.records.len())
            .ok_or(IndexedReadError::InvalidSelector)?;
        Ok(RecordSpan {
            body: self.body,
            shared_backing_bytes: self.shared_backing_bytes,
            batch: self.batch,
            decoded: &self.decoded,
            range: start..end,
        })
    }

    /// A different operation or batch requires selection through the reader.
    /// Exact record bounds, partition and offset remain checked on every visit.
    #[inline]
    pub(crate) fn record(
        &self,
        entry: OffsetIndexEntry,
    ) -> Result<Option<RecordView<'_>>, IndexedReadError> {
        if *self.location != entry.location.operation
            || self.batch_index != entry.location.batch_index
        {
            return Ok(None);
        }
        let record = self
            .batch
            .records
            .get(entry.location.record_index as usize)
            .ok_or(IndexedReadError::InvalidSelector)?;
        let index = u64::from(entry.location.record_index);
        if self.batch.summary.partition != entry.partition
            || self.batch.summary.first_offset.get() + index != entry.offset.get()
        {
            return Err(IndexedReadError::RecordMismatch);
        }
        Ok(Some(RecordView {
            body: self.body,
            shared_backing_bytes: self.shared_backing_bytes,
            batch: self.batch,
            decoded: &self.decoded,
            record,
            index,
        }))
    }
}

/// Selected metadata and payload ranges borrow the one cached operation. Only
/// materializing or compacting this view creates owning output frame slices.
#[derive(Debug)]
pub struct RecordView<'a> {
    body: &'a Bytes,
    shared_backing_bytes: Option<usize>,
    batch: &'a CachedBatch,
    decoded: &'a DecodedCell,
    record: RecordRef<'a>,
    index: u64,
}

impl RecordView<'_> {
    /// Share only when the complete backing allocation has a known size within
    /// the caller's budget. A small slice does not prove a small allocation.
    pub fn payload_backing(
        &self,
        maximum: usize,
    ) -> Option<(&Bytes, impl ExactSizeIterator<Item = Range<usize>> + Clone)> {
        if let Some(prepared) = &self.batch.prepared {
            if prepared.decoded_bytes > maximum {
                return None;
            }
            return Some((
                self.decoded.get(self.batch, self.body),
                self.record.parts.iter(),
            ));
        }
        (self.shared_backing_bytes? <= maximum).then(|| (self.body, self.record.parts.iter()))
    }

    /// Payload representation, without decoding its bytes.
    pub fn encoding(&self) -> ozzy_proto::data::Encoding {
        self.record.encoding
    }

    /// Stable identity of this validated selected record.
    #[inline]
    pub fn message_id(&self) -> MessageId {
        self.record.message_id
    }

    /// Borrow exact multipart bytes without constructing owning frame slices.
    #[inline]
    pub fn parts(&self) -> impl ExactSizeIterator<Item = &[u8]> + Clone {
        let body = self.decoded.get(self.batch, self.body);
        self.record.parts.iter().map(|range| &body[range.clone()])
    }

    /// Part count from the descriptor, without decompressing the payload.
    #[inline]
    pub fn part_count(&self) -> usize {
        self.record.parts.iter().len()
    }

    /// Opaque bytes in this record, excluding descriptors.
    #[inline]
    pub fn payload_bytes(&self) -> usize {
        self.record.parts.iter().map(|range| range.len()).sum()
    }

    /// Create owning output slices only when a record is actually delivered.
    pub fn materialize(&self) -> IndexedRecord {
        self.materialize_reusing(&mut SmallVec::new())
    }

    pub fn offset(&self) -> Offset {
        Offset::new(self.batch.summary.first_offset.get() + self.index)
    }

    pub(crate) fn materialize_reusing(&self, parts: &mut SmallVec<[Bytes; 2]>) -> IndexedRecord {
        debug_assert!(parts.is_empty());
        let body = self.decoded.get(self.batch, self.body);
        parts.extend(
            self.record
                .parts
                .iter()
                .map(|range| body.slice(range.clone())),
        );
        self.with_parts(std::mem::take(parts))
    }

    fn with_parts(&self, parts: SmallVec<[Bytes; 2]>) -> IndexedRecord {
        let summary = self.batch.summary;
        IndexedRecord {
            encoding: self.record.encoding,
            partition: summary.partition,
            owner_epoch: summary.owner_epoch,
            producer_id: summary.producer_id,
            producer_epoch: summary.producer_epoch,
            producer_sequence: ProducerSequence::new(summary.first_sequence.get() + self.index),
            offset: Offset::new(summary.first_offset.get() + self.index),
            append_timestamp_millis: self.batch.timestamp_millis,
            message_id: self.record.message_id,
            parts,
        }
    }
}

fn cache_batch(body: &Bytes, batch: &AppendBatchView<'_>) -> Result<CachedBatch, IndexedReadError> {
    let summary = batch.summary;
    let last = u64::try_from(summary.record_count.saturating_sub(1))
        .map_err(|_| IndexedReadError::InvalidSelector)?;
    summary
        .first_offset
        .get()
        .checked_add(last)
        .ok_or(IndexedReadError::InvalidSelector)?;
    summary
        .first_sequence
        .get()
        .checked_add(last)
        .ok_or(IndexedReadError::InvalidSelector)?;
    if let Some(prepared) = batch.prepared_payload {
        return cache_prepared_batch(body, batch, prepared);
    }
    let decoded = batch.raw_records().expect("raw append payload");
    if let Some(lengths) = decoded.tiny_lengths() {
        let (ids, payload) = decoded.remaining_bytes();
        let mut offsets = Vec::with_capacity(summary.record_count + 1);
        let mut next = shared_range(body, payload)?.start;
        offsets.push(next);
        for &len in lengths {
            next += len as usize;
            offsets.push(next);
        }
        return Ok(CachedBatch {
            summary,
            timestamp_millis: batch.append_timestamp_millis,
            descriptors_end: None,
            records: CachedRecords::Packed {
                ids: ids
                    .as_chunks::<16>()
                    .0
                    .iter()
                    .copied()
                    .map(MessageId::from_bytes)
                    .collect(),
                offsets,
            },
            prepared: None,
        });
    }
    let remaining = decoded.remaining_bytes();
    let descriptor_range = shared_range(body, remaining.0)?;
    let mut records = Vec::with_capacity(summary.record_count);
    let mut descriptor_start = descriptor_range.start;
    for record in decoded {
        let mut parts = SmallVec::new();
        for part in record.parts {
            parts.push(shared_range(body, part)?);
        }
        records.push(CachedRecord {
            descriptor_start,
            encoding: record.encoding,
            message_id: record.message_id,
            parts,
        });
        descriptor_start += 20
            + record.encoding.metadata_bytes()
            + records.last().expect("inserted record").parts.len() * 4;
    }
    Ok(CachedBatch {
        summary,
        timestamp_millis: batch.append_timestamp_millis,
        records: CachedRecords::General(records),
        descriptors_end: Some(descriptor_start),
        prepared: None,
    })
}

fn cache_prepared_batch(
    body: &Bytes,
    batch: &AppendBatchView<'_>,
    prepared: PreparedAppendPayload<'_>,
) -> Result<CachedBatch, IndexedReadError> {
    let (descriptors, lengths) = batch.descriptors().remaining_bytes();
    if lengths.is_some() {
        return Err(IndexedReadError::InvalidSelector);
    }
    let descriptor_range = shared_range(body, descriptors)?;
    let mut records = Vec::with_capacity(batch.summary.record_count);
    let mut descriptor_start = descriptor_range.start;
    let mut payload_start = 0_usize;
    for record in batch.descriptors() {
        let mut parts = SmallVec::new();
        for length in record.part_lengths {
            let end = payload_start
                .checked_add(length)
                .ok_or(IndexedReadError::InvalidSelector)?;
            parts.push(payload_start..end);
            payload_start = end;
        }
        records.push(CachedRecord {
            descriptor_start,
            encoding: record.encoding,
            message_id: record.message_id,
            parts,
        });
        descriptor_start += 20
            + record.encoding.metadata_bytes()
            + records.last().expect("inserted record").parts.len() * 4;
    }
    if payload_start != prepared.decoded_bytes {
        return Err(IndexedReadError::InvalidSelector);
    }
    Ok(CachedBatch {
        summary: batch.summary,
        timestamp_millis: batch.append_timestamp_millis,
        records: CachedRecords::General(records),
        descriptors_end: Some(descriptor_start),
        prepared: Some(CachedPrepared {
            descriptors: descriptor_range,
            encoded_payload: shared_range(body, prepared.encoded)?,
            decoded_bytes: prepared.decoded_bytes,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_journal::operation::{
        Append, AppendBatch, AppendRecord, OperationBody, decode_append_view, encode_operation_body,
    };
    use ozzy_proto::{OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId};

    pub(super) fn body(offset: u64, sequence: u64) -> Bytes {
        Bytes::from(
            encode_operation_body(
                &OperationBody::Append(Append {
                    batches: vec![AppendBatch {
                        partition: PartitionIncarnation::new(),
                        owner_epoch: OwnerEpoch::INITIAL,
                        producer_id: ProducerId::new(),
                        producer_epoch: ProducerEpoch::INITIAL,
                        first_sequence: ProducerSequence::new(sequence),
                        first_offset: Offset::new(offset),
                        append_timestamp_millis: 77,
                        records: (0..2)
                            .map(|_| AppendRecord {
                                encoding: ozzy_proto::data::Encoding::Raw,
                                message_id: MessageId::new(),
                                parts: [b"".as_slice(), b"payload"].into_iter().collect(),
                            })
                            .collect(),
                    }],
                }),
                OperationLimits::default(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn cached_descriptors_do_not_create_per_record_payload_owners() {
        let body = body(20, 30);
        let append = decode_append_view(&body, OperationLimits::default()).unwrap();
        let batch = append.batches().next().unwrap();
        let cached = cache_batch(&body, &batch).unwrap();
        assert!(
            body.is_unique(),
            "descriptors must borrow ranges, not clone Bytes"
        );
        assert!(std::mem::size_of::<CachedRecord>() < std::mem::size_of::<IndexedRecord>());
        let view = RecordView {
            body: &body,
            shared_backing_bytes: None,
            batch: &cached,
            decoded: &DecodedCell::default(),
            record: cached.records.get(1).unwrap(),
            index: 1,
        };
        let materialized = view.materialize();
        assert_eq!(
            materialized
                .parts
                .iter()
                .map(Bytes::as_ref)
                .collect::<Vec<_>>(),
            [b"".as_slice(), b"payload"]
        );
        assert_eq!(materialized.offset.get(), 21);
        assert_eq!(materialized.producer_sequence.get(), 31);
        assert_eq!(materialized.append_timestamp_millis, 77);
    }

    #[test]
    fn wide_materialization_reuses_descriptor_capacity_without_pinning_payloads() {
        let body = body(20, 30);
        let append = decode_append_view(&body, OperationLimits::default()).unwrap();
        let mut cached = cache_batch(&body, &append.batches().next().unwrap()).unwrap();
        // Four valid empty/nonempty ranges exercise the heap descriptor path.
        let CachedRecords::General(records) = &mut cached.records else {
            panic!("multipart")
        };
        records[0].parts = [0..0, 0..1, 1..1, 1..2].into_iter().collect();
        let view = RecordView {
            body: &body,
            shared_backing_bytes: None,
            batch: &cached,
            decoded: &DecodedCell::default(),
            record: cached.records.get(0).unwrap(),
            index: 0,
        };
        let mut scratch = SmallVec::with_capacity(4);
        let pointer = scratch.as_ptr();
        for _ in 0..8 {
            let mut record = view.materialize_reusing(&mut scratch);
            assert_eq!(record.parts.as_ptr(), pointer);
            assert_eq!(
                record.parts.iter().map(Bytes::len).collect::<Vec<_>>(),
                [0, 1, 0, 1]
            );
            scratch = std::mem::take(&mut record.parts);
            scratch.clear();
            assert!(
                body.is_unique(),
                "idle scratch must not own any operation bytes"
            );
        }
    }

    #[test]
    fn cache_rejects_position_overflow_before_any_unchecked_selection() {
        for (offset, sequence) in [(u64::MAX, 0), (0, u64::MAX)] {
            let body = body(0, 0);
            let append = decode_append_view(&body, OperationLimits::default()).unwrap();
            // The wire codec also rejects this. Exercise the cache's own
            // checked boundary using the view's public metadata fields.
            let mut batch = append.batches().next().unwrap();
            batch.summary.first_offset = Offset::new(offset);
            batch.summary.first_sequence = ProducerSequence::new(sequence);
            assert!(matches!(
                cache_batch(&body, &batch),
                Err(IndexedReadError::InvalidSelector)
            ));
        }
    }
}
