//! Contiguous selected records keep their encoded descriptors and payload span.

use super::{Bytes, CachedBatch, DecodedCell, Range, RecordView};
use ozzy_proto::Offset;

/// Descriptor table, decoded byte count, and exact producer block within one body.
pub(super) type PreparedBacking<'a> = (&'a [u8], usize, &'a Bytes, Range<usize>);

/// A bounded range within one immutable canonical batch. No new payload owners.
#[derive(Debug)]
pub struct RecordSpan<'a> {
    pub(super) body: &'a Bytes,
    pub(super) shared_backing_bytes: Option<usize>,
    pub(super) batch: &'a CachedBatch,
    pub(super) decoded: &'a DecodedCell,
    pub(super) range: Range<usize>,
}

impl<'a> RecordSpan<'a> {
    /// Exact whole-APPEND producer block when this span covers its full batch.
    pub fn prepared_backing(&self, maximum: usize) -> Option<PreparedBacking<'a>> {
        if self.range.start != 0
            || self.range.end != self.batch.records.len()
            || self.shared_backing_bytes? > maximum
        {
            return None;
        }
        let prepared = self.batch.prepared.as_ref()?;
        Some((
            &self.body[prepared.descriptors.clone()],
            prepared.decoded_bytes,
            self.body,
            prepared.encoded_payload.clone(),
        ))
    }

    /// Whether this batch stores one compressed producer block.
    pub fn is_prepared(&self) -> bool {
        self.batch.prepared.is_some()
    }

    /// Whether the span starts at its batch's first record.
    pub fn starts_batch(&self) -> bool {
        self.range.start == 0
    }

    /// Records, decoded payload bytes, and parts of the whole batch.
    pub fn batch_totals(&self) -> (usize, usize, usize) {
        let records = self.batch.records.len();
        let bytes = self.batch.prepared.as_ref().map_or_else(
            || {
                Self {
                    range: 0..records,
                    ..*self
                }
                .payload_bytes()
            },
            |prepared| prepared.decoded_bytes,
        );
        (records, bytes, self.batch.records.parts())
    }

    /// Combined payload bytes in this selected record or span.
    pub fn payload_bytes(&self) -> usize {
        if self.is_empty() {
            return 0;
        }
        self.batch.records.payload_range(self.range.clone()).len()
    }

    pub(crate) fn limit(mut self, records: usize, bytes: usize) -> Self {
        self.range.end = self
            .range
            .end
            .min(self.range.start + records.min(self.len()));
        if self.payload_bytes() <= bytes {
            return self;
        }
        let mut low = self.range.start;
        let mut high = self.range.end;
        let mut selected = self.range.end;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            self.range.end = middle;
            if self.payload_bytes() <= bytes {
                low = middle;
            } else {
                high = middle - 1;
            }
            selected = low;
        }
        self.range.end = selected;
        self
    }

    /// Number of selected records.
    pub fn len(&self) -> usize {
        self.range.len()
    }

    /// Whether no selected records remain.
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// First partition-global offset in this selected record span.
    pub fn first_offset(&self) -> Offset {
        Offset::new(self.batch.summary.first_offset.get() + self.range.start as u64)
    }

    /// Iterate borrowed selected records in offset order.
    pub fn records(&self) -> impl ExactSizeIterator<Item = RecordView<'a>> + '_ {
        self.range.clone().map(|index| RecordView {
            body: self.body,
            shared_backing_bytes: self.shared_backing_bytes,
            batch: self.batch,
            decoded: self.decoded,
            record: self
                .batch
                .records
                .get(index, self.body)
                .expect("bounded record range"),
            index: index as u64,
        })
    }

    /// General descriptors have the same wire layout. Tiny packed records use
    /// ordinary record visitation. The complete backing must fit the reply budget.
    pub fn encoded_backing(&self, maximum: usize) -> Option<(&'a [u8], &'a Bytes, Range<usize>)> {
        if self.is_empty() || self.batch.prepared.is_some() || self.shared_backing_bytes? > maximum
        {
            return None;
        }
        let descriptors = self.batch.records.descriptor_range(self.range.clone())?;
        Some((
            &self.body[descriptors],
            self.body,
            self.batch.records.payload_range(self.range.clone()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_journal::operation::{
        Append, AppendBatch, AppendRecord, OperationBody, OperationLimits, decode_append_batches,
        encode_operation_body,
    };
    use ozzy_proto::data::{DataLimits, Encoding, Record, RecordBuffer};
    use ozzy_proto::{
        MessageId, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence,
    };

    fn encoding(i: usize) -> Encoding {
        if i == 5 {
            Encoding::Lz4 { decoded_bytes: 7 }
        } else {
            Encoding::Raw
        }
    }

    fn id(i: usize) -> MessageId {
        MessageId::from_bytes([i as u8 + 1; 16])
    }

    fn input() -> (Vec<Vec<Vec<u8>>>, Bytes) {
        let parts: Vec<Vec<Vec<u8>>> = (0..12)
            .map(|i| {
                if i == 5 {
                    vec![vec![0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0xff]]
                } else {
                    vec![vec![], vec![i as u8; i * 37], vec![]]
                }
            })
            .collect();
        let body = Bytes::from(
            encode_operation_body(
                &OperationBody::Append(Append {
                    batches: vec![AppendBatch {
                        partition: PartitionIncarnation::new(),
                        owner_epoch: OwnerEpoch::INITIAL,
                        producer_id: ProducerId::new(),
                        producer_epoch: ProducerEpoch::INITIAL,
                        first_sequence: ProducerSequence::new(0),
                        first_offset: Offset::new(30),
                        append_timestamp_millis: 0,
                        records: parts
                            .iter()
                            .enumerate()
                            .map(|(i, parts)| AppendRecord {
                                encoding: encoding(i),
                                message_id: id(i),
                                parts: parts.iter().map(Vec::as_slice).collect(),
                            })
                            .collect(),
                    }],
                }),
                OperationLimits::default(),
            )
            .unwrap(),
        );
        (parts, body)
    }

    #[test]
    fn every_partial_span_matches_wire_descriptors_and_respects_byte_limits() {
        let (parts, body) = input();
        let decoded = decode_append_batches(&body, OperationLimits::default()).unwrap();
        let batch = super::super::cache_batch(&body, &decoded[0]).unwrap();
        let decoded = DecodedCell::default();
        let span = |range| RecordSpan {
            body: &body,
            shared_backing_bytes: Some(body.len()),
            batch: &batch,
            decoded: &decoded,
            range,
        };
        for start in 0..12 {
            for end in start + 1..=12 {
                let selected = span(start..end);
                let owned_parts: Vec<Vec<&[u8]>> = parts[start..end]
                    .iter()
                    .map(|p| p.iter().map(Vec::as_slice).collect())
                    .collect();
                let wire: Vec<_> = owned_parts
                    .iter()
                    .enumerate()
                    .map(|(i, parts)| Record {
                        encoding: encoding(start + i),
                        message_id: id(start + i),
                        parts,
                    })
                    .collect();
                let limits = DataLimits {
                    envelope: ozzy_proto::EnvelopeLimits {
                        max_metadata_bytes: 4096,
                        max_payload_bytes: 4096,
                    },
                    ..DataLimits::default()
                };
                let mut buffer = RecordBuffer::new(limits);
                for r in &wire {
                    buffer
                        .push(r.message_id, r.encoding, r.parts.iter().copied(), limits)
                        .unwrap();
                }
                let (descriptors, payload) = buffer.records().encoded();
                let (actual, backing, range) = selected.encoded_backing(body.len()).unwrap();
                assert_eq!(actual, descriptors);
                assert_eq!(&backing[range], payload);
                assert_eq!(selected.payload_bytes(), payload.len());
                assert_eq!(selected.first_offset().get(), 30 + start as u64);
                assert!(selected.encoded_backing(body.len() - 1).is_none());
                for max_records in [0, 1, selected.len(), usize::MAX] {
                    for max_bytes in [
                        0,
                        1,
                        payload.len().saturating_sub(1),
                        payload.len(),
                        usize::MAX,
                    ] {
                        let mut bytes = 0;
                        let count = wire
                            .iter()
                            .take(max_records)
                            .take_while(|r| {
                                let size = r.parts.iter().map(|p| p.len()).sum::<usize>();
                                if size > max_bytes - bytes {
                                    false
                                } else {
                                    bytes += size;
                                    true
                                }
                            })
                            .count();
                        let limited = span(start..end).limit(max_records, max_bytes);
                        assert_eq!(limited.len(), count);
                        assert_eq!(limited.payload_bytes(), bytes);
                    }
                }
            }
        }
    }

    #[test]
    fn full_span_exposes_exact_prepared_group_but_partial_span_does_not() {
        let (body, decoded, encoded, batch) = prepared_batch();
        let full = RecordSpan {
            body: &body,
            shared_backing_bytes: Some(body.len()),
            batch: &batch,
            decoded: &DecodedCell::default(),
            range: 0..1024,
        };
        let (descriptors, decoded_bytes, backing, range) =
            full.prepared_backing(body.len()).unwrap();
        assert_ne!(descriptors.len(), 0);
        assert_eq!(decoded_bytes, decoded.len());
        assert_eq!(&backing[range.clone()], encoded);
        forward_prepared(descriptors, decoded_bytes, backing, range, &full);
        let partial = RecordSpan {
            range: 1..2,
            ..full
        };
        assert!(partial.prepared_backing(body.len()).is_none());
    }

    #[test]
    fn prepared_record_cache_retains_positions_and_borrows_descriptors() {
        let (body, payload, _, batch) = prepared_batch();
        assert!(batch.records.retained_bytes() <= 1025 * 2 * size_of::<usize>());
        let decoded = DecodedCell::default();
        let span = RecordSpan {
            body: &body,
            shared_backing_bytes: Some(body.len()),
            batch: &batch,
            decoded: &decoded,
            range: 0..1024,
        };
        assert!(body.is_unique(), "selectors must borrow the operation body");
        assert_eq!(span.batch_totals(), (1024, payload.len(), 1024));
        for (index, record) in span.records().enumerate() {
            assert_eq!(record.offset(), Offset::new(8 + index as u64));
            assert_eq!(
                record.message_id(),
                MessageId::from_bytes(((index + 1) as u128).to_be_bytes())
            );
            assert_eq!(record.encoding(), Encoding::Raw);
            assert_eq!(record.payload_bytes(), 128);
            assert_eq!(
                record.parts().collect::<Vec<_>>(),
                [&payload[index * 128..(index + 1) * 128]]
            );
        }
    }

    fn prepared_batch() -> (Bytes, Vec<u8>, Vec<u8>, CachedBatch) {
        use ozzy_journal::operation::{AppendHeader, append_prepared_record_batch};
        let payloads: Vec<_> = (0..1024)
            .map(|index| vec![b'a' + (index % 4) as u8; 128])
            .collect();
        let records = payloads.iter().enumerate().map(|(index, payload)| {
            (
                MessageId::from_bytes(((index + 1) as u128).to_be_bytes()),
                Encoding::Raw,
                std::iter::once(payload.as_slice()),
            )
        });
        let decoded = payloads.concat();
        let encoded = lz4rip::block::compress(&decoded);
        let mut body = Vec::new();
        append_prepared_record_batch(
            &mut body,
            AppendHeader {
                partition: ozzy_proto::PartitionIncarnation::from_bytes([3; 16]),
                owner_epoch: ozzy_proto::OwnerEpoch::new(4),
                producer_id: ozzy_proto::ProducerId::from_bytes([5; 16]),
                producer_epoch: ozzy_proto::ProducerEpoch::new(6),
                first_sequence: ozzy_proto::ProducerSequence::new(7),
                first_offset: Offset::new(8),
                append_timestamp_millis: 9,
            },
            records,
            &encoded,
            OperationLimits::default(),
        )
        .unwrap();
        let body = Bytes::from(body);
        let batch = {
            let decoded_batches = decode_append_batches(&body, OperationLimits::default()).unwrap();
            super::super::cache_batch(&body, &decoded_batches[0]).unwrap()
        };
        (body, decoded, encoded, batch)
    }

    fn forward_prepared(
        descriptors: &[u8],
        decoded_bytes: usize,
        backing: &Bytes,
        range: Range<usize>,
        full: &RecordSpan<'_>,
    ) {
        let limits = DataLimits {
            max_records: 1024,
            ..DataLimits::default()
        };
        let source = ozzy_proto::reader::Source::Group {
            authority: ozzy_proto::data::Authority {
                group_id: ozzy_proto::GroupId::from_bytes([11; 16]),
                config_epoch: 1,
                view: 1,
            },
            partition: ozzy_proto::PartitionIncarnation::from_bytes([3; 16]),
            owner_epoch: 4,
        };
        let envelope = ozzy_proto::Envelope {
            opcode: ozzy_proto::Opcode::RecordsPub,
            response: false,
            request_id: None,
            sender: ozzy_proto::NodeId::from_bytes([12; 16]),
            session: None,
        };
        let mut metadata = Vec::with_capacity(limits.envelope.max_metadata_bytes);
        let mut payload = Vec::with_capacity(limits.envelope.max_payload_bytes);
        let mut output = ozzy_proto::reader::RecordsEncoder::publication(
            envelope,
            ozzy_proto::reader::PublicationHeader {
                source,
                first_offset: 8,
            },
            &mut metadata,
            &mut payload,
            limits,
        )
        .unwrap();
        output.allow_shared_payload();
        assert_eq!(
            output.extend_shared_prepared_lz4(
                descriptors,
                decoded_bytes,
                backing,
                range.clone(),
                full.len()
            ),
            Ok(true)
        );
        assert_eq!(output.remaining().max_records, 0);
        assert_eq!(output.decoded_payload_bytes(), decoded_bytes);
        assert!(output.push_raw(MessageId::new(), b"later").is_err());
        let (header, forwarded) = output.finish_with_payload().unwrap();
        let forwarded = forwarded.unwrap();
        let metadata = bytes::Bytes::from(metadata);
        let packet =
            ozzy_proto::decode_packet(&[&header, &metadata, &forwarded], limits.envelope).unwrap();
        let publication = ozzy_proto::reader::decode_owned_publication(
            packet,
            &metadata,
            &forwarded,
            limits,
            &mut bytes::BytesMut::new(),
        )
        .unwrap();
        assert_eq!(publication.records.len(), 1024);
    }
}
