//! Independent wire fixtures cover uniform selection and general fallbacks.

use super::*;
use ozzy_journal::operation::{AppendHeader, append_wire_record_batch, decode_append_batches};
use ozzy_proto::append::PayloadEncoding;
use ozzy_proto::data::{DataLimits, decode_record_descriptors};
use ozzy_proto::{OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId};

struct Fixture {
    metadata: Vec<u8>,
    payload: Vec<u8>,
    records: Vec<(MessageId, Vec<Vec<u8>>)>,
    uniform: Option<usize>,
}

impl Fixture {
    fn new(shape: &[Vec<usize>]) -> Self {
        let mut fixture = Self {
            metadata: (shape.len() as u32).to_be_bytes().to_vec(),
            payload: Vec::new(),
            records: Vec::new(),
            uniform: shape
                .iter()
                .all(|parts| parts.len() == 1 && parts[0] == shape[0][0])
                .then_some(shape[0][0]),
        };
        for (index, parts) in shape.iter().enumerate() {
            let id = [index as u8 + 1; 16];
            fixture.metadata.extend_from_slice(&id);
            fixture
                .metadata
                .extend_from_slice(&(parts.len() as u32).to_be_bytes());
            let mut record = Vec::new();
            for &length in parts {
                fixture
                    .metadata
                    .extend_from_slice(&(length as u32).to_be_bytes());
                let bytes = vec![index as u8 + 7; length];
                fixture.payload.extend_from_slice(&bytes);
                record.push(bytes);
            }
            fixture.records.push((MessageId::from_bytes(id), record));
        }
        fixture
    }

    fn check(&self, encoding: PayloadEncoding) {
        let header = AppendHeader {
            partition: PartitionIncarnation::from_bytes([1; 16]),
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id: ProducerId::from_bytes([2; 16]),
            producer_epoch: ProducerEpoch::INITIAL,
            first_sequence: ozzy_proto::ProducerSequence::new(10),
            first_offset: Offset::new(20),
            append_timestamp_millis: 30,
        };
        let descriptors = decode_record_descriptors(
            &self.metadata,
            self.payload.len(),
            10,
            DataLimits::default(),
        )
        .unwrap();
        assert_eq!(
            descriptors.uniform_part_bytes().map(|n| n as usize),
            self.uniform
        );
        let encoded = match encoding {
            PayloadEncoding::Raw => self.payload.clone(),
            PayloadEncoding::Lz4 => lz4rip::block::compress(&self.payload),
        };
        let mut canonical = Vec::new();
        let (_, proof) = append_wire_record_batch(
            &mut canonical,
            header,
            descriptors,
            encoding,
            &encoded,
            OperationLimits::default(),
        )
        .unwrap();
        let body = Bytes::from(canonical);
        let trusted = proof.batch(&body, OperationLimits::default()).unwrap();
        let ordinary = decode_append_batches(&body, OperationLimits::default()).unwrap();
        for batch in std::iter::once(trusted).chain(ordinary) {
            assert_eq!(batch.uniform_raw_record_bytes(), self.uniform);
            let cached = cache_batch(&body, &batch).unwrap();
            assert_eq!(cached.records.retained_bytes() == 0, self.uniform.is_some());
            assert!(cached.records.get(self.records.len(), &body).is_none());
            let decoded = DecodedCell::default();
            self.check_span(&body, &cached, &decoded, encoding, &encoded);
            for (index, (id, parts)) in self.records.iter().enumerate() {
                let view = RecordView {
                    body: &body,
                    shared_backing_bytes: None,
                    batch: &cached,
                    decoded: &decoded,
                    record: cached.records.get(index, &body).unwrap(),
                    index: index as u64,
                };
                assert_eq!(view.message_id(), *id);
                assert_eq!(
                    view.parts().collect::<Vec<_>>(),
                    parts.iter().map(Vec::as_slice).collect::<Vec<_>>()
                );
            }
        }
    }

    fn check_span(
        &self,
        body: &Bytes,
        cached: &CachedBatch,
        decoded: &DecodedCell,
        encoding: PayloadEncoding,
        encoded: &[u8],
    ) {
        let first = self.records[0].1.iter().map(Vec::len).sum::<usize>();
        let span = RecordSpan {
            body,
            shared_backing_bytes: Some(body.len()),
            batch: cached,
            decoded,
            range: 1..self.records.len(),
        };
        assert_eq!(span.payload_bytes(), self.payload.len() - first);
        assert!(span.encoded_backing(body.len() - 1).is_none());
        if encoding == PayloadEncoding::Raw {
            let (table, backing, selected) = span.encoded_backing(body.len()).unwrap();
            let first_descriptor_bytes = 20 + self.records[0].1.len() * 4;
            assert_eq!(table, &self.metadata[4 + first_descriptor_bytes..]);
            assert_eq!(selected.start, first + 80 + self.metadata.len() - 4);
            assert_eq!(&backing[selected], &self.payload[first..]);
        } else {
            assert!(span.encoded_backing(body.len()).is_none());
            assert!(span.prepared_backing(body.len()).is_none());
            let full = RecordSpan {
                range: 0..self.records.len(),
                ..span
            };
            let (table, count, backing, selected) = full.prepared_backing(body.len()).unwrap();
            assert_eq!(table, &self.metadata[4..]);
            assert_eq!(count, self.payload.len());
            assert_eq!(&backing[selected], encoded);
        }
    }
}

#[test]
fn uniform_wire_layouts_select_exact_ranges_without_position_tables() {
    for shape in [
        vec![vec![0]; 3],
        vec![vec![128]; 3],
        vec![vec![3], vec![5], vec![5]],
        vec![vec![4, 0]; 3],
    ] {
        let fixture = Fixture::new(&shape);
        for encoding in [PayloadEncoding::Raw, PayloadEncoding::Lz4] {
            fixture.check(encoding);
        }
    }
}
