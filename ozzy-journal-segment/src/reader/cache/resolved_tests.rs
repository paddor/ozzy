use super::*;
use ozzy_journal::operation::{
    AppendHeader, AppendResults, Digest, OperationHeader, RetryFingerprint,
    append_resolved_wire_record_batch,
};
use ozzy_proto::append::{AppendDisposition, AppendOutcome, PayloadEncoding};
use ozzy_proto::data::{DataLimits, decode_record_descriptors};
use ozzy_proto::{GroupId, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId};

fn outcome(disposition: AppendDisposition, offset: u64) -> AppendOutcome {
    AppendOutcome {
        disposition,
        offset: Offset::new(offset),
    }
}

fn record_table() -> Vec<u8> {
    let mut table = 4u32.to_be_bytes().to_vec();
    for id in 1..=4 {
        table.extend_from_slice(&[id; 16]);
        table.extend_from_slice(&1u32.to_be_bytes());
        table.extend_from_slice(&4u32.to_be_bytes());
    }
    table
}

fn operation_header() -> OperationHeader {
    OperationHeader {
        group_id: GroupId::from_bytes([4; 16]),
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::ResolvedAppend,
    }
}

#[test]
fn raw_and_compressed_resident_and_cold_reads_preserve_sequence_gaps_near_exhaustion() {
    use AppendDisposition::{Duplicate, Stored};
    let limits = OperationLimits::default();
    let partition = PartitionIncarnation::from_bytes([1; 16]);
    let first = u64::MAX - 5;
    let header = AppendHeader {
        partition,
        owner_epoch: OwnerEpoch::INITIAL,
        producer_id: ProducerId::from_bytes([2; 16]),
        producer_epoch: ProducerEpoch::INITIAL,
        first_sequence: ProducerSequence::new(first),
        first_offset: Offset::new(40),
        append_timestamp_millis: 10,
    };
    let rows = [
        outcome(Stored, 40),
        outcome(Stored, 41),
        outcome(Duplicate, 40),
        outcome(Stored, 42),
        outcome(Stored, 43),
    ];
    let proofs = [RetryFingerprint::from_bytes([3; 32])];
    let results = AppendResults::classified(10, &rows, &proofs, limits).unwrap();
    let table = record_table();
    let payload = b"dataDATAmoreMORE";
    let descriptors =
        decode_record_descriptors(&table, payload.len(), 0, DataLimits::default()).unwrap();
    let location = crate::OperationLocation {
        segment_id: 1,
        entry_offset: 4096,
        entry_bytes: 4096,
        op_number: 1,
        operation_digest: Digest::ZERO,
    };
    let operation_header = operation_header();
    let entries = (0..4)
        .map(|index| OffsetIndexEntry {
            partition,
            offset: Offset::new(40 + u64::from(index)),
            append_timestamp_millis: 10,
            location: crate::RecordLocation {
                operation: location,
                batch_index: 0,
                record_index: index,
            },
        })
        .collect::<Vec<_>>();
    for encoding in [PayloadEncoding::Raw, PayloadEncoding::Lz4] {
        let block = match encoding {
            PayloadEncoding::Raw => payload.to_vec(),
            PayloadEncoding::Lz4 => lz4rip::block::compress(payload),
        };
        let mut output = Vec::new();
        let batch = ozzy_journal::operation::WireBatch {
            header,
            records: descriptors,
            encoding,
            payload: &block,
        };
        append_resolved_wire_record_batch(&mut output, batch, results, limits).unwrap();
        let body = Bytes::from(output);
        let prepared =
            PreparedOperationRecords::new(operation_header, body.clone(), limits).unwrap();
        let decoded = prepared.decoded_batches();
        let actual = prepared
            .records(partition, Offset::new(40), Offset::new(44), &decoded)
            .map(|record| record.materialize())
            .collect::<Vec<_>>();
        assert_eq!(
            actual
                .iter()
                .map(|record| record.producer_sequence.get())
                .collect::<Vec<_>>(),
            [first, first + 1, first + 3, first + 4]
        );
        let resident = prepared.bind(location).unwrap();
        assert_eq!(resident.batches[0].sequence_runs.len(), 2);
        assert!(resident.resident_bytes() >= body.len() + 2 * size_of::<StoredSequenceRun>());
        drop(prepared);
        let cold =
            CachedOperation::from_body(entries[0], operation_header.kind, body, limits).unwrap();
        for cached in [&*resident, &cold] {
            let selected = cached.materialize(&entries, true).unwrap();
            assert_eq!(selected, actual);
            for (index, record) in selected.iter().enumerate() {
                assert_eq!(
                    record.parts[0].as_ref(),
                    &payload[4 * index..4 * (index + 1)]
                );
            }
        }
    }
}
