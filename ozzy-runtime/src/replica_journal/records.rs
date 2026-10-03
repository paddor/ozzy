#[cfg(test)]
use super::append::RetainedAppend;
use super::{JournalError, PartitionReadLimits};
#[cfg(test)]
use ozzy_journal::operation::OperationLimits;
#[cfg(test)]
use ozzy_journal_segment::RecordView;
use ozzy_journal_segment::{DecodedBatches, PreparedOperationRecords};
use ozzy_proto::{Offset, PartitionIncarnation};

/// Immutable operation owners for one bounded read. Capturing shares each
/// selected operation once; the reader worker borrows individual payload parts.
/// Decompressed producer blocks live in `decoded` and drop with this read.
#[derive(Debug)]
pub(in crate::replica_journal) struct CapturedRecords {
    operations: Vec<PreparedOperationRecords>,
    decoded: Vec<DecodedBatches>,
    partition: PartitionIncarnation,
    first: Offset,
    end: Offset,
}

impl CapturedRecords {
    pub(in crate::replica_journal) fn visit_spans(
        &self,
        mut visit: impl FnMut(&ozzy_journal_segment::RecordSpan<'_>) -> usize,
    ) {
        for span in self
            .operations
            .iter()
            .zip(&self.decoded)
            .flat_map(|(operation, decoded)| {
                operation.spans(self.partition, self.first, self.end, decoded)
            })
        {
            if visit(&span) != span.len() {
                break;
            }
        }
    }

    pub(in crate::replica_journal) fn capture<'a>(
        operations: impl Iterator<Item = &'a PreparedOperationRecords>,
        partition: PartitionIncarnation,
        first: Offset,
        end: Offset,
        limits: PartitionReadLimits,
    ) -> Result<Self, JournalError> {
        let mut captured = Self {
            operations: Vec::new(),
            decoded: Vec::new(),
            partition,
            first,
            end: first,
        };
        let mut count = 0;
        let mut bytes = 0usize;
        let mut parts = 0usize;
        'operations: for operation in operations {
            let mut retained = false;
            // Sizes come from descriptors; nothing is decompressed here.
            let unused = operation.decoded_batches();
            for record in operation.records(partition, first, end, &unused) {
                if record.offset() != captured.end {
                    return Err(JournalError::Faulted);
                }
                if count != 0
                    && (count == limits.max_records
                        || record.payload_bytes() > limits.max_payload_bytes.saturating_sub(bytes)
                        || record.part_count() > limits.max_parts.saturating_sub(parts))
                {
                    break 'operations;
                }
                if !retained {
                    captured.operations.push(operation.clone());
                    retained = true;
                }
                count += 1;
                bytes += record.payload_bytes();
                parts += record.part_count();
                captured.end = captured.end.checked_next().ok_or(JournalError::Faulted)?;
                if count == limits.max_records
                    || bytes > limits.max_payload_bytes
                    || parts > limits.max_parts
                {
                    break 'operations;
                }
            }
        }
        if count == 0 {
            return Err(JournalError::Faulted);
        }
        captured.decoded = captured
            .operations
            .iter()
            .map(PreparedOperationRecords::decoded_batches)
            .collect();
        Ok(captured)
    }

    #[cfg(test)]
    pub(in crate::replica_journal) fn visit(&self, mut visit: impl FnMut(&RecordView<'_>) -> bool) {
        for record in self
            .operations
            .iter()
            .zip(&self.decoded)
            .flat_map(|(operation, decoded)| {
                operation.records(self.partition, self.first, self.end, decoded)
            })
        {
            if !visit(&record) {
                break;
            }
        }
    }
}

#[cfg(test)]
pub(in crate::replica_journal) fn validated_memory_records(
    append: &RetainedAppend,
    limits: OperationLimits,
) -> Result<Vec<PreparedOperationRecords>, JournalError> {
    crate::replica_journal::canonical::retained_records(append, limits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica_journal::AppendBuffer;
    use ozzy_journal::operation::{
        Append, AppendBatch, AppendRecord, CanonicalOperation, Digest, OperationBody,
        OperationKind, canonical_body_digest, encode_operation_body,
    };
    use ozzy_proto::{
        GroupId, MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
        ProducerSequence,
    };
    use ozzy_replication::{JournalGeneration, PipelineLimits, PreparedOperation};
    use std::sync::Arc;

    fn multipart_append(limits: OperationLimits) -> AppendBuffer {
        let body = encode_operation_body(
            &OperationBody::Append(Append {
                batches: (1..=2)
                    .map(|partition| AppendBatch {
                        partition: PartitionIncarnation::from_bytes([partition; 16]),
                        owner_epoch: OwnerEpoch::new(9),
                        producer_id: ProducerId::from_bytes([partition + 10; 16]),
                        producer_epoch: ProducerEpoch::new(8),
                        first_sequence: ProducerSequence::new(40),
                        first_offset: Offset::new(80),
                        append_timestamp_millis: 123_456,
                        records: vec![
                            AppendRecord {
                                encoding: ozzy_proto::data::Encoding::Raw,
                                message_id: MessageId::from_bytes([17; 16]),
                                parts: vec![b"\0\xff".as_slice(), b"", b"tail"].into(),
                            },
                            AppendRecord {
                                encoding: ozzy_proto::data::Encoding::Raw,
                                message_id: MessageId::from_bytes([18; 16]),
                                parts: vec![b"".as_slice()].into(),
                            },
                        ]
                        .into(),
                    })
                    .collect(),
            }),
            limits,
        )
        .unwrap();
        let permit = Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .unwrap();
        let mut buffer = AppendBuffer::new(
            JournalGeneration(1),
            PipelineLimits {
                max_operations: 2,
                max_body_bytes: 8192,
            },
            permit,
        );
        for (number, kind, body) in [
            (1, OperationKind::Barrier, [1; 16].as_slice()),
            (2, OperationKind::Append, body.as_slice()),
        ] {
            let operation = CanonicalOperation {
                group_id: GroupId::from_bytes([7; 16]),
                configuration_epoch: 1,
                original_view: 0,
                op_number: number,
                previous_digest: Digest::ZERO,
                kind,
                body,
            };
            buffer.push(operation).unwrap();
            buffer.body_digests.push(canonical_body_digest(body));
            buffer.prepared.push(PreparedOperation::from_verified(
                &operation,
                canonical_body_digest(body),
            ));
        }
        buffer
    }

    #[test]
    fn retained_digests_stay_bound_to_bodies_after_caller_buffer_reuse() {
        let mut buffer = multipart_append(OperationLimits::default());
        let append = buffer.retain();
        let expected = append
            .verified_operations()
            .map(|(operation, digest)| {
                assert_eq!(digest, canonical_body_digest(operation.body));
                (operation.body.to_vec(), digest)
            })
            .collect::<Vec<_>>();
        buffer.clear();
        buffer
            .push(CanonicalOperation {
                body: b"different bytes",
                ..append.operations().next().unwrap()
            })
            .unwrap();
        for (index, (operation, digest)) in append.verified_operations().enumerate() {
            assert_eq!(operation.body, expected[index].0);
            assert_eq!(digest, expected[index].1);
            assert_eq!(digest, canonical_body_digest(operation.body));
        }
    }

    #[test]
    fn borrowed_index_preserves_partitions_positions_and_shared_opaque_parts() {
        let limits = OperationLimits::default();
        let mut buffer = multipart_append(limits);
        let append = buffer.retain();
        let retained_body = append.operations().nth(1).unwrap().body;
        let address = retained_body.as_ptr() as usize;
        let records = validated_memory_records(&append, limits).unwrap();
        assert_eq!(records.len(), 2);
        let decoded = records[1].decoded_batches();
        let records = (1..=2)
            .flat_map(|partition| {
                records[1]
                    .records(
                        PartitionIncarnation::from_bytes([partition; 16]),
                        Offset::new(80),
                        Offset::new(82),
                        &decoded,
                    )
                    .map(|record| record.materialize())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 4);
        for (index, record) in records.iter().enumerate() {
            let partition = 1 + (index / 2) as u8;
            let within = index % 2;
            assert_eq!(
                record.partition,
                PartitionIncarnation::from_bytes([partition; 16])
            );
            assert_eq!(
                record.producer_id,
                ProducerId::from_bytes([partition + 10; 16])
            );
            assert_eq!(record.owner_epoch, OwnerEpoch::new(9));
            assert_eq!(record.producer_epoch, ProducerEpoch::new(8));
            assert_eq!(record.producer_sequence.get(), 40 + within as u64);
            assert_eq!(record.offset.get(), 80 + within as u64);
            assert_eq!(record.append_timestamp_millis, 123_456);
            assert_eq!(
                record.message_id,
                MessageId::from_bytes([17 + within as u8; 16])
            );
            let expected: &[&[u8]] = if within == 0 {
                &[b"\0\xff", b"", b"tail"]
            } else {
                &[b""]
            };
            assert_eq!(
                record.parts.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
                expected
            );
            for part in record.parts.iter().filter(|part| !part.is_empty()) {
                let start = part.as_ptr() as usize;
                assert!(start >= address && start + part.len() <= address + retained_body.len());
            }
        }
        assert!(
            validated_memory_records(
                &append,
                OperationLimits {
                    max_records: 1,
                    ..limits
                }
            )
            .is_err()
        );
        drop((append, buffer));
        assert_eq!(records[0].parts[2], b"tail".as_slice());
    }

    #[test]
    fn captured_read_keeps_operation_owners_and_exact_count_byte_and_part_bounds() {
        let append = multipart_append(OperationLimits::default()).retain();
        let operations = validated_memory_records(&append, OperationLimits::default()).unwrap();
        let partition = PartitionIncarnation::from_bytes([1; 16]);
        let limits = crate::replica_journal::PartitionReadLimits {
            max_records: 2,
            max_parts: 4,
            max_payload_bytes: 6,
        };
        for (limits, end) in [
            (limits, 82),
            (
                crate::replica_journal::PartitionReadLimits {
                    max_records: 1,
                    ..limits
                },
                81,
            ),
            (
                crate::replica_journal::PartitionReadLimits {
                    max_parts: 3,
                    ..limits
                },
                81,
            ),
            // Keep the first whole record so execution can report RecordTooLarge.
            (
                crate::replica_journal::PartitionReadLimits {
                    max_payload_bytes: 1,
                    ..limits
                },
                81,
            ),
        ] {
            let read = CapturedRecords::capture(
                operations.iter(),
                partition,
                Offset::new(80),
                Offset::new(82),
                limits,
            )
            .unwrap();
            assert_eq!(read.end, Offset::new(end));
            assert_eq!(read.operations.len(), 1);
        }
        let read = CapturedRecords::capture(
            operations.iter(),
            partition,
            Offset::new(80),
            Offset::new(82),
            limits,
        )
        .unwrap();
        assert!(
            CapturedRecords::capture(
                operations.iter(),
                partition,
                Offset::new(79),
                Offset::new(82),
                limits
            )
            .is_err()
        );
        drop((operations, append));
        let mut delivered = Vec::new();
        read.visit(|record| {
            delivered.push((
                record.offset(),
                record.parts().map(<[u8]>::to_vec).collect::<Vec<_>>(),
            ));
            true
        });
        assert_eq!(
            delivered,
            vec![
                (
                    Offset::new(80),
                    vec![b"\0\xff".to_vec(), vec![], b"tail".to_vec()]
                ),
                (Offset::new(81), vec![vec![]]),
            ]
        );
        let mut visits = 0;
        read.visit(|_| {
            visits += 1;
            false
        });
        assert_eq!(visits, 1);
    }
}
