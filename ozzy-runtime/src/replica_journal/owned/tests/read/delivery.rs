use super::*;
use crate::replica_journal::OwnedPartitionDelivery;
use ozzy_proto::reader::{RecordHeader, RecordsEncoder, Source, Subscription};
use ozzy_proto::{LinkSessionId, SubscriptionId, data::DataLimits};

fn wire_limits(bytes: usize) -> DataLimits {
    DataLimits {
        envelope: ozzy_proto::EnvelopeLimits {
            max_metadata_bytes: 4096,
            max_payload_bytes: bytes,
        },
        max_records: 8,
        max_parts: 16,
        max_record_bytes: 8192,
    }
}

fn encode(
    delivery: OwnedPartitionDelivery,
    credit: usize,
) -> Result<(bytes::Bytes, ozzy_proto::append::PayloadEncoding), JournalError> {
    let mut metadata = Vec::with_capacity(4096);
    let mut payload = Vec::with_capacity(8192);
    let mut encoder = RecordsEncoder::new(
        ozzy_proto::Envelope {
            opcode: ozzy_proto::Opcode::Records,
            response: false,
            request_id: None,
            sender: NodeId::from_bytes([1; 16]),
            session: Some(LinkSessionId::from_bytes([3; 16])),
        },
        RecordHeader {
            subscription: Subscription {
                id: SubscriptionId::from_bytes([4; 16]),
                generation: 1,
            },
            source: Source::Local {
                producer: ProducerId::from_bytes([12; 16]),
                partition: ozzy_proto::PartitionId::ZERO,
            },
            first_offset: 0,
        },
        &mut metadata,
        &mut payload,
        wire_limits(credit),
    )
    .unwrap();
    encoder.allow_shared_payload();
    let cursor = delivery.encode(&mut encoder, wire_limits(8192))?;
    assert_eq!(cursor.next_offset(), Offset::new(2));
    let (header, shared) = encoder.finish_with_payload().unwrap();
    let shared = shared.expect("resident block shares original payload");
    assert!(
        payload.is_empty(),
        "never copy resident payload before sharing"
    );
    let metadata = bytes::Bytes::from(metadata);
    let packet =
        ozzy_proto::decode_packet(&[&header, &metadata, &shared], wire_limits(8192).envelope)
            .unwrap();
    let read = ozzy_proto::reader::decode_owned_records(
        packet,
        &metadata,
        &shared,
        wire_limits(8192),
        &mut bytes::BytesMut::new(),
    )
    .unwrap();
    assert_eq!(read.records.len(), 2);
    for record in read.records.as_records().iter() {
        assert_eq!(record.parts.collect::<Vec<_>>(), [&[7; 2048][..]]);
    }
    let encoding = read.payload_encoding;
    Ok((shared, encoding))
}

fn compressed(
    controller: &mut Controller,
    replica: &mut Replica,
) -> (JournalCompletion<WriteTicket>, Vec<u8>) {
    use ozzy_journal::operation::{
        AppendPackResult, AppendPackScratch, decode_append_view, pack_append_payload,
    };
    let body = OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id: ProducerId::from_bytes([12; 16]),
            producer_epoch: ProducerEpoch::INITIAL,
            first_sequence: ProducerSequence::new(0),
            first_offset: Offset::ZERO,
            append_timestamp_millis: 123,
            records: (0..2)
                .map(|index| AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([index + 14; 16]),
                    parts: vec![&[7; 2048][..]].into(),
                })
                .collect::<Vec<_>>()
                .into(),
        }],
    });
    let limits = replica.config.limits.operations;
    let mut body = encode_operation_body(&body, limits).unwrap();
    // Fixture supplies the already-compressed SDK body to normal admission.
    assert!(matches!(
        pack_append_payload(&mut body, limits, &mut AppendPackScratch::new(8192)).unwrap(),
        AppendPackResult::Packed { .. }
    ));
    let encoded = decode_append_view(&body, limits)
        .unwrap()
        .batches()
        .next()
        .unwrap()
        .prepared_payload
        .unwrap()
        .encoded
        .to_vec();
    let receipt = admit_encoded(
        controller,
        replica,
        [(ozzy_journal::operation::OperationKind::Append, body)],
    )
    .2;
    (receipt, encoded)
}

#[test]
fn owned_delivery_preserves_sdk_lz4_in_pending_and_installed_records() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut primary = replica(&mut controller, io.clone(), 0, policy, 8192);
        let mut backup = replica(&mut controller, io, 1, policy, 8192);
        let initial1 = initialize_writer(&mut controller, &mut primary);
        let initial2 = initialize_writer(&mut controller, &mut backup);
        let (receipt1, expected) = compressed(&mut controller, &mut primary);
        let (receipt2, _) = compressed(&mut controller, &mut backup);
        if policy == QuorumPolicy::Durable {
            for voter in [&mut primary, &mut backup] {
                let work = prepare(voter);
                finish(&mut controller, voter, work);
            }
            settle(&mut primary, initial1);
            settle(&mut primary, receipt1);
            settle(&mut backup, initial2);
            settle(&mut backup, receipt2);
            synchronize(&mut controller, &mut primary);
            synchronize(&mut controller, &mut backup);
        } else {
            drop((initial1, initial2, receipt1, receipt2));
        }
        confirm(&mut primary, &backup, policy);
        for installed in [false, true] {
            if installed && policy == QuorumPolicy::Replicated {
                for voter in [&mut primary, &mut backup] {
                    let work = prepare(voter);
                    finish(&mut controller, voter, work);
                }
            }
            let mut limits = bounds();
            limits.max_payload_bytes = 4096;
            let work = capture(&primary, 0, limits);
            let done = drive(&mut controller, work.read_delivery());
            let delivery = primary.journal.complete_delivery(done).unwrap();
            assert!(matches!(
                encode(delivery, 8192),
                Err(JournalError::Read(PartitionReadError::Limits))
            ));
            for credit in [2048, 8192] {
                let work = capture(&primary, 0, bounds());
                let mut jobs = 0;
                let done = drive_except(&mut controller, work.read_delivery(), None, |_| {
                    jobs += 1;
                    Effect::Normal
                });
                assert_eq!(jobs, 0);
                let delivery = primary.journal.complete_delivery(done).unwrap();
                let result = encode(delivery, credit);
                if credit == 2048 {
                    assert!(matches!(
                        result,
                        Err(JournalError::Read(
                            PartitionReadError::RecordTooLarge { .. }
                        ))
                    ));
                } else {
                    let (payload, encoding) = result.unwrap();
                    assert_eq!(encoding, ozzy_proto::append::PayloadEncoding::Lz4);
                    assert_eq!(payload.as_ref(), expected);
                }
            }
        }
        let work = capture(&primary, 0, bounds());
        let done = drive(&mut controller, work.read_delivery());
        assert!(backup.journal.complete_delivery(done).is_err());
        assert!(!backup.journal.is_faulted());
        drive(&mut controller, primary.journal.shutdown()).unwrap();
        drive(&mut controller, backup.journal.shutdown()).unwrap();
    }
}
