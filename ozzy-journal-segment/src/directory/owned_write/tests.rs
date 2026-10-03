use super::*;
use crate::{BodyEncoding, DecodeLimits, OperationKind, OperationLimits};
use bytes::Bytes;
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, encode_operation_body,
};
use ozzy_proto::{
    GroupId, MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};

fn body(number: u64) -> Vec<u8> {
    let payload = vec![number as u8; 16 * 1024];
    body_with_payload(number, &payload)
}

fn body_with_payload(number: u64, payload: &[u8]) -> Vec<u8> {
    encode_operation_body(
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: PartitionIncarnation::from_bytes([5; 16]),
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([6; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(number - 1),
                first_offset: Offset::new(number - 1),
                append_timestamp_millis: 1,
                records: vec![AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes(u128::from(number).to_be_bytes()),
                    parts: vec![payload].into(),
                }]
                .into(),
            }],
        }),
        OperationLimits::default(),
    )
    .unwrap()
}

fn operation(body: &[u8]) -> CanonicalOperation<'_> {
    CanonicalOperation {
        group_id: GroupId::from_bytes([1; 16]),
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Append,
        body,
    }
}
fn captured(decode: DecodeLimits, limits: OperationLimits) -> JournalGroupEncoding {
    JournalGroupEncoding::captured(decode, limits, 1, 0, Vec::new())
}

#[test]
fn cached_digests_do_not_bypass_schema_authority_or_storage_bounds() {
    for case in 0..5 {
        let bytes = body(1);
        let mut decode = DecodeLimits::default();
        let mut op = operation(&bytes);
        match case {
            0 => op.body = &bytes[..bytes.len() - 1],
            1 => op.configuration_epoch += 1,
            2 => op.original_view += 1,
            3 => decode.max_group_decoded_body_bytes = bytes.len() - 1,
            4 => decode.max_decoded_body_bytes = bytes.len() - 1,
            _ => unreachable!(),
        }
        assert!(
            captured(decode, OperationLimits::default())
                .encode_verified(
                    &mut JournalGroupEncoder::default(),
                    &[(op, canonical_body_digest(op.body))],
                    BodyEncoding::Raw,
                )
                .is_err()
        );
    }
}

#[test]
fn validated_bodies_skip_only_the_body_walk_and_only_with_the_journal_limits() {
    let limits = OperationLimits {
        max_payload_bytes: 1024,
        ..OperationLimits::default()
    };
    let bytes = Bytes::from(body(1));
    let first = operation(&bytes).header();
    let shared = |epoch| {
        let mut header = first;
        header.configuration_epoch = epoch;
        vec![SharedJournalOperation {
            header,
            body: bytes.clone(),
            body_digest: canonical_body_digest(&bytes),
        }]
    };
    let encode = |validated, epoch| {
        let encoding = captured(DecodeLimits::default(), limits);
        let encoding = match validated {
            Some(proof) => encoding.with_validated_bodies(proof),
            None => encoding,
        };
        encoding.encode_shared_raw(shared(epoch))
    };
    assert!(encode(None, 1).is_err());
    assert!(encode(Some(OperationLimits::default()), 1).is_err());
    assert!(encode(Some(limits), 1).is_ok());
    assert!(encode(Some(limits), 2).is_err());
    assert!(
        captured(DecodeLimits::default(), limits)
            .with_validated_payloads()
            .encode_shared_raw(shared(1))
            .is_err()
    );
    assert!(
        captured(DecodeLimits::default(), OperationLimits::default())
            .with_validated_payloads()
            .encode_shared_raw(shared(1))
            .is_ok()
    );
}
