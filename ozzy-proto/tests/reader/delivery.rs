//! `RECORDS` delivery decoding: fixed metadata, shared and owned records, and
//! rejection of malformed input.

use super::*;

#[test]
fn local_records_fixed_metadata_shared_bytes_and_owned_cursor() {
    let limits = DataLimits::default();
    let e = envelope(Opcode::Records, false);
    let record = Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([8; 16]),
        parts: &[b"ab", b"", b"c"],
    };
    let second = Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([9; 16]),
        parts: &[b"", b"d"],
    };
    let v = RecordHeader {
        subscription: subscription(),
        source: local(),
        first_offset: 10,
    };
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let header = reader::encode_records(
        e,
        v,
        &[record, second],
        Frames {
            metadata: &mut metadata,
            payload: &mut payload,
        },
        limits,
    )
    .unwrap();
    let mut golden = vec![4; 16];
    golden.extend_from_slice(&5_u128.to_be_bytes());
    golden.push(0);
    golden.extend_from_slice(&[6; 16]);
    golden.extend_from_slice(&7_u32.to_be_bytes());
    golden.extend_from_slice(&10_u64.to_be_bytes());
    golden.push(ozzy_proto::append::PayloadEncoding::Raw as u8);
    golden.extend_from_slice(&4_u32.to_be_bytes());
    golden.extend_from_slice(&2_u32.to_be_bytes());
    golden.extend_from_slice(&[8; 16]);
    for n in [3_u32, 2, 0, 1] {
        golden.extend_from_slice(&n.to_be_bytes());
    }
    golden.extend_from_slice(&[9; 16]);
    for n in [2_u32, 0, 1] {
        golden.extend_from_slice(&n.to_be_bytes());
    }
    assert_eq!(metadata, golden);
    assert_eq!(payload, b"abcd");
    let metadata = Bytes::from(metadata);
    let payload = Bytes::from(payload);
    let delivery = reader::decode_records(
        decode_packet(&[&header, &metadata, &payload], limits.envelope).unwrap(),
        limits,
    )
    .unwrap();
    assert_eq!(delivery.header, v);
    let owned = delivery.records.to_owned(&metadata, &payload);
    assert_eq!(owned.len(), 2);
    assert_eq!(owned.payload_bytes_after(0), 4);
    assert_eq!(owned.payload_bytes_after(1), 1);
    assert_eq!(owned.payload_bytes_after(99), 0);
    let pointer = payload.as_ptr();
    drop(metadata);
    drop(payload);
    let mut records = owned.into_records();
    let first = records.next().unwrap();
    assert_eq!(first.payload[0].as_ptr(), pointer);
    assert_eq!(
        first.payload.as_slice(),
        &[
            Bytes::from_static(b"ab"),
            Bytes::new(),
            Bytes::from_static(b"c")
        ]
    );
    assert_eq!(records.len(), 1);
    let second = records.next().unwrap();
    assert_eq!(
        second.payload.as_slice(),
        &[Bytes::new(), Bytes::from_static(b"d")]
    );
    assert_eq!(records.len(), 0);
    assert!(records.next().is_none());
}

#[test]
fn reader_records_reject_malformed_limits_sources_correlation_and_overflow() {
    let limits = DataLimits::default();
    let parts: &[&[u8]] = &[b"", b"x"];
    let records = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([8; 16]),
        parts,
    }];
    for source in sources() {
        let mut metadata = Vec::with_capacity(1024);
        let mut payload = Vec::with_capacity(1024);
        let v = RecordHeader {
            subscription: subscription(),
            source,
            first_offset: 0,
        };
        let e = envelope(Opcode::Records, false);
        reader::encode_records(
            e,
            v,
            &records,
            Frames {
                metadata: &mut metadata,
                payload: &mut payload,
            },
            limits,
        )
        .unwrap();
        for n in 0..metadata.len() {
            assert!(
                reader::decode_records(
                    Packet {
                        envelope: e,
                        metadata: &metadata[..n],
                        payload: &payload
                    },
                    limits
                )
                .is_err()
            );
        }
        let valid = metadata.clone();
        metadata[32] = 2;
        assert!(
            reader::decode_records(
                Packet {
                    envelope: e,
                    metadata: &metadata,
                    payload: &payload
                },
                limits
            )
            .is_err()
        );
        metadata.copy_from_slice(&valid);
        let p = Packet {
            envelope: e,
            metadata: &metadata,
            payload: &payload,
        };
        assert!(
            reader::decode_records(
                p,
                DataLimits {
                    max_parts: 1,
                    ..limits
                }
            )
            .is_err()
        );
        assert!(reader::decode_records(Packet { payload: &[], ..p }, limits).is_err());
        let old = (metadata.clone(), payload.clone());
        assert!(
            reader::encode_records(
                e,
                RecordHeader {
                    first_offset: u64::MAX,
                    ..v
                },
                &records,
                Frames {
                    metadata: &mut metadata,
                    payload: &mut payload
                },
                limits
            )
            .is_err()
        );
        assert_eq!((metadata.clone(), payload.clone()), old);
        let mut tiny = vec![99];
        assert_eq!(
            reader::encode_records(
                e,
                v,
                &records,
                Frames {
                    metadata: &mut tiny,
                    payload: &mut payload
                },
                limits
            ),
            Err(CodecError::Capacity)
        );
        assert_eq!(tiny, [99]);
        assert_eq!(payload, old.1);
        let mut wrong = e;
        wrong.response = true;
        assert_eq!(
            reader::encode_records(
                wrong,
                v,
                &records,
                Frames {
                    metadata: &mut metadata,
                    payload: &mut payload
                },
                limits
            ),
            Err(CodecError::Command)
        );
    }
}
