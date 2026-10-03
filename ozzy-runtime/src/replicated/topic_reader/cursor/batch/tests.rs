use super::*;
use bytes::Bytes;
use omq_tokio::message::Payload;
use ozzy_proto::{
    Envelope, LinkSessionId, MessageId, NodeId, Opcode, ProducerId, SubscriptionId,
    data::Encoding,
    reader::{RecordHeader, RecordsEncoder, Source, Subscription},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct EnvelopeOwner {
    bytes: [u8; 64],
    dropped: Arc<AtomicBool>,
}

impl AsRef<[u8]> for EnvelopeOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for EnvelopeOwner {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy)]
enum WirePayload {
    Raw,
    RecordLz4,
    BatchLz4,
}

fn message(encoding: WirePayload, limits: DataLimits, dropped: Arc<AtomicBool>) -> Message {
    let mut metadata = Vec::with_capacity(4096);
    let mut payload = Vec::with_capacity(1024);
    let mut output = RecordsEncoder::new(
        Envelope {
            opcode: Opcode::Records,
            response: false,
            request_id: None,
            sender: NodeId::from_bytes([1; 16]),
            session: Some(LinkSessionId::from_bytes([2; 16])),
        },
        RecordHeader {
            subscription: Subscription {
                id: SubscriptionId::from_bytes([3; 16]),
                generation: 1,
            },
            source: Source::Local {
                producer: ProducerId::from_bytes([4; 16]),
                partition: ozzy_proto::PartitionId::ZERO,
            },
            first_offset: 0,
        },
        &mut metadata,
        &mut payload,
        limits,
    )
    .unwrap();
    let id = MessageId::from_bytes([5; 16]);
    match encoding {
        WirePayload::Raw | WirePayload::RecordLz4 => {
            let (encoding, encoded) = if matches!(encoding, WirePayload::RecordLz4) {
                let mut encoded = vec![0, 0, 0, 1, 0, 0, 0, 64];
                encoded.extend_from_slice(&lz4rip::block::compress(&[7; 64]));
                (Encoding::Lz4 { decoded_bytes: 64 }, encoded)
            } else {
                (Encoding::Raw, vec![7; 64])
            };
            output
                .extend(std::iter::once((
                    id,
                    encoding,
                    std::iter::once(encoded.as_slice()),
                )))
                .unwrap();
        }
        WirePayload::BatchLz4 => {
            let mut descriptor = [0; 24];
            descriptor[..16].copy_from_slice(id.as_bytes());
            descriptor[19] = 1;
            descriptor[23] = 64;
            let encoded = Bytes::from(lz4rip::block::compress(&[7; 64]));
            output.allow_shared_payload();
            assert!(
                output
                    .extend_shared_prepared_lz4(&descriptor, 64, &encoded, 0..encoded.len(), 1)
                    .unwrap()
            );
        }
    }
    let (header, shared) = output.finish_with_payload().unwrap();
    Message::multipart_payloads([
        Payload::new(),
        Payload::from_bytes(Bytes::from_owner(EnvelopeOwner {
            bytes: header,
            dropped,
        })),
        Payload::from_bytes(Bytes::from(metadata)),
        Payload::from_bytes(shared.unwrap_or_else(|| Bytes::from(payload))),
    ])
}

#[test]
fn decoded_record_alias_retains_the_original_frame_reservation() {
    let limits = DataLimits {
        max_records: 4,
        max_parts: 4,
        max_record_bytes: 1024,
        ..DataLimits::default()
    };
    for encoding in [
        WirePayload::Raw,
        WirePayload::RecordLz4,
        WirePayload::BatchLz4,
    ] {
        let dropped = Arc::new(AtomicBool::new(false));
        let message = message(encoding, limits, dropped.clone());
        let metadata = message.part_bytes(2).unwrap();
        let payload = message.part_bytes(3).unwrap();
        let packet = ozzy_proto::decode_packet(
            &[message.part_slice(1).unwrap(), &metadata, &payload],
            limits.envelope,
        )
        .unwrap();
        let delivery = ozzy_proto::reader::decode_owned_records(
            packet,
            &metadata,
            &payload,
            limits,
            &mut bytes::BytesMut::new(),
        )
        .unwrap();
        let mut batch = FrameRecords::new(delivery.records, &message).into_batch(0);
        drop(message);
        let record = batch
            .next(&mut Decoder::default(), limits)
            .unwrap()
            .unwrap();
        assert_eq!(record.encoding, Encoding::Raw);
        assert_eq!(record.payload[0].as_ref(), &[7; 64]);
        let alias = record.payload[0].clone();
        drop((batch, record));
        assert!(!dropped.load(Ordering::SeqCst));
        drop(alias);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
