use super::*;
use crate::{LinkSessionId, NodeId, RequestId, decode_packet};

fn envelope(opcode: Opcode) -> Envelope {
    Envelope {
        opcode,
        response: opcode == Opcode::ProducerOpened,
        request_id: Some(RequestId::from_bytes([4; 16])),
        sender: NodeId::from_bytes([5; 16]),
        session: Some(LinkSessionId::from_bytes([6; 16])),
    }
}

fn open() -> Open {
    Open {
        authority: Authority {
            group_id: GroupId::from_bytes([1; 16]),
            config_epoch: 1,
            view: 3,
        },
        partition: PartitionIncarnation::from_bytes([2; 16]),
        producer: ProducerId::from_bytes([3; 16]),
        mode: Mode::Resume,
        expected_epoch: None,
        operation: OperationId::from_bytes([7; 16]),
    }
}

fn packet<'a>(header: &'a [u8; ENVELOPE_BYTES], metadata: &'a [u8]) -> Packet<'a> {
    decode_packet(&[header, metadata, &[]], EnvelopeLimits::default()).unwrap()
}

#[test]
fn open_and_opened_round_trip() {
    let mut metadata = Vec::with_capacity(128);
    let limits = EnvelopeLimits::default();
    let request = open();
    let header = encode_open(
        envelope(Opcode::OpenProducer),
        request,
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(metadata.len(), OPEN_BYTES);
    assert_eq!(decode_open(packet(&header, &metadata), limits), Ok(request));

    let mut fenced = request;
    fenced.mode = Mode::Fence;
    fenced.expected_epoch = Some(7);
    let header = encode_open(
        envelope(Opcode::OpenProducer),
        fenced,
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(decode_open(packet(&header, &metadata), limits), Ok(fenced));

    let response = Opened {
        authority: request.authority,
        partition: request.partition,
        producer: request.producer,
        epoch: 8,
        next_sequence: 42,
        retry_floor: 21,
        policy: Policy::QuorumDurable,
    };
    let header = encode_opened(
        envelope(Opcode::ProducerOpened),
        response,
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(metadata.len(), OPENED_BYTES);
    assert_eq!(
        decode_opened(packet(&header, &metadata), limits),
        Ok(response)
    );
}

#[test]
fn malformed_fences_and_unconfirmed_policy_fail_closed() {
    let mut metadata = Vec::with_capacity(128);
    let limits = EnvelopeLimits::default();
    let mut request = open();
    request.mode = Mode::Fence;
    assert_eq!(
        encode_open(
            envelope(Opcode::OpenProducer),
            request,
            &mut metadata,
            limits
        ),
        Err(CodecError::Identity)
    );
    request = open();
    request.operation = OperationId::from_bytes([0; 16]);
    assert_eq!(
        encode_open(
            envelope(Opcode::OpenProducer),
            request,
            &mut metadata,
            limits
        ),
        Err(CodecError::Identity)
    );
    let request = open();
    let header = encode_open(
        envelope(Opcode::OpenProducer),
        request,
        &mut metadata,
        limits,
    )
    .unwrap();
    metadata[65] = 9;
    assert_eq!(
        decode_open(packet(&header, &metadata), limits),
        Err(CodecError::Profile)
    );
    let response = Opened {
        authority: request.authority,
        partition: request.partition,
        producer: request.producer,
        epoch: 1,
        next_sequence: 2,
        retry_floor: 3,
        policy: Policy::LocalDurable,
    };
    assert_eq!(
        encode_opened(
            envelope(Opcode::ProducerOpened),
            response,
            &mut metadata,
            limits
        ),
        Err(CodecError::Profile)
    );
}
