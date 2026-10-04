use super::*;
use bytes::{BufMut, Bytes, BytesMut};
use omq_tokio::proto::{
    Command, Connection, ConnectionConfig, Greeting, MechanismName, PeerProperties, Role, command,
    frame,
};
use omq_tokio::{Frame, FrameFlags, SocketType, message::Payload};
mod identity;

fn ready(limit: usize) -> Connection {
    let mut connection = Connection::new(
        ConnectionConfig::new(Role::Server, SocketType::Peer).max_message_size(limit),
    );
    let mut wire = BytesMut::new();
    Greeting {
        major: 3,
        minor: 1,
        mechanism: MechanismName::NULL,
        as_server: false,
    }
    .encode(&mut wire);
    let mut body = BytesMut::new();
    command::encode(
        &Command::Ready(PeerProperties::default().with_socket_type(SocketType::Peer)),
        &mut body,
    );
    frame::encode_frame(
        &Frame {
            flags: FrameFlags::COMMAND,
            payload: Payload::from_bytes(body.freeze()),
        },
        &mut wire,
    );
    connection.handle_input(wire.freeze()).unwrap();
    assert!(connection.is_ready());
    connection
}

#[test]
fn forged_lengths_fail_on_header_before_direct_payload_allocation() {
    for payload_limit in [1024 * 1024, 100 * 1024 * 1024] {
        let limit = message_size_limit(EnvelopeLimits {
            max_payload_bytes: payload_limit,
            ..EnvelopeLimits::default()
        })
        .unwrap();
        for declared in [10_u64 * 1024 * 1024 * 1024, u64::MAX] {
            let mut header = BytesMut::new();
            header.put_u8(0x02); // ZMTP long frame.
            header.put_u64(declared);
            for split in 0..header.len() {
                let mut connection = ready(limit);
                connection
                    .handle_input(Bytes::copy_from_slice(&header[..split]))
                    .unwrap();
                assert!(connection.begin_supplied_payload().is_none());
                assert!(
                    connection
                        .handle_input(Bytes::copy_from_slice(&header[split..]))
                        .is_err()
                );
                assert!(connection.poll_message().is_none());
                assert!(connection.begin_supplied_payload().is_none());
            }
        }
    }
}

#[test]
fn multipart_empty_frames_charge_memory_and_stop_at_limit() {
    let limit = message_size_limit(EnvelopeLimits {
        max_metadata_bytes: 512,
        max_payload_bytes: 128,
    })
    .unwrap();
    let mut connection = ready(limit);
    let slots = limit / size_of::<Payload>();
    for index in 0..=slots {
        let result = connection.handle_input(Bytes::from_static(&[1, 0]));
        if index == slots {
            assert!(matches!(
                result,
                Err(omq_tokio::Error::MessageTooLarge { .. })
            ));
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn configured_payload_and_metadata_fit_with_framing() {
    let limits = EnvelopeLimits {
        max_metadata_bytes: 512,
        max_payload_bytes: 1024,
    };
    let limit = message_size_limit(limits).unwrap();
    let message = omq_tokio::Message::multipart([
        peer_identity(NodeId::from_bytes([0; 16])),
        Bytes::from_static(&[0; ozzy_proto::ENVELOPE_BYTES]),
        Bytes::from(vec![0; limits.max_metadata_bytes]),
        Bytes::from(vec![0; limits.max_payload_bytes]),
    ]);
    assert_eq!(message.max_message_size_len(), limit);
    assert!(
        message_size_limit(EnvelopeLimits {
            max_payload_bytes: usize::MAX,
            ..limits
        })
        .is_none()
    );
}
