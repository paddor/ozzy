//! OMQ socket defaults and receive limits derived from bounded Ozzy commands.

use bytes::Bytes;
use ozzy_proto::{EnvelopeLimits, NodeId};

/// Canonical OMQ label for an unchanged native node identity. OMQ reserves a
/// leading zero byte, so those UUIDs are escaped with a nonzero byte.
pub fn peer_identity(node: NodeId) -> Bytes {
    if node.as_bytes()[0] == 0 {
        let mut escaped = [1; 17];
        escaped[1..].copy_from_slice(node.as_bytes());
        Bytes::copy_from_slice(&escaped)
    } else {
        Bytes::copy_from_slice(node.as_bytes())
    }
}

/// Decode only a canonical transport label; distinct labels never name one node.
pub fn decode_peer_identity(identity: &[u8]) -> Option<NodeId> {
    let native = match identity {
        [1, 0, ..] if identity.len() == 17 => &identity[1..],
        [first, ..] if *first != 0 && identity.len() == 16 => identity,
        _ => return None,
    };
    Some(NodeId::from_bytes(native.try_into().ok()?))
}

/// Restore native metadata while retaining the original receive backing.
pub fn native_peer_identity(identity: Bytes) -> Option<Bytes> {
    decode_peer_identity(&identity)?;
    Some(if identity.len() == 17 {
        identity.slice(1..)
    } else {
        identity
    })
}

/// Base options for every Ozzy socket. A compressing transport such as
/// `lz4+tcp://` compresses on the socket's own OMQ I/O thread, never on
/// Tokio's blocking pool.
#[must_use]
pub fn socket_options() -> omq_tokio::Options {
    omq_tokio::Options::default().compression_offload_threshold(None)
}

/// Submit an existing native routing envelope through a checked identity view.
/// The body is unchanged on Full, including its original buffer ownership.
/// Routing frames remain internal Ozzy metadata until their owner is converted.
pub fn try_send_peer(
    socket: &omq_tokio::IdentitySocket,
    mut packet: omq_tokio::Message,
) -> Result<(), omq_tokio::TrySendError> {
    // Preserve the existing owner until socket admission succeeds. Materializing
    // an inline body from a parts vector would otherwise promote it into Bytes
    // when OMQ adds its routing ID, defeating the compact control fast path.
    if packet.len() == 2 && packet.part_slice(1).is_some_and(|body| body.len() <= 51) {
        let body = omq_tokio::Message::from_slice(packet.part_slice(1).unwrap());
        let identity = send_identity(packet.part_slice(0).unwrap())?;
        return match socket.try_send_to(&identity, body) {
            Err(omq_tokio::TrySendError::Full(_)) => Err(omq_tokio::TrySendError::Full(packet)),
            result => result,
        };
    }
    let Some(identity) = packet.pop_front_payload() else {
        return Err(omq_tokio::TrySendError::Error(omq_tokio::Error::Protocol(
            "missing Ozzy peer identity".into(),
        )));
    };
    let transport = send_identity(identity.as_slice())?;
    match socket.try_send_to(&transport, packet) {
        Err(omq_tokio::TrySendError::Full(body)) => Err(omq_tokio::TrySendError::Full(
            omq_tokio::Message::with_prefix(identity.as_bytes(), body),
        )),
        result => result,
    }
}

/// Wait on the same physical destination used by native peer submission.
pub async fn wait_send_peer(socket: &omq_tokio::IdentitySocket, packet: &omq_tokio::Message) {
    if packet
        .part_slice(0)
        .is_some_and(|identity| identity.len() == 16 && identity[0] == 0)
    {
        let mut body = packet.clone();
        let identity = body.pop_front_payload().expect("checked routing identity");
        let transport = peer_identity(NodeId::from_bytes(
            identity
                .as_slice()
                .try_into()
                .expect("checked identity length"),
        ));
        socket
            .wait_send_progress_for(&omq_tokio::Message::with_prefix(transport, body))
            .await;
    } else {
        socket.wait_send_progress_for(packet).await;
    }
}

/// OMQ charges bytes and one payload slot per frame. Include the PEER identity,
/// Ozzy envelope, metadata and packed payload; retain room for handshakes/control.
/// For a shared endpoint, use the maximum over every enabled command family.
/// Limits are local configuration, never increased by remote negotiation.
pub fn message_size_limit(limits: EnvelopeLimits) -> Option<usize> {
    if limits.max_metadata_bytes > u32::MAX as usize || limits.max_payload_bytes > u32::MAX as usize
    {
        return None;
    }
    let overhead = 17 + ozzy_proto::ENVELOPE_BYTES + 4 * size_of::<omq_tokio::message::Payload>();
    limits
        .max_metadata_bytes
        .checked_add(limits.max_payload_bytes)?
        .checked_add(overhead)
        .map(|bytes| bytes.max(1024))
}

fn send_identity(identity: &[u8]) -> Result<Bytes, omq_tokio::TrySendError> {
    let node = identity.try_into().map_err(|_| {
        omq_tokio::TrySendError::Error(omq_tokio::Error::Protocol(
            "invalid Ozzy peer identity".into(),
        ))
    })?;
    Ok(peer_identity(NodeId::from_bytes(node)))
}

#[cfg(test)]
mod tests;
