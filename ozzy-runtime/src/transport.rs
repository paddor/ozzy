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

/// Exact-arity views of a multipart message.
pub trait Frames {
    /// Every frame, only when the message has exactly `N`.
    fn frames<const N: usize>(&self) -> Option<[&[u8]; N]>;

    /// Every frame as shared bytes, only when the message has exactly `N`.
    fn frame_bytes<const N: usize>(&self) -> Option<[Bytes; N]>;

    /// Every frame in order, borrowed.
    fn slices(&self) -> impl Iterator<Item = &[u8]>;

    /// A routing frame followed by one Ozzy packet: header, metadata, payload.
    fn routed_packet(&self) -> Option<(&[u8], [&[u8]; 3])> {
        let [route, header, metadata, payload] = self.frames()?;
        Some((route, [header, metadata, payload]))
    }

    /// [`Self::routed_packet`] as shared bytes.
    fn routed_packet_bytes(&self) -> Option<(Bytes, [Bytes; 3])> {
        let [route, header, metadata, payload] = self.frame_bytes()?;
        Some((route, [header, metadata, payload]))
    }
}

impl Frames for omq_tokio::Message {
    fn frames<const N: usize>(&self) -> Option<[&[u8]; N]> {
        if self.len() != N {
            return None;
        }
        let mut frames = [&[][..]; N];
        for (index, frame) in frames.iter_mut().enumerate() {
            *frame = self.part_slice(index)?;
        }
        Some(frames)
    }

    fn frame_bytes<const N: usize>(&self) -> Option<[Bytes; N]> {
        self.try_as_parts().ok()
    }

    fn slices(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).filter_map(|index| self.part_slice(index))
    }
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
    if let Some([route, body]) = packet.frames().filter(|[_, body]| body.len() <= 51) {
        let body = omq_tokio::Message::from_slice(body);
        let identity = send_identity(route)?;
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
    if let Some(identity) = packet
        .part_slice(0)
        .and_then(|identity| <[u8; 16]>::try_from(identity).ok())
        && identity[0] == 0
    {
        let mut body = packet.clone();
        body.pop_front_payload();
        let transport = peer_identity(NodeId::from_bytes(identity));
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
