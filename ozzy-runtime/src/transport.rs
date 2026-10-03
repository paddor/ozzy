//! OMQ socket defaults and receive limits derived from bounded Ozzy commands.

use ozzy_proto::EnvelopeLimits;

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
pub(crate) fn try_send_peer(
    socket: &omq_tokio::IdentitySocket,
    mut packet: omq_tokio::Message,
) -> Result<(), omq_tokio::TrySendError> {
    // Preserve the existing owner until socket admission succeeds. Materializing
    // an inline body from a parts vector would otherwise promote it into Bytes
    // when OMQ adds its routing ID, defeating the compact control fast path.
    if packet.len() == 2 && packet.part_slice(1).is_some_and(|body| body.len() <= 51) {
        let body = omq_tokio::Message::from_slice(packet.part_slice(1).unwrap());
        return match socket.try_send_to(packet.part_slice(0).unwrap(), body) {
            Err(omq_tokio::TrySendError::Full(_)) => Err(omq_tokio::TrySendError::Full(packet)),
            result => result,
        };
    }
    let Some(identity) = packet.pop_front_payload() else {
        return Err(omq_tokio::TrySendError::Error(omq_tokio::Error::Protocol(
            "missing Ozzy peer identity".into(),
        )));
    };
    match socket.try_send_to(identity.as_slice(), packet) {
        Err(omq_tokio::TrySendError::Full(body)) => Err(omq_tokio::TrySendError::Full(
            omq_tokio::Message::with_prefix(identity.as_bytes(), body),
        )),
        result => result,
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
    let overhead = 16 + ozzy_proto::ENVELOPE_BYTES + 4 * size_of::<omq_tokio::message::Payload>();
    limits
        .max_metadata_bytes
        .checked_add(limits.max_payload_bytes)?
        .checked_add(overhead)
        .map(|bytes| bytes.max(1024))
}

#[cfg(test)]
mod tests;
