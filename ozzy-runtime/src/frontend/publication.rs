//! One bounded publication awaiting the broker's shared PUB socket.

use super::Service;
use omq_tokio::Message;
use ozzy_proto::{decode_packet, reader};

/// Local publication admission. Neither admission nor transmission confirms a record.
pub type PublicationResult = Result<(), (PublicationError, Message)>;

/// Publication refused by metadata validation or finite outgoing capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PublicationError {
    /// Invalid framing, opcode, source, or subscription prefix.
    #[error("invalid frontend reader publication")]
    Source,
    /// The source partition belongs to another configured shard.
    #[error("reader publication belongs to another shard")]
    Destination,
    /// The actor observation no longer names this broker as the source leader.
    #[error("stale reader publication source")]
    Stale,
    /// One bounded publication already awaits socket admission.
    #[error("reader publication queue full")]
    Full,
}

impl Service {
    pub(super) fn publish(&mut self, shard: u32, message: Message) -> PublicationResult {
        if let Err(error) = self.check_publication(shard, &message) {
            return Err((error, message));
        }
        if self.publication.is_some() {
            return Err((PublicationError::Full, message));
        }
        self.publication = Some((shard, message));
        Ok(())
    }

    /// Take at most one validated publication for nonblocking PUB admission.
    /// A full socket may drop it. Its retained-byte ownership follows every
    /// transmitted alias until physical release; PEER repairs any loss.
    pub fn take_publication(&mut self) -> Option<Message> {
        let (shard, message) = self.publication.take()?;
        self.check_publication(shard, &message).ok()?;
        Some(message)
    }

    fn check_publication(&self, shard: u32, message: &Message) -> Result<(), PublicationError> {
        if message.len() != 4 {
            return Err(PublicationError::Source);
        }
        let frames: [&[u8]; 3] =
            std::array::from_fn(|i| message.part_slice(i + 1).expect("four frames"));
        let packet = decode_packet(&frames, self.dispatcher.routes.limits)
            .map_err(|_| PublicationError::Source)?;
        if packet.envelope.sender != self.local()
            || packet.envelope.session.is_some()
            || packet.envelope.request_id.is_some()
        {
            return Err(PublicationError::Source);
        }
        let source = reader::route_publication(packet, self.dispatcher.routes.limits)
            .map_err(|_| PublicationError::Source)?;
        if message.part_slice(0)
            != Some(
                reader::publication_topic(source)
                    .map_err(|_| PublicationError::Source)?
                    .as_slice(),
            )
        {
            return Err(PublicationError::Source);
        }
        let reader::Source::Group {
            authority,
            partition,
            ..
        } = source
        else {
            return Err(PublicationError::Source);
        };
        if self
            .dispatcher
            .routes
            .partitions
            .get(&authority.group_id)
            .is_none_or(|placement| placement.shard != shard || placement.partition != partition)
        {
            return Err(PublicationError::Destination);
        }
        if self
            .watches
            .as_ref()
            .and_then(|watches| watches.route(authority.group_id))
            .is_none_or(|route| {
                route.config_epoch != authority.config_epoch
                    || route.view != authority.view
                    || route.leader != Some(self.local())
            })
        {
            return Err(PublicationError::Stale);
        }
        Ok(())
    }
}
