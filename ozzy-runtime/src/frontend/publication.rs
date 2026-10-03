//! One bounded publication per broker PUB socket.

use super::Service;
use omq_tokio::Message;
use ozzy_proto::{decode_packet, reader};

/// Local publication admission. Neither admission nor transmission confirms a record.
pub type PublicationResult = Result<(), (PublicationError, Message)>;

/// Publication refused by metadata validation or finite outgoing capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PublicationError {
    /// Invalid framing, opcode, source, or subscription prefix.
    #[error("invalid frontend publication")]
    Source,
    /// The source partition belongs to another configured shard.
    #[error("publication belongs to another shard")]
    Destination,
    /// The actor observation no longer names this broker as the source leader.
    #[error("stale publication source")]
    Stale,
    /// One bounded publication already awaits socket admission.
    #[error("publication queue full")]
    Full,
}

impl Service {
    pub(super) fn publish(&mut self, shard: u32, message: Message) -> PublicationResult {
        if let Err(error) = self.check_publication(shard, &message) {
            return Err((error, message));
        }
        let slot = usize::from(
            message
                .part_slice(0)
                .is_some_and(|prefix| prefix.len() == 32),
        );
        if self.publications[slot].is_some() {
            return Err((PublicationError::Full, message));
        }
        self.publications[slot] = Some((shard, message));
        Ok(())
    }

    /// Take at most one validated publication for nonblocking PUB admission.
    /// A full socket may drop it. Its retained-byte ownership follows every
    /// transmitted alias until physical release; PEER repairs any loss.
    pub fn take_publication(&mut self) -> Option<Message> {
        let slot = (0..2)
            .map(|offset| (self.publication_next + offset) % 2)
            .find(|&slot| self.publications[slot].is_some())?;
        let (shard, message) = self.publications[slot].take()?;
        self.publication_next = (slot + 1) % 2;
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
        if packet.envelope.opcode == ozzy_proto::Opcode::PreparePub {
            let scope = ozzy_replication::wire::route(packet, self.dispatcher.routes.limits)
                .map_err(|_| PublicationError::Source)?;
            if message.part_slice(0) != Some(scope.group_id.as_bytes().as_slice()) {
                return Err(PublicationError::Source);
            }
            if self
                .dispatcher
                .routes
                .partitions
                .get(&scope.group_id)
                .is_none_or(|placement| placement.shard != shard)
            {
                return Err(PublicationError::Destination);
            }
            if self
                .watches
                .as_ref()
                .and_then(|watches| watches.route(scope.group_id))
                .is_none_or(|route| {
                    route.config_epoch != scope.configuration_epoch
                        || route.view != scope.view
                        || route.leader != Some(self.local())
                })
            {
                return Err(PublicationError::Stale);
            }
            return Ok(());
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
