//! One bounded publication per broker PUB socket.

use super::Service;
use crate::transport::Frames;
use omq_tokio::Message;
use ozzy_proto::{GroupId, Packet, decode_packet, reader};

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
        let (_, frames) = message.routed_packet().ok_or(PublicationError::Source)?;
        let packet = decode_packet(&frames, self.dispatcher.routes.limits)
            .map_err(|_| PublicationError::Source)?;
        if packet.envelope.sender != self.local()
            || packet.envelope.session.is_some()
            || packet.envelope.request_id.is_some()
        {
            return Err(PublicationError::Source);
        }
        if packet.envelope.opcode == ozzy_proto::Opcode::PreparePub {
            self.check_prepare(shard, message, packet)
        } else {
            self.check_records(shard, message, packet)
        }
    }

    fn check_prepare(
        &self,
        shard: u32,
        message: &Message,
        packet: Packet<'_>,
    ) -> Result<(), PublicationError> {
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
        self.check_leader(scope.group_id, scope.configuration_epoch, scope.view)
    }

    fn check_records(
        &self,
        shard: u32,
        message: &Message,
        packet: Packet<'_>,
    ) -> Result<(), PublicationError> {
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
        self.check_leader(authority.group_id, authority.config_epoch, authority.view)
    }

    /// The actor observation must still name this broker as the source leader.
    fn check_leader(
        &self,
        group: GroupId,
        config_epoch: u64,
        view: u64,
    ) -> Result<(), PublicationError> {
        if self
            .watches
            .as_ref()
            .and_then(|watches| watches.route(group))
            .is_none_or(|route| {
                route.config_epoch != config_epoch
                    || route.view != view
                    || route.leader != Some(self.local())
            })
        {
            return Err(PublicationError::Stale);
        }
        Ok(())
    }
}
