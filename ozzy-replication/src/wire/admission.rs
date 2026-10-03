//! Actor-selected receive fences checked before spending dispatch capacity.

use ozzy_proto::{EnvelopeLimits, Opcode, Packet};

use super::{FetchOps, Reader, WireError};
use crate::flow::{Channel, FlowError, ReceiveEpoch};

/// Exact purpose of one actor-backed replica receive reservation.
/// This is not authentication, history validation, or replication authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiveFence {
    /// Normal data for the receiver's current group/configuration/view/epoch.
    Normal(Channel),
    /// Response to one exact outstanding selected-history request.
    History(FetchOps),
}

impl ReceiveFence {
    /// Group selected by the owning actor, never by incoming payload bytes.
    pub const fn scope(self) -> crate::Scope {
        match self {
            Self::Normal(channel) => channel.scope,
            Self::History(request) => request.scope,
        }
    }

    /// Inspect only bounded protocol metadata. A mismatch must not consume the
    /// fresh reservation. The actor must still decode/hash bodies and validate
    /// membership, history, application state, and its current authority.
    pub fn validate_routing(
        self,
        packet: Packet<'_>,
        limits: EnvelopeLimits,
    ) -> Result<(), WireError> {
        packet
            .envelope
            .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
        let mut reader = Reader::new(packet.metadata);
        if reader.routing_scope(packet.envelope.sender)? != self.scope() {
            return Err(WireError::Scope);
        }
        match self {
            Self::Normal(channel) => {
                if packet.envelope.opcode != Opcode::PrepareFlow {
                    return Err(WireError::UnsupportedCommand);
                }
                if packet.envelope.response || packet.envelope.request_id.is_some() {
                    return Err(WireError::Correlation);
                }
                let epoch = ReceiveEpoch::new(u128::from_be_bytes(reader.bytes()?))?;
                if epoch != channel.epoch {
                    return Err(FlowError::Channel.into());
                }
                Ok(())
            }
            Self::History(request) => {
                if packet.envelope.opcode != Opcode::Ops {
                    return Err(WireError::UnsupportedCommand);
                }
                super::transfer::validate_routing(
                    request,
                    packet.envelope,
                    reader,
                    packet.payload.len(),
                )
            }
        }
    }
}
