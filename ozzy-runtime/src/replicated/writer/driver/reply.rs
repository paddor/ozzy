//! Broker replies to sent requests: NACKs, and confirmations in sequence order.

use super::{Error, Failure, Session, SessionBinding};
use crate::transport::Frames;
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::append::stream;
use ozzy_proto::nack::{self, AuthorityHint, RetryClass};
use ozzy_proto::{NodeId, Opcode, decode_packet};

impl Session {
    pub(super) fn receive(
        &mut self,
        shared: &mut super::super::state::Driver,
        binding: &SessionBinding<'_>,
        message: &Message,
    ) -> Result<(), Failure> {
        let Some(frames) = frames(message, binding.remote) else {
            return Ok(());
        };
        let Ok(packet) = decode_packet(
            &frames.each_ref().map(AsRef::as_ref),
            shared.config.limits.envelope,
        ) else {
            return Ok(());
        };
        if packet.envelope.sender != binding.remote
            || packet.envelope.session != binding.connection.session()
            || !packet.envelope.response
        {
            return Ok(());
        }
        let Some(id) = packet
            .envelope
            .request_id
            .filter(|&id| self.window.correlates(id))
        else {
            return Ok(());
        };
        if packet.envelope.opcode == Opcode::Nack {
            return self.receive_nack(shared, binding, packet, id);
        }
        self.receive_confirmation(shared, binding, packet, id)
    }

    /// A NACK of request `id`. A refusal to try later backs off and replays
    /// from the earliest refused request; any other fails or retries.
    fn receive_nack(
        &mut self,
        shared: &super::super::state::Driver,
        binding: &SessionBinding<'_>,
        packet: ozzy_proto::Packet<'_>,
        id: ozzy_proto::RequestId,
    ) -> Result<(), Failure> {
        let reply = nack::decode(packet, shared.config.limits.envelope)
            .map_err(|_| Failure::Fatal(Error::Response))?;
        if reply.retry == RetryClass::AfterBackoff {
            crate::profiling::event(crate::profiling::Event::SdkAdmissionRefusal);
            if self
                .refused
                .is_none_or(|earlier| self.window.precedes(id, earlier))
            {
                self.refused = Some(id);
            }
            self.back_off(&binding.retry);
            return Ok(());
        }
        reject_packet(packet, shared.config.limits.envelope)
    }

    /// The confirmation of request `id`, applied in sequence order only.
    fn receive_confirmation(
        &mut self,
        shared: &mut super::super::state::Driver,
        binding: &SessionBinding<'_>,
        packet: ozzy_proto::Packet<'_>,
        id: ozzy_proto::RequestId,
    ) -> Result<(), Failure> {
        let apply_at = crate::profiling::start();
        let reply = stream::decode_confirmed(packet, shared.config.limits.envelope)
            .map_err(|error| Failure::Fatal(error.into()))?;
        if !confirms_producer(&reply, binding, &shared.config) {
            return Err(Failure::Fatal(Error::Response));
        }
        let key = reply.key;
        let end_sequence = reply.end_sequence;
        let confirmed = shared.progress.confirmed();
        if self
            .window
            .skips(id, key.first_sequence, end_sequence, confirmed)
        {
            // The broker holds the records before this range from an earlier
            // attempt, and refused or has not yet answered their resent
            // request. Only their own confirmation carries their offsets.
            self.back_off(&binding.retry);
            return Ok(());
        }
        if !self
            .window
            .accepts(id, key.first_sequence, end_sequence, confirmed)
        {
            return Err(Failure::Fatal(Error::Response));
        }
        if end_sequence <= confirmed {
            return Ok(());
        }
        let confirmed_bytes = shared
            .confirm_outcomes(
                key.first_sequence,
                reply
                    .outcomes()
                    .map_err(|error| Failure::Fatal(error.into()))?,
            )
            .map_err(|_| Failure::Fatal(Error::Response))?;
        self.window.confirm(end_sequence, confirmed_bytes);
        binding.connection.confirm(end_sequence);
        self.deadline = (!self.window.is_empty() || self.pending.is_some()).then(|| {
            self.clock
                .now()
                .saturating_add(binding.retry.response_timeout)
        });
        crate::profiling::finish(crate::profiling::Stage::ConfirmationApply, apply_at);
        Ok(())
    }
}

/// Whether `reply` confirms this producer: its authority, partition, owner
/// and producer epochs, producer identity, and policy.
fn confirms_producer(
    reply: &stream::Confirmed,
    binding: &SessionBinding<'_>,
    config: &super::super::WriterConfig,
) -> bool {
    reply.authority == binding.authority
        && reply.partition == config.partition
        && reply.owner_epoch == config.owner_epoch
        && reply.key.producer_id == config.producer_id
        && reply.key.producer_epoch == config.producer_epoch
        && reply.policy == config.policy
}

fn reject_packet(
    packet: ozzy_proto::Packet<'_>,
    limits: ozzy_proto::EnvelopeLimits,
) -> Result<(), Failure> {
    let reply = nack::decode(packet, limits).map_err(|_| Failure::Fatal(Error::Response))?;
    let hint = if matches!(reply.code, 5 | 12 | 13) {
        Some(AuthorityHint::decode(reply.detail).map_err(|_| Failure::Fatal(Error::Response))?)
    } else {
        None
    };
    Err(if reply.retry == RetryClass::Permanent {
        Failure::Fatal(Error::Rejected {
            code: reply.code,
            retry: reply.retry,
            hint,
        })
    } else {
        Failure::Retry(hint)
    })
}

fn frames(message: &Message, remote: NodeId) -> Option<[Bytes; 3]> {
    let (route, frames) = message.routed_packet_bytes()?;
    (route.as_ref() == remote.as_bytes()).then_some(frames)
}
