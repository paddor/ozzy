//! Check a writer request against its writer and copy it into a slot's arena.

use ozzy_proto::{
    Opcode, Packet, append, handshake,
    nack::{AuthorityHint, RetryClass},
    producer,
};

use super::{Command, NativeIntake};
use crate::{frontend::Link, replica_journal::JournalError};

#[derive(Clone, Copy)]
pub(super) enum PrepareFailure {
    Busy,
    Reject(u16, RetryClass),
}

impl From<(u16, RetryClass)> for PrepareFailure {
    fn from((code, retry): (u16, RetryClass)) -> Self {
        Self::Reject(code, retry)
    }
}

pub(super) fn prepare_failure(error: &JournalError) -> PrepareFailure {
    match error {
        JournalError::Io(source) if source.kind() == std::io::ErrorKind::WouldBlock => {
            PrepareFailure::Busy
        }
        _ => PrepareFailure::from(crate::replica_actor::response::rejection(error)),
    }
}

impl NativeIntake {
    /// Copy a request into the arena of `slot`, one of its writer's slots.
    pub(super) fn prepare(
        &mut self,
        slot: usize,
        packet: Packet<'_>,
        link: Link,
        hint: AuthorityHint,
    ) -> Result<Command, PrepareFailure> {
        let writer = slot / self.stride();
        let buffer = self.slots[slot]
            .buffer
            .as_mut()
            .ok_or(PrepareFailure::Busy)?;
        buffer.clear();
        let authorized = self.writers.producer(writer).ok_or(PrepareFailure::Busy)?;
        if hint.authority.group_id != self.config.group || hint.primary != self.config.local {
            return Err((5, RetryClass::AfterAuthorityRefresh).into());
        }
        if packet.envelope.opcode == Opcode::OpenProducer {
            let open = producer::decode_open(packet, self.config.limits.envelope)
                .map_err(|_| (1, RetryClass::Permanent))?;
            if open.producer != authorized || open.partition != self.config.partition {
                return Err((3, RetryClass::Permanent).into());
            }
            if open.authority != hint.authority {
                return Err((5, RetryClass::AfterAuthorityRefresh).into());
            }
            buffer
                .prepare_producer_open(open, self.config.policy)
                .map_err(|error| prepare_failure(&error))?;
            return Ok(Command::Open);
        }
        let append = append::validate_append(packet, self.config.limits)
            .map_err(|_| (1, RetryClass::Permanent))?;
        if !append.records.nonzero_message_ids() {
            return Err((1, RetryClass::Permanent).into());
        }
        if link.remote.capabilities & handshake::OWNER_STREAM == 0
            || append.policy != self.config.policy
        {
            return Err((2, RetryClass::Permanent).into());
        }
        if append.key.producer_id != authorized || append.partition != self.config.partition {
            return Err((3, RetryClass::Permanent).into());
        }
        if append.authority != hint.authority {
            return Err((5, RetryClass::AfterAuthorityRefresh).into());
        }
        let end_sequence = append
            .key
            .first_sequence
            .checked_add(append.records.len() as u64)
            .ok_or((1, RetryClass::Permanent))?;
        let confirmed = append::stream::Confirmed {
            authority: append.authority,
            partition: append.partition,
            owner_epoch: append.owner_epoch,
            key: append.key,
            end_sequence,
            first_offset: 0,
            disposition: append::AppendDisposition::Stored,
            policy: append.policy,
        };
        buffer
            .prepare_stream_wire_append(append)
            .map_err(|error| prepare_failure(&error))?;
        Ok(Command::Append(confirmed))
    }
}
