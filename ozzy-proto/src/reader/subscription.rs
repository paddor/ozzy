//! Exact subscription selectors and cumulative, generation-fenced capacity.

use super::{
    Authority, Cancel, CodecError, ENVELOPE_BYTES, Envelope, EnvelopeLimits, Opcode, Packet,
    PartitionId, Reader, Source, Subscribed, Subscription, Topic, Unsubscribe, control, end,
    prepare, read_text, text,
};

/// Exact log selection. Local logs do not invent replication authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A named log owned by the addressed broker.
    Local {
        /// Logical stream/topic name.
        topic: Topic,
        /// Owner-local partition number.
        partition: PartitionId,
    },
    /// A preprovisioned partition in a configured group.
    Group {
        /// Current expected group authority.
        authority: Authority,
        /// Stable partition incarnation.
        partition: crate::PartitionIncarnation,
        /// Expected partition ownership fence.
        owner_epoch: u64,
    },
}

impl Target {
    /// Group source selected by this target; local identity comes from its owner.
    pub fn group_source(&self) -> Option<Source> {
        match *self {
            Self::Group {
                authority,
                partition,
                owner_epoch,
            } => Some(Source::Group {
                authority,
                partition,
                owner_epoch,
            }),
            Self::Local { .. } => None,
        }
    }

    pub(super) fn validate(&self) -> Result<(), CodecError> {
        if let Some(source) = self.group_source() {
            source.validate()?;
        }
        Ok(())
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "Topic::new caps the stream and name at MAX_NAME_LEN"
    )]
    pub(super) fn size(&self) -> usize {
        match self {
            Self::Local { topic, .. } => 13 + topic.stream().len() + topic.name().len(),
            Self::Group { .. } => 57,
        }
    }

    pub(super) fn encode(&self, output: &mut Vec<u8>) {
        if let Some(source) = self.group_source() {
            source.encode(output);
        } else if let Self::Local { topic, partition } = self {
            output.push(0);
            text(output, topic.stream());
            text(output, topic.name());
            output.extend_from_slice(&partition.get().to_be_bytes());
        }
    }

    pub(super) fn decode(cursor: &mut Reader<'_>) -> Result<Self, CodecError> {
        if cursor.rest().first() == Some(&0) {
            cursor.u8()?;
            let stream = read_text(cursor)?;
            let name = read_text(cursor)?;
            let topic = Topic::new(stream, name).map_err(|_| CodecError::Profile)?;
            Ok(Self::Local {
                topic,
                partition: PartitionId::new(cursor.u32()?),
            })
        } else {
            let Source::Group {
                authority,
                partition,
                owner_epoch,
            } = Source::decode(cursor)?
            else {
                return Err(CodecError::Profile);
            };
            Ok(Self::Group {
                authority,
                partition,
                owner_epoch,
            })
        }
    }
}

fn encode_close(
    envelope: Envelope,
    unsubscribe: Unsubscribe,
    response: bool,
    output: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    let selected = unsubscribe.selected;
    selected.subscription.validate()?;
    selected.source.validate()?;
    let opcode = if response {
        Opcode::Unsubscribed
    } else {
        Opcode::Unsubscribe
    };
    let header = prepare(
        envelope,
        opcode,
        crate::data::add(41, selected.source.size())?,
        output,
        limits,
    )?;
    selected.subscription.encode(output);
    selected.source.encode(output);
    output.extend_from_slice(&selected.resolved_offset.to_be_bytes());
    output.push(unsubscribe.cancel as u8);
    Ok(header)
}

fn decode_close(
    packet: Packet<'_>,
    response: bool,
    limits: EnvelopeLimits,
) -> Result<Unsubscribe, CodecError> {
    let opcode = if response {
        Opcode::Unsubscribed
    } else {
        Opcode::Unsubscribe
    };
    let mut cursor = control(packet, opcode, limits)?;
    let selected = Subscribed {
        subscription: Subscription::decode(&mut cursor)?,
        source: Source::decode(&mut cursor)?,
        resolved_offset: cursor.u64()?,
    };
    let cancel = match cursor.u8()? {
        0 => Cancel::Subscription,
        1 => Cancel::Replay,
        _ => return Err(CodecError::Profile),
    };
    end(cursor)?;
    Ok(Unsubscribe { selected, cancel })
}

/// Encode explicit cancellation of one subscription generation, or of only
/// its replay stream.
pub fn encode_unsubscribe(
    envelope: Envelope,
    unsubscribe: Unsubscribe,
    output: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    encode_close(envelope, unsubscribe, false, output, limits)
}

/// Decode explicit cancellation of one subscription generation, or of only
/// its replay stream.
pub fn decode_unsubscribe(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<Unsubscribe, CodecError> {
    decode_close(packet, false, limits)
}

/// Encode confirmation of a cancellation, echoing it.
pub fn encode_unsubscribed(
    envelope: Envelope,
    unsubscribe: Unsubscribe,
    output: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    encode_close(envelope, unsubscribe, true, output, limits)
}

/// Decode confirmation of a cancellation.
pub fn decode_unsubscribed(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<Unsubscribe, CodecError> {
    decode_close(packet, true, limits)
}
