//! Exact subscription selectors and cumulative, generation-fenced capacity.

use super::{
    Authority, CodecError, Cursor, ENVELOPE_BYTES, Envelope, EnvelopeLimits, Opcode, Packet,
    PartitionId, Source, Subscribed, Subscription, Topic, control, end, prepare, read_text, text,
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

    pub(super) fn size(&self) -> usize {
        match self {
            Self::Local { topic, .. } => 13 + topic.stream().len() + topic.name().len(),
            Self::Group { .. } => 57,
        }
    }

    pub(super) fn encode(&self, output: &mut Vec<u8>) {
        match self {
            Self::Local { topic, partition } => {
                output.push(0);
                text(output, topic.stream());
                text(output, topic.name());
                output.extend_from_slice(&partition.get().to_be_bytes());
            }
            Self::Group { .. } => self.group_source().expect("group target").encode(output),
        }
    }

    pub(super) fn decode(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        if cursor.0.first() == Some(&0) {
            cursor.byte()?;
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
    subscription: Subscribed,
    response: bool,
    output: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    subscription.subscription.validate()?;
    subscription.source.validate()?;
    let opcode = if response {
        Opcode::Unsubscribed
    } else {
        Opcode::Unsubscribe
    };
    let header = prepare(
        envelope,
        opcode,
        response,
        40 + subscription.source.size(),
        output,
        limits,
    )?;
    subscription.subscription.encode(output);
    subscription.source.encode(output);
    output.extend_from_slice(&subscription.resolved_offset.to_be_bytes());
    Ok(header)
}

fn decode_close(
    packet: Packet<'_>,
    response: bool,
    limits: EnvelopeLimits,
) -> Result<Subscribed, CodecError> {
    let opcode = if response {
        Opcode::Unsubscribed
    } else {
        Opcode::Unsubscribe
    };
    let mut cursor = control(packet, opcode, response, limits)?;
    let subscription = Subscribed {
        subscription: Subscription::decode(&mut cursor)?,
        source: Source::decode(&mut cursor)?,
        resolved_offset: cursor.u64()?,
    };
    end(cursor)?;
    Ok(subscription)
}

/// Encode explicit cancellation of one subscription generation.
pub fn encode_unsubscribe(
    envelope: Envelope,
    subscription: Subscribed,
    output: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    encode_close(envelope, subscription, false, output, limits)
}

/// Decode explicit cancellation of one subscription generation.
pub fn decode_unsubscribe(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<Subscribed, CodecError> {
    decode_close(packet, false, limits)
}

/// Encode confirmation that a subscription generation has been canceled.
pub fn encode_unsubscribed(
    envelope: Envelope,
    subscription: Subscribed,
    output: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    encode_close(envelope, subscription, true, output, limits)
}

/// Decode confirmation that a subscription generation has been canceled.
pub fn decode_unsubscribed(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<Subscribed, CodecError> {
    decode_close(packet, true, limits)
}
