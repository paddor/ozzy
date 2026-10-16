//! Single-partition subscriptions and bounded multipart reader delivery.
//! Correlated delivery supports request pipelines; notifications support push.
//! Decoding source metadata never proves ownership, persistence, or group confirmation.

mod delivery;
mod encoder;
mod start;
mod subscription;
pub use start::{IdPolicy, Start};

pub use delivery::{
    Delivery, OwnedDelivery, Publication, PublicationHeader, RecordHeader,
    decode_owned_publication, decode_owned_records, decode_publication, decode_records,
    encode_records, encode_records_from, publication_topic,
};
pub use encoder::{PreparedBlock, RecordsEncoder};

pub use subscription::{
    Target, decode_unsubscribe, decode_unsubscribed, encode_unsubscribe, encode_unsubscribed,
};

use crate::data::{self, Authority, CodecError, DataLimits, capacity};
use crate::layout::Reader;
use crate::{
    ENVELOPE_BYTES, Envelope, EnvelopeLimits, GroupId, Opcode, Packet, PartitionId,
    PartitionIncarnation, ProducerId, SubscriptionId, Topic,
};

/// Identity of one subscribed log. Local logs do not fabricate group authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// Embedded owner log; topic is fixed by the subscription.
    Local {
        /// Stable producer owning the log.
        producer: ProducerId,
        /// Producer-local partition number.
        partition: PartitionId,
    },
    /// Confirmed group log; runtime must validate all authority fields.
    Group {
        /// Group and leader generation.
        authority: Authority,
        /// Immutable partition incarnation.
        partition: PartitionIncarnation,
        /// Nonzero ownership fence.
        owner_epoch: u64,
    },
}

/// A subscription replacement must use a fresh generation, even on the same link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Subscription {
    /// Stable subscription identity.
    pub id: SubscriptionId,
    /// Nonzero generation UUID.
    pub generation: u128,
}

/// Exact-offset, single-topic reader request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    /// Session-scoped subscription generation.
    pub subscription: Subscription,
    /// Validated logical stream and topic.
    pub target: Target,
    /// Initial selector, resolved once to an exact retained offset.
    pub start: Start,
}

/// Decoded exact-offset subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSubscribe {
    /// Session-scoped subscription generation.
    pub subscription: Subscription,
    /// Validated logical stream and topic.
    pub target: Target,
    /// Initial selector, resolved once to an exact retained offset.
    pub start: Start,
}

/// Confirmed subscription source. Does not itself claim durable data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Subscribed {
    /// Session-scoped subscription generation.
    pub subscription: Subscription,
    /// Source used unchanged by records and receipt observations.
    pub source: Source,
    /// First resolved offset; reconnect resumes ordinary delivered offsets.
    pub resolved_offset: u64,
}

/// Independent cumulative observations. No durable progress commit is implied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    /// Session-scoped subscription generation.
    pub subscription: Subscription,
    /// Same source as the delivered batch.
    pub source: Source,
    /// Highest contiguous offset accepted by the receiving runtime.
    pub received: Option<u64>,
    /// Highest contiguous offset processed by the application.
    pub processed: Option<u64>,
}

/// Read a shared-partition command's fixed subscription/source prefix. This
/// performs no record decoding or payload validation. Legacy local-log selectors
/// are excluded; native broker partitions have explicit group/incarnation IDs.
/// The destination must still decode the complete command and enforce its source.
pub fn route(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<Source, CodecError> {
    route_subscription(packet, limits).map(|(_, source)| source)
}

/// Inspect a shared reader command's fixed generation and source without
/// decoding records. Session and full payload validation remain with the owner.
pub fn route_subscription(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<(Subscription, Source), CodecError> {
    if !matches!(
        packet.envelope.opcode,
        Opcode::Subscribed
            | Opcode::Unsubscribed
            | Opcode::Subscribe
            | Opcode::Records
            | Opcode::Unsubscribe
            | Opcode::Ack
    ) {
        return Err(CodecError::Command);
    }
    command(packet.envelope, packet.envelope.opcode)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if packet.envelope.opcode != Opcode::Records && !packet.payload.is_empty() {
        return Err(CodecError::Length);
    }
    let mut cursor = Reader::new(packet.metadata);
    let subscription = Subscription::decode(&mut cursor)?;
    if cursor.rest().first() != Some(&1) {
        return Err(CodecError::Profile);
    }
    Source::decode(&mut cursor).map(|source| (subscription, source))
}

/// Inspect the fixed source of a live publication. Record descriptors and
/// payloads remain opaque until the destination performs complete decoding.
pub fn route_publication(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<Source, CodecError> {
    command(packet.envelope, Opcode::RecordsPub)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    let source = Source::decode(&mut Reader::new(packet.metadata))?;
    publication_topic(source)?;
    Ok(source)
}

/// Encode SUBSCRIBE without growing storage; failure preserves its contents.
pub fn encode_subscribe(
    e: Envelope,
    v: &Subscribe,
    out: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    v.subscription.validate()?;
    v.target.validate()?;
    v.start.validate()?;
    let size = data::sum(&[32, v.target.size(), v.start.size()])?;
    let header = prepare(e, Opcode::Subscribe, size, out, limits)?;
    v.subscription.encode(out);
    v.target.encode(out);
    v.start.encode(out);
    Ok(header)
}

/// Decode and bound both topic names before allocating their validated value.
pub fn decode_subscribe(
    p: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<DecodedSubscribe, CodecError> {
    let mut c = control(p, Opcode::Subscribe, limits)?;
    let subscription = Subscription::decode(&mut c)?;
    let target = Target::decode(&mut c)?;
    let start = Start::decode(&mut c)?;
    end(c)?;
    Ok(DecodedSubscribe {
        subscription,
        target,
        start,
    })
}

/// Encode the source accepted for this subscription.
pub fn encode_subscribed(
    e: Envelope,
    v: Subscribed,
    out: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    v.subscription.validate()?;
    v.source.validate()?;
    let header = prepare(
        e,
        Opcode::Subscribed,
        data::add(40, v.source.size())?,
        out,
        limits,
    )?;
    v.subscription.encode(out);
    v.source.encode(out);
    out.extend_from_slice(&v.resolved_offset.to_be_bytes());
    Ok(header)
}

/// Decode the source accepted for this subscription.
pub fn decode_subscribed(p: Packet<'_>, limits: EnvelopeLimits) -> Result<Subscribed, CodecError> {
    let mut c = control(p, Opcode::Subscribed, limits)?;
    let v = Subscribed {
        subscription: Subscription::decode(&mut c)?,
        source: Source::decode(&mut c)?,
        resolved_offset: c.u64()?,
    };
    end(c)?;
    Ok(v)
}

/// Encode cumulative receipt/processing observations; never claim persistence.
pub fn encode_ack(
    e: Envelope,
    v: Ack,
    out: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    v.validate()?;
    let size = data::sum(&[
        34,
        v.source.size(),
        v.received.map_or(0, |_| 8),
        v.processed.map_or(0, |_| 8),
    ])?;
    let header = prepare(e, Opcode::Ack, size, out, limits)?;
    v.subscription.encode(out);
    v.source.encode(out);
    position(out, v.received);
    position(out, v.processed);
    Ok(header)
}

/// Decode independent progress positions, rejecting processing beyond receipt.
pub fn decode_ack(p: Packet<'_>, limits: EnvelopeLimits) -> Result<Ack, CodecError> {
    let mut c = control(p, Opcode::Ack, limits)?;
    let v = Ack {
        subscription: Subscription::decode(&mut c)?,
        source: Source::decode(&mut c)?,
        received: read_position(&mut c)?,
        processed: read_position(&mut c)?,
    };
    end(c)?;
    v.validate()?;
    Ok(v)
}

impl Ack {
    fn validate(self) -> Result<(), CodecError> {
        self.subscription.validate()?;
        self.source.validate()?;
        if self
            .processed
            .is_some_and(|p| self.received.is_none_or(|r| p > r))
        {
            return Err(CodecError::Position);
        }
        Ok(())
    }
}

impl Subscription {
    fn validate(self) -> Result<(), CodecError> {
        nonzero(self.id.as_bytes())?;
        nonzero(&self.generation.to_be_bytes())
    }
    fn encode(self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.id.as_bytes());
        out.extend_from_slice(&self.generation.to_be_bytes());
    }
    fn decode(c: &mut Reader<'_>) -> Result<Self, CodecError> {
        let v = Self {
            id: SubscriptionId::from_bytes(c.array()?),
            generation: u128::from_be_bytes(c.array()?),
        };
        v.validate()?;
        Ok(v)
    }
}

impl Source {
    fn size(self) -> usize {
        match self {
            Self::Local { .. } => 21,
            Self::Group { .. } => 57,
        }
    }
    fn validate(self) -> Result<(), CodecError> {
        match self {
            Self::Local { producer, .. } => nonzero(producer.as_bytes()),
            Self::Group {
                authority,
                partition,
                owner_epoch,
            } => {
                nonzero(authority.group_id.as_bytes())?;
                nonzero(partition.as_bytes())?;
                if authority.config_epoch == 0 || owner_epoch == 0 {
                    return Err(CodecError::Identity);
                }
                Ok(())
            }
        }
    }
    fn encode(self, out: &mut Vec<u8>) {
        match self {
            Self::Local {
                producer,
                partition,
            } => {
                out.push(0);
                out.extend_from_slice(producer.as_bytes());
                out.extend_from_slice(&partition.get().to_be_bytes());
            }
            Self::Group {
                authority,
                partition,
                owner_epoch,
            } => {
                out.push(1);
                out.extend_from_slice(authority.group_id.as_bytes());
                out.extend_from_slice(&authority.config_epoch.to_be_bytes());
                out.extend_from_slice(&authority.view.to_be_bytes());
                out.extend_from_slice(partition.as_bytes());
                out.extend_from_slice(&owner_epoch.to_be_bytes());
            }
        }
    }
    fn decode(c: &mut Reader<'_>) -> Result<Self, CodecError> {
        let v = match c.u8()? {
            0 => Self::Local {
                producer: ProducerId::from_bytes(c.array()?),
                partition: PartitionId::new(c.u32()?),
            },
            1 => Self::Group {
                authority: Authority {
                    group_id: GroupId::from_bytes(c.array()?),
                    config_epoch: c.u64()?,
                    view: c.u64()?,
                },
                partition: PartitionIncarnation::from_bytes(c.array()?),
                owner_epoch: c.u64()?,
            },
            _ => return Err(CodecError::Profile),
        };
        v.validate()?;
        Ok(v)
    }
}

fn nonzero(id: &[u8; 16]) -> Result<(), CodecError> {
    if id == &[0; 16] {
        return Err(CodecError::Identity);
    }
    Ok(())
}

fn text(out: &mut Vec<u8>, v: &str) {
    out.extend_from_slice(&(v.len() as u32).to_be_bytes());
    out.extend_from_slice(v.as_bytes());
}

fn read_text<'a>(c: &mut Reader<'a>) -> Result<&'a str, CodecError> {
    let n = c.u32()? as usize;
    if n == 0 || n > 255 {
        return Err(CodecError::Profile);
    }
    std::str::from_utf8(c.take(n)?).map_err(|_| CodecError::Profile)
}

fn position(out: &mut Vec<u8>, v: Option<u64>) {
    out.push(u8::from(v.is_some()));
    if let Some(v) = v {
        out.extend_from_slice(&v.to_be_bytes());
    }
}

fn read_position(c: &mut Reader<'_>) -> Result<Option<u64>, CodecError> {
    match c.u8()? {
        0 => Ok(None),
        1 => Ok(Some(c.u64()?)),
        _ => Err(CodecError::Profile),
    }
}

/// Replies are SUBSCRIBED and UNSUBSCRIBED; an ACK goes either way.
fn command(e: Envelope, opcode: Opcode) -> Result<(), CodecError> {
    let response = match opcode {
        Opcode::Subscribed | Opcode::Unsubscribed => true,
        Opcode::Ack => e.response,
        _ => false,
    };
    if e.opcode != opcode
        || e.response != response
        || (!matches!(opcode, Opcode::Records | Opcode::RecordsPub | Opcode::Ack)
            && e.request_id.is_none())
        || (opcode == Opcode::RecordsPub && e.request_id.is_some())
        || (opcode == Opcode::Ack && e.response && e.request_id.is_none())
    {
        return Err(CodecError::Command);
    }
    Ok(())
}

fn prepare(
    e: Envelope,
    opcode: Opcode,
    size: usize,
    out: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(e, opcode)?;
    let header = e.encode_header(size, 0, limits)?;
    capacity(out, size)?;
    out.clear();
    Ok(header)
}

fn control(
    p: Packet<'_>,
    opcode: Opcode,
    limits: EnvelopeLimits,
) -> Result<Reader<'_>, CodecError> {
    command(p.envelope, opcode)?;
    p.envelope
        .validate_frames(p.metadata.len(), p.payload.len(), limits)?;
    if !p.payload.is_empty() {
        return Err(CodecError::Length);
    }
    Ok(Reader::new(p.metadata))
}

fn end(c: Reader<'_>) -> Result<(), CodecError> {
    if c.rest().is_empty() {
        Ok(())
    } else {
        Err(CodecError::Length)
    }
}
