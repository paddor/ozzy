//! Single-partition subscriptions and bounded multipart reader delivery.
//! Correlated delivery supports request pipelines; notifications support push.
//! Decoding source metadata never proves ownership, persistence, or group confirmation.

mod encoder;
mod subscription;

pub use encoder::RecordsEncoder;

pub use subscription::{
    Credit, Target, decode_credit, decode_unsubscribe, decode_unsubscribed, encode_credit,
    encode_unsubscribe, encode_unsubscribed,
};

use crate::data::{
    self, Authority, CodecError, Cursor, DataLimits, OwnedRecords, Record, Records, capacity,
};
use crate::{
    ENVELOPE_BYTES, Envelope, EnvelopeLimits, GroupId, Opcode, Packet, PartitionId,
    PartitionIncarnation, ProducerId, SubscriptionId, Topic,
};
use bytes::{Bytes, BytesMut};

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
    /// Exact first desired offset.
    pub start: u64,
}

/// Decoded exact-offset subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSubscribe {
    /// Session-scoped subscription generation.
    pub subscription: Subscription,
    /// Validated logical stream and topic.
    pub target: Target,
    /// Exact first desired offset.
    pub start: u64,
}

/// Confirmed subscription source. Does not itself claim durable data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Subscribed {
    /// Session-scoped subscription generation.
    pub subscription: Subscription,
    /// Source used unchanged by records and receipt observations.
    pub source: Source,
}

/// Common identity and offset fields of one contiguous reader batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    /// Session-scoped subscription generation.
    pub subscription: Subscription,
    /// Owner/partition identity.
    pub source: Source,
    /// First offset; all following offsets are contiguous.
    pub first_offset: u64,
}

/// Identity and offset of one batch shared by every live reader of a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationHeader {
    /// Confirmed group log. Receivers compare it with their subscribed source.
    pub source: Source,
    /// First offset; all following offsets are contiguous.
    pub first_offset: u64,
}

/// Validated shared records borrowing both received frames. Receipt proves
/// neither the sender's authority nor that no earlier publication was lost.
#[derive(Debug, Clone, Copy)]
pub struct Publication<'a> {
    /// Source and first offset.
    pub header: PublicationHeader,
    /// Encoding of the complete records payload frame.
    pub payload_encoding: crate::append::PayloadEncoding,
    /// Opaque multipart records, fully validated before iteration.
    pub records: Records<'a>,
}

/// Validated records borrowing both received frames.
#[derive(Debug, Clone, Copy)]
pub struct Delivery<'a> {
    /// Source, subscription and first offset.
    pub header: RecordHeader,
    /// Encoding of the complete records payload frame.
    pub payload_encoding: crate::append::PayloadEncoding,
    /// Opaque multipart records, fully validated before iteration.
    pub records: Records<'a>,
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
    let response = match packet.envelope.opcode {
        Opcode::Subscribed | Opcode::Unsubscribed => true,
        Opcode::Subscribe | Opcode::Records | Opcode::Unsubscribe => false,
        Opcode::Ack | Opcode::Credit => packet.envelope.response,
        _ => return Err(CodecError::Command),
    };
    command(packet.envelope, packet.envelope.opcode, response)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if packet.envelope.opcode != Opcode::Records && !packet.payload.is_empty() {
        return Err(CodecError::Length);
    }
    let mut cursor = Cursor(packet.metadata);
    let subscription = Subscription::decode(&mut cursor)?;
    if cursor.0.first() != Some(&1) {
        return Err(CodecError::Profile);
    }
    Source::decode(&mut cursor).map(|source| (subscription, source))
}

/// Inspect the fixed source of a live publication. Record descriptors and
/// payloads remain opaque until the destination performs complete decoding.
pub fn route_publication(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<Source, CodecError> {
    command(packet.envelope, Opcode::RecordsPub, false)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    let source = Source::decode(&mut Cursor(packet.metadata))?;
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
    let size = 40 + v.target.size();
    let header = prepare(e, Opcode::Subscribe, false, size, out, limits)?;
    v.subscription.encode(out);
    v.target.encode(out);
    out.extend_from_slice(&v.start.to_be_bytes());
    Ok(header)
}

/// Decode and bound both topic names before allocating their validated value.
pub fn decode_subscribe(
    p: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<DecodedSubscribe, CodecError> {
    let mut c = control(p, Opcode::Subscribe, false, limits)?;
    let subscription = Subscription::decode(&mut c)?;
    let target = Target::decode(&mut c)?;
    let start = c.u64()?;
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
        true,
        32 + v.source.size(),
        out,
        limits,
    )?;
    v.subscription.encode(out);
    v.source.encode(out);
    Ok(header)
}

/// Decode the source accepted for this subscription.
pub fn decode_subscribed(p: Packet<'_>, limits: EnvelopeLimits) -> Result<Subscribed, CodecError> {
    let mut c = control(p, Opcode::Subscribed, true, limits)?;
    let v = Subscribed {
        subscription: Subscription::decode(&mut c)?,
        source: Source::decode(&mut c)?,
    };
    end(c)?;
    Ok(v)
}

/// Encode one nonempty contiguous batch using caller-reserved frames.
pub fn encode_records(
    e: Envelope,
    v: RecordHeader,
    records: &[Record<'_>],
    metadata: &mut Vec<u8>,
    payload: &mut Vec<u8>,
    limits: DataLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    encode_records_from(
        e,
        v,
        records
            .iter()
            .map(|r| (r.message_id, r.encoding, r.parts.iter().copied())),
        metadata,
        payload,
        limits,
    )
}

/// Encode directly from a repeatable record iterator, without a temporary record table.
/// Cloning the iterator must yield the same IDs and multipart bytes.
pub fn encode_records_from<'a, I, P>(
    e: Envelope,
    v: RecordHeader,
    records: I,
    metadata: &mut Vec<u8>,
    payload: &mut Vec<u8>,
    limits: DataLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError>
where
    I: ExactSizeIterator<Item = (crate::MessageId, data::Encoding, P)> + Clone,
    P: Iterator<Item = &'a [u8]>,
{
    command(e, Opcode::Records, false)?;
    v.subscription.validate()?;
    v.source.validate()?;
    let (descriptors, bytes) = data::record_sizes(records.clone(), v.first_offset, limits)?;
    let size = data::add(45 + v.source.size(), descriptors)?;
    let header = e.encode_header(size, bytes, limits.envelope)?;
    capacity(metadata, size)?;
    capacity(payload, bytes)?;
    metadata.clear();
    payload.clear();
    v.subscription.encode(metadata);
    v.source.encode(metadata);
    metadata.extend_from_slice(&v.first_offset.to_be_bytes());
    crate::append::encode_payload_encoding(metadata, crate::append::PayloadEncoding::Raw, bytes)?;
    data::write_records(records, metadata, payload);
    Ok(header)
}

/// Decode complete descriptors and payload lengths before exposing any record.
pub fn decode_records(p: Packet<'_>, limits: DataLimits) -> Result<Delivery<'_>, CodecError> {
    command(p.envelope, Opcode::Records, false)?;
    p.envelope
        .validate_frames(p.metadata.len(), p.payload.len(), limits.envelope)?;
    let mut c = Cursor(p.metadata);
    let header = RecordHeader {
        subscription: Subscription::decode(&mut c)?,
        source: Source::decode(&mut c)?,
        first_offset: c.u64()?,
    };
    let payload_encoding = crate::append::PayloadEncoding::try_from(c.byte()?)?;
    let decoded_bytes = c.u32()? as usize;
    crate::append::validate_payload_encoding(
        payload_encoding,
        decoded_bytes,
        p.payload.len(),
        limits,
    )?;
    if payload_encoding != crate::append::PayloadEncoding::Raw {
        return Err(CodecError::Profile);
    }
    let records = data::decode_records(c.0, p.payload, header.first_offset, limits)?;
    Ok(Delivery {
        header,
        payload_encoding,
        records,
    })
}

/// Owned records of one delivery or publication.
#[derive(Debug, Clone)]
pub struct OwnedDelivery<H> {
    /// Delivery or publication header.
    pub header: H,
    /// Encoding of the received payload frame.
    pub payload_encoding: crate::append::PayloadEncoding,
    /// Records sharing the received frames or the decompressed payload.
    pub records: OwnedRecords,
}

fn owned_delivery<H>(
    mut c: Cursor<'_>,
    first_offset: u64,
    header: H,
    metadata: &Bytes,
    payload: &Bytes,
    limits: DataLimits,
    buffer: &mut BytesMut,
) -> Result<OwnedDelivery<H>, CodecError> {
    let (payload_encoding, payload) =
        crate::append::decode_owned_payload(&mut c, payload, buffer, limits)?;
    let records = data::decode_records(c.0, &payload, first_offset, limits)?;
    Ok(OwnedDelivery {
        header,
        payload_encoding,
        records: records.to_owned(metadata, &payload),
    })
}

/// Decode a raw or whole-group LZ4 delivery into owned records. Raw records
/// share `metadata` and `payload`, the frames `p` was decoded from. LZ4 records
/// own bytes decompressed into `buffer`, whose allocation is reused once every
/// record of earlier deliveries is released.
pub fn decode_owned_records(
    p: Packet<'_>,
    metadata: &Bytes,
    payload: &Bytes,
    limits: DataLimits,
    buffer: &mut BytesMut,
) -> Result<OwnedDelivery<RecordHeader>, CodecError> {
    command(p.envelope, Opcode::Records, false)?;
    p.envelope
        .validate_frames(p.metadata.len(), p.payload.len(), limits.envelope)?;
    let mut c = Cursor(p.metadata);
    let header = RecordHeader {
        subscription: Subscription::decode(&mut c)?,
        source: Source::decode(&mut c)?,
        first_offset: c.u64()?,
    };
    owned_delivery(
        c,
        header.first_offset,
        header,
        metadata,
        payload,
        limits,
        buffer,
    )
}

/// Decode one shared publication. Descriptors and payload lengths are complete
/// before any record is exposed.
pub fn decode_publication(
    p: Packet<'_>,
    limits: DataLimits,
) -> Result<Publication<'_>, CodecError> {
    command(p.envelope, Opcode::RecordsPub, false)?;
    p.envelope
        .validate_frames(p.metadata.len(), p.payload.len(), limits.envelope)?;
    let mut c = Cursor(p.metadata);
    let header = PublicationHeader {
        source: Source::decode(&mut c)?,
        first_offset: c.u64()?,
    };
    publication_topic(header.source)?;
    let payload_encoding = crate::append::PayloadEncoding::try_from(c.byte()?)?;
    let decoded_bytes = c.u32()? as usize;
    crate::append::validate_payload_encoding(
        payload_encoding,
        decoded_bytes,
        p.payload.len(),
        limits,
    )?;
    if payload_encoding != crate::append::PayloadEncoding::Raw {
        return Err(CodecError::Profile);
    }
    let records = data::decode_records(c.0, p.payload, header.first_offset, limits)?;
    Ok(Publication {
        header,
        payload_encoding,
        records,
    })
}

/// Decode a raw or whole-group LZ4 publication into owned records, with the
/// frame and buffer ownership of [`decode_owned_records`].
pub fn decode_owned_publication(
    p: Packet<'_>,
    metadata: &Bytes,
    payload: &Bytes,
    limits: DataLimits,
    buffer: &mut BytesMut,
) -> Result<OwnedDelivery<PublicationHeader>, CodecError> {
    command(p.envelope, Opcode::RecordsPub, false)?;
    p.envelope
        .validate_frames(p.metadata.len(), p.payload.len(), limits.envelope)?;
    let mut c = Cursor(p.metadata);
    let header = PublicationHeader {
        source: Source::decode(&mut c)?,
        first_offset: c.u64()?,
    };
    publication_topic(header.source)?;
    owned_delivery(
        c,
        header.first_offset,
        header,
        metadata,
        payload,
        limits,
        buffer,
    )
}

/// Live-stream topic of one group partition: the group, then the partition. A
/// reader that subscribes to the bare group receives all of its partitions.
/// Local logs have no shared live stream.
pub fn publication_topic(source: Source) -> Result<[u8; 32], CodecError> {
    let Source::Group {
        authority,
        partition,
        ..
    } = source
    else {
        return Err(CodecError::Profile);
    };
    let mut topic = [0; 32];
    topic[..16].copy_from_slice(authority.group_id.as_bytes());
    topic[16..].copy_from_slice(partition.as_bytes());
    Ok(topic)
}

/// Encode cumulative receipt/processing observations; never claim persistence.
pub fn encode_ack(
    e: Envelope,
    v: Ack,
    out: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    v.validate()?;
    let size = 34
        + v.source.size()
        + 8 * (usize::from(v.received.is_some()) + usize::from(v.processed.is_some()));
    let header = prepare(e, Opcode::Ack, e.response, size, out, limits)?;
    v.subscription.encode(out);
    v.source.encode(out);
    position(out, v.received);
    position(out, v.processed);
    Ok(header)
}

/// Decode independent progress positions, rejecting processing beyond receipt.
pub fn decode_ack(p: Packet<'_>, limits: EnvelopeLimits) -> Result<Ack, CodecError> {
    let mut c = control(p, Opcode::Ack, p.envelope.response, limits)?;
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
    fn decode(c: &mut Cursor<'_>) -> Result<Self, CodecError> {
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
    fn decode(c: &mut Cursor<'_>) -> Result<Self, CodecError> {
        let v = match c.byte()? {
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

fn read_text<'a>(c: &mut Cursor<'a>) -> Result<&'a str, CodecError> {
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

fn read_position(c: &mut Cursor<'_>) -> Result<Option<u64>, CodecError> {
    match c.byte()? {
        0 => Ok(None),
        1 => Ok(Some(c.u64()?)),
        _ => Err(CodecError::Profile),
    }
}

fn command(e: Envelope, opcode: Opcode, response: bool) -> Result<(), CodecError> {
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
    response: bool,
    size: usize,
    out: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(e, opcode, response)?;
    let header = e.encode_header(size, 0, limits)?;
    capacity(out, size)?;
    out.clear();
    Ok(header)
}

fn control(
    p: Packet<'_>,
    opcode: Opcode,
    response: bool,
    limits: EnvelopeLimits,
) -> Result<Cursor<'_>, CodecError> {
    command(p.envelope, opcode, response)?;
    p.envelope
        .validate_frames(p.metadata.len(), p.payload.len(), limits)?;
    if !p.payload.is_empty() {
        return Err(CodecError::Length);
    }
    Ok(Cursor(p.metadata))
}

fn end(c: Cursor<'_>) -> Result<(), CodecError> {
    if c.0.is_empty() {
        Ok(())
    } else {
        Err(CodecError::Length)
    }
}
