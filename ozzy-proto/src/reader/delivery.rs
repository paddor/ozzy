//! `RECORDS` deliveries and `RECORDS_PUB` publications: encoding from caller
//! frames, and borrowed or owned decoding with complete validation first.

use super::{Source, Subscription, command};
use crate::data::{self, CodecError, DataLimits, OwnedRecords, Record, Records, capacity};
use crate::layout::Reader;
use crate::{ENVELOPE_BYTES, Envelope, Frames, Opcode, Packet};
use bytes::{Bytes, BytesMut};

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

/// Encode one nonempty contiguous batch using caller-reserved frames.
pub fn encode_records(
    e: Envelope,
    v: RecordHeader,
    records: &[Record<'_>],
    frames: Frames<&mut Vec<u8>>,
    limits: DataLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    encode_records_from(
        e,
        v,
        records
            .iter()
            .map(|r| (r.message_id, r.encoding, r.parts.iter().copied())),
        frames,
        limits,
    )
}

/// Encode directly from a repeatable record iterator, without a temporary record table.
/// Cloning the iterator must yield the same IDs and multipart bytes.
pub fn encode_records_from<'a, I, P>(
    e: Envelope,
    v: RecordHeader,
    records: I,
    Frames { metadata, payload }: Frames<&mut Vec<u8>>,
    limits: DataLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError>
where
    I: ExactSizeIterator<Item = (crate::MessageId, data::Encoding, P)> + Clone,
    P: Iterator<Item = &'a [u8]>,
{
    command(e, Opcode::Records)?;
    v.subscription.validate()?;
    v.source.validate()?;
    let (descriptors, bytes) = data::record_sizes(records.clone(), v.first_offset, limits)?;
    let size = data::sum(&[45, v.source.size(), descriptors])?;
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
    command(p.envelope, Opcode::Records)?;
    p.envelope
        .validate_frames(p.metadata.len(), p.payload.len(), limits.envelope)?;
    let mut c = Reader::new(p.metadata);
    let header = RecordHeader {
        subscription: Subscription::decode(&mut c)?,
        source: Source::decode(&mut c)?,
        first_offset: c.u64()?,
    };
    let (payload_encoding, _) =
        crate::append::read_payload_encoding(&mut c, p.payload.len(), limits)?;
    if payload_encoding != crate::append::PayloadEncoding::Raw {
        return Err(CodecError::Profile);
    }
    let records = data::decode_records(c.rest(), p.payload, header.first_offset, limits)?;
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

fn owned_records(
    mut c: Reader<'_>,
    first_offset: u64,
    frames: Frames<&Bytes>,
    limits: DataLimits,
    buffer: &mut BytesMut,
) -> Result<(crate::append::PayloadEncoding, OwnedRecords), CodecError> {
    let (payload_encoding, payload) =
        crate::append::decode_owned_payload(&mut c, frames.payload, buffer, limits)?;
    let records = data::decode_records(c.rest(), &payload, first_offset, limits)?;
    Ok((
        payload_encoding,
        records.to_owned(frames.metadata, &payload),
    ))
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
    command(p.envelope, Opcode::Records)?;
    p.envelope
        .validate_frames(p.metadata.len(), p.payload.len(), limits.envelope)?;
    let mut c = Reader::new(p.metadata);
    let header = RecordHeader {
        subscription: Subscription::decode(&mut c)?,
        source: Source::decode(&mut c)?,
        first_offset: c.u64()?,
    };
    let (payload_encoding, records) = owned_records(
        c,
        header.first_offset,
        Frames { metadata, payload },
        limits,
        buffer,
    )?;
    Ok(OwnedDelivery {
        header,
        payload_encoding,
        records,
    })
}

/// Decode one shared publication. Descriptors and payload lengths are complete
/// before any record is exposed.
pub fn decode_publication(
    p: Packet<'_>,
    limits: DataLimits,
) -> Result<Publication<'_>, CodecError> {
    command(p.envelope, Opcode::RecordsPub)?;
    p.envelope
        .validate_frames(p.metadata.len(), p.payload.len(), limits.envelope)?;
    let mut c = Reader::new(p.metadata);
    let header = PublicationHeader {
        source: Source::decode(&mut c)?,
        first_offset: c.u64()?,
    };
    publication_topic(header.source)?;
    let (payload_encoding, _) =
        crate::append::read_payload_encoding(&mut c, p.payload.len(), limits)?;
    if payload_encoding != crate::append::PayloadEncoding::Raw {
        return Err(CodecError::Profile);
    }
    let records = data::decode_records(c.rest(), p.payload, header.first_offset, limits)?;
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
    command(p.envelope, Opcode::RecordsPub)?;
    p.envelope
        .validate_frames(p.metadata.len(), p.payload.len(), limits.envelope)?;
    let mut c = Reader::new(p.metadata);
    let header = PublicationHeader {
        source: Source::decode(&mut c)?,
        first_offset: c.u64()?,
    };
    publication_topic(header.source)?;
    let (payload_encoding, records) = owned_records(
        c,
        header.first_offset,
        Frames { metadata, payload },
        limits,
        buffer,
    )?;
    Ok(OwnedDelivery {
        header,
        payload_encoding,
        records,
    })
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
