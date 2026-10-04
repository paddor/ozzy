//! Broker-owned producer sessions. An open reply follows canonical confirmation.

use crate::{
    ENVELOPE_BYTES, Envelope, EnvelopeLimits, GroupId, Opcode, OperationId, Packet,
    PartitionIncarnation, ProducerId,
    append::{Authority, Policy},
    data::{CodecError, Cursor},
};

const GROUP_TAG: u8 = 1;
const OPEN_BYTES: usize = 90;
const OPENED_BYTES: usize = 90;

/// Create, resume, or conditionally fence a partition-local writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Mode {
    /// Return the current session, optionally requiring its exact epoch.
    /// An unused partition creates epoch one.
    Resume = 0,
    /// Commit a strictly newer epoch, fencing the expected old session.
    Fence = 1,
    /// Create epoch one. An existing identity must be resumed explicitly.
    Create = 2,
}

impl TryFrom<u8> for Mode {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Resume),
            1 => Ok(Self::Fence),
            2 => Ok(Self::Create),
            _ => Err(CodecError::Profile),
        }
    }
}

/// Correlated logical writer open. Stable operation ID survives link retries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Open {
    /// Leader generation claimed by the client, validated by the actor.
    pub authority: Authority,
    /// Fixed partition identity, independent of broker shard placement.
    pub partition: PartitionIncarnation,
    /// Stable logical writer identity, shared across its partition sessions.
    pub producer: ProducerId,
    /// Create, resume, or conditionally advance the writer fence.
    pub mode: Mode,
    /// Optional expected epoch for resume; required for fence, absent for create.
    pub expected_epoch: Option<u64>,
    /// Idempotence identity for a new session transition.
    pub operation: OperationId,
}

/// Session state after the required local or group confirmation boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Opened {
    /// Confirming leader generation. The client still validates membership.
    pub authority: Authority,
    /// Fixed partition identity.
    pub partition: PartitionIncarnation,
    /// The writer that was opened or resumed.
    pub producer: ProducerId,
    /// Confirmed nonzero writer fence.
    pub epoch: u64,
    /// Next fresh record sequence; retries may use older retained coordinates.
    pub next_sequence: u64,
    /// Exclusive floor below which exact retry results are no longer retained.
    pub retry_floor: u64,
    /// Configured confirmation policy, never downgraded by request or reply.
    pub policy: Policy,
}

/// Encode a correlated `OPEN_PRODUCER` request into reserved metadata.
pub fn encode_open(
    envelope: Envelope,
    open: Open,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::OpenProducer)?;
    validate_open(open)?;
    let header = envelope.encode_header(OPEN_BYTES, 0, limits)?;
    reserve(metadata, OPEN_BYTES)?;
    common(metadata, open.authority, open.partition, open.producer);
    metadata.push(open.mode as u8);
    metadata.extend_from_slice(&open.expected_epoch.unwrap_or(0).to_be_bytes());
    metadata.extend_from_slice(open.operation.as_bytes());
    Ok(header)
}

/// Decode structure only. Session, leader, and canonical state remain actor checks.
pub fn decode_open(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<Open, CodecError> {
    check(packet, Opcode::OpenProducer, OPEN_BYTES, limits)?;
    let mut cursor = Cursor(packet.metadata);
    let (authority, partition, producer) = read_common(&mut cursor)?;
    let mode = Mode::try_from(cursor.byte()?)?;
    let old = cursor.u64()?;
    let open = Open {
        authority,
        partition,
        producer,
        mode,
        expected_epoch: (old != 0).then_some(old),
        operation: OperationId::from_bytes(cursor.array()?),
    };
    validate_open(open)?;
    Ok(open)
}

/// Encode a confirmed session response into reserved metadata.
pub fn encode_opened(
    envelope: Envelope,
    opened: Opened,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::ProducerOpened)?;
    validate_opened(opened)?;
    let header = envelope.encode_header(OPENED_BYTES, 0, limits)?;
    reserve(metadata, OPENED_BYTES)?;
    common(
        metadata,
        opened.authority,
        opened.partition,
        opened.producer,
    );
    metadata.extend_from_slice(&opened.epoch.to_be_bytes());
    metadata.extend_from_slice(&opened.next_sequence.to_be_bytes());
    metadata.extend_from_slice(&opened.retry_floor.to_be_bytes());
    metadata.push(opened.policy as u8);
    Ok(header)
}

/// Decode a structurally valid response before SDK identity/session checks.
pub fn decode_opened(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<Opened, CodecError> {
    check(packet, Opcode::ProducerOpened, OPENED_BYTES, limits)?;
    let mut cursor = Cursor(packet.metadata);
    let (authority, partition, producer) = read_common(&mut cursor)?;
    let opened = Opened {
        authority,
        partition,
        producer,
        epoch: cursor.u64()?,
        next_sequence: cursor.u64()?,
        retry_floor: cursor.u64()?,
        policy: Policy::try_from(cursor.byte()?)?,
    };
    validate_opened(opened)?;
    Ok(opened)
}

fn command(envelope: Envelope, opcode: Opcode) -> Result<(), CodecError> {
    if envelope.opcode != opcode
        || envelope.response != (opcode == Opcode::ProducerOpened)
        || envelope.request_id.is_none()
        || envelope.session.is_none()
    {
        return Err(CodecError::Command);
    }
    Ok(())
}

fn check(
    packet: Packet<'_>,
    opcode: Opcode,
    length: usize,
    limits: EnvelopeLimits,
) -> Result<(), CodecError> {
    command(packet.envelope, opcode)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if packet.metadata.len() != length || !packet.payload.is_empty() {
        return Err(CodecError::Length);
    }
    Ok(())
}

fn reserve(metadata: &mut Vec<u8>, length: usize) -> Result<(), CodecError> {
    if metadata.capacity() < length {
        return Err(CodecError::Capacity);
    }
    metadata.clear();
    Ok(())
}

fn common(
    metadata: &mut Vec<u8>,
    authority: Authority,
    partition: PartitionIncarnation,
    producer: ProducerId,
) {
    metadata.push(GROUP_TAG);
    metadata.extend_from_slice(authority.group_id.as_bytes());
    metadata.extend_from_slice(&authority.config_epoch.to_be_bytes());
    metadata.extend_from_slice(&authority.view.to_be_bytes());
    metadata.extend_from_slice(partition.as_bytes());
    metadata.extend_from_slice(producer.as_bytes());
}

fn read_common(
    cursor: &mut Cursor<'_>,
) -> Result<(Authority, PartitionIncarnation, ProducerId), CodecError> {
    if cursor.byte()? != GROUP_TAG {
        return Err(CodecError::Profile);
    }
    let authority = Authority {
        group_id: GroupId::from_bytes(cursor.array()?),
        config_epoch: cursor.u64()?,
        view: cursor.u64()?,
    };
    let partition = PartitionIncarnation::from_bytes(cursor.array()?);
    let producer = ProducerId::from_bytes(cursor.array()?);
    validate_common(authority, partition, producer)?;
    Ok((authority, partition, producer))
}

fn validate_common(
    authority: Authority,
    partition: PartitionIncarnation,
    producer: ProducerId,
) -> Result<(), CodecError> {
    if authority.group_id.as_bytes() == &[0; 16]
        || authority.config_epoch == 0
        || partition.as_bytes() == &[0; 16]
        || producer.as_bytes() == &[0; 16]
    {
        return Err(CodecError::Identity);
    }
    Ok(())
}

fn validate_open(open: Open) -> Result<(), CodecError> {
    validate_common(open.authority, open.partition, open.producer)?;
    if open.operation.as_bytes() == &[0; 16]
        || open.expected_epoch == Some(0)
        || (open.mode == Mode::Fence && open.expected_epoch.is_none())
        || (open.mode == Mode::Create && open.expected_epoch.is_some())
    {
        return Err(CodecError::Identity);
    }
    Ok(())
}

fn validate_opened(opened: Opened) -> Result<(), CodecError> {
    validate_common(opened.authority, opened.partition, opened.producer)?;
    if opened.epoch == 0
        || opened.retry_floor > opened.next_sequence
        || !matches!(
            opened.policy,
            Policy::LocalDurable | Policy::QuorumDurable | Policy::QuorumReplicatedPersisting
        )
    {
        return Err(CodecError::Profile);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
