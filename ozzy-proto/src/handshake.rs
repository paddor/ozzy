//! Bounded native HELLO/WELCOME properties. Negotiation is not authentication.

use super::append::DataLimits;
use super::{ENVELOPE_BYTES, Envelope, EnvelopeError, EnvelopeLimits, Opcode, Packet};
use crate::layout::{LayoutError, Reader};
use smallvec::SmallVec;

/// Owner-append capability, including exact retry reconciliation.
pub const OWNER_APPEND: u16 = 1;
/// Consumer-group membership and source activation; requires durable progress.
pub const OWNER_GROUPS: u16 = 1 << 7;
/// Typed authority hints for fixed-group owner discovery and unchanged retries.
pub const OWNER_ROUTING: u16 = 1 << 9;
/// Single-partition reader delivery and receipt/processing observations.
pub const OWNER_READ: u16 = 1 << 10;
/// Durable group processing progress, with exact member/assignment fencing.
pub const OWNER_PROGRESS: u16 = 1 << 11;
/// Pipelined owner append with record-based retries and per-writer range replies.
pub const OWNER_STREAM: u16 = 1 << 12;
/// Reader role, not independent authorization.
pub const CONSUMER: u32 = 2;
/// Producer role. A role claim does not authorize a producer identity.
pub const PRODUCER: u32 = 1;
/// Log-owner role. Serving authority still comes from the active group.
pub const OWNER: u32 = 4;
// Capability 14 is unassigned. Required use must fail, never imply delivery.
const KNOWN_CAPABILITIES: u16 = 0x1fff;
const MAX_PROPERTIES: usize = 32;
const NAMES: [&[u8]; 10] = [
    b"versions",
    b"capabilities",
    b"required-capabilities",
    b"max-metadata-bytes",
    b"max-payload-bytes",
    b"max-batch-records",
    b"max-payload-parts",
    b"roles",
    b"max-record-bytes",
    b"superseded-hello",
];

/// One endpoint's advertised receive limits and supported capability set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parameters {
    /// Bit `id - 1` for each supported capability in the allocated 1..=13 range.
    pub capabilities: u16,
    /// Same bit encoding; each required capability must be selected.
    pub required_capabilities: u16,
    /// Directional command bounds. Never replace the opposite direction's limits.
    pub receive: DataLimits,
    /// Producer/consumer/owner/replica/directory occupy bits 0..=4.
    pub roles: u32,
}

impl Parameters {
    /// Pipelined append profile with directional packet bounds.
    /// Both append and streaming support are mandatory; no legacy downgrade.
    pub fn streaming(receive: DataLimits, roles: u32) -> Result<Self, HandshakeError> {
        let parameters = Self {
            capabilities: OWNER_APPEND | OWNER_STREAM,
            required_capabilities: OWNER_APPEND | OWNER_STREAM,
            receive,
            roles,
        };
        parameters.validate()?;
        Ok(parameters)
    }
    /// Reader profile with fixed receive bounds. Transport pressure bounds replay.
    pub fn reader(receive: DataLimits, roles: u32) -> Result<Self, HandshakeError> {
        let mut parameters = Self::append(receive, roles)?;
        parameters.capabilities = OWNER_READ;
        parameters.required_capabilities = OWNER_READ;
        Ok(parameters)
    }

    /// Reader profile requiring durable progress in addition to record delivery.
    pub fn processing(receive: DataLimits, roles: u32) -> Result<Self, HandshakeError> {
        let mut parameters = Self::reader(receive, roles)?;
        parameters.capabilities |= OWNER_PROGRESS;
        parameters.required_capabilities |= OWNER_PROGRESS;
        parameters.validate()?;
        Ok(parameters)
    }

    /// Processing profile that also requires automatic group membership.
    pub fn group(receive: DataLimits, roles: u32) -> Result<Self, HandshakeError> {
        let mut parameters = Self::processing(receive, roles)?;
        parameters.capabilities |= OWNER_GROUPS;
        parameters.required_capabilities |= OWNER_GROUPS;
        parameters.validate()?;
        Ok(parameters)
    }
    /// Single-request append profile; other command families remain unadvertised.
    pub fn append(receive: DataLimits, roles: u32) -> Result<Self, HandshakeError> {
        let parameters = Self {
            capabilities: OWNER_APPEND,
            required_capabilities: OWNER_APPEND,
            receive,
            roles,
        };
        parameters.validate()?;
        Ok(parameters)
    }

    /// Validate configuration before allocating handshake or command buffers.
    pub fn validate(self) -> Result<(), HandshakeError> {
        let sizes = [
            self.receive.envelope.max_metadata_bytes,
            self.receive.envelope.max_payload_bytes,
            self.receive.max_records,
            self.receive.max_parts,
            self.receive.max_record_bytes,
        ];
        if sizes.iter().any(|&n| n == 0 || u32::try_from(n).is_err())
            || self.receive.envelope.max_metadata_bytes < 512
            || self.capabilities & !KNOWN_CAPABILITIES != 0
            || (self.capabilities & OWNER_STREAM != 0 && self.capabilities & OWNER_APPEND == 0)
            || (self.capabilities & OWNER_PROGRESS != 0 && self.capabilities & OWNER_READ == 0)
            || (self.capabilities & OWNER_GROUPS != 0 && self.capabilities & OWNER_PROGRESS == 0)
            || self.required_capabilities & !self.capabilities != 0
            || self.roles == 0
            || self.roles & !31 != 0
        {
            return Err(HandshakeError::Parameters);
        }
        Ok(())
    }

    /// Select the supported intersection, checking requirements in both directions.
    /// Each endpoint retains its own directional receive limits.
    pub fn select(self, remote: Self) -> Result<Self, HandshakeError> {
        self.validate()?;
        remote.validate()?;
        let selected = self.capabilities & remote.capabilities;
        if (self.required_capabilities | remote.required_capabilities) & !selected != 0 {
            return Err(HandshakeError::Capabilities);
        }
        Ok(Self {
            capabilities: selected,
            ..self
        })
    }
}

/// HELLO properties or WELCOME properties with the echoed initiator nonce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handshake {
    /// WELCOME-only nonce of a simultaneous HELLO abandoned by this responder.
    /// Lets the initiator ignore that HELLO even if it arrives after WELCOME.
    pub superseded_hello: Option<u128>,
    /// Fresh nonzero process incarnation; not the persistent node identity.
    pub instance_id: u128,
    /// Fresh nonzero HELLO attempt identity, echoed unchanged by WELCOME.
    pub hello_nonce: u128,
    /// Sender's receive bounds and capability advertisement/selection.
    pub parameters: Parameters,
}

/// Encode RFC-style property names/values into reserved storage without growth.
/// Validation failure leaves the previous metadata untouched. Payload is empty.
pub fn encode(
    envelope: Envelope,
    handshake: Handshake,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], HandshakeError> {
    validate_command(envelope, handshake)?;
    let p = handshake.parameters;
    let caps = capabilities(p.capabilities);
    let required = capabilities(p.required_capabilities);
    let properties: [(&[u8], &[u8]); 9] = [
        (b"versions", &[0, 0, 0, 1, super::VERSION]),
        (b"capabilities", caps.as_slice()),
        (b"required-capabilities", required.as_slice()),
        (
            b"max-metadata-bytes",
            &(p.receive.envelope.max_metadata_bytes as u32).to_be_bytes(),
        ),
        (
            b"max-payload-bytes",
            &(p.receive.envelope.max_payload_bytes as u32).to_be_bytes(),
        ),
        (
            b"max-batch-records",
            &(p.receive.max_records as u32).to_be_bytes(),
        ),
        (
            b"max-payload-parts",
            &(p.receive.max_parts as u32).to_be_bytes(),
        ),
        (b"roles", &p.roles.to_be_bytes()),
        (
            b"max-record-bytes",
            &(p.receive.max_record_bytes as u32).to_be_bytes(),
        ),
    ];
    let superseded = handshake.superseded_hello.map(u128::to_be_bytes);
    let properties = properties.into_iter().chain(
        superseded
            .as_ref()
            .map(|nonce| (&b"superseded-hello"[..], &nonce[..])),
    );
    // Identity, then one length-prefixed name and value per property.
    let size = properties
        .clone()
        .try_fold(32_usize, |size, (name, value)| {
            size.checked_add(5)?
                .checked_add(name.len())?
                .checked_add(value.len())
        })
        .ok_or(HandshakeError::Length)?;
    let header = envelope.encode_header(size, 0, limits)?;
    if metadata.capacity() < size {
        return Err(HandshakeError::Capacity);
    }
    metadata.clear();
    metadata.extend_from_slice(&handshake.instance_id.to_be_bytes());
    metadata.extend_from_slice(&handshake.hello_nonce.to_be_bytes());
    for (name, value) in properties {
        write_property(metadata, name, value)?;
    }
    Ok(header)
}

/// One length-prefixed property name and value.
fn write_property(metadata: &mut Vec<u8>, name: &[u8], value: &[u8]) -> Result<(), HandshakeError> {
    let name_bytes = u8::try_from(name.len()).map_err(|_| HandshakeError::Properties)?;
    let value_bytes = u32::try_from(value.len()).map_err(|_| HandshakeError::Properties)?;
    metadata.push(name_bytes);
    metadata.extend_from_slice(name);
    metadata.extend_from_slice(&value_bytes.to_be_bytes());
    metadata.extend_from_slice(value);
    Ok(())
}

/// Decode without allocating. Unknown optional properties/capabilities are ignored.
/// All names, including unknown names, participate in case-insensitive duplicate
/// rejection. Property count is bounded independently of the metadata byte limit.
pub fn decode(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<Handshake, HandshakeError> {
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if !packet.payload.is_empty() {
        return Err(HandshakeError::Command);
    }
    let mut reader = Reader::new(packet.metadata);
    let instance_id = u128::from_be_bytes(reader.array()?);
    let hello_nonce = u128::from_be_bytes(reader.array()?);
    let values = read_properties(reader)?;
    let mut v = [&[][..]; 9];
    for (to, from) in v.iter_mut().zip(values) {
        *to = from.ok_or(HandshakeError::Properties)?;
    }
    check_versions(v[0], packet.envelope.opcode)?;
    let parameters = decode_parameters(&v)?;
    let handshake = Handshake {
        superseded_hello: values[9]
            .map(|v| {
                v.try_into()
                    .map(u128::from_be_bytes)
                    .map_err(|_| HandshakeError::Length)
            })
            .transpose()?,
        instance_id,
        hello_nonce,
        parameters,
    };
    validate_command(packet.envelope, handshake)?;
    Ok(handshake)
}

/// Known property values in [`NAMES`] order. Unknown names count only toward
/// the property bound and duplicate rejection.
fn read_properties(mut reader: Reader<'_>) -> Result<[Option<&[u8]>; NAMES.len()], HandshakeError> {
    let mut names: SmallVec<[&[u8]; MAX_PROPERTIES]> = SmallVec::new();
    let mut values = [None; NAMES.len()];
    while !reader.rest().is_empty() {
        if names.len() == MAX_PROPERTIES {
            return Err(HandshakeError::Properties);
        }
        let length = usize::from(reader.u8()?);
        let name = reader.take(length)?;
        if name.is_empty()
            || !name.is_ascii()
            || names.iter().any(|old| old.eq_ignore_ascii_case(name))
        {
            return Err(HandshakeError::Properties);
        }
        names.push(name);
        let length = reader.u32()? as usize;
        let value = reader.take(length)?;
        if let Some(slot) = NAMES
            .iter()
            .zip(&mut values)
            .find_map(|(known, slot)| known.eq_ignore_ascii_case(name).then_some(slot))
        {
            *slot = Some(value);
        }
    }
    Ok(values)
}

/// A HELLO offers this version among others; a WELCOME selects only it.
fn check_versions(value: &[u8], opcode: Opcode) -> Result<(), HandshakeError> {
    let mut versions = Reader::new(value);
    let count = versions.u32()? as usize;
    let versions = versions.rest();
    if count == 0
        || count > 256
        || count != versions.len()
        || !versions.contains(&super::VERSION)
        || (opcode == Opcode::Welcome && versions != [super::VERSION])
    {
        return Err(HandshakeError::Version);
    }
    Ok(())
}

/// Parameters from the required property values, in [`NAMES`] order.
fn decode_parameters(v: &[&[u8]; 9]) -> Result<Parameters, HandshakeError> {
    Ok(Parameters {
        capabilities: decode_capabilities(v[1], false)?,
        required_capabilities: decode_capabilities(v[2], true)?,
        receive: DataLimits {
            envelope: EnvelopeLimits {
                max_metadata_bytes: number(v[3])? as usize,
                max_payload_bytes: number(v[4])? as usize,
            },
            max_records: number(v[5])? as usize,
            max_parts: number(v[6])? as usize,
            max_record_bytes: number(v[8])? as usize,
        },
        roles: number(v[7])?,
    })
}

fn validate_command(envelope: Envelope, handshake: Handshake) -> Result<(), HandshakeError> {
    if !matches!(envelope.opcode, Opcode::Hello | Opcode::Welcome)
        || envelope.response != (envelope.opcode == Opcode::Welcome)
        || handshake.instance_id == 0
        || handshake.hello_nonce == 0
        || handshake.superseded_hello == Some(0)
        || (envelope.opcode == Opcode::Hello && handshake.superseded_hello.is_some())
    {
        return Err(HandshakeError::Command);
    }
    handshake.parameters.validate()
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "capability ids lie in 1..=14"
)]
fn capabilities(bits: u16) -> SmallVec<[u8; 32]> {
    let mut bytes = SmallVec::new();
    bytes.extend_from_slice(&bits.count_ones().to_be_bytes());
    for id in (1..=14_u16).filter(|id| bits & (1 << (id - 1)) != 0) {
        bytes.extend_from_slice(&id.to_be_bytes());
    }
    bytes
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the count is capped at 64 first, and set ids lie in 1..=14"
)]
fn decode_capabilities(bytes: &[u8], required: bool) -> Result<u16, HandshakeError> {
    let mut reader = Reader::new(bytes);
    let count = reader.u32()? as usize;
    let bytes = reader.rest();
    if count > 64 || bytes.len() != count * 2 {
        return Err(HandshakeError::Capabilities);
    }
    let mut bits = 0;
    let mut previous = 0;
    for chunk in bytes.as_chunks::<2>().0 {
        let id = u16::from_be_bytes(*chunk);
        if id <= previous || (required && id > 14) {
            return Err(HandshakeError::Capabilities);
        }
        previous = id;
        if id <= 14 {
            bits |= 1 << (id - 1);
        }
    }
    Ok(bits)
}

fn number(bytes: &[u8]) -> Result<u32, HandshakeError> {
    Ok(u32::from_be_bytes(
        bytes.try_into().map_err(|_| HandshakeError::Length)?,
    ))
}

/// No handshake rejection is authority to admit application work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HandshakeError {
    /// Invalid outer command/session shape or frame bound.
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
    /// Wrong command direction or zero instance/nonce.
    #[error("invalid handshake command or identity")]
    Command,
    /// Truncated field or inconsistent counted value.
    #[error("invalid handshake field length")]
    Length,
    /// Missing, duplicate, invalid, or too many properties.
    #[error("invalid handshake properties")]
    Properties,
    /// Unsupported version or invalid WELCOME selection.
    #[error("no supported protocol version")]
    Version,
    /// Required capability unavailable or malformed capability set.
    #[error("unsupported handshake capabilities")]
    Capabilities,
    /// Invalid advertised bounds, roles, or local capability configuration.
    #[error("invalid handshake parameters")]
    Parameters,
    /// Caller did not reserve enough metadata space.
    #[error("handshake encode buffer capacity exhausted")]
    Capacity,
}

impl From<LayoutError> for HandshakeError {
    fn from(_: LayoutError) -> Self {
        Self::Length
    }
}
