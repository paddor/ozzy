//! Bounded native HELLO/WELCOME properties. Negotiation is not authentication.

use super::append::DataLimits;
use super::{ENVELOPE_BYTES, Envelope, EnvelopeError, EnvelopeLimits, Opcode, Packet};

/// Owner-append capability, including exact retry reconciliation.
pub const OWNER_APPEND: u16 = 1;
/// Typed authority hints for fixed-group owner discovery and unchanged retries.
pub const OWNER_ROUTING: u16 = 1 << 9;
/// Single-partition reader delivery and receipt/processing observations.
pub const OWNER_READ: u16 = 1 << 10;
/// Pipelined owner append with record-based retries and per-writer range replies.
pub const OWNER_STREAM: u16 = 1 << 12;
/// Reader role, not independent authorization.
pub const CONSUMER: u32 = 2;
/// Producer role. A role claim does not authorize a producer identity.
pub const PRODUCER: u32 = 1;
/// Log-owner role. Serving authority still comes from the active group.
pub const OWNER: u32 = 4;
// Capabilities 12 and 14 are unassigned. Required use must fail, never imply delivery.
/// Default reader credit window, in full messages. More than one lets an owner
/// send the next message while the reader handles the previous one.
pub const READER_WINDOW_MESSAGES: usize = 4;
/// Upper bound on any advertised record window.
pub const MAX_READER_WINDOW_RECORDS: u64 = 65_536;
const KNOWN_CAPABILITIES: u16 = 0x17ff;
const MAX_PROPERTIES: usize = 32;
const NAMES: [&[u8]; 12] = [
    b"versions",
    b"capabilities",
    b"required-capabilities",
    b"max-metadata-bytes",
    b"max-payload-bytes",
    b"max-batch-records",
    b"max-payload-parts",
    b"max-inflight-records",
    b"max-inflight-bytes",
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
    /// Maximum retained records across outstanding operations on this link.
    pub inflight_records: u64,
    /// Maximum retained payload bytes across outstanding operations on this link.
    pub inflight_bytes: u64,
    /// Producer/consumer/owner/replica/directory occupy bits 0..=4.
    pub roles: u32,
}

impl Parameters {
    /// Pipelined append profile with independent bounded in-flight windows.
    /// Both append and streaming support are mandatory; no legacy downgrade.
    pub fn streaming(
        receive: DataLimits,
        roles: u32,
        inflight_records: u64,
        inflight_bytes: u64,
    ) -> Result<Self, HandshakeError> {
        let parameters = Self {
            capabilities: OWNER_APPEND | OWNER_STREAM,
            required_capabilities: OWNER_APPEND | OWNER_STREAM,
            receive,
            inflight_records,
            inflight_bytes,
            roles,
        };
        parameters.validate()?;
        Ok(parameters)
    }
    /// Reader profile with a credit window of `READER_WINDOW_MESSAGES` full
    /// messages. Does not advertise durable progress or group membership.
    pub fn reader(receive: DataLimits, roles: u32) -> Result<Self, HandshakeError> {
        Self::reader_window(receive, roles, READER_WINDOW_MESSAGES)
    }

    /// Reader profile whose credit window holds `messages` full messages, so
    /// an owner can send the next message while the reader handles one.
    /// Records are capped at 65,536 and never below one message.
    pub fn reader_window(
        receive: DataLimits,
        roles: u32,
        messages: usize,
    ) -> Result<Self, HandshakeError> {
        if messages == 0 {
            return Err(HandshakeError::Parameters);
        }
        let mut parameters = Self::append(receive, roles)?;
        parameters.capabilities = OWNER_READ;
        parameters.required_capabilities = OWNER_READ;
        parameters.inflight_records = (receive.max_records.saturating_mul(messages) as u64)
            .min(MAX_READER_WINDOW_RECORDS)
            .max(receive.max_records as u64);
        parameters.inflight_bytes =
            receive.envelope.max_payload_bytes.saturating_mul(messages) as u64;
        parameters.validate()?;
        Ok(parameters)
    }
    /// Single-request append profile; other command families remain unadvertised.
    pub fn append(receive: DataLimits, roles: u32) -> Result<Self, HandshakeError> {
        let parameters = Self {
            capabilities: OWNER_APPEND,
            required_capabilities: OWNER_APPEND,
            inflight_records: receive.max_records as u64,
            inflight_bytes: receive.envelope.max_payload_bytes as u64,
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
            || self.inflight_records < self.receive.max_records as u64
            || self.inflight_bytes < self.receive.envelope.max_payload_bytes as u64
            || self.capabilities & !KNOWN_CAPABILITIES != 0
            || (self.capabilities & OWNER_STREAM != 0 && self.capabilities & OWNER_APPEND == 0)
            || self.required_capabilities & !self.capabilities != 0
            || self.roles == 0
            || self.roles & !31 != 0
        {
            return Err(HandshakeError::Parameters);
        }
        Ok(())
    }

    /// Select the supported intersection, checking requirements in both directions.
    /// Each endpoint retains its own directional receive and in-flight limits.
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
    let (caps, caps_len) = capabilities(p.capabilities);
    let (required, required_len) = capabilities(p.required_capabilities);
    let properties: [(&[u8], &[u8]); 11] = [
        (b"versions", &[0, 0, 0, 1, super::VERSION]),
        (b"capabilities", &caps[..caps_len]),
        (b"required-capabilities", &required[..required_len]),
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
        (b"max-inflight-records", &p.inflight_records.to_be_bytes()),
        (b"max-inflight-bytes", &p.inflight_bytes.to_be_bytes()),
        (b"roles", &p.roles.to_be_bytes()),
        (
            b"max-record-bytes",
            &(p.receive.max_record_bytes as u32).to_be_bytes(),
        ),
    ];
    let size = 32
        + properties
            .iter()
            .map(|(name, value)| 5 + name.len() + value.len())
            .sum::<usize>()
        + usize::from(handshake.superseded_hello.is_some()) * 37;
    let header = envelope.encode_header(size, 0, limits)?;
    if metadata.capacity() < size {
        return Err(HandshakeError::Capacity);
    }
    metadata.clear();
    metadata.extend_from_slice(&handshake.instance_id.to_be_bytes());
    metadata.extend_from_slice(&handshake.hello_nonce.to_be_bytes());
    for (name, value) in properties {
        metadata.push(u8::try_from(name.len()).expect("fixed ASCII property name"));
        metadata.extend_from_slice(name);
        metadata.extend_from_slice(
            &u32::try_from(value.len())
                .expect("fixed property size")
                .to_be_bytes(),
        );
        metadata.extend_from_slice(value);
    }
    if let Some(nonce) = handshake.superseded_hello {
        metadata.push(16);
        metadata.extend_from_slice(b"superseded-hello");
        metadata.extend_from_slice(&16_u32.to_be_bytes());
        metadata.extend_from_slice(&nonce.to_be_bytes());
    }
    Ok(header)
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
    let mut bytes = packet.metadata;
    let instance_id = u128::from_be_bytes(take(&mut bytes, 16)?.try_into().expect("field size"));
    let hello_nonce = u128::from_be_bytes(take(&mut bytes, 16)?.try_into().expect("field size"));
    let mut names: [&[u8]; MAX_PROPERTIES] = [&[]; MAX_PROPERTIES];
    let mut seen = 0;
    let mut values: [Option<&[u8]>; 12] = [None; 12];
    while !bytes.is_empty() {
        if seen == MAX_PROPERTIES {
            return Err(HandshakeError::Properties);
        }
        let length = usize::from(take(&mut bytes, 1)?[0]);
        let name = take(&mut bytes, length)?;
        if name.is_empty()
            || !name.is_ascii()
            || names[..seen]
                .iter()
                .any(|old| old.eq_ignore_ascii_case(name))
        {
            return Err(HandshakeError::Properties);
        }
        names[seen] = name;
        seen += 1;
        let length = number(take(&mut bytes, 4)?)? as usize;
        let value = take(&mut bytes, length)?;
        if let Some(index) = NAMES
            .iter()
            .position(|known| known.eq_ignore_ascii_case(name))
        {
            values[index] = Some(value);
        }
    }
    let mut v = [&[][..]; 11];
    for (to, from) in v.iter_mut().zip(values) {
        *to = from.ok_or(HandshakeError::Properties)?;
    }
    let mut versions = v[0];
    let count = number(take(&mut versions, 4)?)? as usize;
    if count == 0
        || count > 256
        || count != versions.len()
        || !versions.contains(&super::VERSION)
        || (packet.envelope.opcode == Opcode::Welcome && versions != [super::VERSION])
    {
        return Err(HandshakeError::Version);
    }
    let parameters = Parameters {
        capabilities: decode_capabilities(v[1], false)?,
        required_capabilities: decode_capabilities(v[2], true)?,
        receive: DataLimits {
            envelope: EnvelopeLimits {
                max_metadata_bytes: number(v[3])? as usize,
                max_payload_bytes: number(v[4])? as usize,
            },
            max_records: number(v[5])? as usize,
            max_parts: number(v[6])? as usize,
            max_record_bytes: number(v[10])? as usize,
        },
        inflight_records: u64::from_be_bytes(v[7].try_into().map_err(|_| HandshakeError::Length)?),
        inflight_bytes: u64::from_be_bytes(v[8].try_into().map_err(|_| HandshakeError::Length)?),
        roles: number(v[9])?,
    };
    let handshake = Handshake {
        superseded_hello: values[11]
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

fn capabilities(bits: u16) -> ([u8; 32], usize) {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&bits.count_ones().to_be_bytes());
    let mut size = 4;
    for id in 1..=14_u16 {
        if bits & (1 << (id - 1)) != 0 {
            bytes[size..size + 2].copy_from_slice(&id.to_be_bytes());
            size += 2;
        }
    }
    (bytes, size)
}

fn decode_capabilities(mut bytes: &[u8], required: bool) -> Result<u16, HandshakeError> {
    let count = number(take(&mut bytes, 4)?)? as usize;
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

fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Result<&'a [u8], HandshakeError> {
    let (value, rest) = bytes
        .split_at_checked(count)
        .ok_or(HandshakeError::Length)?;
    *bytes = rest;
    Ok(value)
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
