//! Bounded Ozzy framing shared by every command family.

use crate::{LinkSessionId, NodeId, RequestId};

/// Current wire version. This pre-release protocol has no compatibility decoder.
pub const VERSION: u8 = 1;

/// Exact size of the native envelope, excluding OMQ routing metadata.
pub const ENVELOPE_BYTES: usize = 64;

macro_rules! opcodes {
    ($($variant:ident = $value:literal => $name:literal),+ $(,)?) => {
        /// Command allocations in `doc/PROTOCOL.md`, not capability promises.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[repr(u8)]
        pub enum Opcode {
            $(#[doc = $name] $variant = $value,)+
        }

        impl Opcode {
            fn decode(value: u8) -> Result<Self, EnvelopeError> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(EnvelopeError::Opcode(value)),
                }
            }
        }
    };
}

opcodes! {
    Hello = 0x01 => "`HELLO`: initiate link negotiation.",
    Welcome = 0x02 => "`WELCOME`: establish the negotiated link session.",
    Error = 0x03 => "`ERROR`: bounded protocol error.",
    Append = 0x10 => "`APPEND`: submit producer records.",
    Appended = 0x11 => "`APPENDED`: confirm the configured append boundary.",
    LookupAppend = 0x12 => "`LOOKUP_APPEND`: resolve an uncertain append.",
    AppendState = 0x13 => "`APPEND_STATE`: report an append outcome.",
    OpenProducer = 0x14 => "`OPEN_PRODUCER`: open or fence a producer session.",
    ProducerOpened = 0x15 => "`PRODUCER_OPENED`: confirm producer authority.",
    AckResults = 0x16 => "`ACK_RESULTS`: advance producer retry-result floors.",
    ResultsAcked = 0x17 => "`RESULTS_ACKED`: confirm result-floor commit.",
    Subscribe = 0x20 => "`SUBSCRIBE`: request record delivery.",
    Subscribed = 0x21 => "`SUBSCRIBED`: describe assigned delivery ranges.",
    Records = 0x22 => "`RECORDS`: deliver committed records.",
    Ack = 0x23 => "`ACK`: report consumer receipt or processing observations.",
    ProgressCommit = 0x24 => "`PROGRESS_COMMIT`: request durable consumer progress.",
    ProgressCommitted = 0x25 => "`PROGRESS_COMMITTED`: confirm committed progress.",
    Unsubscribe = 0x27 => "`UNSUBSCRIBE`: cancel a subscription generation.",
    Unsubscribed = 0x28 => "`UNSUBSCRIBED`: confirm subscription cancellation.",
    ReplicaOpen = 0x30 => "`REPLICA_OPEN`: open a configured replica group.",
    ReplicaState = 0x31 => "`REPLICA_STATE`: report replica state and volatile receipt.",
    Prepare = 0x32 => "`PREPARE`: replicate canonical operations.",
    PrepareOk = 0x33 => "`PREPARE_OK`: report contiguous replication evidence.",
    Commit = 0x34 => "`COMMIT`: announce the quorum-committed prefix.",
    StartViewChange = 0x35 => "`START_VIEW_CHANGE`: initiate a higher view.",
    DoViewChange = 0x36 => "`DO_VIEW_CHANGE`: report immutable selected history.",
    StartView = 0x37 => "`START_VIEW`: announce installed view and history.",
    Recovery = 0x38 => "`RECOVERY`: request nonvoting recovery evidence.",
    RecoveryState = 0x39 => "`RECOVERY_STATE`: reply to a fresh recovery nonce.",
    FetchOps = 0x3a => "`FETCH_OPS`: request bounded canonical history.",
    Ops = 0x3b => "`OPS`: transfer canonical operation bodies.",
    SnapshotBegin = 0x3c => "`SNAPSHOT_BEGIN`: describe a checkpoint transfer.",
    SnapshotChunk = 0x3d => "`SNAPSHOT_CHUNK`: transfer a bounded checkpoint chunk.",
    SnapshotEnd = 0x3e => "`SNAPSHOT_END`: finish a checkpoint transfer.",
    SnapshotInstalled = 0x3f => "`SNAPSHOT_INSTALLED`: confirm checkpoint installation.",
    StateSnapshotRequest = 0x40 => "`STATE_SNAPSHOT_REQUEST`: request directory state.",
    StateSnapshot = 0x41 => "`STATE_SNAPSHOT`: transfer directory state.",
    StateUpdate = 0x42 => "`STATE_UPDATE`: advance directory state.",
    StateResync = 0x43 => "`STATE_RESYNC`: request directory resynchronization.",
    ExitView = 0x50 => "`EXIT_VIEW`: request quorum-supported departure from the current view.",
    PrepareFlow = 0x51 => "`PREPARE_FLOW`: replicate canonical operations in a fenced receive epoch.",
    PreparePub = 0x52 => "`PREPARE_PUB`: publish canonical operations to followers.",
    RecordsPub = 0x53 => "`RECORDS_PUB`: publish confirmed records to every live reader.",
    ReplicaReceipt = 0x54 => "`REPLICA_RECEIPT`: compact same-channel volatile receipt.",
    HistoryRetired = 0x55 => "`HISTORY_RETIRED`: require nonvoting checkpoint recovery below retained history.",
    Nack = 0x7f => "`NACK`: report a typed negative outcome.",
}

/// Directional receive bounds checked before command decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopeLimits {
    /// Maximum bytes in the metadata frame.
    pub max_metadata_bytes: usize,
    /// Maximum bytes in the packed payload frame.
    pub max_payload_bytes: usize,
}

impl Default for EnvelopeLimits {
    fn default() -> Self {
        Self {
            max_metadata_bytes: 64 * 1024,
            max_payload_bytes: 1024 * 1024,
        }
    }
}

/// Routing-independent command identity and link fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Envelope {
    /// Allocated command number, still requiring capability and schema checks.
    pub opcode: Opcode,
    /// Whether this command responds to the named nonzero request.
    pub response: bool,
    /// Correlation identity; absent only for uncorrelated notifications.
    pub request_id: Option<RequestId>,
    /// Claimed sender, which the authenticated link must independently verify.
    pub sender: NodeId,
    /// Negotiated session; absent for HELLO and the receiver-fenced publications
    /// `PREPARE_PUB` and `RECORDS_PUB`.
    pub session: Option<LinkSessionId>,
}

impl Envelope {
    /// Validate common identities and exact frame sizes without encoding a
    /// throwaway header. Command-specific metadata and payload remain unchecked.
    pub fn validate_frames(
        self,
        metadata_bytes: usize,
        payload_bytes: usize,
        limits: EnvelopeLimits,
    ) -> Result<(), EnvelopeError> {
        validate_lengths(metadata_bytes, payload_bytes, limits)?;
        self.validate(payload_bytes)?;
        u32::try_from(metadata_bytes).map_err(|_| EnvelopeError::Length)?;
        u32::try_from(payload_bytes).map_err(|_| EnvelopeError::Length)?;
        Ok(())
    }

    /// Encode into a fixed stack-sized header; no frame or payload allocation.
    ///
    /// Lengths must describe the exact frames submitted with this header. This
    /// does not encode or validate any command-specific metadata or payload.
    pub fn encode_header(
        self,
        metadata_bytes: usize,
        payload_bytes: usize,
        limits: EnvelopeLimits,
    ) -> Result<[u8; ENVELOPE_BYTES], EnvelopeError> {
        self.validate_frames(metadata_bytes, payload_bytes, limits)?;
        let mut bytes = [0; ENVELOPE_BYTES];
        bytes[..4].copy_from_slice(b"OZY\0");
        bytes[4] = VERSION;
        bytes[5] = self.opcode as u8;
        bytes[6..8].copy_from_slice(&u16::from(self.response).to_be_bytes());
        if let Some(request) = self.request_id {
            bytes[8..24].copy_from_slice(request.as_bytes());
        }
        bytes[24..40].copy_from_slice(self.sender.as_bytes());
        if let Some(session) = self.session {
            bytes[40..56].copy_from_slice(session.as_bytes());
        }
        bytes[56..60].copy_from_slice(&(metadata_bytes as u32).to_be_bytes());
        bytes[60..64].copy_from_slice(&(payload_bytes as u32).to_be_bytes());
        Ok(bytes)
    }

    fn validate(self, payload_bytes: usize) -> Result<(), EnvelopeError> {
        if self.sender.as_bytes() == &[0; 16]
            || self.request_id.is_some_and(|id| id.as_bytes() == &[0; 16])
            || self.session.is_some_and(|id| id.as_bytes() == &[0; 16])
        {
            return Err(EnvelopeError::ZeroIdentity);
        }
        if self.response && self.request_id.is_none() {
            return Err(EnvelopeError::Correlation);
        }
        if self.opcode == Opcode::Hello {
            if self.session.is_some()
                || self.response
                || self.request_id.is_none()
                || payload_bytes != 0
            {
                return Err(EnvelopeError::Handshake);
            }
        } else if matches!(self.opcode, Opcode::PreparePub | Opcode::RecordsPub) {
            if self.session.is_some() || self.response || self.request_id.is_some() {
                return Err(EnvelopeError::Session);
            }
        } else if self.session.is_none() {
            return Err(EnvelopeError::Session);
        } else if self.opcode == Opcode::Welcome && (!self.response || self.request_id.is_none()) {
            return Err(EnvelopeError::Handshake);
        }
        Ok(())
    }
}

/// Structurally valid frames borrowing the caller's packet storage.
///
/// Metadata and payload remain untrusted command bytes. Session, capability,
/// schema, and authority validation must precede any application or quorum effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Packet<'a> {
    /// Decoded command and link identity.
    pub envelope: Envelope,
    /// Exact bounded metadata frame.
    pub metadata: &'a [u8],
    /// Exact bounded packed payload frame.
    pub payload: &'a [u8],
}

/// Decode exactly three Ozzy frames, after the transport removes routing frames.
///
/// Reject unsupported versions and old frame shapes. All header reads are
/// allocation-free; no length from the peer controls an allocation here.
pub fn decode_packet<'a>(
    frames: &[&'a [u8]],
    limits: EnvelopeLimits,
) -> Result<Packet<'a>, EnvelopeError> {
    let [header, metadata, payload] = frames else {
        return Err(EnvelopeError::FrameCount);
    };
    if header.len() != ENVELOPE_BYTES {
        return Err(EnvelopeError::HeaderLength);
    }
    validate_lengths(metadata.len(), payload.len(), limits)?;
    if &header[..4] != b"OZY\0" {
        return Err(EnvelopeError::Magic);
    }
    if header[4] != VERSION {
        return Err(EnvelopeError::Major(header[4]));
    }
    let opcode = Opcode::decode(header[5])?;
    let flags = u16::from_be_bytes(header[6..8].try_into().expect("fixed field"));
    if flags & !1 != 0 {
        return Err(EnvelopeError::Flags);
    }
    let metadata_bytes = u32::from_be_bytes(header[56..60].try_into().expect("fixed field"));
    let payload_bytes = u32::from_be_bytes(header[60..64].try_into().expect("fixed field"));
    if usize::try_from(metadata_bytes) != Ok(metadata.len())
        || usize::try_from(payload_bytes) != Ok(payload.len())
    {
        return Err(EnvelopeError::Length);
    }
    let request: [u8; 16] = header[8..24].try_into().expect("fixed field");
    let session: [u8; 16] = header[40..56].try_into().expect("fixed field");
    let envelope = Envelope {
        opcode,
        response: flags == 1,
        request_id: (request != [0; 16]).then(|| RequestId::from_bytes(request)),
        sender: NodeId::from_bytes(header[24..40].try_into().expect("fixed field")),
        session: (session != [0; 16]).then(|| LinkSessionId::from_bytes(session)),
    };
    envelope.validate(payload.len())?;
    Ok(Packet {
        envelope,
        metadata,
        payload,
    })
}

fn validate_lengths(
    metadata: usize,
    payload: usize,
    limits: EnvelopeLimits,
) -> Result<(), EnvelopeError> {
    if metadata > limits.max_metadata_bytes || payload > limits.max_payload_bytes {
        return Err(EnvelopeError::Limit);
    }
    Ok(())
}

/// Structural native envelope rejection, without interpreting command semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    /// Exactly three frames are required, including present empty frames.
    #[error("native packet requires exactly three Ozzy frames")]
    FrameCount,
    /// Header is not exactly 64 bytes.
    #[error("invalid native envelope length")]
    HeaderLength,
    /// Magic does not identify Ozzy.
    #[error("invalid Ozzy magic")]
    Magic,
    /// No implicit legacy fallback or future-major interpretation.
    #[error("unsupported Ozzy major {0}")]
    Major(u8),
    /// Unallocated command number.
    #[error("unknown Ozzy opcode {0}")]
    Opcode(u8),
    /// Flags other than the response bit are reserved.
    #[error("unsupported Ozzy envelope flags")]
    Flags,
    /// Claimed sizes differ from actual frames or exceed the wire integer width.
    #[error("Ozzy frame length mismatch or overflow")]
    Length,
    /// Frame exceeds the directional receiver's configured bound.
    #[error("Ozzy frame exceeds receive limit")]
    Limit,
    /// Mandatory sender or explicitly supplied optional ID is zero.
    #[error("zero Ozzy envelope identity")]
    ZeroIdentity,
    /// A response cannot be uncorrelated.
    #[error("response requires a nonzero request ID")]
    Correlation,
    /// Only HELLO can omit the link session.
    #[error("command requires a negotiated link session")]
    Session,
    /// Invalid HELLO/WELCOME direction, correlation, session, or payload.
    #[error("invalid handshake envelope")]
    Handshake,
}
