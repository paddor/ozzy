//! Correlated, generation-bound canonical history transfer. No implicit voting.

use ozzy_proto::{Envelope, EnvelopeLimits, Opcode};
use ozzy_proto::{LinkSessionId, NodeId, RequestId};

use super::prepare::{DecodedBatch, decode_batch, metadata_length, validate_batch, write_batch};
use super::{
    COMMON_BYTES, ControlEncoding, Operation, Operations, PeerBinding, PrepareEncoding, Reader,
    WireError, WireLimits, Writer, validate_prefix, validate_scope,
};
use crate::{JournalGeneration, LogSource, Prefix, Scope};

const SOURCE_BYTES: usize = 16 + 16 + 40;
const FETCH_BYTES: usize = COMMON_BYTES + SOURCE_BYTES + 40 + 4 + 4;
const OPS_FIXED_BYTES: usize = COMMON_BYTES + SOURCE_BYTES + 40 + 4;

/// One bounded request into a previously advertised immutable history.
///
/// The serving adapter must check that it owns and still pins this exact source.
/// A request does not create a pin, prove history, or authorize role changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchOps {
    /// Active transfer view and complete group configuration.
    pub scope: Scope,
    /// Unique outstanding exchange in the established link session.
    pub request_id: RequestId,
    /// Exact configured voter, frozen writer incarnation, and accepted tail.
    pub source: LogSource,
    /// Last verified prefix before the requested consecutive operations.
    pub predecessor: Prefix,
    /// Upper bound on complete operations in the response, never zero.
    pub max_operations: u32,
    /// Upper bound on canonical body bytes in the response, never zero.
    pub max_body_bytes: u32,
}

impl FetchOps {
    fn validate(self) -> Result<(), WireError> {
        validate_scope(self.scope)?;
        validate_source(self.source)?;
        validate_prefix(self.predecessor)?;
        if self.predecessor.op >= self.source.accepted.op {
            return Err(WireError::History);
        }
        if self.max_operations == 0 || self.max_body_bytes == 0 {
            return Err(WireError::Limit);
        }
        Ok(())
    }
}

/// Borrowed, hash-validated response from one frozen source generation.
///
/// Intermediate chunks prove only their own chain from the predecessor. The
/// full selected lineage is verified only when that chain reaches its tail hash.
/// No transfer response supplies normal voting or application commit authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpsBatch<'a> {
    request_id: RequestId,
    source: LogSource,
    batch: DecodedBatch<'a>,
}

impl<'a> OpsBatch<'a> {
    /// Active transfer view and immutable group configuration.
    pub const fn scope(self) -> Scope {
        self.batch.scope
    }
    /// Correlation echoed from `FETCH_OPS`.
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }
    /// Exact frozen source named by the response, still requiring request matching.
    pub const fn source(self) -> LogSource {
        self.source
    }
    /// Exact prefix immediately before the returned operations.
    pub const fn predecessor(self) -> Prefix {
        self.batch.predecessor
    }
    /// End of this nonempty chunk, not necessarily the selected source's tail.
    pub const fn end(self) -> Prefix {
        self.batch.accepted
    }
    /// Iterate immutable canonical bodies without allocating or rehashing.
    pub fn operations(self) -> Operations<'a> {
        self.batch.operations()
    }

    /// Match the live outstanding request after authenticated session decoding.
    ///
    /// The adapter invalidates pending requests when their session or role ends.
    /// Success here does not advance the installation cursor or imply disk sync.
    pub fn validate_response(self, request: FetchOps) -> Result<(), WireError> {
        request.validate()?;
        if self.request_id != request.request_id
            || self.scope() != request.scope
            || self.source != request.source
            || self.predecessor() != request.predecessor
            || self.operations().len() > request.max_operations as usize
            || self.batch.payload.len() > request.max_body_bytes as usize
        {
            return Err(WireError::Transfer);
        }
        Ok(())
    }
}

/// Encode complete canonical operations from the exact pinned request source.
///
/// Reuses verified body digests and caller buffers. Honors both request bounds
/// and local wire limits; errors leave metadata and payload output unchanged.
/// The serving adapter independently verifies the source pin and role lifetime.
pub fn encode_ops(
    sender: NodeId,
    session: LinkSessionId,
    request: FetchOps,
    operations: &[Operation<'_>],
    metadata: &mut [u8],
    payload: &mut [u8],
    limits: WireLimits,
) -> Result<PrepareEncoding, WireError> {
    request.validate()?;
    if sender != request.source.voter {
        return Err(WireError::Peer);
    }
    let limits = WireLimits {
        max_operations: limits.max_operations.min(request.max_operations as usize),
        envelope: EnvelopeLimits {
            max_metadata_bytes: limits.envelope.max_metadata_bytes,
            max_payload_bytes: limits
                .envelope
                .max_payload_bytes
                .min(request.max_body_bytes as usize),
        },
    };
    let metadata_bytes = metadata_length(operations.len(), OPS_FIXED_BYTES, limits)?;
    let payload_bytes =
        validate_batch(request.scope, request.predecessor, None, operations, limits)?;
    validate_tail(
        request.source,
        operations.last().expect("nonempty validated").prefix(),
    )?;
    let header = Envelope {
        opcode: Opcode::Ops,
        response: true,
        request_id: Some(request.request_id),
        sender,
        session: Some(session),
    }
    .encode_header(metadata_bytes, payload_bytes, limits.envelope)?;
    if metadata.len() < metadata_bytes || payload.len() < payload_bytes {
        return Err(WireError::Capacity);
    }
    let mut writer = Writer::new(&mut metadata[..metadata_bytes]);
    writer.scope(request.scope, sender);
    write_source(&mut writer, request.source);
    writer.prefix(request.predecessor);
    write_batch(&mut writer, Some(payload), operations);
    Ok(PrepareEncoding {
        header,
        metadata_bytes,
        payload_bytes,
    })
}

pub(super) fn decode_ops<'a>(
    scope: Scope,
    envelope: Envelope,
    mut reader: Reader<'a>,
    payload: &'a [u8],
    limits: WireLimits,
) -> Result<OpsBatch<'a>, WireError> {
    let request_id = envelope.request_id.ok_or(WireError::Correlation)?;
    if !envelope.response {
        return Err(WireError::Correlation);
    }
    let source = read_source(&mut reader)?;
    if source.voter != envelope.sender {
        return Err(WireError::Peer);
    }
    let predecessor = reader.prefix()?;
    let batch = decode_batch(scope, predecessor, None, reader, payload, limits)?;
    validate_tail(source, batch.accepted)?;
    Ok(OpsBatch {
        request_id,
        source,
        batch,
    })
}

pub(super) fn validate_routing(
    request: FetchOps,
    envelope: Envelope,
    mut reader: Reader<'_>,
    payload_bytes: usize,
) -> Result<(), WireError> {
    request.validate()?;
    if !envelope.response || envelope.request_id != Some(request.request_id) {
        return Err(WireError::Correlation);
    }
    if envelope.sender != request.source.voter
        || read_source(&mut reader)? != request.source
        || reader.prefix()? != request.predecessor
    {
        return Err(WireError::Transfer);
    }
    let operations = reader.u32()?;
    if operations == 0
        || operations > request.max_operations
        || payload_bytes > request.max_body_bytes as usize
    {
        return Err(WireError::Limit);
    }
    Ok(())
}

fn validate_tail(source: LogSource, end: Prefix) -> Result<(), WireError> {
    if end.op > source.accepted.op || (end.op == source.accepted.op && end != source.accepted) {
        return Err(WireError::History);
    }
    Ok(())
}

/// Encode a correlated request without allocating or modifying output on error.
///
/// Response limits are upper bounds, not allocation instructions. The source
/// also bounds each read/response by its own storage, scheduling, and wire limits.
pub fn encode_fetch(
    sender: NodeId,
    session: LinkSessionId,
    request: FetchOps,
    metadata: &mut [u8],
    limits: WireLimits,
) -> Result<ControlEncoding, WireError> {
    request.validate()?;
    let header = Envelope {
        opcode: Opcode::FetchOps,
        response: false,
        request_id: Some(request.request_id),
        sender,
        session: Some(session),
    }
    .encode_header(FETCH_BYTES, 0, limits.envelope)?;
    if metadata.len() < FETCH_BYTES {
        return Err(WireError::Capacity);
    }
    let mut writer = Writer::new(&mut metadata[..FETCH_BYTES]);
    writer.scope(request.scope, sender);
    write_source(&mut writer, request.source);
    writer.prefix(request.predecessor);
    writer.bytes(&request.max_operations.to_be_bytes());
    writer.bytes(&request.max_body_bytes.to_be_bytes());
    Ok(ControlEncoding {
        header,
        metadata_bytes: FETCH_BYTES,
    })
}

pub(super) fn decode_fetch(
    scope: Scope,
    envelope: Envelope,
    mut reader: Reader<'_>,
    payload: &[u8],
    binding: PeerBinding,
) -> Result<FetchOps, WireError> {
    let request_id = envelope.request_id.ok_or(WireError::Correlation)?;
    if envelope.response {
        return Err(WireError::Correlation);
    }
    if !payload.is_empty() {
        return Err(WireError::Payload);
    }
    let source = read_source(&mut reader)?;
    binding.configuration.voter_index(source.voter)?;
    let request = FetchOps {
        scope,
        request_id,
        source,
        predecessor: reader.prefix()?,
        max_operations: reader.u32()?,
        max_body_bytes: reader.u32()?,
    };
    reader.finish()?;
    request.validate()?;
    Ok(request)
}

fn validate_source(source: LogSource) -> Result<(), WireError> {
    validate_prefix(source.accepted)?;
    if source.generation.0 == 0 || source.voter.as_bytes() == &[0; 16] {
        return Err(WireError::History);
    }
    Ok(())
}

fn write_source(writer: &mut Writer<'_>, source: LogSource) {
    writer.bytes(source.voter.as_bytes());
    writer.bytes(&source.generation.0.to_be_bytes());
    writer.prefix(source.accepted);
}

fn read_source(reader: &mut Reader<'_>) -> Result<LogSource, WireError> {
    let source = LogSource {
        voter: NodeId::from_bytes(reader.bytes()?),
        generation: JournalGeneration(u128::from_be_bytes(reader.bytes()?)),
        accepted: reader.prefix()?,
    };
    validate_source(source)?;
    Ok(source)
}
