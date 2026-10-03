//! Bounded canonical operation batches, borrowing metadata and payload frames.

use std::slice::Iter;

use ozzy_journal::operation::{
    CanonicalOperation, OperationKind, canonical_body_digest,
    logical_operation_digest_with_body_digest,
};
use ozzy_proto::{Envelope, Opcode};
use ozzy_proto::{LinkSessionId, NodeId};

use super::{COMMON_BYTES, Reader, WireError, WireLimits, Writer, validate_prefix, validate_scope};
use crate::flow::ReceiveEpoch;
use crate::{Digest, OpNumber, Prefix, Scope};

const DESCRIPTOR_BYTES: usize = 86;
const BATCH_BYTES: usize = COMMON_BYTES + 8 + 32 + 40 + 4;

/// Immutable canonical bytes and their already computed body/envelope digests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Operation<'a> {
    canonical: CanonicalOperation<'a>,
    body_digest: Digest,
    digest: Digest,
}

impl<'a> Operation<'a> {
    /// Reuse an independently verified body digest, hashing only its envelope.
    ///
    /// The caller asserts body bytes/digest and canonical application transition
    /// have been validated. This constructor deliberately does not rehash payload.
    pub fn from_verified(canonical: CanonicalOperation<'a>, body_digest: Digest) -> Self {
        Self {
            digest: logical_operation_digest_with_body_digest(&canonical, body_digest),
            canonical,
            body_digest,
        }
    }
    /// Borrow the exact canonical body and envelope fields.
    pub const fn canonical(self) -> CanonicalOperation<'a> {
        self.canonical
    }
    /// Verified body digest, reusable by journal encoding after application validation.
    pub const fn body_digest(self) -> Digest {
        self.body_digest
    }
    /// Complete chain position at this operation.
    pub const fn prefix(self) -> Prefix {
        Prefix {
            op: OpNumber(self.canonical.op_number),
            digest: self.digest,
        }
    }
}

/// Decoder-verified body and envelope, borrowing the exact immutable packet bytes.
///
/// Only a successfully decoded batch can produce this proof. Unlike
/// [`Operation::from_verified`], there is no caller-asserted constructor. It
/// allows admission to reuse hashing, not to skip schema, state, role, scope,
/// history, or capacity validation. It is neither authentication nor a vote.
///
/// A caller-asserted outgoing operation cannot be promoted into a receive proof:
///
/// ```compile_fail
/// use ozzy_replication::wire::{Operation, VerifiedOperation};
/// fn trust(operation: Operation<'_>) -> VerifiedOperation<'_> {
///     operation.into()
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedOperation<'a>(Operation<'a>);

impl<'a> VerifiedOperation<'a> {
    /// Borrow the body and envelope covered by the decoder's integrity checks.
    pub const fn canonical(self) -> CanonicalOperation<'a> {
        self.0.canonical()
    }

    /// Body digest recomputed and matched by the decoder, not merely supplied.
    pub const fn body_digest(self) -> Digest {
        self.0.body_digest()
    }

    /// Hash-validated chain position, not a retained or committed boundary.
    pub const fn prefix(self) -> Prefix {
        self.0.prefix()
    }
}

/// One bounded outbound prepare, borrowing prevalidated canonical operations.
#[derive(Debug, Clone, Copy)]
pub struct Prepare<'a> {
    /// Active view; each operation preserves its own original view.
    pub scope: Scope,
    /// Primary's piggyback commit knowledge, independently checked by the core.
    pub committed: Prefix,
    /// Consecutive whole operations; never split a canonical operation on the wire.
    pub operations: &'a [Operation<'a>],
}

/// Exact prefixes of caller-owned metadata/payload buffers for PREPARE or OPS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrepareEncoding {
    /// First Ozzy frame.
    pub header: [u8; 64],
    /// Valid second-frame bytes.
    pub metadata_bytes: usize,
    /// Valid third-frame bytes.
    pub payload_bytes: usize,
}

/// Canonical metadata encoded before a transport session or receive epoch is
/// available. This is not a sendable packet and contains no envelope header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrepareMetadata {
    /// Exact metadata prefix in the supplied buffer, without a receive epoch.
    pub metadata_bytes: usize,
    /// Exact length of the shared canonical body backing to pair with it.
    pub payload_bytes: usize,
}

/// Encode bounded canonical descriptors without inventing a link session.
/// Bind a real session, receive epoch, and current commit before transmission.
/// Invalid fields or capacity leave the output unchanged, as for wire encoding.
pub fn encode_prepare_unbound(
    sender: NodeId,
    prepare: Prepare<'_>,
    metadata: &mut [u8],
    limits: WireLimits,
) -> Result<PrepareMetadata, WireError> {
    encode((sender, None), None, prepare, metadata, None, limits).map(|(_, sizes)| sizes)
}

/// Encode into reusable caller-owned buffers, preserving them on rejection.
///
/// Reuses the supplied body/envelope digests; it does not recompress or rehash
/// payloads. The caller must retain the exact output until transport releases it.
pub fn encode_prepare(
    sender: NodeId,
    session: LinkSessionId,
    prepare: Prepare<'_>,
    metadata: &mut [u8],
    payload: &mut [u8],
    limits: WireLimits,
) -> Result<PrepareEncoding, WireError> {
    encode(
        (sender, Some(session)),
        None,
        prepare,
        metadata,
        Some(payload),
        limits,
    )
    .map(bound_encoding)
}

/// Encode only PREPARE framing. Pair with the identical canonical body backing.
pub fn encode_prepare_metadata(
    sender: NodeId,
    session: LinkSessionId,
    prepare: Prepare<'_>,
    metadata: &mut [u8],
    limits: WireLimits,
) -> Result<PrepareEncoding, WireError> {
    encode(
        (sender, Some(session)),
        None,
        prepare,
        metadata,
        None,
        limits,
    )
    .map(bound_encoding)
}

/// Encode credited normal data with a mandatory receiver-issued epoch.
///
/// Distinct `PREPARE_FLOW` bytes prevent silent mixing with legacy PREPARE schemas.
/// Adds exactly 16 metadata bytes per batch. Canonical identities/body digests
/// are unchanged, and no additional body hashing or allocation is performed.
pub fn encode_flow_prepare(
    sender: NodeId,
    session: LinkSessionId,
    epoch: ReceiveEpoch,
    prepare: Prepare<'_>,
    metadata: &mut [u8],
    payload: &mut [u8],
    limits: WireLimits,
) -> Result<PrepareEncoding, WireError> {
    encode(
        (sender, Some(session)),
        Some(epoch),
        prepare,
        metadata,
        Some(payload),
        limits,
    )
    .map(bound_encoding)
}

/// Encode only the header and metadata of a credited `PREPARE_FLOW`.
///
/// The payload frame of a prepare is the concatenation of its canonical bodies
/// and does not depend on the receiver or its epoch. A sender that already
/// encoded that exact payload for another peer can pair it with this metadata
/// instead of copying the bodies again. `payload_bytes` reports the length the
/// paired payload must have.
pub fn encode_flow_prepare_metadata(
    sender: NodeId,
    session: LinkSessionId,
    epoch: ReceiveEpoch,
    prepare: Prepare<'_>,
    metadata: &mut [u8],
    limits: WireLimits,
) -> Result<PrepareEncoding, WireError> {
    encode(
        (sender, Some(session)),
        Some(epoch),
        prepare,
        metadata,
        None,
        limits,
    )
    .map(bound_encoding)
}

fn bound_encoding((header, sizes): (Option<[u8; 64]>, PrepareMetadata)) -> PrepareEncoding {
    PrepareEncoding {
        header: header.expect("wire encoder supplied a session"),
        metadata_bytes: sizes.metadata_bytes,
        payload_bytes: sizes.payload_bytes,
    }
}

fn encode(
    (sender, session): (NodeId, Option<LinkSessionId>),
    epoch: Option<ReceiveEpoch>,
    prepare: Prepare<'_>,
    metadata: &mut [u8],
    payload: Option<&mut [u8]>,
    limits: WireLimits,
) -> Result<(Option<[u8; 64]>, PrepareMetadata), WireError> {
    if sender.as_bytes() == &[0; 16] {
        return Err(ozzy_proto::EnvelopeError::ZeroIdentity.into());
    }
    validate_scope(prepare.scope)?;
    validate_prefix(prepare.committed)?;
    let count = prepare.operations.len();
    let metadata_bytes = metadata_length(
        count,
        BATCH_BYTES + usize::from(epoch.is_some()) * 16,
        limits,
    )?;
    let first = prepare.operations[0].canonical;
    let predecessor = Prefix {
        op: OpNumber(first.op_number.checked_sub(1).ok_or(WireError::Chain)?),
        digest: first.previous_digest,
    };
    let payload_bytes = validate_batch(
        prepare.scope,
        predecessor,
        Some(prepare.committed),
        prepare.operations,
        limits,
    )?;
    u32::try_from(metadata_bytes).map_err(|_| WireError::Limit)?;
    u32::try_from(payload_bytes).map_err(|_| WireError::Limit)?;
    let header = session
        .map(|session| {
            Envelope {
                opcode: if epoch.is_some() {
                    Opcode::PrepareFlow
                } else {
                    Opcode::Prepare
                },
                response: false,
                request_id: None,
                sender,
                session: Some(session),
            }
            .encode_header(metadata_bytes, payload_bytes, limits.envelope)
        })
        .transpose()?;
    if metadata.len() < metadata_bytes
        || payload
            .as_ref()
            .is_some_and(|payload| payload.len() < payload_bytes)
    {
        return Err(WireError::Capacity);
    }
    let mut writer = Writer::new(&mut metadata[..metadata_bytes]);
    writer.scope(prepare.scope, sender);
    if let Some(epoch) = epoch {
        writer.bytes(&epoch.get().to_be_bytes());
    }
    writer.u64(first.op_number);
    writer.bytes(predecessor.digest.as_bytes());
    writer.prefix(prepare.committed);
    write_batch(&mut writer, payload, prepare.operations);
    Ok((
        header,
        PrepareMetadata {
            metadata_bytes,
            payload_bytes,
        },
    ))
}

/// Hash-validated prepare borrowing the packet's backing buffers.
///
/// No per-operation vector is allocated. Canonical body schemas and application
/// transitions still need validation before constructing core prepare metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrepareBatch<'a> {
    committed: Prefix,
    batch: DecodedBatch<'a>,
}

impl<'a> PrepareBatch<'a> {
    /// Active primary view and immutable group configuration.
    pub const fn scope(self) -> Scope {
        self.batch.scope
    }
    /// Piggyback commit claim, not local commit authority.
    pub const fn committed(self) -> Prefix {
        self.committed
    }
    /// Exact prefix immediately before the first operation.
    pub const fn predecessor(self) -> Prefix {
        self.batch.predecessor
    }
    /// Exact hash-validated end of this batch, not a durable or committed boundary.
    pub const fn end(self) -> Prefix {
        self.batch.accepted
    }
    /// Iterate verified descriptors and borrowed bodies without hashing again.
    pub fn operations(self) -> Operations<'a> {
        self.batch.operations()
    }

    /// Iterate immutable decoder proofs without allocating or rehashing bodies.
    pub fn verified_operations(
        self,
    ) -> impl ExactSizeIterator<Item = VerifiedOperation<'a>> + std::iter::FusedIterator {
        self.operations().map(VerifiedOperation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DecodedBatch<'a> {
    pub scope: Scope,
    pub predecessor: Prefix,
    pub accepted: Prefix,
    descriptors: &'a [u8],
    pub payload: &'a [u8],
}

impl<'a> DecodedBatch<'a> {
    pub(super) fn operations(self) -> Operations<'a> {
        Operations {
            descriptors: self.descriptors.as_chunks::<DESCRIPTOR_BYTES>().0.iter(),
            payload: self.payload,
            scope: self.scope,
            previous: self.predecessor,
        }
    }
}

/// Allocation-free iteration over an already checked operation batch.
#[derive(Debug, Clone)]
pub struct Operations<'a> {
    descriptors: Iter<'a, [u8; DESCRIPTOR_BYTES]>,
    payload: &'a [u8],
    scope: Scope,
    previous: Prefix,
}

impl<'a> Iterator for Operations<'a> {
    type Item = Operation<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let descriptor = self.descriptors.next()?;
        let mut reader = Reader::new(descriptor);
        let operation = read_operation(&mut reader, &mut self.payload, self.scope, self.previous)
            .expect("validated batch");
        self.previous = operation.prefix();
        Some(operation)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.descriptors.size_hint()
    }
}

impl ExactSizeIterator for Operations<'_> {}
impl std::iter::FusedIterator for Operations<'_> {}

pub(super) fn decode_prepare<'a>(
    scope: Scope,
    mut reader: Reader<'a>,
    payload: &'a [u8],
    limits: WireLimits,
) -> Result<PrepareBatch<'a>, WireError> {
    let first_op = reader.u64()?;
    let predecessor = Prefix {
        op: OpNumber(first_op.checked_sub(1).ok_or(WireError::Chain)?),
        digest: reader.digest()?,
    };
    validate_prefix(predecessor)?;
    let committed = reader.prefix()?;
    let batch = decode_batch(scope, predecessor, Some(committed), reader, payload, limits)?;
    Ok(PrepareBatch { committed, batch })
}

pub(super) fn decode_batch<'a>(
    scope: Scope,
    predecessor: Prefix,
    committed: Option<Prefix>,
    mut reader: Reader<'a>,
    mut payload: &'a [u8],
    limits: WireLimits,
) -> Result<DecodedBatch<'a>, WireError> {
    validate_prefix(predecessor)?;
    if let Some(committed) = committed {
        check_commit(committed, predecessor)?;
    }
    let count = usize::try_from(reader.u32()?).map_err(|_| WireError::Limit)?;
    let descriptor_bytes = metadata_length(count, 0, limits)?;
    if reader.remaining.len() != descriptor_bytes {
        return Err(WireError::Length);
    }
    let descriptors = reader.remaining;
    let original_payload = payload;
    let mut previous = predecessor;
    for _ in 0..count {
        let operation = read_operation(&mut reader, &mut payload, scope, previous)?;
        validate_step(scope, previous, operation)?;
        if canonical_body_digest(operation.canonical.body) != operation.body_digest
            || logical_operation_digest_with_body_digest(
                &operation.canonical,
                operation.body_digest,
            ) != operation.digest
        {
            return Err(WireError::Digest);
        }
        if let Some(committed) = committed {
            check_commit(committed, operation.prefix())?;
        }
        previous = operation.prefix();
    }
    if !payload.is_empty() {
        return Err(WireError::Payload);
    }
    reader.finish()?;
    Ok(DecodedBatch {
        scope,
        predecessor,
        accepted: previous,
        descriptors,
        payload: original_payload,
    })
}

fn read_operation<'a>(
    reader: &mut Reader<'_>,
    payload: &mut &'a [u8],
    scope: Scope,
    previous: Prefix,
) -> Result<Operation<'a>, WireError> {
    let op_number = reader.u64()?;
    let original_view = reader.u64()?;
    let kind = u16::from_be_bytes(reader.bytes()?);
    let kind = OperationKind::try_from(kind).map_err(|_| WireError::OperationKind(kind))?;
    let bytes = usize::try_from(reader.u32()?).map_err(|_| WireError::Limit)?;
    if bytes == 0 {
        return Err(WireError::Limit);
    }
    let body_digest = reader.digest()?;
    let digest = reader.digest()?;
    let (body, rest) = payload.split_at_checked(bytes).ok_or(WireError::Length)?;
    *payload = rest;
    Ok(Operation {
        canonical: CanonicalOperation {
            group_id: scope.group_id,
            configuration_epoch: scope.configuration_epoch,
            original_view,
            op_number,
            previous_digest: previous.digest,
            kind,
            body,
        },
        body_digest,
        digest,
    })
}

fn validate_step(
    scope: Scope,
    previous: Prefix,
    operation: Operation<'_>,
) -> Result<(), WireError> {
    let canonical = operation.canonical;
    validate_prefix(operation.prefix())?;
    if canonical.group_id != scope.group_id
        || canonical.configuration_epoch != scope.configuration_epoch
        || canonical.original_view > scope.view
        || previous.op.0.checked_add(1) != Some(canonical.op_number)
        || canonical.previous_digest != previous.digest
    {
        return Err(WireError::Chain);
    }
    Ok(())
}

pub(super) fn metadata_length(
    count: usize,
    fixed_bytes: usize,
    limits: WireLimits,
) -> Result<usize, WireError> {
    if count == 0 || count > limits.max_operations || u32::try_from(count).is_err() {
        return Err(WireError::Limit);
    }
    let bytes = count
        .checked_mul(DESCRIPTOR_BYTES)
        .and_then(|bytes| bytes.checked_add(fixed_bytes))
        .ok_or(WireError::Limit)?;
    if bytes > limits.envelope.max_metadata_bytes {
        return Err(WireError::Limit);
    }
    Ok(bytes)
}

pub(super) fn validate_batch(
    scope: Scope,
    predecessor: Prefix,
    committed: Option<Prefix>,
    operations: &[Operation<'_>],
    limits: WireLimits,
) -> Result<usize, WireError> {
    metadata_length(operations.len(), 0, limits)?;
    validate_prefix(predecessor)?;
    if let Some(committed) = committed {
        check_commit(committed, predecessor)?;
    }
    let mut previous = predecessor;
    let mut bytes = 0usize;
    for &operation in operations {
        validate_step(scope, previous, operation)?;
        if let Some(committed) = committed {
            check_commit(committed, operation.prefix())?;
        }
        bytes = bytes
            .checked_add(operation.canonical.body.len())
            .ok_or(WireError::Limit)?;
        if bytes > limits.envelope.max_payload_bytes || operation.canonical.body.is_empty() {
            return Err(WireError::Limit);
        }
        previous = operation.prefix();
    }
    Ok(bytes)
}

// Call only after complete count/length validation and output capacity checks.
// `None` writes the per-operation metadata without copying bodies, for a
// caller that pairs the metadata with an already encoded identical payload.
pub(super) fn write_batch(
    writer: &mut Writer<'_>,
    mut payload: Option<&mut [u8]>,
    operations: &[Operation<'_>],
) {
    writer.bytes(&(operations.len() as u32).to_be_bytes());
    let mut offset = 0;
    for &operation in operations {
        let canonical = operation.canonical;
        writer.u64(canonical.op_number);
        writer.u64(canonical.original_view);
        writer.bytes(&(canonical.kind as u16).to_be_bytes());
        writer.bytes(&(canonical.body.len() as u32).to_be_bytes());
        writer.bytes(operation.body_digest.as_bytes());
        writer.bytes(operation.digest.as_bytes());
        let end = offset + canonical.body.len();
        if let Some(payload) = payload.as_deref_mut() {
            payload[offset..end].copy_from_slice(canonical.body);
        }
        offset = end;
    }
}

fn check_commit(committed: Prefix, position: Prefix) -> Result<(), WireError> {
    if committed.op == position.op && committed != position {
        Err(WireError::Chain)
    } else {
        Ok(())
    }
}
