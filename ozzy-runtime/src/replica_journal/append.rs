//! Owned, bounded buffers crossing the protocol/storage boundary.

mod body;
mod metadata;
mod trim;

use std::ops::Range;

use ozzy_core::state::PreparedCanonicalGroup;
use ozzy_journal::operation::{CanonicalOperation, Digest, ValidatedWireAppend};
use ozzy_replication::driver::ValidationTicket;
use ozzy_replication::{JournalGeneration, PipelineLimits, PreparedOperation, WriteTicket};
use tokio::sync::OwnedSemaphorePermit;

use super::JournalError;

/// Per-command operation bound, independent of individual replica packet limits.
/// A canonical operation may contain many records; this is not a record limit.
pub const MAX_APPEND_OPERATIONS: usize = 256;

#[derive(Debug, Clone)]
struct Descriptor {
    envelope: CanonicalOperation<'static>,
    body: Range<usize>,
    /// Body digest already verified against these bytes by wire decoding.
    digest: Option<Digest>,
    /// Application-thread proof for one producer wire APPEND body.
    proof: Option<ValidatedWireAppend>,
}

/// Immutable bounded history retained independently of a reusable caller arena.
#[derive(Debug)]
pub(super) struct RetainedAppend {
    entries: Vec<(Descriptor, Digest)>,
    bodies: bytes::Bytes,
    retained_bytes: usize,
    validated: bool,
    pub(super) prepared: Vec<PreparedOperation>,
}

impl RetainedAppend {
    pub(super) fn operations(&self) -> impl ExactSizeIterator<Item = CanonicalOperation<'_>> {
        self.entries.iter().map(|(entry, _)| CanonicalOperation {
            body: &self.bodies[entry.body.clone()],
            ..entry.envelope
        })
    }

    pub(super) fn verified_operations(
        &self,
    ) -> impl ExactSizeIterator<Item = (CanonicalOperation<'_>, Digest)> {
        self.operations()
            .zip(self.entries.iter().map(|(_, digest)| *digest))
    }

    pub(super) fn payload(&self, part: &[u8]) -> bytes::Bytes {
        self.bodies.slice_ref(part)
    }
    pub(super) fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    /// Producer proofs in operation order; `None` for operations without one.
    pub(super) fn proofs(&self) -> impl ExactSizeIterator<Item = Option<ValidatedWireAppend>> {
        self.entries.iter().map(|(entry, _)| entry.proof)
    }
    /// Whether every operation in `range` carries a proof, and whether all of
    /// their payloads are producer-prepared.
    pub(super) fn validated_payloads_prepared(&self, range: Range<usize>) -> Option<bool> {
        let mut prepared = true;
        for (entry, _) in self.entries.get(range)? {
            prepared &= entry.proof?.payload_is_prepared();
        }
        Some(prepared)
    }
    /// Whether the journal owner decoded every body with its own limits.
    pub(super) const fn validated(&self) -> bool {
        self.validated
    }
}

/// Reusable canonical-body arena leased from one journal owner.
///
/// Reserve leases before serving traffic. Shard-budgeted leases allocate payload
/// lazily. Canonical bytes become immutable after validation, shared by transport
/// and persistence. Clear returns charged backing to the shared owner cache;
/// outstanding references retain their immutable allocation and full charge.
/// Count/body limits bound each lease and every independent retention window.
/// Retry mutation cannot modify already published bytes. Leases survive journal
/// installation on their owning worker; writer tickets do not. Creating a new
/// worker invalidates old leases.
#[derive(Debug)]
pub struct AppendBuffer {
    request_kind: RequestKind,
    pub(super) producer_stream: bool,
    /// Exact results for a single streaming retry, including a fresh suffix.
    /// At most one range per admitted record. Fresh-only proposals use headers.
    pub(super) retry_results: Option<Box<ProducerRetryResults>>,
    validated: bool,
    pub(super) proposal_policy: Option<ozzy_proto::append::Policy>,
    pub(super) proposal_authority: Option<ozzy_proto::append::Authority>,
    pub(super) producer_session: Option<Box<ProducerSession>>,
    owner_generation: JournalGeneration,
    limits: PipelineLimits,
    entries: Vec<Descriptor>,
    bodies: body::Body,
    pub(super) prepared: Vec<PreparedOperation>,
    pub(super) body_digests: Vec<Digest>,
    _lease: OwnedSemaphorePermit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestKind {
    Canonical,
    Producer,
    Partition,
}

#[derive(Debug)]
pub(super) struct ProducerSession {
    pub request: ozzy_proto::producer::Open,
    pub opened: Option<ozzy_proto::producer::Opened>,
}

#[derive(Debug)]
pub(super) struct ProducerRetryResults {
    pub ranges: smallvec::SmallVec<[ozzy_core::state::ProducerResultSpan; 2]>,
    pub trimmed: bool,
}

impl AppendBuffer {
    pub(super) fn is_producer(&self) -> bool {
        self.request_kind == RequestKind::Producer
    }

    pub(super) fn is_partition_creation(&self) -> bool {
        self.request_kind == RequestKind::Partition
    }

    /// Reserve a complete incoming chunk before copying any operation. Shared
    /// shard exhaustion leaves the chunk retryable and its old bytes untouched.
    pub(crate) fn reserve_incoming(&mut self, bytes: usize) -> Result<bool, JournalError> {
        if bytes > self.limits.max_body_bytes - self.bodies.len() {
            return Err(JournalError::AppendCapacity);
        }
        match self.bodies.reserve(bytes) {
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Ok(false)
            }
            result => result.map(|()| true),
        }
    }

    pub(super) fn reserve_body_bytes(&mut self, bytes: usize) -> Result<(), JournalError> {
        self.check_producer_entry()?;
        if bytes > self.limits.max_body_bytes - self.bodies.len() {
            return Err(JournalError::AppendCapacity);
        }
        self.bodies.reserve(bytes)
    }

    /// Producer entries accumulate only after other producer entries, one
    /// operation each, within the leased operation count.
    fn check_producer_entry(&self) -> Result<(), JournalError> {
        if !self.is_empty() && !self.is_producer() {
            return Err(JournalError::AppendMismatch);
        }
        if self.entries.len() == self.limits.max_operations {
            return Err(JournalError::AppendCapacity);
        }
        Ok(())
    }

    pub(super) fn retain(&mut self) -> RetainedAppend {
        assert_eq!(self.entries.len(), self.body_digests.len());
        let bodies = self.bodies.freeze();
        RetainedAppend {
            entries: self
                .entries
                .iter()
                .cloned()
                .zip(self.body_digests.iter().copied())
                .collect(),
            bodies,
            retained_bytes: self.bodies.retained_bytes(),
            validated: self.validated,
            prepared: self.prepared.clone(),
        }
    }
    pub(crate) fn shared_bodies(&mut self) -> bytes::Bytes {
        self.bodies.freeze()
    }

    pub(super) fn retained_bytes(&self) -> usize {
        self.bodies.retained_bytes()
    }

    /// Producer proofs in operation order; `None` for operations without one.
    pub(super) fn proofs(&self) -> impl ExactSizeIterator<Item = Option<ValidatedWireAppend>> {
        self.entries.iter().map(|entry| entry.proof)
    }

    /// Record that every body was just decoded completely with the journal's
    /// operation limits. Any later body change clears it.
    pub(super) const fn set_validated(&mut self, validated: bool) {
        self.validated = validated;
    }

    #[cfg(test)]
    pub(super) fn new(
        owner_generation: JournalGeneration,
        limits: PipelineLimits,
        lease: OwnedSemaphorePermit,
    ) -> Self {
        Self::new_with_memory(owner_generation, limits, lease, None)
    }

    pub(super) fn new_with_memory(
        owner_generation: JournalGeneration,
        limits: PipelineLimits,
        lease: OwnedSemaphorePermit,
        memory: Option<crate::memory::Allocator>,
    ) -> Self {
        Self {
            request_kind: RequestKind::Canonical,
            producer_stream: false,
            retry_results: None,
            validated: false,
            proposal_authority: None,
            proposal_policy: None,
            producer_session: None,
            owner_generation,
            limits,
            entries: Vec::with_capacity(limits.max_operations),
            bodies: memory.map_or_else(
                || body::Body::new(limits.max_body_bytes),
                |allocator| body::Body::charged(allocator.with_limit(limits.max_body_bytes)),
            ),
            prepared: Vec::with_capacity(limits.max_operations),
            body_digests: Vec::with_capacity(limits.max_operations),
            _lease: lease,
        }
    }

    /// Copy an encoded body and envelope within the declared body/count bounds.
    /// Syntax, hash-chain, and application validation happen on the worker.
    pub fn push(&mut self, operation: CanonicalOperation<'_>) -> Result<(), JournalError> {
        self.push_entry(operation, None)
    }

    /// Like `push`, keeping the body digest that wire decoding verified for
    /// these exact bytes, so the worker does not hash the body again.
    pub(crate) fn push_verified(
        &mut self,
        operation: ozzy_replication::wire::VerifiedOperation<'_>,
    ) -> Result<(), JournalError> {
        self.push_entry(operation.canonical(), Some(operation.body_digest()))
    }

    fn push_entry(
        &mut self,
        operation: CanonicalOperation<'_>,
        digest: Option<Digest>,
    ) -> Result<(), JournalError> {
        self.validated = false;
        if self.entries.len() == self.limits.max_operations
            || operation.body.len() > self.limits.max_body_bytes - self.bodies.len()
        {
            return Err(JournalError::AppendCapacity);
        }
        let start = self.bodies.len();
        self.bodies.extend_from_slice(operation.body)?;
        self.entries.push(Descriptor {
            envelope: CanonicalOperation {
                body: &[],
                ..operation
            },
            body: start..self.bodies.len(),
            digest,
            proof: None,
        });
        Ok(())
    }

    /// Release contents and retain the journal lease. Charged allocations return
    /// to their shared owner cache after the final immutable observer releases them.
    pub fn clear(&mut self) {
        self.request_kind = RequestKind::Canonical;
        self.producer_stream = false;
        self.retry_results = None;
        self.validated = false;
        self.proposal_authority = None;
        self.proposal_policy = None;
        self.producer_session = None;
        self.entries.clear();
        self.bodies.clear();
        self.prepared.clear();
        self.body_digests.clear();
    }

    pub(crate) fn bind_capacity(
        &mut self,
        capacity: &crate::memory::Capacity,
    ) -> Result<(), JournalError> {
        self.bind_allocator(&capacity.allocator())
    }

    pub(crate) fn allocator(&self) -> Option<crate::memory::Allocator> {
        self.bodies.allocator()
    }

    /// General history work regains its original allocation source after a
    /// receive arena returns. Intake allowances never back unrelated reads.
    pub(crate) fn restore_allocator(
        &mut self,
        allocator: Option<&crate::memory::Allocator>,
    ) -> Result<(), JournalError> {
        if let Some(allocator) = allocator {
            self.bind_allocator(allocator)
        } else if !self.is_empty() {
            Err(JournalError::AppendMismatch)
        } else {
            if self.bodies.is_charged() {
                self.bodies = body::Body::new(0);
            }
            Ok(())
        }
    }

    pub(crate) fn bind_allocator(
        &mut self,
        allocator: &crate::memory::Allocator,
    ) -> Result<(), JournalError> {
        if !self.is_empty() {
            return Err(JournalError::AppendMismatch);
        }
        self.bodies = body::Body::charged(allocator.clone().with_limit(self.limits.max_body_bytes));
        Ok(())
    }

    /// Number of canonical operations, independent of record count.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no operations have been appended to this buffer.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Total encoded canonical body bytes.
    pub fn body_bytes(&self) -> usize {
        self.bodies.len()
    }

    /// Borrow immutable canonical operations for replica transmission or inspection.
    pub fn operations(&self) -> impl ExactSizeIterator<Item = CanonicalOperation<'_>> {
        self.entries.iter().map(|entry| CanonicalOperation {
            body: &self.bodies[entry.body.clone()],
            ..entry.envelope
        })
    }

    pub(crate) const fn owner_generation(&self) -> JournalGeneration {
        self.owner_generation
    }

    pub(super) fn read_bytes(&self) -> &[u8] {
        &self.bodies
    }

    /// Reserve the parts of one stored group before copying any of them. A
    /// charged body leases an arena of its exact new total, so growing per
    /// part would lease, clear, and copy the whole read again for every part.
    pub(super) fn reserve_read_parts(&mut self, bytes: usize) -> Result<(), JournalError> {
        if bytes > self.limits.max_body_bytes - self.bodies.len() {
            return Err(JournalError::AppendCapacity);
        }
        self.bodies.reserve(bytes)
    }

    pub(super) async fn reserve_history_payload(
        &mut self,
        bytes: usize,
    ) -> Result<(), JournalError> {
        if bytes > self.limits.max_body_bytes - self.bodies.len() {
            return Err(JournalError::AppendCapacity);
        }
        self.bodies.reserve_when_available(bytes).await
    }

    pub(super) fn push_read_part(&mut self, part: &[u8]) -> Result<Range<usize>, JournalError> {
        self.validated = false;
        if part.len() > self.limits.max_body_bytes - self.bodies.len() {
            return Err(JournalError::AppendCapacity);
        }
        let first = self.bodies.len();
        self.bodies.extend_from_slice(part)?;
        Ok(first..self.bodies.len())
    }

    pub(super) fn wire_operations(
        &self,
    ) -> impl ExactSizeIterator<Item = ozzy_replication::wire::Operation<'_>> {
        assert_eq!(self.body_digests.len(), self.len());
        self.operations()
            .zip(&self.body_digests)
            .map(|(operation, digest)| {
                ozzy_replication::wire::Operation::from_verified(operation, *digest)
            })
    }

    /// Declared operation/body capacity. Clearing never changes these bounds.
    pub const fn limits(&self) -> PipelineLimits {
        self.limits
    }

    pub(super) fn prepare_producer_append(
        &mut self,
        request: super::ProducerAppend<'_>,
    ) -> Result<(), JournalError> {
        self.prepare_producer_batch(ozzy_journal::operation::AppendBatch {
            partition: request.partition,
            owner_epoch: request.owner_epoch,
            producer_id: request.producer_id,
            producer_epoch: request.producer_epoch,
            first_sequence: request.first_sequence,
            first_offset: ozzy_proto::Offset::ZERO,
            append_timestamp_millis: 0,
            records: request.records.into(),
        })
    }

    pub(super) fn prepare_producer_batch(
        &mut self,
        batch: ozzy_journal::operation::AppendBatch<'_>,
    ) -> Result<(), JournalError> {
        use ozzy_journal::operation::{Append, OperationBody};
        self.check_producer_entry()?;
        let body = OperationBody::Append(Append {
            batches: vec![batch],
        });
        self.push_typed_body(&body)?;
        self.request_kind = RequestKind::Producer;
        Ok(())
    }

    pub(super) fn prepare_partition(
        &mut self,
        partition: ozzy_journal::operation::CreatePartition<'_>,
    ) -> Result<(), JournalError> {
        if !self.is_empty() {
            return Err(JournalError::AppendMismatch);
        }
        self.push_typed_body(&ozzy_journal::operation::OperationBody::CreatePartition(
            partition,
        ))?;
        self.request_kind = RequestKind::Partition;
        Ok(())
    }

    pub(super) fn prepare_producer_open(
        &mut self,
        request: ozzy_proto::producer::Open,
        policy: ozzy_proto::append::Policy,
    ) -> Result<(), JournalError> {
        if !self.is_empty() {
            return Err(JournalError::AppendMismatch);
        }
        let new_epoch = match (request.mode, request.expected_epoch) {
            (ozzy_proto::producer::Mode::Resume, None) => 1,
            (ozzy_proto::producer::Mode::Resume, Some(epoch)) => epoch,
            (ozzy_proto::producer::Mode::Fence, Some(epoch)) => {
                epoch.checked_add(1).ok_or(JournalError::AppendMismatch)?
            }
            (ozzy_proto::producer::Mode::Fence, None) => return Err(JournalError::AppendMismatch),
        };
        if new_epoch == 0
            || request.expected_epoch == Some(0)
            || request.authority.group_id.as_bytes() == &[0; 16]
            || request.authority.config_epoch == 0
            || request.partition.as_bytes() == &[0; 16]
            || request.operation.as_bytes() == &[0; 16]
            || request.producer.as_bytes() == &[0; 16]
        {
            return Err(JournalError::AppendMismatch);
        }
        self.push_typed_body(&ozzy_journal::operation::OperationBody::OpenProducer(
            ozzy_journal::operation::OpenProducer {
                partition: request.partition,
                producer_id: request.producer,
                expected_epoch: request.expected_epoch.map(ozzy_proto::ProducerEpoch::new),
                new_epoch: ozzy_proto::ProducerEpoch::new(new_epoch),
                operation_id: request.operation,
            },
        ))?;
        self.producer_session = Some(Box::new(ProducerSession {
            request,
            opened: None,
        }));
        self.proposal_authority = Some(request.authority);
        self.proposal_policy = Some(policy);
        Ok(())
    }

    fn push_typed_body(
        &mut self,
        body: &ozzy_journal::operation::OperationBody<'_>,
    ) -> Result<(), JournalError> {
        use ozzy_journal::operation::{OperationLimits, append_operation_body};
        if self.entries.len() == self.limits.max_operations {
            return Err(JournalError::AppendCapacity);
        }
        let room = self.limits.max_body_bytes - self.bodies.len();
        if self.bodies.is_charged() {
            let mut size = body::EncodedSize::default();
            ozzy_journal::operation::append_operation_body_to(
                &mut size,
                body,
                OperationLimits {
                    max_body_bytes: room,
                    max_payload_bytes: room,
                    max_records: room / 20,
                    max_parts: room / 4,
                    ..OperationLimits::default()
                },
            )?;
            self.bodies.reserve(size.0)?;
        }
        let range = append_operation_body(
            self.bodies.mutable()?,
            body,
            OperationLimits {
                max_body_bytes: room,
                max_payload_bytes: room,
                // Encoding is bounded by the leased arena. The journal owner
                // separately enforces its configured canonical record limits.
                max_records: room / 20,
                max_parts: room / 4,
                ..OperationLimits::default()
            },
        )?;
        self.entries.push(Descriptor {
            envelope: CanonicalOperation {
                group_id: ozzy_proto::GroupId::from_bytes([0; 16]),
                configuration_epoch: 0,
                original_view: 0,
                op_number: 0,
                previous_digest: Digest::ZERO,
                kind: body.kind(),
                body: &[],
            },
            body: range,
            digest: None,
            proof: None,
        });
        Ok(())
    }

    pub(super) fn prepare_wire_producer_batch(
        &mut self,
        request: ozzy_proto::append::ValidatedAppend<'_>,
    ) -> Result<(), JournalError> {
        use ozzy_journal::operation::{
            AppendHeader, OperationKind, OperationLimits, append_wire_record_batch,
        };
        self.check_producer_entry()?;
        let room = self.limits.max_body_bytes - self.bodies.len();
        let limits = OperationLimits {
            max_body_bytes: room,
            max_payload_bytes: room,
            max_records: room / 20,
            max_parts: room / 4,
            ..OperationLimits::default()
        };
        let bytes = 80_usize
            .checked_add(request.records.encoded().len())
            .and_then(|bytes| bytes.checked_add(request.encoded_payload.len()))
            .and_then(|bytes| {
                bytes.checked_add(
                    usize::from(
                        request.payload_encoding == ozzy_proto::append::PayloadEncoding::Lz4,
                    ) * 9,
                )
            })
            .ok_or(JournalError::AppendCapacity)?;
        self.reserve_body_bytes(bytes)?;
        let (range, proof) = append_wire_record_batch(
            self.bodies.mutable()?,
            AppendHeader {
                partition: request.partition,
                owner_epoch: ozzy_proto::OwnerEpoch::new(request.owner_epoch),
                producer_id: request.key.producer_id,
                producer_epoch: ozzy_proto::ProducerEpoch::new(request.key.producer_epoch),
                first_sequence: ozzy_proto::ProducerSequence::new(request.key.first_sequence),
                first_offset: ozzy_proto::Offset::ZERO,
                append_timestamp_millis: 0,
            },
            request.records,
            request.payload_encoding,
            request.encoded_payload,
            limits,
        )?;
        self.entries.push(Descriptor {
            envelope: CanonicalOperation {
                group_id: ozzy_proto::GroupId::from_bytes([0; 16]),
                configuration_epoch: 0,
                original_view: 0,
                op_number: 0,
                previous_digest: Digest::ZERO,
                kind: OperationKind::Append,
                body: &[],
            },
            body: range,
            digest: None,
            proof: Some(proof),
        });
        self.request_kind = RequestKind::Producer;
        Ok(())
    }

    /// Remove a verified streaming retry prefix in place. Only the newly
    /// accepted suffix becomes a canonical operation. No replacement arena.
    pub(super) fn trim_producer_prefix(
        &mut self,
        count: usize,
        limits: ozzy_journal::operation::OperationLimits,
    ) -> Result<(), JournalError> {
        use ozzy_journal::operation::decode_append_view;
        self.validated = false;
        if self.entries.len() != 1 {
            return Err(JournalError::AppendMismatch);
        }
        let mut batches = decode_append_view(&self.bodies, limits)?.batches();
        let batch = batches.next().ok_or(JournalError::AppendMismatch)?;
        if batches.next().is_some()
            || count == 0
            || count >= batch.summary.record_count
            || !self.producer_stream
        {
            return Err(JournalError::AppendMismatch);
        }
        if batch.prepared_payload.is_some() {
            return self.trim_prepared_prefix(count, limits);
        }
        let remaining = (batch.summary.record_count - count) as u32;
        let sequence = batch
            .summary
            .first_sequence
            .get()
            .checked_add(count as u64)
            .ok_or(JournalError::AppendMismatch)?;
        let mut records = batch.raw_records().ok_or(JournalError::AppendMismatch)?;
        let (descriptors, payload) = records.remaining_bytes();
        let descriptor_bytes = descriptors.len();
        let length_bytes = records.tiny_lengths().map_or(0, <[u8]>::len);
        let payload_bytes = payload.len();
        records.nth(count - 1).ok_or(JournalError::AppendMismatch)?;
        let (descriptors, payload) = records.remaining_bytes();
        let kept_descriptors = descriptors.len();
        let kept_lengths = records.tiny_lengths().map_or(0, <[u8]>::len);
        let kept_payload = payload.len();
        let descriptor_start = 80 + descriptor_bytes - kept_descriptors;
        let length_start = 80 + descriptor_bytes + length_bytes - kept_lengths;
        let payload_start = 80 + descriptor_bytes + length_bytes + payload_bytes - kept_payload;
        let end = 80 + kept_descriptors + kept_lengths + kept_payload;
        self.bodies.reserve(0)?;
        // Move IDs/descriptors, optional compact lengths, and payload separately.
        self.bodies
            .mutable()?
            .copy_within(descriptor_start..80 + descriptor_bytes, 80);
        self.bodies.mutable()?.copy_within(
            length_start..80 + descriptor_bytes + length_bytes,
            80 + kept_descriptors,
        );
        self.bodies
            .mutable()?
            .copy_within(payload_start.., 80 + kept_descriptors + kept_lengths);
        self.bodies.truncate(end)?;
        self.bodies.mutable()?[52..60].copy_from_slice(&sequence.to_be_bytes());
        let remaining = remaining | if length_bytes != 0 { 1 << 31 } else { 0 };
        self.bodies.mutable()?[76..80].copy_from_slice(&remaining.to_be_bytes());
        self.entries[0].body.end = end;
        self.entries[0].proof = None;
        Ok(())
    }

    // One-batch canonical header: count, partition, owner, producer, epoch,
    // sequence, then offset and timestamp. Called only after full typed decoding.
    pub(super) fn replace_append_position(
        &mut self,
        index: usize,
        position: [u8; 16],
    ) -> Result<[u8; 16], JournalError> {
        const START: usize = 4 + 16 + 8 + 16 + 8 + 8;
        self.validated = false;
        assert!(self.is_producer() && index < self.len());
        let start = self.entries[index].body.start + START;
        let target = &mut self.bodies.mutable()?[start..start + 16];
        let previous = target.try_into().expect("fixed position fields");
        target.copy_from_slice(&position);
        Ok(previous)
    }

    pub(super) fn validated_producer_summary(
        &self,
        index: usize,
    ) -> Option<ozzy_journal::operation::AppendBatchSummary> {
        let entry = self.entries.get(index)?;
        entry.proof?.summary(&self.bodies[entry.body.clone()]).ok()
    }
}

/// Worker-validated application plans, not admission, persistence, or quorum evidence.
///
/// Call `ReplicaDriver::prepare_validated` with this ticket and metadata before
/// submitting a write or transmitting PREPARE. Changed authority/image requires
/// discarding these plans and validating again. This initial command accepts
/// only a fresh contiguous suffix; duplicate/overlap handling remains actor work.
#[derive(Debug)]
pub struct ValidatedAppend {
    pub(super) validation: ValidationTicket,
    pub(super) buffer: AppendBuffer,
    pub(super) plan: PreparedCanonicalGroup,
    pub(super) records: Vec<ozzy_journal_segment::PreparedOperationRecords>,
}

impl ValidatedAppend {
    pub(crate) fn shared_bodies(&mut self) -> bytes::Bytes {
        self.buffer.shared_bodies()
    }

    /// Exact voter/writer/view and image against which application validation ran.
    pub const fn validation(&self) -> ValidationTicket {
        self.validation
    }

    /// Verified metadata for core admission. Payload remains owned by this value.
    pub fn prepared(&self) -> &[PreparedOperation] {
        &self.buffer.prepared
    }

    /// Immutable payload retained until written, transmitted, or explicitly discarded.
    pub const fn buffer(&self) -> &AppendBuffer {
        &self.buffer
    }

    /// Use worker-computed body digests for wire encoding without hashing payload on the actor.
    pub fn wire_operations(
        &self,
    ) -> impl ExactSizeIterator<Item = ozzy_replication::wire::Operation<'_>> {
        self.buffer.wire_operations()
    }

    /// Discard unadmitted plans and recover the reusable payload arena.
    pub fn into_buffer(self) -> AppendBuffer {
        self.buffer
    }
}

/// Installed canonical state with its queued physical write. The write
/// completion follows independently; returning the caller arena does not
/// release the configured write backlog.
#[derive(Debug)]
pub struct AdmittedAppend {
    pub(super) ticket: WriteTicket,
    pub(super) buffer: AppendBuffer,
    pub(super) persisted: super::JournalCompletion<WriteTicket>,
}

impl AdmittedAppend {
    /// Return the exact admission, reusable arena, and independent write completion.
    pub fn into_parts(
        self,
    ) -> (
        WriteTicket,
        AppendBuffer,
        super::JournalCompletion<WriteTicket>,
    ) {
        (self.ticket, self.buffer, self.persisted)
    }
}
