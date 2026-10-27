//! Owned, bounded buffers crossing the protocol/storage boundary.

mod assignment;
mod body;
pub(in crate::replica_journal) mod dedup;
mod metadata;
mod producer;
mod progress;
mod release;
pub(in crate::replica_journal) use progress::JoinedProgress;
mod retained;
pub(in crate::replica_journal) use retained::RetainedAppend;
mod schedule;
mod split;
mod trim;
mod validated;
pub use validated::{AdmittedAppend, ValidatedAppend};

use std::ops::Range;

use ozzy_journal::operation::{CanonicalOperation, Digest, ValidatedWireAppend};
use ozzy_replication::{JournalGeneration, PipelineLimits, PreparedOperation};
use tokio::sync::OwnedSemaphorePermit;

use super::JournalError;

/// Per-command operation bound, independent of individual replica packet limits.
/// A canonical operation may contain many records; this is not a record limit.
pub const MAX_APPEND_OPERATIONS: usize = 256;

/// Byte offsets in a one-batch canonical body: batch count, partition, owner
/// epoch, producer, producer epoch, first sequence, then the position (first
/// offset and timestamp).
const PARTITION: usize = 4;
const FIRST_SEQUENCE: usize = PARTITION + 16 + 8 + 16 + 8;
const POSITION: usize = FIRST_SEQUENCE + 8;

#[derive(Debug, Clone)]
struct Descriptor {
    envelope: CanonicalOperation<'static>,
    body: Range<usize>,
    /// Body digest already verified against these bytes by wire decoding.
    digest: Option<Digest>,
    /// Live PREPARE from the authenticated primary. Its APPEND payload was
    /// validated before canonical construction on that primary.
    primary_payload_validated: bool,
    /// Application-thread proof for one producer wire APPEND body.
    proof: Option<ValidatedWireAppend>,
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
    /// Due times of a scheduled producer batch, kept beside its APPEND body
    /// until the leader assigns schedule sequences.
    schedule: Option<Box<schedule::Scheduled>>,
    dedup_original: Option<Box<dedup::Original>>,
    dedup_charge: Option<crate::memory::Charge>,
    validated: bool,
    pub(super) proposal_policy: Option<ozzy_proto::append::Policy>,
    pub(super) proposal_authority: Option<ozzy_proto::append::Authority>,
    pub(super) producer_session: Option<Box<ProducerSession>>,
    /// The producer of every APPEND entry; none for zero or several producers.
    producer: Option<ozzy_proto::ProducerId>,
    pub(super) group_progress: Option<Box<super::progress::Session>>,
    /// Progress updates joined into this proposal after the first, one per
    /// later operation.
    pub(super) progress_riders: Vec<super::progress::Session>,
    pub(super) group_assignment: Option<Box<super::assignment::Session>>,
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
        if bytes > self.room() {
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
        if bytes > self.room() {
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

    /// Body bytes the lease still admits.
    fn room(&self) -> usize {
        self.limits.max_body_bytes - self.bodies.len()
    }

    pub(super) fn retain(&mut self) -> RetainedAppend {
        // Canonical acceptance ends the reversible proposal's lifetime.
        self.dedup_original = None;
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

    pub(crate) fn retained_bytes(&self) -> usize {
        self.bodies.retained_bytes()
    }

    /// Producer proofs in operation order; `None` for operations without one.
    pub(super) fn proofs(&self) -> impl ExactSizeIterator<Item = Option<ValidatedWireAppend>> {
        self.entries.iter().map(|entry| entry.proof)
    }

    /// Only live primary PREPARE bodies may reuse the primary's LZ4 check.
    /// Ordinary canonical pushes and recovered history still validate locally.
    pub(super) fn primary_payloads_validated(&self) -> bool {
        self.entries
            .iter()
            .all(|entry| entry.primary_payload_validated)
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
            dedup_original: None,
            dedup_charge: None,
            retry_results: None,
            schedule: None,
            validated: false,
            proposal_authority: None,
            proposal_policy: None,
            producer_session: None,
            producer: None,
            group_progress: None,
            progress_riders: Vec::new(),
            group_assignment: None,
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
        self.push_entry(operation, None, false)
    }

    /// Called only after the live PREPARE sender was checked against the current
    /// primary. Keep the wire-verified digest and primary payload provenance.
    pub(crate) fn push_primary_verified(
        &mut self,
        operation: ozzy_replication::wire::VerifiedOperation<'_>,
    ) -> Result<(), JournalError> {
        self.push_entry(operation.canonical(), Some(operation.body_digest()), true)
    }

    fn push_entry(
        &mut self,
        operation: CanonicalOperation<'_>,
        digest: Option<Digest>,
        primary_payload_validated: bool,
    ) -> Result<(), JournalError> {
        self.validated = false;
        if self.entries.len() == self.limits.max_operations || operation.body.len() > self.room() {
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
            primary_payload_validated,
            proof: None,
        });
        Ok(())
    }

    /// Release contents and retain the journal lease. Charged allocations return
    /// to their shared owner cache after the final immutable observer releases them.
    pub fn clear(&mut self) {
        self.dedup_original = None;
        self.dedup_charge = None;
        self.request_kind = RequestKind::Canonical;
        self.producer_stream = false;
        self.retry_results = None;
        self.schedule = None;
        self.validated = false;
        self.proposal_authority = None;
        self.proposal_policy = None;
        self.producer_session = None;
        self.producer = None;
        self.group_progress = None;
        self.progress_riders.clear();
        self.group_assignment = None;
        self.entries.clear();
        self.bodies.clear();
        self.prepared.clear();
        self.body_digests.clear();
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
        if bytes > self.room() {
            return Err(JournalError::AppendCapacity);
        }
        self.bodies.reserve(bytes)
    }

    pub(super) async fn reserve_history_payload(
        &mut self,
        bytes: usize,
    ) -> Result<(), JournalError> {
        if bytes > self.room() {
            return Err(JournalError::AppendCapacity);
        }
        self.bodies.reserve_when_available(bytes).await
    }

    pub(super) fn push_read_part(&mut self, part: &[u8]) -> Result<Range<usize>, JournalError> {
        self.validated = false;
        if part.len() > self.room() {
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

    pub(super) fn push_typed_body(
        &mut self,
        body: &ozzy_journal::operation::OperationBody<'_>,
    ) -> Result<(), JournalError> {
        use ozzy_journal::operation::{OperationLimits, append_operation_body};
        if self.entries.len() == self.limits.max_operations {
            return Err(JournalError::AppendCapacity);
        }
        let room = self.room();
        let limits = OperationLimits {
            max_body_bytes: room,
            max_payload_bytes: room,
            // Encoding is bounded by the leased arena. The journal owner
            // separately enforces its configured canonical record limits.
            max_records: room / 20,
            max_parts: room / 4,
            ..OperationLimits::default()
        };
        if self.bodies.is_charged() {
            let mut size = body::EncodedSize::default();
            ozzy_journal::operation::append_operation_body_to(&mut size, body, limits)?;
            self.bodies.reserve(size.0)?;
        }
        let range = append_operation_body(self.bodies.mutable()?, body, limits)?;
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
            primary_payload_validated: false,
            proof: None,
        });
        Ok(())
    }
}
