//! Body-only proposals. Consensus coordinates belong to the active primary.

mod partition;
pub(super) use partition::partition_retry;

use ozzy_journal::operation::{AppendRecord, CanonicalOperation, Digest, OperationKind};
use ozzy_proto::{
    GroupId, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence,
};
use ozzy_replication::PipelineLimits;

use super::{AppendBuffer, JournalError, ValidatedAppend};

/// One producer sequence range, without caller-selected offsets or timestamps.
///
/// The producer preserves this session, sequence range, record IDs, bytes, and
/// multipart boundaries across retries. Partition creation and session opening
/// are separate canonical operations. This service API does not discover leaders
/// or persist a client outbox.
#[derive(Debug)]
pub struct ProducerAppend<'a> {
    /// Stable partition incarnation, not a topic name or transport route.
    pub partition: PartitionIncarnation,
    /// Expected partition owner fence, independent of the consensus view.
    pub owner_epoch: OwnerEpoch,
    /// Producer identity bound to the partition.
    pub producer_id: ProducerId,
    /// Previously opened producer session.
    pub producer_epoch: ProducerEpoch,
    /// First sequence in this request; subsequent records are contiguous.
    pub first_sequence: ProducerSequence,
    /// Frozen nonzero IDs and exact multipart payloads, copied into a leased arena.
    pub records: Vec<AppendRecord<'a>>,
}

/// A producer request rejected without admitting a new operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AppendAdmissionError {
    /// The request names another group, configuration epoch, or primary view.
    #[error("producer request authority is stale or foreign")]
    Authority,
    /// A request must match the immutable group completion policy.
    #[error("producer request policy does not match the group")]
    Policy,
    /// No partition with this incarnation exists in the selected application image.
    #[error("unknown producer partition")]
    UnknownPartition,
    /// The partition identity, owner fence, or opened producer session differs.
    #[error("producer identity or epoch is fenced")]
    Fenced,
    /// A sequence range is invalid or cannot fit in sequence space.
    #[error("producer sequence does not match the next accepted sequence")]
    Sequence,
    /// A predecessor range has not been accepted yet. The same record identity
    /// can be retried after that predecessor arrives.
    #[error("producer append awaits predecessor sequence")]
    SequenceGap,
    /// This request predates the retained result or payload floor.
    #[error("producer retry history expired")]
    RetryHistoryExpired,
    /// A retained sequence has a different record ID, payload, or multipart shape.
    #[error("producer retry conflicts with accepted records")]
    RetryConflict,
}

/// Reusable bounded canonical bodies without caller-selected consensus coordinates.
///
/// This is a service API, not the producer SDK. `push` accepts already assigned
/// canonical bodies; their offsets and sequences are validated, never reassigned.
/// `prepare_append` instead freezes one producer request without assigned offsets
/// or timestamps. The worker assigns those fields and resolves exact retries.
/// Streaming preparation additionally verifies and removes retained retry records
/// before admitting a fresh suffix. Every path captures the primary image;
/// the actor rechecks it before admission,
/// transmission, or returning a retained result.
/// Lease at startup and reuse the same arena after rejection or completion.
#[derive(Debug)]
pub struct ProposalBuffer(pub(crate) AppendBuffer);

impl ProposalBuffer {
    /// Bind an empty startup arena to a shard-owned bounded memory pool.
    /// Direct producer backpressure controls admission before allocation.
    pub fn bind_owner(&mut self, owner: &crate::memory::Owner) -> Result<(), JournalError> {
        self.0.bind_allocator(&owner.allocator())
    }

    /// Open or resume one broker-owned writer through canonical proposal rules.
    /// The actor must validate the current authenticated link and writer identity.
    /// A repeated operation ID keeps its original expected epoch and mode.
    /// Requires the shard-owned journal executor. Legacy journal workers reject it.
    pub fn prepare_producer_open(
        &mut self,
        request: ozzy_proto::producer::Open,
        policy: ozzy_proto::append::Policy,
    ) -> Result<(), JournalError> {
        self.0.prepare_producer_open(request, policy)
    }

    /// Validated session coordinates. These become a wire reply only after the
    /// actor confirms the returned proposal prefix under the configured policy.
    pub fn producer_opened(&self) -> Option<ozzy_proto::producer::Opened> {
        self.0
            .producer_session
            .as_ref()
            .and_then(|session| session.opened)
    }

    /// Group independently retryable streaming records from one writer. A group
    /// may overlap retained records and a fresh suffix; identity remains per record.
    /// Streaming proposals may carry several such operations, one per writer.
    pub fn prepare_stream_append(
        &mut self,
        authority: ozzy_proto::append::Authority,
        request: ProducerAppend<'_>,
    ) -> Result<(), JournalError> {
        self.prepare_append(request)?;
        self.0.producer_stream = true;
        self.0.proposal_authority = Some(authority);
        Ok(())
    }

    pub(crate) fn prepare_stream_wire_append(
        &mut self,
        request: ozzy_proto::append::ValidatedAppend<'_>,
    ) -> Result<(), JournalError> {
        let authority = request.authority;
        self.prepare_wire_append(request)?;
        self.0.producer_stream = true;
        self.0.proposal_authority = Some(authority);
        Ok(())
    }

    /// Freeze one native append into an empty leased arena before submission.
    ///
    /// Preparation copies/encodes request bytes; `try_submit` remains enqueue-only.
    /// Offsets and timestamps are private placeholders until worker validation.
    /// No allocation can grow the body arena beyond its startup byte bound. Clear
    /// before preparing another request; never regenerate IDs after uncertainty.
    pub fn prepare_append(&mut self, request: ProducerAppend<'_>) -> Result<(), JournalError> {
        self.0.prepare_producer_append(request)
    }

    /// Freeze a validated native APPEND and retain its expected authority.
    ///
    /// The caller must first authenticate the link, validate its current session,
    /// and check negotiated capabilities/receive limits. This method does not do
    /// those checks. Worker admission compares the retained group/config/view with
    /// its exact validation ticket, including for retries resolved from history.
    /// Worker admission checks the retained policy against the group configuration.
    ///
    /// Preparation copies clear descriptors and the unchanged raw or LZ4 payload
    /// into the existing arena. It never decodes a whole-payload codec. Submission
    /// remains enqueue-only. Clear before changing authority; stable IDs, sequence,
    /// and multipart data must survive.
    pub fn prepare_wire_append(
        &mut self,
        request: ozzy_proto::append::ValidatedAppend<'_>,
    ) -> Result<(), JournalError> {
        if !matches!(
            request.policy,
            ozzy_proto::append::Policy::LocalDurable
                | ozzy_proto::append::Policy::QuorumDurable
                | ozzy_proto::append::Policy::QuorumReplicatedPersisting
        ) {
            return Err(AppendAdmissionError::Policy.into());
        }
        if !self.is_empty()
            && (!self.0.is_producer() || self.0.proposal_authority != Some(request.authority))
        {
            return Err(JournalError::AppendMismatch);
        }
        // The one-batch canonical header is 80 bytes. IDs and part descriptors
        // have identical lengths in both schemas. Bound the entire body before
        // copying descriptors or payload.
        let body_bytes = 80_usize
            .checked_add(request.records.encoded().len())
            .and_then(|bytes| {
                if request.payload_encoding == ozzy_proto::append::PayloadEncoding::Lz4 {
                    bytes.checked_add(9 + request.encoded_payload.len())
                } else {
                    bytes.checked_add(request.encoded_payload.len())
                }
            })
            .ok_or(JournalError::AppendCapacity)?;
        if body_bytes > self.0.limits().max_body_bytes - self.0.body_bytes() {
            return Err(JournalError::AppendCapacity);
        }
        self.0.prepare_wire_producer_batch(request)?;
        self.0.proposal_authority = Some(request.authority);
        self.0.proposal_policy = Some(request.policy);
        Ok(())
    }

    pub(crate) const fn owner_generation(&self) -> ozzy_replication::JournalGeneration {
        self.0.owner_generation()
    }

    /// Copy one encoded body without hashing, decoding, or allocating past capacity.
    /// Bodies are private until worker validation establishes their canonical form.
    pub fn push(&mut self, kind: OperationKind, body: &[u8]) -> Result<(), JournalError> {
        if self.0.is_producer()
            || self.0.is_partition_creation()
            || self.0.producer_session.is_some()
        {
            return Err(JournalError::AppendMismatch);
        }
        self.0.push(CanonicalOperation {
            group_id: GroupId::from_bytes([0; 16]),
            configuration_epoch: 0,
            original_view: 0,
            op_number: 0,
            previous_digest: Digest::ZERO,
            kind,
            body,
        })
    }

    /// Release logical contents while retaining all allocations and the worker lease.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Number of proposed canonical operations, independent of record count.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the proposal has no operations.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Total encoded body bytes charged to this lease.
    pub fn body_bytes(&self) -> usize {
        self.0.body_bytes()
    }

    /// Operation kinds and bodies, including after validation rejection.
    /// Native offset/timestamp fields are provisional until a committed reply;
    /// IDs, payload bytes, and multipart boundaries stay frozen. Streaming
    /// validation may remove an exactly verified, already accepted retry prefix.
    pub fn bodies(&self) -> impl ExactSizeIterator<Item = (OperationKind, &[u8])> {
        self.0
            .operations()
            .map(|operation| (operation.kind, operation.body))
    }

    /// Exact verified coordinates for a single streaming retry. Fresh-only
    /// proposals use their canonical batch headers instead. Coordinates become
    /// confirmation results only after the actor establishes the required policy.
    pub fn producer_retry_results(&self) -> &[ozzy_core::state::ProducerResultSpan] {
        self.0
            .retry_results
            .as_ref()
            .map_or(&[], |results| results.ranges.as_slice())
    }

    pub(crate) fn has_trimmed_retry(&self) -> bool {
        self.0
            .retry_results
            .as_ref()
            .is_some_and(|results| results.trimmed)
    }

    /// Fixed fields of operation `index`, read from a body built from a
    /// validated writer request. `None` without that request's proof.
    pub(crate) fn validated_producer_summary(
        &self,
        index: usize,
    ) -> Option<ozzy_journal::operation::AppendBatchSummary> {
        self.0.validated_producer_summary(index)
    }

    /// Hard operation/body bounds of this startup-allocated arena.
    pub const fn limits(&self) -> PipelineLimits {
        self.0.limits()
    }
}

impl From<AppendBuffer> for ProposalBuffer {
    /// Retain bodies, discarding any authority implied by previous envelope fields.
    /// Reproposing admitted work requires the owner's explicit retry reconciliation;
    /// this conversion does not establish that the previous attempt failed.
    /// A trimmed retry must be rebuilt from the original SDK request, not reproposed.
    fn from(buffer: AppendBuffer) -> Self {
        Self(buffer)
    }
}

/// Read-only proposal validation result. Neither variant admits or persists work.
#[derive(Debug)]
pub enum ProposalValidation {
    /// Recheck the ticket with the driver before sending PREPARE or submitting a write.
    Ready(ValidatedAppend),
    /// Exact producer bytes, a writer session, or partition creation already exist.
    /// No new operation was proposed.
    /// Recheck this image and await quorum/application through the returned prefix.
    Resolved {
        /// Authority and accepted image used for exact journal reads.
        validation: ozzy_replication::driver::ValidationTicket,
        /// Prefix required for this retry. Partition creation may conservatively
        /// wait through the accepted tail when creation is not applied yet.
        through: ozzy_replication::Prefix,
        /// Original request. APPENDs retain their first offset and timestamp.
        /// Streaming callers consume `producer_retry_results()` for every range.
        buffer: ProposalBuffer,
    },
    /// No write was submitted. The same bounded arena remains available for reuse.
    Rejected {
        /// Syntax, application, authority, capacity, or storage-read failure.
        /// Uncertain storage failures also fence the worker.
        reason: JournalError,
        /// Original operation bodies, unchanged even if consensus assignment failed.
        buffer: ProposalBuffer,
    },
}
