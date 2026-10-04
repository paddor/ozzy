//! Deterministic checkpoint image for canonical committed state.

use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_journal::operation::{Digest, RetentionPolicy};
use ozzy_proto::{ConsumerGroupId, ConsumerMemberId, Offset, PartitionIncarnation, SubscriptionId};
use thiserror::Error;

use super::{AssignmentKey, AssignmentState, CanonicalState, ProgressKey, StateLimits};

mod encoding;
mod partition;
use partition::decode_partition;
#[cfg(test)]
mod cooperation_tests;

fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => {
            unreachable!("synchronous codec uses only immediately ready work callbacks")
        }
    }
}

/// Exact encoded canonical-state snapshot header length.
pub const STATE_SNAPSHOT_HEADER_BYTES: usize = 256;

const SNAPSHOT_MAGIC: &[u8; 8] = b"OZYSTA01";
const SNAPSHOT_VERSION: u16 = 2;
const PROGRESS_BYTES: usize = 48;
const ASSIGNMENT_BYTES: usize = 64;
const SNAPSHOT_DIGEST_START: usize = 56;
const SNAPSHOT_DIGEST_END: usize = 88;
const SNAPSHOT_HASH_CONTEXT: &str = "ozzy canonical state snapshot v1";
const SNAPSHOT_SCHEMA_CONTEXT: &str = "ozzy canonical state snapshot schema";
const SNAPSHOT_SCHEMA: &[u8] =
    b"OZYSTA01:256:partition96+names+producer72+retry-span24:progress48:assignment64:network-order";
const RETENTION_MAX_AGE: u32 = 1 << 0;
const RETENTION_MAX_BYTES: u32 = 1 << 1;

type ProgressSortKey = (u8, [u8; 16], [u8; 16]);

#[derive(Debug, Clone, Copy)]
struct SnapshotShape {
    revision: u64,
    partition_count: usize,
    progress_count: usize,
    assignment_count: usize,
    producer_count: usize,
    retry_span_count: usize,
}

/// Allocation and input bounds independent of live-state count limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateSnapshotLimits {
    /// Maximum encoded snapshot bytes, including its header.
    pub max_snapshot_bytes: usize,
    /// Maximum UTF-8 bytes per encoded stream or topic name.
    pub max_name_bytes: usize,
}

impl Default for StateSnapshotLimits {
    fn default() -> Self {
        Self {
            max_snapshot_bytes: 256 * 1024 * 1024,
            max_name_bytes: 255,
        }
    }
}

/// Stable digest placed in the outer checkpoint manifest.
pub fn canonical_state_schema_digest() -> Digest {
    let mut hasher = Hasher::new(SNAPSHOT_SCHEMA_CONTEXT);
    hasher.update(SNAPSHOT_SCHEMA);
    hasher.finish()
}

impl CanonicalState {
    /// Encode live committed metadata in deterministic key order.
    pub fn encode_snapshot(
        &self,
        limits: StateSnapshotLimits,
    ) -> Result<Vec<u8>, StateSnapshotError> {
        ready(self.encode_snapshot_cooperative(limits, async |_| {}))
    }

    /// Encode the same deterministic image with caller-owned scheduling.
    /// Sorting, validation, initialization, and hashing call `step` between
    /// bounded work units. The output stays private until fully encoded;
    /// cancellation leaves this state unchanged.
    pub async fn encode_snapshot_cooperative(
        &self,
        limits: StateSnapshotLimits,
        mut step: impl AsyncFnMut(usize),
    ) -> Result<Vec<u8>, StateSnapshotError> {
        encoding::encode(self, limits, &mut step).await
    }

    /// Restore a complete state image after strict structural and semantic checks.
    pub fn decode_snapshot(
        input: &[u8],
        state_limits: StateLimits,
        limits: StateSnapshotLimits,
    ) -> Result<Self, StateSnapshotError> {
        ready(Self::decode_snapshot_cooperative(
            input,
            state_limits,
            limits,
            async |_| {},
        ))
    }

    /// Restore the same strictly validated image with caller-owned scheduling.
    /// Each callback accounts one bounded hash chunk, metadata entry, retry span,
    /// or sorting step. No state escapes before all integrity and semantic checks
    /// complete. Dropping the future releases only private decoding state.
    pub async fn decode_snapshot_cooperative(
        input: &[u8],
        state_limits: StateLimits,
        limits: StateSnapshotLimits,
        mut step: impl AsyncFnMut(usize),
    ) -> Result<Self, StateSnapshotError> {
        let shape = decode_snapshot_shape(input, state_limits, limits, &mut step).await?;

        let mut state = Self::new(state_limits);
        state.revision = shape.revision;
        let mut cursor = STATE_SNAPSHOT_HEADER_BYTES;
        let mut previous_partition: Option<PartitionIncarnation> = None;
        for _ in 0..shape.partition_count {
            let (partition, value, next, spans) = decode_partition(
                input,
                cursor,
                limits,
                state_limits.max_producers - state.producer_count,
                state_limits.max_retry_spans - state.retry_span_count,
                &mut step,
            )
            .await?;
            if previous_partition.is_some_and(|previous| previous >= partition)
                || state.partitions.contains_key(&partition)
                || state.addresses.contains_key(&value.address)
            {
                return Err(StateSnapshotError::UnsortedOrDuplicate);
            }
            previous_partition = Some(partition);
            state.producer_count += value.producers.len();
            state.retry_span_count += spans;
            state.addresses.insert(value.address.clone(), partition);
            state.partitions.insert(partition, value);
            cursor = next;
        }
        if state.producer_count != shape.producer_count
            || state.retry_span_count != shape.retry_span_count
        {
            return Err(StateSnapshotError::LengthMismatch);
        }

        let progress_bytes = shape
            .progress_count
            .checked_mul(PROGRESS_BYTES)
            .ok_or(StateSnapshotError::LengthOverflow)?;
        let assignment_bytes = shape
            .assignment_count
            .checked_mul(ASSIGNMENT_BYTES)
            .ok_or(StateSnapshotError::LengthOverflow)?;
        if cursor
            .checked_add(progress_bytes)
            .and_then(|value| value.checked_add(assignment_bytes))
            != Some(input.len())
        {
            return Err(StateSnapshotError::LengthMismatch);
        }

        let mut previous_progress: Option<ProgressSortKey> = None;
        for _ in 0..shape.progress_count {
            let (key, value, sort_key) = decode_progress(&input[cursor..cursor + PROGRESS_BYTES])?;
            let partition_id = progress_partition(&key);
            if previous_progress.is_some_and(|previous| previous >= sort_key) {
                return Err(StateSnapshotError::UnsortedOrDuplicate);
            }
            if !state.partitions.contains_key(&partition_id) {
                return Err(StateSnapshotError::UnknownPartition);
            }
            if state.progress.insert(key, value).is_some() {
                return Err(StateSnapshotError::UnsortedOrDuplicate);
            }
            let partition = state
                .partitions
                .get(&partition_id)
                .ok_or(StateSnapshotError::UnknownPartition)?;
            if value >= partition.next_offset {
                return Err(StateSnapshotError::ProgressBeyondState);
            }
            previous_progress = Some(sort_key);
            cursor += PROGRESS_BYTES;
            step(PROGRESS_BYTES).await;
        }

        let mut previous_assignment: Option<([u8; 16], [u8; 16])> = None;
        for _ in 0..shape.assignment_count {
            let (key, value) = decode_assignment(&input[cursor..cursor + ASSIGNMENT_BYTES])?;
            let sort_key = (*key.0.as_bytes(), *key.1.as_bytes());
            if previous_assignment.is_some_and(|previous| previous >= sort_key) {
                return Err(StateSnapshotError::UnsortedOrDuplicate);
            }
            if !state.partitions.contains_key(&key.1) {
                return Err(StateSnapshotError::UnknownPartition);
            }
            if state.assignments.insert(key, value).is_some() {
                return Err(StateSnapshotError::UnsortedOrDuplicate);
            }
            previous_assignment = Some(sort_key);
            cursor += ASSIGNMENT_BYTES;
            step(ASSIGNMENT_BYTES).await;
        }
        debug_assert_eq!(cursor, input.len());
        Ok(state)
    }
}

async fn decode_snapshot_shape(
    input: &[u8],
    state_limits: StateLimits,
    limits: StateSnapshotLimits,
    step: &mut impl AsyncFnMut(usize),
) -> Result<SnapshotShape, StateSnapshotError> {
    validate_limits(limits)?;
    enforce_limit(
        "state snapshot bytes",
        input.len(),
        limits.max_snapshot_bytes,
    )?;
    if input.len() < STATE_SNAPSHOT_HEADER_BYTES {
        return Err(StateSnapshotError::Truncated);
    }
    if &input[..8] != SNAPSHOT_MAGIC {
        return Err(StateSnapshotError::WrongMagic);
    }
    let version = read_u16(input, 8);
    if version != SNAPSHOT_VERSION {
        return Err(StateSnapshotError::UnsupportedVersion(version));
    }
    if read_u16(input, 10) != STATE_SNAPSHOT_HEADER_BYTES as u16
        || read_u32(input, 12) != 0
        || read_u32(input, 44) != 0
        || input[104..STATE_SNAPSHOT_HEADER_BYTES]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(StateSnapshotError::UnsupportedFields);
    }
    let total_bytes = u64_to_usize(read_u64(input, 16))?;
    let body_bytes = u64_to_usize(read_u64(input, 48))?;
    if total_bytes != input.len()
        || STATE_SNAPSHOT_HEADER_BYTES.checked_add(body_bytes) != Some(total_bytes)
    {
        return Err(StateSnapshotError::LengthMismatch);
    }
    if snapshot_digest_cooperative(input, step).await.as_bytes()
        != &input[SNAPSHOT_DIGEST_START..SNAPSHOT_DIGEST_END]
    {
        return Err(StateSnapshotError::DigestMismatch);
    }
    let shape = SnapshotShape {
        revision: read_u64(input, 24),
        partition_count: read_u32(input, 32) as usize,
        progress_count: read_u32(input, 36) as usize,
        assignment_count: read_u32(input, 40) as usize,
        producer_count: u64_to_usize(read_u64(input, 88))?,
        retry_span_count: u64_to_usize(read_u64(input, 96))?,
    };
    enforce_limit(
        "partitions",
        shape.partition_count,
        state_limits.max_partitions,
    )?;
    enforce_limit(
        "progress owners",
        shape.progress_count,
        state_limits.max_progress,
    )?;
    enforce_limit(
        "assignments",
        shape.assignment_count,
        state_limits.max_assignments,
    )?;
    enforce_limit(
        "producers",
        shape.producer_count,
        state_limits.max_producers,
    )?;
    enforce_limit(
        "producer retry spans",
        shape.retry_span_count,
        state_limits.max_retry_spans,
    )?;
    Ok(shape)
}

fn encode_progress(output: &mut [u8], key: &ProgressKey, offset: Offset) {
    let (kind, owner, partition) = match key {
        ProgressKey::Subscription(owner, partition) => (1, owner.as_bytes(), partition),
        ProgressKey::ConsumerGroup(owner, partition) => (2, owner.as_bytes(), partition),
    };
    output[0] = kind;
    output[8..24].copy_from_slice(owner);
    output[24..40].copy_from_slice(partition.as_bytes());
    put_u64(output, 40, offset.get());
}

fn decode_progress(
    input: &[u8],
) -> Result<(ProgressKey, Offset, ProgressSortKey), StateSnapshotError> {
    if input[1..8].iter().any(|byte| *byte != 0) {
        return Err(StateSnapshotError::UnsupportedFields);
    }
    let owner = array_16(input, 8);
    let partition = PartitionIncarnation::from_bytes(array_16(input, 24));
    require_nonzero("progress owner", &owner)?;
    require_nonzero("partition", partition.as_bytes())?;
    let key = match input[0] {
        1 => ProgressKey::Subscription(SubscriptionId::from_bytes(owner), partition),
        2 => ProgressKey::ConsumerGroup(ConsumerGroupId::from_bytes(owner), partition),
        _ => return Err(StateSnapshotError::InvalidProgress),
    };
    Ok((
        key,
        Offset::new(read_u64(input, 40)),
        (input[0], owner, *partition.as_bytes()),
    ))
}

fn encode_assignment(output: &mut [u8], key: AssignmentKey, value: AssignmentState) {
    output[..16].copy_from_slice(key.0.as_bytes());
    output[16..32].copy_from_slice(key.1.as_bytes());
    put_u64(output, 32, value.epoch);
    if let Some(member) = value.member {
        output[40] = 1;
        output[48..64].copy_from_slice(member.as_bytes());
    }
}

fn decode_assignment(input: &[u8]) -> Result<(AssignmentKey, AssignmentState), StateSnapshotError> {
    if input[41..48].iter().any(|byte| *byte != 0) {
        return Err(StateSnapshotError::UnsupportedFields);
    }
    let group = ConsumerGroupId::from_bytes(array_16(input, 0));
    let partition = PartitionIncarnation::from_bytes(array_16(input, 16));
    require_nonzero("consumer group", group.as_bytes())?;
    require_nonzero("partition", partition.as_bytes())?;
    let member_bytes = array_16(input, 48);
    let member = match input[40] {
        0 if member_bytes == [0_u8; 16] => None,
        1 => {
            require_nonzero("consumer member", &member_bytes)?;
            Some(ConsumerMemberId::from_bytes(member_bytes))
        }
        _ => return Err(StateSnapshotError::InvalidAssignment),
    };
    let epoch = read_u64(input, 32);
    if epoch == 0 {
        return Err(StateSnapshotError::InvalidAssignment);
    }
    Ok((
        AssignmentKey(group, partition),
        AssignmentState { epoch, member },
    ))
}

fn progress_sort_key(key: &ProgressKey) -> ProgressSortKey {
    match key {
        ProgressKey::Subscription(owner, partition) => {
            (1, *owner.as_bytes(), *partition.as_bytes())
        }
        ProgressKey::ConsumerGroup(owner, partition) => {
            (2, *owner.as_bytes(), *partition.as_bytes())
        }
    }
}

fn progress_partition(key: &ProgressKey) -> PartitionIncarnation {
    match key {
        ProgressKey::Subscription(_, partition) | ProgressKey::ConsumerGroup(_, partition) => {
            *partition
        }
    }
}

fn encode_retention(retention: RetentionPolicy) -> (u32, u64, u64) {
    let mut flags = 0_u32;
    let age = retention.max_age_millis.map_or(0, |value| {
        flags |= RETENTION_MAX_AGE;
        value.get()
    });
    let bytes = retention.max_bytes.map_or(0, |value| {
        flags |= RETENTION_MAX_BYTES;
        value.get()
    });
    (flags, age, bytes)
}

fn decode_retention(
    flags: u32,
    age: u64,
    bytes: u64,
) -> Result<RetentionPolicy, StateSnapshotError> {
    if flags & !(RETENTION_MAX_AGE | RETENTION_MAX_BYTES) != 0 {
        return Err(StateSnapshotError::InvalidRetention);
    }
    let max_age_millis = decode_retention_value(flags, RETENTION_MAX_AGE, age)?;
    let max_bytes = decode_retention_value(flags, RETENTION_MAX_BYTES, bytes)?;
    Ok(RetentionPolicy {
        max_age_millis,
        max_bytes,
    })
}

fn decode_retention_value(
    flags: u32,
    flag: u32,
    value: u64,
) -> Result<Option<std::num::NonZeroU64>, StateSnapshotError> {
    match (flags & flag != 0, std::num::NonZeroU64::new(value)) {
        (false, None) => Ok(None),
        (true, Some(value)) => Ok(Some(value)),
        _ => Err(StateSnapshotError::InvalidRetention),
    }
}

fn validate_name(value: &str, limits: StateSnapshotLimits) -> Result<(), StateSnapshotError> {
    if value.is_empty() {
        return Err(StateSnapshotError::EmptyName);
    }
    enforce_limit("state name bytes", value.len(), limits.max_name_bytes)?;
    if value.len() > u16::MAX as usize {
        return Err(StateSnapshotError::LengthOverflow);
    }
    Ok(())
}

#[cfg(test)]
fn snapshot_digest(input: &[u8]) -> Digest {
    ready(snapshot_digest_cooperative(input, &mut async |_| {}))
}

async fn snapshot_digest_cooperative(input: &[u8], step: &mut impl AsyncFnMut(usize)) -> Digest {
    let mut hasher = Hasher::new(SNAPSHOT_HASH_CONTEXT);
    hasher.update(&input[..SNAPSHOT_DIGEST_START]);
    hasher.update(&[0; SNAPSHOT_DIGEST_END - SNAPSHOT_DIGEST_START]);
    for bytes in input[SNAPSHOT_DIGEST_END..].chunks(64 * 1024) {
        hasher.update(bytes);
        step(bytes.len()).await;
    }
    hasher.finish()
}

fn require_nonzero(kind: &'static str, input: &[u8]) -> Result<(), StateSnapshotError> {
    if input.iter().all(|byte| *byte == 0) {
        Err(StateSnapshotError::ZeroIdentity(kind))
    } else {
        Ok(())
    }
}

fn validate_limits(limits: StateSnapshotLimits) -> Result<(), StateSnapshotError> {
    if limits.max_snapshot_bytes < STATE_SNAPSHOT_HEADER_BYTES || limits.max_name_bytes == 0 {
        Err(StateSnapshotError::InvalidLimits)
    } else {
        Ok(())
    }
}

fn enforce_limit(
    kind: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), StateSnapshotError> {
    if actual > limit {
        Err(StateSnapshotError::LimitExceeded {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

fn usize_to_u16(value: usize) -> Result<u16, StateSnapshotError> {
    u16::try_from(value).map_err(|_| StateSnapshotError::LengthOverflow)
}

fn usize_to_u32(value: usize) -> Result<u32, StateSnapshotError> {
    u32::try_from(value).map_err(|_| StateSnapshotError::LengthOverflow)
}

fn usize_to_u64(value: usize) -> Result<u64, StateSnapshotError> {
    u64::try_from(value).map_err(|_| StateSnapshotError::LengthOverflow)
}

fn u64_to_usize(value: u64) -> Result<usize, StateSnapshotError> {
    usize::try_from(value).map_err(|_| StateSnapshotError::LengthOverflow)
}

fn array_16(input: &[u8], offset: usize) -> [u8; 16] {
    input[offset..offset + 16]
        .try_into()
        .expect("validated fixed-width field")
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

fn read_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(
        input[offset..offset + 2]
            .try_into()
            .expect("fixed-width field"),
    )
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        input[offset..offset + 4]
            .try_into()
            .expect("fixed-width field"),
    )
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        input[offset..offset + 8]
            .try_into()
            .expect("fixed-width field"),
    )
}

/// Canonical state checkpoint codec failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StateSnapshotError {
    #[error("wrong canonical state snapshot magic")]
    /// Snapshot bytes do not identify the canonical state format.
    WrongMagic,
    #[error("unsupported canonical state snapshot version {0}")]
    /// The snapshot format version is unsupported.
    UnsupportedVersion(u16),
    #[error("unsupported canonical state snapshot fields")]
    /// Reserved or unsupported snapshot fields are present.
    UnsupportedFields,
    #[error("canonical state snapshot is truncated")]
    /// Snapshot bytes end before a complete field.
    Truncated,
    #[error("canonical state snapshot lengths do not agree")]
    /// Encoded lengths disagree with available snapshot bytes.
    LengthMismatch,
    #[error("canonical state snapshot digest mismatch")]
    /// Snapshot integrity does not match its checksum.
    DigestMismatch,
    #[error("canonical state snapshot resource limits are invalid")]
    /// Snapshot decoding bounds are invalid.
    InvalidLimits,
    #[error("canonical state partition is invalid")]
    /// A decoded partition entry violates its state contract.
    InvalidPartition,
    #[error("canonical state progress entry is invalid")]
    /// A decoded progress entry violates its state contract.
    InvalidProgress,
    #[error("canonical state assignment entry is invalid")]
    /// A decoded assignment entry violates its state contract.
    InvalidAssignment,
    #[error("canonical state retention policy is invalid")]
    /// A decoded retention policy is invalid.
    InvalidRetention,
    #[error("canonical state entries are unsorted or duplicated")]
    /// Canonical entries are out of order or repeat a key.
    UnsortedOrDuplicate,
    #[error("canonical state entry references an unknown partition")]
    /// The referenced partition incarnation does not exist.
    UnknownPartition,
    #[error("canonical progress lies beyond partition state")]
    /// Decoded progress passes the available partition state.
    ProgressBeyondState,
    #[error("canonical state name is empty")]
    /// A stream or topic name is empty.
    EmptyName,
    #[error("canonical state name is not UTF-8")]
    /// A stream or topic name is not valid UTF-8.
    InvalidUtf8,
    #[error("canonical state {0} identity is zero")]
    /// A required persistent identity is zero.
    ZeroIdentity(&'static str),
    #[error("canonical state snapshot integer or length overflow")]
    /// Encoded byte or entry counts overflow the supported range.
    LengthOverflow,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    /// The operation or snapshot exceeds a configured resource bound.
    LimitExceeded {
        /// Resource bound that rejected the snapshot.
        kind: &'static str,
        /// Observed size or count.
        actual: usize,
        /// Configured maximum size or count.
        limit: usize,
    },
}
