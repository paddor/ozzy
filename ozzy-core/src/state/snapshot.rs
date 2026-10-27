//! Deterministic checkpoint image for canonical committed state.

use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_journal::operation::{Digest, RetentionPolicy};
use ozzy_proto::layout::{Fixed, LayoutError};
use ozzy_proto::{ConsumerGroupId, ConsumerMemberId, Offset, PartitionIncarnation, SubscriptionId};
use thiserror::Error;

use super::{AssignmentKey, AssignmentState, CanonicalState, ProgressKey, StateLimits};

#[cfg(test)]
mod cooperation_tests;
mod decoding;
mod encoding;
mod groups;
mod partition;

#[expect(
    clippy::unreachable,
    reason = "the synchronous codec passes a step that never waits"
)]
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

/// Encoded snapshot bytes of one held scheduled record.
pub const PENDING_SCHEDULE_SNAPSHOT_BYTES: usize = 48;

const SNAPSHOT_MAGIC: &[u8; 8] = b"OZYSTA01";
const SNAPSHOT_VERSION: u16 = 4;
const PROGRESS_BYTES: usize = 48;
const ASSIGNMENT_BYTES: usize = 80;
const SNAPSHOT_DIGEST_START: usize = 56;
const SNAPSHOT_DIGEST_END: usize = 88;
const SNAPSHOT_HASH_CONTEXT: &str = "ozzy canonical state snapshot v1";
const SNAPSHOT_SCHEMA_CONTEXT: &str = "ozzy canonical state snapshot schema";
const SNAPSHOT_SCHEMA: &[u8] =
    b"OZYSTA01:256:partition144-dedup-time-and-window-retired-epoch-delay+names+producer80+retry-span40-stored-proof-or-schedule-op+pending48:progress48:assignment80-plan-and-resume:coordinator-group-plan:network-order";
const RETENTION_MAX_AGE: u32 = 1 << 0;
const RETENTION_MAX_BYTES: u32 = 1 << 1;

type ProgressSortKey = (u8, [u8; 16], [u8; 16]);

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
    hasher.update(b"OZYCIMG-v1:104:core+optional-partition-OZYDIDX-v1-header64+rows32");
    hasher.update(b"progress-assignment:epoch8+member16");
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
        decoding::decode(input, state_limits, limits, &mut step).await
    }
}

fn encode_progress(output: &mut [u8; PROGRESS_BYTES], key: &ProgressKey, offset: Offset) {
    let (kind, owner, partition) = match key {
        ProgressKey::Subscription(owner, partition) => (1, owner.as_bytes(), partition),
        ProgressKey::ConsumerGroup(owner, partition) => (2, owner.as_bytes(), partition),
    };
    output.put_u8::<0>(kind);
    output.put::<8, 16>(*owner);
    output.put::<24, 16>(*partition.as_bytes());
    output.put_u64::<40>(offset.get());
}

fn decode_progress(
    input: &[u8; PROGRESS_BYTES],
) -> Result<(ProgressKey, Offset, ProgressSortKey), StateSnapshotError> {
    if input.slice::<1, 7>().iter().any(|byte| *byte != 0) {
        return Err(StateSnapshotError::UnsupportedFields);
    }
    let owner = input.array::<8, 16>();
    let partition = PartitionIncarnation::from_bytes(input.array::<24, 16>());
    require_nonzero("progress owner", &owner)?;
    require_nonzero("partition", partition.as_bytes())?;
    let key = match input.u8_at::<0>() {
        1 => ProgressKey::Subscription(SubscriptionId::from_bytes(owner), partition),
        2 => ProgressKey::ConsumerGroup(ConsumerGroupId::from_bytes(owner), partition),
        _ => return Err(StateSnapshotError::InvalidProgress),
    };
    Ok((
        key,
        Offset::new(input.u64_at::<40>()),
        (input.u8_at::<0>(), owner, *partition.as_bytes()),
    ))
}

fn encode_assignment(
    output: &mut [u8; ASSIGNMENT_BYTES],
    key: AssignmentKey,
    value: AssignmentState,
) {
    output.put::<0, 16>(*key.0.as_bytes());
    output.put::<16, 16>(*key.1.as_bytes());
    output.put_u64::<32>(value.epoch);
    output.put_u64::<64>(value.plan_revision);
    output.put_u64::<72>(value.resume_cursor.get());
    if let Some(member) = value.member {
        output.put_u8::<40>(1);
        output.put::<48, 16>(*member.as_bytes());
    }
}

fn decode_assignment(
    input: &[u8; ASSIGNMENT_BYTES],
) -> Result<(AssignmentKey, AssignmentState), StateSnapshotError> {
    if input.slice::<41, 7>().iter().any(|byte| *byte != 0) {
        return Err(StateSnapshotError::UnsupportedFields);
    }
    let group = ConsumerGroupId::from_bytes(input.array::<0, 16>());
    let partition = PartitionIncarnation::from_bytes(input.array::<16, 16>());
    require_nonzero("consumer group", group.as_bytes())?;
    require_nonzero("partition", partition.as_bytes())?;
    let member_bytes = input.array::<48, 16>();
    let member = match input.u8_at::<40>() {
        0 if member_bytes == [0_u8; 16] => None,
        1 => {
            require_nonzero("consumer member", &member_bytes)?;
            Some(ConsumerMemberId::from_bytes(member_bytes))
        }
        _ => return Err(StateSnapshotError::InvalidAssignment),
    };
    let epoch = input.u64_at::<32>();
    let plan_revision = input.u64_at::<64>();
    if epoch == 0 || plan_revision < epoch {
        return Err(StateSnapshotError::InvalidAssignment);
    }
    Ok((
        AssignmentKey(group, partition),
        AssignmentState {
            epoch,
            member,
            plan_revision,
            resume_cursor: Offset::new(input.u64_at::<72>()),
        },
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
fn snapshot_digest(input: &[u8]) -> Result<Digest, StateSnapshotError> {
    ready(snapshot_digest_cooperative(input, &mut async |_| {}))
}

async fn snapshot_digest_cooperative(
    input: &[u8],
    step: &mut impl AsyncFnMut(usize),
) -> Result<Digest, StateSnapshotError> {
    let (head, body) = input
        .split_first_chunk::<SNAPSHOT_DIGEST_END>()
        .ok_or(StateSnapshotError::Truncated)?;
    let mut hasher = Hasher::new(SNAPSHOT_HASH_CONTEXT);
    hasher.update(head.slice::<0, SNAPSHOT_DIGEST_START>());
    hasher.update(&[0; SNAPSHOT_DIGEST_END - SNAPSHOT_DIGEST_START]);
    for bytes in body.chunks(64 * 1024) {
        hasher.update(bytes);
        step(bytes.len()).await;
    }
    Ok(hasher.finish())
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

impl From<LayoutError> for StateSnapshotError {
    fn from(error: LayoutError) -> Self {
        match error {
            LayoutError::Truncated { .. } => Self::Truncated,
            LayoutError::Trailing(_) => Self::LengthMismatch,
            LayoutError::Overflow { .. } => Self::LengthOverflow,
        }
    }
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
    #[error("canonical coordinator group is invalid")]
    /// A coordinator image violates its membership or source intent contract.
    InvalidGroup,
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
