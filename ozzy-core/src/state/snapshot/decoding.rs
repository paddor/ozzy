//! Snapshot restore: header shape, limits and digest checks, then the
//! partition, progress, assignment and group tables in order.

use super::partition::decode_partition;
use super::{
    ASSIGNMENT_BYTES, PROGRESS_BYTES, ProgressSortKey, SNAPSHOT_DIGEST_END, SNAPSHOT_DIGEST_START,
    SNAPSHOT_MAGIC, SNAPSHOT_VERSION, STATE_SNAPSHOT_HEADER_BYTES, StateSnapshotError,
    StateSnapshotLimits, decode_assignment, decode_progress, enforce_limit, groups,
    progress_partition, snapshot_digest_cooperative, u64_to_usize, validate_limits,
};
use crate::state::{CanonicalState, ProgressKey, StateLimits};
use ozzy_proto::PartitionIncarnation;
use ozzy_proto::layout::{self, Fixed, Reader};

#[derive(Debug, Clone, Copy)]
struct SnapshotShape {
    revision: u64,
    partition_count: usize,
    progress_count: usize,
    assignment_count: usize,
    producer_count: usize,
    retry_span_count: usize,
    group_count: usize,
    group_member_count: usize,
    group_target_count: usize,
}

/// Restore a complete image: header shape and digest first, then each entry
/// table in order, with no state escaping before every check passes.
pub(super) async fn decode(
    input: &[u8],
    state_limits: StateLimits,
    limits: StateSnapshotLimits,
    step: &mut impl AsyncFnMut(usize),
) -> Result<CanonicalState, StateSnapshotError> {
    let shape = decode_snapshot_shape(input, state_limits, limits, step).await?;
    let mut state = CanonicalState::new(state_limits);
    state.revision = shape.revision;
    let mut entries = Reader::new(input);
    entries.take(STATE_SNAPSHOT_HEADER_BYTES)?;
    decode_partitions(
        &mut state,
        &mut entries,
        shape.partition_count,
        limits,
        step,
    )
    .await?;
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
    let fixed_bytes = progress_bytes
        .checked_add(assignment_bytes)
        .ok_or(StateSnapshotError::LengthOverflow)?;
    if fixed_bytes > entries.rest().len() {
        return Err(StateSnapshotError::LengthMismatch);
    }
    decode_progress_entries(&mut state, &mut entries, shape.progress_count, step).await?;
    decode_assignments(&mut state, &mut entries, shape.assignment_count, step).await?;
    groups::decode(&mut state, &mut entries, shape.group_count, limits, step).await?;
    if !entries.rest().is_empty()
        || state.group_member_count != shape.group_member_count
        || state.group_target_count != shape.group_target_count
    {
        return Err(StateSnapshotError::LengthMismatch);
    }
    Ok(state)
}

/// Decode partition entries in ascending order, each with a unique address.
async fn decode_partitions(
    state: &mut CanonicalState,
    entries: &mut Reader<'_>,
    count: usize,
    limits: StateSnapshotLimits,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(), StateSnapshotError> {
    let mut previous: Option<PartitionIncarnation> = None;
    for _ in 0..count {
        let (partition, value, spans) = decode_partition(entries, limits, state, step).await?;
        if previous.is_some_and(|previous| previous >= partition)
            || state.partitions.contains_key(&partition)
            || state.addresses.contains_key(&value.address)
        {
            return Err(StateSnapshotError::UnsortedOrDuplicate);
        }
        previous = Some(partition);
        state.producer_count += value.producers.len();
        state.retry_span_count += spans;
        state.addresses.insert(value.address.clone(), partition);
        state.partitions.insert(partition, value);
    }
    Ok(())
}

/// Decode progress entries in ascending order, none beyond its partition's
/// records.
async fn decode_progress_entries(
    state: &mut CanonicalState,
    entries: &mut Reader<'_>,
    count: usize,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(), StateSnapshotError> {
    let mut previous: Option<ProgressSortKey> = None;
    for _ in 0..count {
        let (key, value, sort_key) = decode_progress(entries.chunk()?)?;
        let partition_id = progress_partition(&key);
        if previous.is_some_and(|previous| previous >= sort_key) {
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
        if value > partition.next_offset {
            return Err(StateSnapshotError::ProgressBeyondState);
        }
        previous = Some(sort_key);
        step(PROGRESS_BYTES).await;
    }
    Ok(())
}

async fn decode_assignments(
    state: &mut CanonicalState,
    entries: &mut Reader<'_>,
    count: usize,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(), StateSnapshotError> {
    let mut previous_assignment: Option<([u8; 16], [u8; 16])> = None;
    for _ in 0..count {
        let (key, value) = decode_assignment(entries.chunk()?)?;
        let sort_key = (*key.0.as_bytes(), *key.1.as_bytes());
        if previous_assignment.is_some_and(|previous| previous >= sort_key) {
            return Err(StateSnapshotError::UnsortedOrDuplicate);
        }
        if !state.partitions.contains_key(&key.1) {
            return Err(StateSnapshotError::UnknownPartition);
        }
        if state
            .progress
            .get(&ProgressKey::ConsumerGroup(key.0, key.1))
            .is_none_or(|cursor| *cursor < value.resume_cursor)
        {
            return Err(StateSnapshotError::InvalidAssignment);
        }
        if state.assignments.insert(key, value).is_some() {
            return Err(StateSnapshotError::UnsortedOrDuplicate);
        }
        previous_assignment = Some(sort_key);
        step(ASSIGNMENT_BYTES).await;
    }
    Ok(())
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
    let header: &[u8; STATE_SNAPSHOT_HEADER_BYTES] = layout::record(input, 0)?;
    if header.array::<0, 8>() != *SNAPSHOT_MAGIC {
        return Err(StateSnapshotError::WrongMagic);
    }
    let version = header.u16_at::<8>();
    if version != SNAPSHOT_VERSION {
        return Err(StateSnapshotError::UnsupportedVersion(version));
    }
    if header.u16_at::<10>() != STATE_SNAPSHOT_HEADER_BYTES as u16
        || header.u32_at::<12>() != 0
        || header.u32_at::<44>() != 0
        || header
            .slice::<124, { STATE_SNAPSHOT_HEADER_BYTES - 124 }>()
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(StateSnapshotError::UnsupportedFields);
    }
    let total_bytes = u64_to_usize(header.u64_at::<16>())?;
    let body_bytes = u64_to_usize(header.u64_at::<48>())?;
    if total_bytes != input.len()
        || STATE_SNAPSHOT_HEADER_BYTES.checked_add(body_bytes) != Some(total_bytes)
    {
        return Err(StateSnapshotError::LengthMismatch);
    }
    if *snapshot_digest_cooperative(input, step).await?.as_bytes()
        != header.array::<SNAPSHOT_DIGEST_START, { SNAPSHOT_DIGEST_END - SNAPSHOT_DIGEST_START }>()
    {
        return Err(StateSnapshotError::DigestMismatch);
    }
    let shape = SnapshotShape::decode(header)?;
    shape.enforce(state_limits)?;
    Ok(shape)
}

impl SnapshotShape {
    fn decode(header: &[u8; STATE_SNAPSHOT_HEADER_BYTES]) -> Result<Self, StateSnapshotError> {
        Ok(Self {
            revision: header.u64_at::<24>(),
            partition_count: header.u32_at::<32>() as usize,
            progress_count: header.u32_at::<36>() as usize,
            assignment_count: header.u32_at::<40>() as usize,
            producer_count: u64_to_usize(header.u64_at::<88>())?,
            retry_span_count: u64_to_usize(header.u64_at::<96>())?,
            group_count: header.u32_at::<104>() as usize,
            group_member_count: u64_to_usize(header.u64_at::<108>())?,
            group_target_count: u64_to_usize(header.u64_at::<116>())?,
        })
    }

    /// Check every declared count against the live-state limits before any
    /// entry is decoded.
    fn enforce(&self, limits: StateLimits) -> Result<(), StateSnapshotError> {
        enforce_limit("partitions", self.partition_count, limits.max_partitions)?;
        enforce_limit("consumer groups", self.group_count, limits.max_groups)?;
        enforce_limit(
            "group members",
            self.group_member_count,
            limits.max_group_members,
        )?;
        enforce_limit(
            "group source partitions",
            self.group_target_count,
            limits.max_group_partitions,
        )?;
        enforce_limit("progress owners", self.progress_count, limits.max_progress)?;
        enforce_limit("assignments", self.assignment_count, limits.max_assignments)?;
        enforce_limit("producers", self.producer_count, limits.max_producers)?;
        enforce_limit(
            "producer retry spans",
            self.retry_span_count,
            limits.max_retry_spans,
        )
    }
}
