use super::{
    ASSIGNMENT_BYTES, CanonicalState, PROGRESS_BYTES, SNAPSHOT_DIGEST_START, SNAPSHOT_MAGIC,
    SNAPSHOT_VERSION, STATE_SNAPSHOT_HEADER_BYTES, StateSnapshotError, StateSnapshotLimits,
    encode_assignment, encode_progress, enforce_limit, partition, progress_sort_key,
    snapshot_digest_cooperative, usize_to_u32, usize_to_u64, validate_limits, validate_name,
};
use crate::state::{
    AssignmentKey, AssignmentState, CanonicalPartition, CoordinatorGroup, ProgressKey,
};
use ozzy_proto::layout::{self, Fixed};
use ozzy_proto::{ConsumerGroupId, Offset, PartitionIncarnation};
use std::sync::Arc;

pub(super) async fn encode(
    state: &CanonicalState,
    limits: StateSnapshotLimits,
    step: &mut impl AsyncFnMut(usize),
) -> Result<Vec<u8>, StateSnapshotError> {
    validate_limits(limits)?;
    let entries = Entries::sorted(state, step).await;
    let body_bytes = entries.body_bytes(state, limits, step).await?;
    let total_bytes = STATE_SNAPSHOT_HEADER_BYTES
        .checked_add(body_bytes)
        .ok_or(StateSnapshotError::LengthOverflow)?;
    enforce_limit(
        "state snapshot bytes",
        total_bytes,
        limits.max_snapshot_bytes,
    )?;
    let mut output = zeroed(total_bytes, step).await;
    header(&mut output, state, entries.counts(), body_bytes)?;
    entries
        .write(&mut output, state.revision, limits, step)
        .await?;
    let digest = snapshot_digest_cooperative(&output, step).await?;
    layout::patch(&mut output, SNAPSHOT_DIGEST_START, *digest.as_bytes())?;
    Ok(output)
}

/// Live state entries in their deterministic snapshot order.
struct Entries<'s> {
    partitions: Vec<(&'s PartitionIncarnation, &'s CanonicalPartition)>,
    progress: Vec<(&'s ProgressKey, &'s Offset)>,
    assignments: Vec<(&'s AssignmentKey, &'s AssignmentState)>,
    groups: Vec<(&'s ConsumerGroupId, &'s Arc<CoordinatorGroup>)>,
}

impl<'s> Entries<'s> {
    async fn sorted(state: &'s CanonicalState, step: &mut impl AsyncFnMut(usize)) -> Self {
        Self {
            partitions: sorted(
                state.partitions.iter(),
                |(left, _), (right, _)| left.cmp(right),
                step,
            )
            .await,
            progress: sorted(
                state.progress.iter(),
                |(left, _), (right, _)| progress_sort_key(left).cmp(&progress_sort_key(right)),
                step,
            )
            .await,
            assignments: sorted(
                state.assignments.iter(),
                |(left, _), (right, _)| {
                    (*left.0.as_bytes(), *left.1.as_bytes())
                        .cmp(&(*right.0.as_bytes(), *right.1.as_bytes()))
                },
                step,
            )
            .await,
            groups: sorted(
                state.groups.iter(),
                |(left, _), (right, _)| left.cmp(right),
                step,
            )
            .await,
        }
    }

    const fn counts(&self) -> [usize; 4] {
        [
            self.partitions.len(),
            self.progress.len(),
            self.assignments.len(),
            self.groups.len(),
        ]
    }

    async fn body_bytes(
        &self,
        state: &CanonicalState,
        limits: StateSnapshotLimits,
        step: &mut impl AsyncFnMut(usize),
    ) -> Result<usize, StateSnapshotError> {
        let mut group_bytes = 0usize;
        for (_, group) in &self.groups {
            group_bytes = group_bytes
                .checked_add(super::groups::size_cooperative(group, limits, step).await?)
                .ok_or(StateSnapshotError::LengthOverflow)?;
        }
        let partition_bytes = partition_size(state, limits, step).await?;
        let progress_bytes = self
            .progress
            .len()
            .checked_mul(PROGRESS_BYTES)
            .ok_or(StateSnapshotError::LengthOverflow)?;
        let assignment_bytes = self
            .assignments
            .len()
            .checked_mul(ASSIGNMENT_BYTES)
            .ok_or(StateSnapshotError::LengthOverflow)?;
        partition_bytes
            .checked_add(progress_bytes)
            .and_then(|value| value.checked_add(assignment_bytes))
            .and_then(|value| value.checked_add(group_bytes))
            .ok_or(StateSnapshotError::LengthOverflow)
    }

    /// Write every entry after the header of an output already sized for them.
    async fn write(
        self,
        output: &mut [u8],
        revision: u64,
        limits: StateSnapshotLimits,
        step: &mut impl AsyncFnMut(usize),
    ) -> Result<(), StateSnapshotError> {
        let mut cursor = STATE_SNAPSHOT_HEADER_BYTES;
        for (partition, value) in self.partitions {
            let entry = (*partition, value);
            cursor = partition::encode_partition(output, cursor, entry, revision, step).await?;
        }
        for (key, offset) in self.progress {
            encode_progress(layout::record_mut(output, cursor)?, key, *offset);
            cursor += PROGRESS_BYTES;
            step(PROGRESS_BYTES).await;
        }
        for (key, value) in self.assignments {
            encode_assignment(layout::record_mut(output, cursor)?, *key, *value);
            cursor += ASSIGNMENT_BYTES;
            step(ASSIGNMENT_BYTES).await;
        }
        for (_, group) in self.groups {
            cursor = super::groups::encode(output, cursor, group, limits, step).await?;
        }
        debug_assert_eq!(cursor, output.len());
        Ok(())
    }
}

pub(super) async fn sorted<T>(
    values: impl ExactSizeIterator<Item = T>,
    compare: fn(&T, &T) -> std::cmp::Ordering,
    step: &mut impl AsyncFnMut(usize),
) -> Vec<T> {
    let mut entries = Vec::with_capacity(values.len());
    for value in values {
        entries.push(value);
        step(size_of::<T>()).await;
    }
    ozzy_journal::work::sort_by(&mut entries, compare, step).await;
    entries
}

async fn zeroed(bytes: usize, step: &mut impl AsyncFnMut(usize)) -> Vec<u8> {
    let mut output = Vec::with_capacity(bytes);
    while output.len() < bytes {
        let chunk = (bytes - output.len()).min(64 * 1024);
        output.resize(output.len() + chunk, 0);
        step(chunk).await;
    }
    output
}

fn header(
    output: &mut [u8],
    state: &CanonicalState,
    counts: [usize; 4],
    body_bytes: usize,
) -> Result<(), StateSnapshotError> {
    let total_bytes = usize_to_u64(output.len())?;
    let header: &mut [u8; STATE_SNAPSHOT_HEADER_BYTES] = layout::record_mut(output, 0)?;
    header.put::<0, 8>(*SNAPSHOT_MAGIC);
    header.put_u16::<8>(SNAPSHOT_VERSION);
    header.put_u16::<10>(STATE_SNAPSHOT_HEADER_BYTES as u16);
    header.put_u64::<16>(total_bytes);
    header.put_u64::<24>(state.revision);
    header.put_u32::<32>(usize_to_u32(counts[0])?);
    header.put_u32::<36>(usize_to_u32(counts[1])?);
    header.put_u32::<40>(usize_to_u32(counts[2])?);
    header.put_u64::<48>(usize_to_u64(body_bytes)?);
    header.put_u64::<88>(usize_to_u64(state.producer_count)?);
    header.put_u64::<96>(usize_to_u64(state.retry_span_count)?);
    header.put_u32::<104>(usize_to_u32(counts[3])?);
    header.put_u64::<108>(usize_to_u64(state.group_member_count)?);
    header.put_u64::<116>(usize_to_u64(state.group_target_count)?);

    Ok(())
}

async fn partition_size(
    state: &CanonicalState,
    limits: StateSnapshotLimits,
    step: &mut impl AsyncFnMut(usize),
) -> Result<usize, StateSnapshotError> {
    let mut partition_bytes = 0_usize;
    for value in state.partitions.values() {
        validate_name(&value.address.stream, limits)?;
        validate_name(&value.address.topic, limits)?;
        partition_bytes = partition_bytes
            .checked_add(partition::partition_bytes(value, step).await?)
            .ok_or(StateSnapshotError::LengthOverflow)?;
    }
    Ok(partition_bytes)
}
