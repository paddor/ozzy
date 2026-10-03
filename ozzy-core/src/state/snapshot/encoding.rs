use super::{
    ASSIGNMENT_BYTES, CanonicalState, PROGRESS_BYTES, SNAPSHOT_DIGEST_END, SNAPSHOT_DIGEST_START,
    SNAPSHOT_MAGIC, SNAPSHOT_VERSION, STATE_SNAPSHOT_HEADER_BYTES, StateSnapshotError,
    StateSnapshotLimits, encode_assignment, encode_progress, enforce_limit, partition,
    progress_sort_key, put_u16, put_u32, put_u64, snapshot_digest_cooperative, usize_to_u32,
    usize_to_u64, validate_limits, validate_name,
};

pub(super) async fn encode(
    state: &CanonicalState,
    limits: StateSnapshotLimits,
    step: &mut impl AsyncFnMut(usize),
) -> Result<Vec<u8>, StateSnapshotError> {
    validate_limits(limits)?;
    let partitions = sorted(
        state.partitions.iter(),
        |(left, _), (right, _)| left.cmp(right),
        step,
    )
    .await;
    let progress = sorted(
        state.progress.iter(),
        |(left, _), (right, _)| progress_sort_key(left).cmp(&progress_sort_key(right)),
        step,
    )
    .await;
    let assignments = sorted(
        state.assignments.iter(),
        |(left, _), (right, _)| {
            (*left.0.as_bytes(), *left.1.as_bytes())
                .cmp(&(*right.0.as_bytes(), *right.1.as_bytes()))
        },
        step,
    )
    .await;

    let mut partition_bytes = 0_usize;
    for (_, value) in &partitions {
        validate_name(&value.address.stream, limits)?;
        validate_name(&value.address.topic, limits)?;
        partition_bytes = partition_bytes
            .checked_add(partition::partition_bytes(value, step).await?)
            .ok_or(StateSnapshotError::LengthOverflow)?;
    }
    let progress_bytes = progress
        .len()
        .checked_mul(PROGRESS_BYTES)
        .ok_or(StateSnapshotError::LengthOverflow)?;
    let assignment_bytes = assignments
        .len()
        .checked_mul(ASSIGNMENT_BYTES)
        .ok_or(StateSnapshotError::LengthOverflow)?;
    let body_bytes = partition_bytes
        .checked_add(progress_bytes)
        .and_then(|value| value.checked_add(assignment_bytes))
        .ok_or(StateSnapshotError::LengthOverflow)?;
    let total_bytes = STATE_SNAPSHOT_HEADER_BYTES
        .checked_add(body_bytes)
        .ok_or(StateSnapshotError::LengthOverflow)?;
    enforce_limit(
        "state snapshot bytes",
        total_bytes,
        limits.max_snapshot_bytes,
    )?;

    let mut output = Vec::with_capacity(total_bytes);
    while output.len() < total_bytes {
        let bytes = (total_bytes - output.len()).min(64 * 1024);
        output.resize(output.len() + bytes, 0);
        step(bytes).await;
    }
    output[..8].copy_from_slice(SNAPSHOT_MAGIC);
    put_u16(&mut output, 8, SNAPSHOT_VERSION);
    put_u16(&mut output, 10, STATE_SNAPSHOT_HEADER_BYTES as u16);
    put_u64(&mut output, 16, usize_to_u64(total_bytes)?);
    put_u64(&mut output, 24, state.revision);
    put_u32(&mut output, 32, usize_to_u32(partitions.len())?);
    put_u32(&mut output, 36, usize_to_u32(progress.len())?);
    put_u32(&mut output, 40, usize_to_u32(assignments.len())?);
    put_u64(&mut output, 48, usize_to_u64(body_bytes)?);
    put_u64(&mut output, 88, usize_to_u64(state.producer_count)?);
    put_u64(&mut output, 96, usize_to_u64(state.retry_span_count)?);

    let mut cursor = STATE_SNAPSHOT_HEADER_BYTES;
    for (partition, value) in partitions {
        cursor = partition::encode_partition(&mut output, cursor, *partition, value, step).await?;
    }
    for (key, offset) in progress {
        encode_progress(&mut output[cursor..cursor + PROGRESS_BYTES], key, *offset);
        cursor += PROGRESS_BYTES;
        step(PROGRESS_BYTES).await;
    }
    for (key, value) in assignments {
        encode_assignment(&mut output[cursor..cursor + ASSIGNMENT_BYTES], *key, *value);
        cursor += ASSIGNMENT_BYTES;
        step(ASSIGNMENT_BYTES).await;
    }
    debug_assert_eq!(cursor, output.len());
    let digest = snapshot_digest_cooperative(&output, step).await;
    output[SNAPSHOT_DIGEST_START..SNAPSHOT_DIGEST_END].copy_from_slice(digest.as_bytes());
    Ok(output)
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
