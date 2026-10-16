use super::{
    CanonicalState, StateSnapshotError, StateSnapshotLimits, enforce_limit, put_u32, read_u32,
};
use crate::state::{CoordinatorGroup, groups::validate_image};
use ozzy_journal::operation::{
    OperationBody, OperationCodecError, OperationKind, OperationLimits, OperationOutput,
    append_operation_body_to, decode_operation_body, encode_operation_body,
};
use std::{collections::BTreeSet, sync::Arc};

#[derive(Default)]
struct Size(usize);
impl OperationOutput for Size {
    fn len(&self) -> usize {
        self.0
    }
    fn truncate(&mut self, len: usize) {
        self.0 = len;
    }
    fn extend(&mut self, bytes: &[u8]) -> Result<(), OperationCodecError> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or(OperationCodecError::LengthOverflow)?;
        Ok(())
    }
}

fn limits(snapshot: StateSnapshotLimits) -> OperationLimits {
    OperationLimits {
        max_body_bytes: snapshot.max_snapshot_bytes,
        max_name_bytes: snapshot.max_name_bytes,
        ..Default::default()
    }
}

pub(super) fn size(
    group: &CoordinatorGroup,
    bounds: StateSnapshotLimits,
) -> Result<usize, StateSnapshotError> {
    let mut size = Size::default();
    append_operation_body_to(
        &mut size,
        &OperationBody::GroupPlan(group.image()),
        limits(bounds),
    )
    .map_err(|_| StateSnapshotError::InvalidGroup)?;
    size.0
        .checked_add(4)
        .ok_or(StateSnapshotError::LengthOverflow)
}

pub(super) async fn size_cooperative(
    group: &CoordinatorGroup,
    bounds: StateSnapshotLimits,
    step: &mut impl AsyncFnMut(usize),
) -> Result<usize, StateSnapshotError> {
    let bytes = size(group, bounds)?;
    for chunk in (0..bytes).step_by(64 * 1024) {
        step((bytes - chunk).min(64 * 1024)).await;
    }
    Ok(bytes)
}

pub(super) async fn encode(
    output: &mut [u8],
    cursor: usize,
    group: &CoordinatorGroup,
    bounds: StateSnapshotLimits,
    step: &mut impl AsyncFnMut(usize),
) -> Result<usize, StateSnapshotError> {
    let body = encode_operation_body(&OperationBody::GroupPlan(group.image()), limits(bounds))
        .map_err(|_| StateSnapshotError::InvalidGroup)?;
    put_u32(
        output,
        cursor,
        u32::try_from(body.len()).map_err(|_| StateSnapshotError::LengthOverflow)?,
    );
    let mut next = cursor + 4;
    for chunk in body.chunks(64 * 1024) {
        output[next..next + chunk.len()].copy_from_slice(chunk);
        next += chunk.len();
        step(chunk.len()).await;
    }
    Ok(next)
}

pub(super) async fn decode(
    state: &mut CanonicalState,
    input: &[u8],
    mut cursor: usize,
    count: usize,
    bounds: StateSnapshotLimits,
    step: &mut impl AsyncFnMut(usize),
) -> Result<usize, StateSnapshotError> {
    let mut previous = None;
    let mut names = BTreeSet::new();
    for _ in 0..count {
        if input.len().saturating_sub(cursor) < 4 {
            return Err(StateSnapshotError::Truncated);
        }
        let length = read_u32(input, cursor) as usize;
        cursor += 4;
        let end = cursor
            .checked_add(length)
            .ok_or(StateSnapshotError::LengthOverflow)?;
        let body = input
            .get(cursor..end)
            .ok_or(StateSnapshotError::Truncated)?;
        let OperationBody::GroupPlan(plan) =
            decode_operation_body(OperationKind::GroupPlan, body, limits(bounds))
                .map_err(|_| StateSnapshotError::InvalidGroup)?
        else {
            unreachable!()
        };
        validate_image(&plan).map_err(|_| StateSnapshotError::InvalidGroup)?;
        if plan.revision > state.revision || !state.partitions.contains_key(&plan.coordinator) {
            return Err(StateSnapshotError::InvalidGroup);
        }
        if previous.is_some_and(|id| id >= plan.group)
            || !names.insert((plan.coordinator, plan.name))
        {
            return Err(StateSnapshotError::UnsortedOrDuplicate);
        }
        previous = Some(plan.group);
        state.group_member_count = state
            .group_member_count
            .checked_add(plan.members.len())
            .ok_or(StateSnapshotError::LengthOverflow)?;
        state.group_target_count = state
            .group_target_count
            .checked_add(plan.targets.len())
            .ok_or(StateSnapshotError::LengthOverflow)?;
        enforce_limit(
            "group members",
            state.group_member_count,
            state.limits.max_group_members,
        )?;
        enforce_limit(
            "group source partitions",
            state.group_target_count,
            state.limits.max_group_partitions,
        )?;
        state
            .groups
            .insert(plan.group, Arc::new(CoordinatorGroup::from_plan(&plan)));
        cursor = end;
        for chunk in body.chunks(64 * 1024) {
            step(chunk.len()).await;
        }
    }
    Ok(cursor)
}
