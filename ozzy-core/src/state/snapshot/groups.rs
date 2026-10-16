use super::{CanonicalState, StateSnapshotError, StateSnapshotLimits, enforce_limit};
use crate::state::{CoordinatorGroup, groups::validate_image};
use ozzy_journal::operation::{
    OperationBody, OperationCodecError, OperationKind, OperationLimits, OperationOutput,
    append_operation_body_to, decode_operation_body, encode_operation_body,
};
use ozzy_proto::layout::{Reader, Writer};
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
    let mut writer = Writer::new(output.get_mut(cursor..).unwrap_or_default());
    writer.u32(u32::try_from(body.len()).map_err(|_| StateSnapshotError::LengthOverflow)?)?;
    for chunk in body.chunks(64 * 1024) {
        writer.bytes(chunk)?;
        step(chunk.len()).await;
    }
    Ok(cursor + writer.offset())
}

pub(super) async fn decode(
    state: &mut CanonicalState,
    entries: &mut Reader<'_>,
    count: usize,
    bounds: StateSnapshotLimits,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(), StateSnapshotError> {
    let mut previous = None;
    let mut names = BTreeSet::new();
    for _ in 0..count {
        let length = entries.u32()? as usize;
        let body = entries.take(length)?;
        let OperationBody::GroupPlan(plan) =
            decode_operation_body(OperationKind::GroupPlan, body, limits(bounds))
                .map_err(|_| StateSnapshotError::InvalidGroup)?
        else {
            return Err(StateSnapshotError::InvalidGroup);
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
        for chunk in body.chunks(64 * 1024) {
            step(chunk.len()).await;
        }
    }
    Ok(())
}
