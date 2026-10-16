use super::{Encoder, ReadFields, Reader, enforce_limit};
use crate::operation::{
    GroupDelete, GroupMember, GroupPlan, GroupTarget, OperationCodecError, OperationLimits,
    OperationOutput,
};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, NodeId, OperationId, PartitionIncarnation, group::Start,
};

const FORMAT: u8 = 1;
const DELETE_FORMAT: u8 = 1;
const MEMBER_BYTES: usize = 48;
const MIN_TARGET_BYTES: usize = 26;

pub(super) fn encode(
    value: &GroupPlan<'_>,
    limits: OperationLimits,
    out: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    super::primitives::validate_name("consumer group", value.name, limits.max_name_bytes)?;
    enforce_limit("group members", value.members.len(), member_limit(limits))?;
    enforce_limit(
        "group source partitions",
        value.targets.len(),
        partition_limit(limits),
    )?;
    out.u8(FORMAT)?;
    out.id(value.coordinator.as_bytes())?;
    out.id(value.group.as_bytes())?;
    out.text(value.name)?;
    out.u64(value.expected_revision)?;
    out.u64(value.revision)?;
    encode_start(value.initial, out)?;
    out.count(value.members.len())?;
    for &member in &value.members {
        encode_member(member, out)?;
    }
    out.count(value.targets.len())?;
    for target in &value.targets {
        out.id(target.partition.as_bytes())?;
        out.u64(target.plan_revision)?;
        encode_optional_member(target.member, out)?;
        encode_optional_member(target.prior_member, out)?;
    }
    out.id(value.operation_id.as_bytes())
}

pub(super) fn decode<'a, const MATERIALIZE: bool>(
    input: &mut Reader<'a>,
    limits: OperationLimits,
) -> Result<GroupPlan<'a>, OperationCodecError> {
    if input.u8()? != FORMAT {
        return Err(OperationCodecError::InvalidGroupPlan);
    }
    let coordinator = PartitionIncarnation::from_bytes(input.id()?);
    let group = ConsumerGroupId::from_bytes(input.id()?);
    let name = input.text("consumer group", limits.max_name_bytes)?;
    let expected_revision = input.u64()?;
    let revision = input.u64()?;
    let initial = decode_start(input)?;
    let members = decode_list::<_, MATERIALIZE>(
        input,
        "group members",
        member_limit(limits),
        MEMBER_BYTES,
        decode_member,
    )?;
    let targets = decode_list::<_, MATERIALIZE>(
        input,
        "group source partitions",
        partition_limit(limits),
        MIN_TARGET_BYTES,
        decode_target,
    )?;
    Ok(GroupPlan {
        coordinator,
        group,
        name,
        expected_revision,
        revision,
        initial,
        members,
        targets,
        operation_id: OperationId::from_bytes(input.id()?),
    })
}

fn member_limit(limits: OperationLimits) -> usize {
    limits
        .max_group_members
        .min(crate::operation::GROUP_PLAN_MAX_MEMBERS)
}

fn partition_limit(limits: OperationLimits) -> usize {
    limits
        .max_group_partitions
        .min(crate::operation::GROUP_PLAN_MAX_PARTITIONS)
}

/// Decode a counted list after checking that the input holds `min_bytes`
/// per entry, so an untrusted count reserves nothing for missing entries.
fn decode_list<'a, T, const MATERIALIZE: bool>(
    input: &mut Reader<'a>,
    kind: &'static str,
    limit: usize,
    min_bytes: usize,
    mut decode: impl FnMut(&mut Reader<'a>) -> Result<T, OperationCodecError>,
) -> Result<Vec<T>, OperationCodecError> {
    let count = input.count(kind, limit)?;
    let mut backing = *input;
    backing.take(
        count
            .checked_mul(min_bytes)
            .ok_or(OperationCodecError::LengthOverflow)?,
    )?;
    let mut entries = Vec::with_capacity(if MATERIALIZE { count } else { 0 });
    for _ in 0..count {
        let entry = decode(input)?;
        if MATERIALIZE {
            entries.push(entry);
        }
    }
    Ok(entries)
}

fn decode_target(input: &mut Reader<'_>) -> Result<GroupTarget, OperationCodecError> {
    Ok(GroupTarget {
        partition: PartitionIncarnation::from_bytes(input.id()?),
        plan_revision: input.u64()?,
        member: decode_optional_member(input)?,
        prior_member: decode_optional_member(input)?,
    })
}

pub(super) fn encode_delete(
    value: GroupDelete,
    out: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    out.u8(DELETE_FORMAT)?;
    out.id(value.coordinator.as_bytes())?;
    out.id(value.group.as_bytes())?;
    out.u64(value.revision)?;
    out.id(value.operation_id.as_bytes())
}

pub(super) fn decode_delete(input: &mut Reader<'_>) -> Result<GroupDelete, OperationCodecError> {
    if input.u8()? != DELETE_FORMAT {
        return Err(OperationCodecError::InvalidGroupPlan);
    }
    Ok(GroupDelete {
        coordinator: PartitionIncarnation::from_bytes(input.id()?),
        group: ConsumerGroupId::from_bytes(input.id()?),
        revision: input.u64()?,
        operation_id: OperationId::from_bytes(input.id()?),
    })
}

fn encode_start(
    start: Start,
    out: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    let (tag, value) = start.to_wire();
    out.u8(tag)?;
    out.u64(value)
}

fn decode_start(input: &mut Reader<'_>) -> Result<Start, OperationCodecError> {
    let (tag, value) = (input.u8()?, input.u64()?);
    Start::from_wire(tag, value).ok_or(OperationCodecError::InvalidGroupPlan)
}

fn encode_member(
    member: GroupMember,
    out: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    out.id(member.id.as_bytes())?;
    out.id(member.node.as_bytes())?;
    out.id(member.join.as_bytes())
}

fn decode_member(input: &mut Reader<'_>) -> Result<GroupMember, OperationCodecError> {
    Ok(GroupMember {
        id: ConsumerMemberId::from_bytes(input.id()?),
        node: NodeId::from_bytes(input.id()?),
        join: OperationId::from_bytes(input.id()?),
    })
}

fn encode_optional_member(
    member: Option<GroupMember>,
    out: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    out.u8(u8::from(member.is_some()))?;
    if let Some(member) = member {
        encode_member(member, out)?;
    }
    Ok(())
}

fn decode_optional_member(
    input: &mut Reader<'_>,
) -> Result<Option<GroupMember>, OperationCodecError> {
    match input.u8()? {
        0 => Ok(None),
        1 => decode_member(input).map(Some),
        tag => Err(OperationCodecError::InvalidOptionTag(tag)),
    }
}
