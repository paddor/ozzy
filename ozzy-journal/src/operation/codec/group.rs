use super::{Decoder, Encoder, enforce_limit};
use crate::operation::{
    GroupDelete, GroupMember, GroupPlan, GroupTarget, OperationCodecError, OperationLimits,
    OperationOutput,
};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, NodeId, Offset, OperationId, PartitionIncarnation,
    group::Start,
};

const FORMAT: u8 = 2;
const DELETE_FORMAT: u8 = 1;
const MEMBER_BYTES: usize = 48;
const MIN_TARGET_BYTES: usize = 26;

pub(super) fn encode(
    value: &GroupPlan<'_>,
    limits: OperationLimits,
    out: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    super::primitives::validate_name("consumer group", value.name, limits.max_name_bytes)?;
    enforce_limit(
        "group members",
        value.members.len(),
        limits
            .max_group_members
            .min(crate::operation::GROUP_PLAN_MAX_MEMBERS),
    )?;
    enforce_limit(
        "group source partitions",
        value.targets.len(),
        limits
            .max_group_partitions
            .min(crate::operation::GROUP_PLAN_MAX_PARTITIONS),
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
    input: &mut Decoder<'a>,
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
    let count = input.count(
        "group members",
        limits
            .max_group_members
            .min(crate::operation::GROUP_PLAN_MAX_MEMBERS),
    )?;
    // Validate the declared backing before any count-sized allocation.
    let mut backing = *input;
    backing.take(
        count
            .checked_mul(MEMBER_BYTES)
            .ok_or(OperationCodecError::LengthOverflow)?,
    )?;
    let mut members = Vec::with_capacity(if MATERIALIZE { count } else { 0 });
    for _ in 0..count {
        let member = decode_member(input)?;
        if MATERIALIZE {
            members.push(member);
        }
    }
    let count = input.count(
        "group source partitions",
        limits
            .max_group_partitions
            .min(crate::operation::GROUP_PLAN_MAX_PARTITIONS),
    )?;
    let mut backing = *input;
    backing.take(
        count
            .checked_mul(MIN_TARGET_BYTES)
            .ok_or(OperationCodecError::LengthOverflow)?,
    )?;
    let mut targets = Vec::with_capacity(if MATERIALIZE { count } else { 0 });
    for _ in 0..count {
        let target = GroupTarget {
            partition: PartitionIncarnation::from_bytes(input.id()?),
            plan_revision: input.u64()?,
            member: decode_optional_member(input)?,
            prior_member: decode_optional_member(input)?,
        };
        if MATERIALIZE {
            targets.push(target);
        }
    }
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

pub(super) fn decode_delete(input: &mut Decoder<'_>) -> Result<GroupDelete, OperationCodecError> {
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
    let (tag, value) = match start {
        Start::Earliest => (0, 0),
        Start::Latest => (1, 0),
        Start::Offset(offset) => (2, offset.get()),
    };
    out.u8(tag)?;
    out.u64(value)
}

fn decode_start(input: &mut Decoder<'_>) -> Result<Start, OperationCodecError> {
    match (input.u8()?, input.u64()?) {
        (0, 0) => Ok(Start::Earliest),
        (1, 0) => Ok(Start::Latest),
        (2, value) => Ok(Start::Offset(Offset::new(value))),
        _ => Err(OperationCodecError::InvalidGroupPlan),
    }
}

fn encode_member(
    member: GroupMember,
    out: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    out.id(member.id.as_bytes())?;
    out.id(member.node.as_bytes())?;
    out.id(member.join.as_bytes())
}

fn decode_member(input: &mut Decoder<'_>) -> Result<GroupMember, OperationCodecError> {
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
    input: &mut Decoder<'_>,
) -> Result<Option<GroupMember>, OperationCodecError> {
    match input.u8()? {
        0 => Ok(None),
        1 => decode_member(input).map(Some),
        tag => Err(OperationCodecError::InvalidOptionTag(tag)),
    }
}
