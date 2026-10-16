//! Canonical partition, producer, consumer, and retention control bodies.

use super::primitives::{Decoder, Encoder, validate_name};
use crate::operation::{
    Assign, CreatePartition, DedupWatermark, OpenProducer, OperationCodecError, OperationLimits,
    OperationOutput, PartitionPolicy, ProducerResultFloor, Progress, ProgressAssignment,
    ProgressOwner, RetentionPolicy, Trim,
};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, Offset, OperationId, OwnerEpoch, PartitionId,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, SubscriptionId,
};
use std::num::NonZeroU64;

const RETENTION_VERSION: u8 = 1;
const RETENTION_MAX_AGE: u8 = 1 << 0;
const RETENTION_MAX_BYTES: u8 = 1 << 1;
const RETENTION_KNOWN_FLAGS: u8 = RETENTION_MAX_AGE | RETENTION_MAX_BYTES;

pub(super) fn encode_dedup_watermark(
    value: DedupWatermark,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.id(value.partition.as_bytes())?;
    encoder.u64(value.owner_epoch.get())?;
    encoder.u64(value.logical_time)
}

pub(super) fn decode_dedup_watermark(
    decoder: &mut Decoder<'_>,
) -> Result<DedupWatermark, OperationCodecError> {
    Ok(DedupWatermark {
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        owner_epoch: OwnerEpoch::new(decoder.u64()?),
        logical_time: decoder.u64()?,
    })
}

pub(super) fn encode_create(
    value: &CreatePartition<'_>,
    limits: OperationLimits,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    validate_name("stream", value.stream, limits.max_name_bytes)?;
    validate_name("topic", value.topic, limits.max_name_bytes)?;
    encoder.id(value.partition.as_bytes())?;
    encoder.text(value.stream)?;
    encoder.text(value.topic)?;
    encoder.u32(value.partition_id.get())?;
    encoder.u64(value.owner_epoch.get())?;
    encoder.u64(value.dedup_window_millis.map_or(0, NonZeroU64::get))?;
    encode_retention(value.retention, encoder)
}

pub(super) fn decode_create<'a>(
    decoder: &mut Decoder<'a>,
    limits: OperationLimits,
) -> Result<CreatePartition<'a>, OperationCodecError> {
    let partition = PartitionIncarnation::from_bytes(decoder.id()?);
    let stream = decoder.text("stream", limits.max_name_bytes)?;
    let topic = decoder.text("topic", limits.max_name_bytes)?;
    Ok(CreatePartition {
        partition,
        stream,
        topic,
        partition_id: PartitionId::new(decoder.u32()?),
        owner_epoch: OwnerEpoch::new(decoder.u64()?),
        dedup_window_millis: NonZeroU64::new(decoder.u64()?),
        retention: decode_retention(decoder)?,
    })
}

pub(super) fn encode_open_producer(
    value: OpenProducer,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.id(value.partition.as_bytes())?;
    encoder.id(value.producer_id.as_bytes())?;
    encoder.optional_u64(value.expected_epoch.map(ProducerEpoch::get))?;
    encoder.u64(value.new_epoch.get())?;
    encoder.id(value.operation_id.as_bytes())
}

pub(super) fn decode_open_producer(
    decoder: &mut Decoder<'_>,
) -> Result<OpenProducer, OperationCodecError> {
    Ok(OpenProducer {
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        producer_id: ProducerId::from_bytes(decoder.id()?),
        expected_epoch: decoder.optional_u64()?.map(ProducerEpoch::new),
        new_epoch: ProducerEpoch::new(decoder.u64()?),
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

pub(super) fn encode_progress(
    value: Progress,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    match value.owner {
        ProgressOwner::Subscription(id) => {
            if value.assignment.is_some() {
                return Err(OperationCodecError::UnexpectedProgressAssignment);
            }
            encoder.u8(1)?;
            encoder.id(id.as_bytes())?;
        }
        ProgressOwner::ConsumerGroup(id) => {
            if value.assignment.is_none() {
                return Err(OperationCodecError::MissingProgressAssignment);
            }
            encoder.u8(2)?;
            encoder.id(id.as_bytes())?;
        }
    }
    encoder.id(value.partition.as_bytes())?;
    match value.assignment {
        None => encoder.u8(0)?,
        Some(assignment) => {
            // Tag 1 was the incomplete epoch-only fence and is unsupported.
            encoder.u8(2)?;
            encoder.u64(assignment.epoch)?;
            encoder.id(assignment.member.as_bytes())?;
        }
    }
    encoder.optional_u64(value.expected_progress.map(Offset::get))?;
    encoder.u64(value.new_progress.get())?;
    encoder.id(value.operation_id.as_bytes())
}

pub(super) fn decode_progress(decoder: &mut Decoder<'_>) -> Result<Progress, OperationCodecError> {
    let owner = match decoder.u8()? {
        1 => ProgressOwner::Subscription(SubscriptionId::from_bytes(decoder.id()?)),
        2 => ProgressOwner::ConsumerGroup(ConsumerGroupId::from_bytes(decoder.id()?)),
        tag => return Err(OperationCodecError::InvalidProgressOwnerTag(tag)),
    };
    let partition = PartitionIncarnation::from_bytes(decoder.id()?);
    let assignment = match decoder.u8()? {
        0 => None,
        2 => Some(ProgressAssignment {
            epoch: decoder.u64()?,
            member: ConsumerMemberId::from_bytes(decoder.id()?),
        }),
        tag => return Err(OperationCodecError::InvalidOptionTag(tag)),
    };
    match owner {
        ProgressOwner::Subscription(_) if assignment.is_some() => {
            return Err(OperationCodecError::UnexpectedProgressAssignment);
        }
        ProgressOwner::ConsumerGroup(_) if assignment.is_none() => {
            return Err(OperationCodecError::MissingProgressAssignment);
        }
        _ => {}
    }
    Ok(Progress {
        owner,
        partition,
        assignment,
        expected_progress: decoder.optional_u64()?.map(Offset::new),
        new_progress: Offset::new(decoder.u64()?),
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

pub(super) fn encode_assign(
    value: Assign,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.u8(2)?;
    encoder.id(value.consumer_group_id.as_bytes())?;
    encoder.id(value.partition.as_bytes())?;
    encoder.u64(value.expected_assignment_epoch)?;
    encoder.u64(value.new_assignment_epoch)?;
    encoder.u64(value.plan_revision)?;
    encoder.u64(value.resume_cursor.get())?;
    encoder.optional_id(value.new_member.map(|id| *id.as_bytes()))?;
    encoder.id(value.operation_id.as_bytes())
}

pub(super) fn decode_assign(decoder: &mut Decoder<'_>) -> Result<Assign, OperationCodecError> {
    let format = decoder.u8()?;
    if format != 2 {
        return Err(OperationCodecError::UnsupportedAssignmentFormat(format));
    }
    Ok(Assign {
        consumer_group_id: ConsumerGroupId::from_bytes(decoder.id()?),
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        expected_assignment_epoch: decoder.u64()?,
        new_assignment_epoch: decoder.u64()?,
        plan_revision: decoder.u64()?,
        resume_cursor: Offset::new(decoder.u64()?),
        new_member: decoder.optional_id()?.map(ConsumerMemberId::from_bytes),
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

pub(super) fn encode_trim(
    value: Trim,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.id(value.partition.as_bytes())?;
    encoder.u64(value.expected_floor.get())?;
    encoder.u64(value.new_floor.get())?;
    encoder.id(value.operation_id.as_bytes())
}

pub(super) fn decode_trim(decoder: &mut Decoder<'_>) -> Result<Trim, OperationCodecError> {
    Ok(Trim {
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        expected_floor: Offset::new(decoder.u64()?),
        new_floor: Offset::new(decoder.u64()?),
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

pub(super) fn encode_policy(
    value: PartitionPolicy,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.id(value.partition.as_bytes())?;
    encoder.u64(value.expected_revision)?;
    encoder.u64(value.new_revision)?;
    encode_retention(value.retention, encoder)?;
    encoder.id(value.operation_id.as_bytes())
}

pub(super) fn decode_policy(
    decoder: &mut Decoder<'_>,
) -> Result<PartitionPolicy, OperationCodecError> {
    Ok(PartitionPolicy {
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        expected_revision: decoder.u64()?,
        new_revision: decoder.u64()?,
        retention: decode_retention(decoder)?,
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

pub(super) fn encode_producer_result_floor(
    value: ProducerResultFloor,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.id(value.partition.as_bytes())?;
    encoder.id(value.producer_id.as_bytes())?;
    encoder.u64(value.producer_epoch.get())?;
    encoder.u64(value.expected_floor.get())?;
    encoder.u64(value.new_floor.get())?;
    encoder.id(value.operation_id.as_bytes())
}

pub(super) fn decode_producer_result_floor(
    decoder: &mut Decoder<'_>,
) -> Result<ProducerResultFloor, OperationCodecError> {
    Ok(ProducerResultFloor {
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        producer_id: ProducerId::from_bytes(decoder.id()?),
        producer_epoch: ProducerEpoch::new(decoder.u64()?),
        expected_floor: ProducerSequence::new(decoder.u64()?),
        new_floor: ProducerSequence::new(decoder.u64()?),
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

pub(super) fn encode_retention(
    policy: RetentionPolicy,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    let mut flags = 0_u8;
    if policy.max_age_millis.is_some() {
        flags |= RETENTION_MAX_AGE;
    }
    if policy.max_bytes.is_some() {
        flags |= RETENTION_MAX_BYTES;
    }
    encoder.u8(RETENTION_VERSION)?;
    encoder.u8(flags)?;
    encoder.u16(0)?;
    encoder.u64(policy.max_age_millis.map_or(0, NonZeroU64::get))?;
    encoder.u64(policy.max_bytes.map_or(0, NonZeroU64::get))
}

pub(super) fn decode_retention(
    decoder: &mut Decoder<'_>,
) -> Result<RetentionPolicy, OperationCodecError> {
    let version = decoder.u8()?;
    let flags = decoder.u8()?;
    let reserved = decoder.u16()?;
    let age = decoder.u64()?;
    let bytes = decoder.u64()?;
    if version != RETENTION_VERSION || flags & !RETENTION_KNOWN_FLAGS != 0 || reserved != 0 {
        return Err(OperationCodecError::InvalidRetention);
    }
    let max_age_millis = retention_value(flags, RETENTION_MAX_AGE, age)?;
    let max_bytes = retention_value(flags, RETENTION_MAX_BYTES, bytes)?;
    Ok(RetentionPolicy {
        max_age_millis,
        max_bytes,
    })
}

fn retention_value(
    flags: u8,
    flag: u8,
    value: u64,
) -> Result<Option<NonZeroU64>, OperationCodecError> {
    match (flags & flag != 0, NonZeroU64::new(value)) {
        (false, None) => Ok(None),
        (true, Some(value)) => Ok(Some(value)),
        _ => Err(OperationCodecError::InvalidRetention),
    }
}
