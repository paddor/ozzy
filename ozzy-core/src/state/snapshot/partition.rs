use ahash::AHashMap;
use ozzy_proto::{
    Offset, OwnerEpoch, PartitionId, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};

use super::{
    StateSnapshotError, StateSnapshotLimits, array_16, decode_retention, encode_retention,
    enforce_limit, put_u16, put_u32, put_u64, read_u16, read_u32, read_u64, require_nonzero,
    usize_to_u16, usize_to_u32, validate_name,
};
use crate::state::{CanonicalPartition, CanonicalProducer, PartitionAddress, ProducerResultSpan};

const PARTITION_BYTES: usize = 96;
const PRODUCER_BYTES: usize = 48;
const SPAN_BYTES: usize = 24;

pub(super) async fn partition_bytes(
    value: &CanonicalPartition,
    step: &mut impl AsyncFnMut(usize),
) -> Result<usize, StateSnapshotError> {
    let mut total = value
        .address
        .stream
        .len()
        .checked_add(value.address.topic.len())
        .and_then(|bytes| bytes.checked_add(PARTITION_BYTES))
        .ok_or(StateSnapshotError::LengthOverflow)?;
    for producer in value.producers.values() {
        total = producer
            .results
            .len()
            .checked_mul(SPAN_BYTES)
            .and_then(|bytes| bytes.checked_add(PRODUCER_BYTES))
            .and_then(|bytes| bytes.checked_add(total))
            .ok_or(StateSnapshotError::LengthOverflow)?;
        step(PRODUCER_BYTES).await;
    }
    step(PARTITION_BYTES).await;
    Ok(total)
}

pub(super) async fn encode_partition(
    output: &mut [u8],
    start: usize,
    partition: PartitionIncarnation,
    value: &CanonicalPartition,
    step: &mut impl AsyncFnMut(usize),
) -> Result<usize, StateSnapshotError> {
    validate_partition_cooperative(partition, value, step).await?;
    let entry_bytes = partition_bytes(value, step).await?;
    let end = start
        .checked_add(entry_bytes)
        .ok_or(StateSnapshotError::LengthOverflow)?;
    let target = output
        .get_mut(start..end)
        .ok_or(StateSnapshotError::Truncated)?;
    put_u32(target, 0, usize_to_u32(entry_bytes)?);
    put_u16(target, 4, usize_to_u16(value.address.stream.len())?);
    put_u16(target, 6, usize_to_u16(value.address.topic.len())?);
    target[8..24].copy_from_slice(partition.as_bytes());
    put_u32(target, 24, value.address.partition_id.get());
    put_u32(target, 28, usize_to_u32(value.producers.len())?);
    put_u64(target, 32, value.owner_epoch.get());
    put_u64(target, 40, value.next_offset.get());
    put_u64(target, 48, value.retained_from.get());
    put_u64(target, 56, value.policy_revision);
    let (flags, age, bytes) = encode_retention(value.retention);
    put_u32(target, 64, flags);
    put_u64(target, 72, age);
    put_u64(target, 80, bytes);
    let stream_end = PARTITION_BYTES + value.address.stream.len();
    let mut cursor = stream_end + value.address.topic.len();
    target[PARTITION_BYTES..stream_end].copy_from_slice(value.address.stream.as_bytes());
    target[stream_end..cursor].copy_from_slice(value.address.topic.as_bytes());
    step(cursor).await;
    let producers = super::encoding::sorted(
        value.producers.iter(),
        |(left, _), (right, _)| left.cmp(right),
        step,
    )
    .await;
    for (id, producer) in producers {
        let fixed = &mut target[cursor..cursor + PRODUCER_BYTES];
        fixed[..16].copy_from_slice(id.as_bytes());
        put_u64(fixed, 16, producer.producer_epoch.get());
        put_u64(fixed, 24, producer.next_producer_sequence.get());
        put_u64(fixed, 32, producer.producer_result_floor.get());
        put_u32(fixed, 40, usize_to_u32(producer.results.len())?);
        cursor += PRODUCER_BYTES;
        step(PRODUCER_BYTES).await;
        for span in &producer.results {
            put_u64(target, cursor, span.first_sequence.get());
            put_u64(target, cursor + 8, span.first_offset.get());
            put_u64(target, cursor + 16, span.records);
            cursor += SPAN_BYTES;
            step(SPAN_BYTES).await;
        }
    }
    debug_assert_eq!(cursor, entry_bytes);
    Ok(end)
}

pub(super) async fn decode_partition(
    input: &[u8],
    start: usize,
    limits: StateSnapshotLimits,
    remaining_producers: usize,
    remaining_spans: usize,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(PartitionIncarnation, CanonicalPartition, usize, usize), StateSnapshotError> {
    let fixed_end = start
        .checked_add(PARTITION_BYTES)
        .ok_or(StateSnapshotError::LengthOverflow)?;
    let fixed = input
        .get(start..fixed_end)
        .ok_or(StateSnapshotError::Truncated)?;
    if read_u32(fixed, 68) != 0 || read_u64(fixed, 88) != 0 {
        return Err(StateSnapshotError::UnsupportedFields);
    }
    let entry_bytes = read_u32(fixed, 0) as usize;
    let stream_bytes = read_u16(fixed, 4) as usize;
    let topic_bytes = read_u16(fixed, 6) as usize;
    enforce_limit("stream name bytes", stream_bytes, limits.max_name_bytes)?;
    enforce_limit("topic name bytes", topic_bytes, limits.max_name_bytes)?;
    let producer_count = read_u32(fixed, 28) as usize;
    enforce_limit("producers", producer_count, remaining_producers)?;
    let end = start
        .checked_add(entry_bytes)
        .ok_or(StateSnapshotError::LengthOverflow)?;
    let entry = input.get(start..end).ok_or(StateSnapshotError::Truncated)?;
    let stream_end = PARTITION_BYTES + stream_bytes;
    let mut cursor = stream_end + topic_bytes;
    let stream = std::str::from_utf8(
        entry
            .get(PARTITION_BYTES..stream_end)
            .ok_or(StateSnapshotError::LengthMismatch)?,
    )
    .map_err(|_| StateSnapshotError::InvalidUtf8)?
    .to_owned();
    let topic = std::str::from_utf8(
        entry
            .get(stream_end..cursor)
            .ok_or(StateSnapshotError::LengthMismatch)?,
    )
    .map_err(|_| StateSnapshotError::InvalidUtf8)?
    .to_owned();
    validate_name(&stream, limits)?;
    validate_name(&topic, limits)?;
    let mut producers = AHashMap::new();
    let mut previous = None;
    let mut spans = 0;
    for _ in 0..producer_count {
        let (id, producer, next) =
            decode_producer(entry, cursor, remaining_spans - spans, step).await?;
        if previous.is_some_and(|previous| previous >= id) {
            return Err(StateSnapshotError::UnsortedOrDuplicate);
        }
        previous = Some(id);
        spans += producer.results.len();
        producers.insert(id, producer);
        cursor = next;
    }
    if cursor != entry_bytes {
        return Err(StateSnapshotError::LengthMismatch);
    }
    let partition = PartitionIncarnation::from_bytes(array_16(fixed, 8));
    let value = CanonicalPartition {
        address: PartitionAddress {
            stream,
            topic,
            partition_id: PartitionId::new(read_u32(fixed, 24)),
        },
        owner_epoch: OwnerEpoch::new(read_u64(fixed, 32)),
        producers,
        next_offset: Offset::new(read_u64(fixed, 40)),
        retained_from: Offset::new(read_u64(fixed, 48)),
        policy_revision: read_u64(fixed, 56),
        retention: decode_retention(
            read_u32(fixed, 64),
            read_u64(fixed, 72),
            read_u64(fixed, 80),
        )?,
    };
    validate_partition_cooperative(partition, &value, step).await?;
    Ok((partition, value, end, spans))
}

async fn decode_producer(
    input: &[u8],
    start: usize,
    remaining_spans: usize,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(ProducerId, CanonicalProducer, usize), StateSnapshotError> {
    let mut cursor = start
        .checked_add(PRODUCER_BYTES)
        .ok_or(StateSnapshotError::LengthOverflow)?;
    let fixed = input
        .get(start..cursor)
        .ok_or(StateSnapshotError::Truncated)?;
    if read_u32(fixed, 44) != 0 {
        return Err(StateSnapshotError::UnsupportedFields);
    }
    let id = ProducerId::from_bytes(array_16(fixed, 0));
    require_nonzero("producer", id.as_bytes())?;
    let mut producer = CanonicalProducer::new(ProducerEpoch::new(read_u64(fixed, 16)));
    producer.next_producer_sequence = ProducerSequence::new(read_u64(fixed, 24));
    producer.producer_result_floor = ProducerSequence::new(read_u64(fixed, 32));
    let count = read_u32(fixed, 40) as usize;
    enforce_limit("producer retry spans", count, remaining_spans)?;
    let bytes = count
        .checked_mul(SPAN_BYTES)
        .and_then(|bytes| cursor.checked_add(bytes))
        .ok_or(StateSnapshotError::LengthOverflow)?;
    if bytes > input.len() {
        return Err(StateSnapshotError::Truncated);
    }
    for _ in 0..count {
        producer.results.push_back(ProducerResultSpan {
            first_sequence: ProducerSequence::new(read_u64(input, cursor)),
            first_offset: Offset::new(read_u64(input, cursor + 8)),
            records: read_u64(input, cursor + 16),
        });
        cursor += SPAN_BYTES;
        step(SPAN_BYTES).await;
    }
    step(PRODUCER_BYTES).await;
    Ok((id, producer, cursor))
}

async fn validate_partition_cooperative(
    partition: PartitionIncarnation,
    value: &CanonicalPartition,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(), StateSnapshotError> {
    require_nonzero("partition", partition.as_bytes())?;
    if value.owner_epoch.get() == 0
        || value.policy_revision == 0
        || value.retained_from > value.next_offset
        || (value.producers.is_empty() && value.next_offset != Offset::ZERO)
    {
        return Err(StateSnapshotError::InvalidPartition);
    }
    let mut occupied = Vec::new();
    for (id, producer) in &value.producers {
        require_nonzero("producer", id.as_bytes())?;
        validate_producer(producer, value, &mut occupied, step).await?;
    }
    ozzy_journal::work::sort_by(&mut occupied, <(u64, u64)>::cmp, step).await;
    for pair in occupied.windows(2) {
        if pair[0].1 > pair[1].0 {
            return Err(StateSnapshotError::InvalidPartition);
        }
        step(size_of::<(u64, u64)>()).await;
    }
    step(PARTITION_BYTES + value.address.stream.len() + value.address.topic.len()).await;
    Ok(())
}

async fn validate_producer(
    producer: &CanonicalProducer,
    partition: &CanonicalPartition,
    occupied: &mut Vec<(u64, u64)>,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(), StateSnapshotError> {
    if producer.producer_epoch.get() == 0
        || producer.producer_result_floor > producer.next_producer_sequence
    {
        return Err(StateSnapshotError::InvalidPartition);
    }
    let mut sequence = producer.producer_result_floor.get();
    let mut previous_end = None;
    for span in &producer.results {
        let end = span
            .first_offset
            .get()
            .checked_add(span.records)
            .ok_or(StateSnapshotError::InvalidPartition)?;
        if span.records == 0
            || span.first_sequence.get() != sequence
            || span.first_offset < partition.retained_from
            || end > partition.next_offset.get()
            || previous_end.is_some_and(|previous| previous >= span.first_offset.get())
        {
            return Err(StateSnapshotError::InvalidPartition);
        }
        sequence = sequence
            .checked_add(span.records)
            .ok_or(StateSnapshotError::InvalidPartition)?;
        previous_end = Some(end);
        occupied.push((span.first_offset.get(), end));
        step(SPAN_BYTES).await;
    }
    if sequence != producer.next_producer_sequence.get() {
        return Err(StateSnapshotError::InvalidPartition);
    }
    step(PRODUCER_BYTES).await;
    Ok(())
}
