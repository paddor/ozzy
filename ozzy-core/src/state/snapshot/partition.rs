use ahash::AHashMap;
use ozzy_journal::operation::ReleaseKey;
use ozzy_proto::{
    Offset, OperationId, OwnerEpoch, PartitionId, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};

use ozzy_proto::layout::{self, Fixed, Reader, Writer};
use std::ops::Range;

use super::{
    StateSnapshotError, StateSnapshotLimits, decode_retention, encode_retention, enforce_limit,
    require_nonzero, usize_to_u16, usize_to_u32, validate_name,
};
use crate::state::{
    CanonicalPartition, CanonicalProducer, CanonicalState, DelayState, PartitionAddress,
    ProducerResultSource, ProducerResultSpan,
};

const PARTITION_BYTES: usize = 176;
const PRODUCER_BYTES: usize = 80;
const SPAN_BYTES: usize = super::RETRY_SPAN_SNAPSHOT_BYTES;

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

/// Encode one partition entry at `start`; returns its end.
pub(super) async fn encode_partition(
    output: &mut [u8],
    start: usize,
    (partition, value): (PartitionIncarnation, &CanonicalPartition),
    revision: u64,
    step: &mut impl AsyncFnMut(usize),
) -> Result<usize, StateSnapshotError> {
    validate_partition_cooperative(partition, value, revision, step).await?;
    let entry_bytes = partition_bytes(value, step).await?;
    let end = start
        .checked_add(entry_bytes)
        .ok_or(StateSnapshotError::LengthOverflow)?;
    let target = output
        .get_mut(start..end)
        .ok_or(StateSnapshotError::Truncated)?;
    let (fixed, rest) = target
        .split_first_chunk_mut::<PARTITION_BYTES>()
        .ok_or(StateSnapshotError::Truncated)?;
    fixed.put_u32::<0>(usize_to_u32(entry_bytes)?);
    fixed.put_u16::<4>(usize_to_u16(value.address.stream.len())?);
    fixed.put_u16::<6>(usize_to_u16(value.address.topic.len())?);
    fixed.put::<8, 16>(*partition.as_bytes());
    fixed.put_u32::<24>(value.address.partition_id.get());
    fixed.put_u32::<28>(usize_to_u32(value.producers.len())?);
    fixed.put_u64::<32>(value.owner_epoch.get());
    fixed.put_u64::<40>(value.next_offset.get());
    fixed.put_u64::<48>(value.retained_from.get());
    fixed.put_u64::<56>(value.policy_revision);
    let (flags, age, bytes) = encode_retention(value.retention);
    fixed.put_u32::<64>(flags);
    fixed.put_u64::<72>(age);
    fixed.put_u64::<80>(bytes);
    fixed.put_u64::<88>(value.dedup_time);
    fixed.put_u64::<96>(
        value
            .dedup_window_millis
            .map_or(0, std::num::NonZeroU64::get),
    );
    fixed.put_u64::<104>(value.retired_epoch);
    if let Some(delay) = &value.delay {
        encode_delay(fixed, delay);
    }
    let mut names = Writer::new(rest);
    names.bytes(value.address.stream.as_bytes())?;
    names.bytes(value.address.topic.as_bytes())?;
    let mut cursor = PARTITION_BYTES + names.offset();
    step(cursor).await;
    let producers = super::encoding::sorted(
        value.producers.iter(),
        |(left, _), (right, _)| left.cmp(right),
        step,
    )
    .await;
    for (id, producer) in producers {
        cursor = encode_producer(target, cursor, *id, producer, step).await?;
    }
    debug_assert_eq!(cursor, entry_bytes);
    Ok(end)
}

fn encode_delay(fixed: &mut [u8; PARTITION_BYTES], delay: &DelayState) {
    fixed.put_u64::<112>(delay.max_delay_millis.get());
    fixed.put_u64::<120>(delay.next_schedule);
    fixed.put_u64::<128>(delay.time);
    if let Some(released) = delay.released() {
        fixed.put_u32::<136>(1);
        fixed.put_u64::<144>(released.millis);
        fixed.put_u64::<152>(released.schedule);
    }
    fixed.put_u64::<160>(delay.held_records());
    fixed.put_u64::<168>(delay.held_bytes());
}

/// Encode one producer and its retry spans at `cursor`; returns their end.
async fn encode_producer(
    target: &mut [u8],
    mut cursor: usize,
    id: ProducerId,
    producer: &CanonicalProducer,
    step: &mut impl AsyncFnMut(usize),
) -> Result<usize, StateSnapshotError> {
    let fixed: &mut [u8; PRODUCER_BYTES] = layout::record_mut(target, cursor)?;
    fixed.put::<0, 16>(*id.as_bytes());
    fixed.put_u64::<16>(producer.producer_epoch.get());
    fixed.put_u64::<24>(producer.next_producer_sequence.get());
    fixed.put_u64::<32>(producer.producer_result_floor.get());
    fixed.put_u32::<40>(usize_to_u32(producer.results.len())?);
    fixed.put_u64::<72>(producer.last_stored_end.get());
    if let Some(transition) = producer.transition {
        fixed.put_u32::<44>(1);
        fixed.put::<48, 16>(*transition.operation_id.as_bytes());
        fixed.put_u64::<64>(transition.expected_epoch.map_or(0, ProducerEpoch::get));
    }
    cursor += PRODUCER_BYTES;
    step(PRODUCER_BYTES).await;
    for span in &producer.results {
        let entry: &mut [u8; SPAN_BYTES] = layout::record_mut(target, cursor)?;
        entry.put_u64::<0>(span.first_sequence.get());
        entry.put_u64::<8>(span.first_offset.get());
        entry.put_u64::<16>(span.records);
        match span.source {
            ProducerResultSource::Stored => {}
            ProducerResultSource::Duplicate { operation } => {
                entry.put_u8::<24>(1);
                entry.put_u64::<32>(operation);
            }
            ProducerResultSource::Scheduled { operation } => {
                entry.put_u8::<24>(2);
                entry.put_u64::<32>(operation);
            }
        }
        cursor += SPAN_BYTES;
        step(SPAN_BYTES).await;
    }
    Ok(cursor)
}

/// Decode the next partition entry into the producers and retry spans `state`
/// still has room for. Returns the partition, its state and its span count.
pub(super) async fn decode_partition(
    entries: &mut Reader<'_>,
    limits: StateSnapshotLimits,
    state: &CanonicalState,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(PartitionIncarnation, CanonicalPartition, usize), StateSnapshotError> {
    let remaining_producers = state.limits.max_producers - state.producer_count;
    let remaining_spans = state.limits.max_retry_spans - state.retry_span_count;
    let fixed: &[u8; PARTITION_BYTES] = layout::record(entries.rest(), 0)?;
    if fixed.u32_at::<68>() != 0 || fixed.u32_at::<136>() > 1 || fixed.u32_at::<140>() != 0 {
        return Err(StateSnapshotError::UnsupportedFields);
    }
    let entry_bytes = fixed.u32_at::<0>() as usize;
    let stream_bytes = fixed.u16_at::<4>() as usize;
    let topic_bytes = fixed.u16_at::<6>() as usize;
    enforce_limit("stream name bytes", stream_bytes, limits.max_name_bytes)?;
    enforce_limit("topic name bytes", topic_bytes, limits.max_name_bytes)?;
    let producer_count = fixed.u32_at::<28>() as usize;
    enforce_limit("producers", producer_count, remaining_producers)?;
    let entry = entries.take(entry_bytes)?;
    let stream_end = PARTITION_BYTES + stream_bytes;
    let mut cursor = stream_end + topic_bytes;
    let stream = decode_name(entry, PARTITION_BYTES..stream_end)?;
    let topic = decode_name(entry, stream_end..cursor)?;
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
    let partition = PartitionIncarnation::from_bytes(fixed.array::<8, 16>());
    let address = PartitionAddress {
        stream,
        topic,
        partition_id: PartitionId::new(fixed.u32_at::<24>()),
    };
    let value = decode_fields(fixed, address, producers)?;
    validate_partition_cooperative(partition, &value, state.revision, step).await?;
    Ok((partition, value, spans))
}

fn decode_name(entry: &[u8], range: Range<usize>) -> Result<String, StateSnapshotError> {
    let bytes = entry.get(range).ok_or(StateSnapshotError::LengthMismatch)?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| StateSnapshotError::InvalidUtf8)
}

/// The partition state stored in the fixed fields of its entry.
fn decode_fields(
    fixed: &[u8; PARTITION_BYTES],
    address: PartitionAddress,
    producers: AHashMap<ProducerId, CanonicalProducer>,
) -> Result<CanonicalPartition, StateSnapshotError> {
    let delay = match std::num::NonZeroU64::new(fixed.u64_at::<112>()) {
        None if fixed.array::<120, 56>() == [0; 56] => None,
        None => return Err(StateSnapshotError::InvalidPartition),
        Some(max_delay_millis) => {
            let mut delay = DelayState::new(max_delay_millis);
            delay.next_schedule = fixed.u64_at::<120>();
            delay.time = fixed.u64_at::<128>();
            let released = match fixed.u32_at::<136>() {
                0 if fixed.array::<144, 16>() == [0; 16] => None,
                0 => return Err(StateSnapshotError::InvalidPartition),
                _ => Some(ReleaseKey {
                    millis: fixed.u64_at::<144>(),
                    schedule: fixed.u64_at::<152>(),
                }),
            };
            delay.restore(released, fixed.u64_at::<160>(), fixed.u64_at::<168>());
            Some(delay)
        }
    };
    Ok(CanonicalPartition {
        address,
        owner_epoch: OwnerEpoch::new(fixed.u64_at::<32>()),
        producers,
        next_offset: Offset::new(fixed.u64_at::<40>()),
        retained_from: Offset::new(fixed.u64_at::<48>()),
        policy_revision: fixed.u64_at::<56>(),
        retention: decode_retention(
            fixed.u32_at::<64>(),
            fixed.u64_at::<72>(),
            fixed.u64_at::<80>(),
        )?,
        dedup_time: fixed.u64_at::<88>(),
        dedup_window_millis: std::num::NonZeroU64::new(fixed.u64_at::<96>()),
        retired_epoch: fixed.u64_at::<104>(),
        delay,
    })
}

async fn decode_producer(
    input: &[u8],
    start: usize,
    remaining_spans: usize,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(ProducerId, CanonicalProducer, usize), StateSnapshotError> {
    let fixed: &[u8; PRODUCER_BYTES] = layout::record(input, start)?;
    let mut cursor = start + PRODUCER_BYTES;
    let transition = match fixed.u32_at::<44>() {
        0 if fixed.array::<48, 24>() == [0; 24] => None,
        1 => {
            let operation_id = OperationId::from_bytes(fixed.array::<48, 16>());
            require_nonzero("producer transition", operation_id.as_bytes())?;
            let expected = fixed.u64_at::<64>();
            Some(crate::state::ProducerTransition {
                operation_id,
                expected_epoch: (expected != 0).then_some(ProducerEpoch::new(expected)),
            })
        }
        _ => {
            return Err(StateSnapshotError::UnsupportedFields);
        }
    };
    let id = ProducerId::from_bytes(fixed.array::<0, 16>());
    require_nonzero("producer", id.as_bytes())?;
    let mut producer = CanonicalProducer::new(ProducerEpoch::new(fixed.u64_at::<16>()));
    producer.next_producer_sequence = ProducerSequence::new(fixed.u64_at::<24>());
    producer.producer_result_floor = ProducerSequence::new(fixed.u64_at::<32>());
    producer.transition = transition;
    producer.last_stored_end = Offset::new(fixed.u64_at::<72>());
    let count = fixed.u32_at::<40>() as usize;
    enforce_limit("producer retry spans", count, remaining_spans)?;
    let bytes = count
        .checked_mul(SPAN_BYTES)
        .and_then(|bytes| cursor.checked_add(bytes))
        .ok_or(StateSnapshotError::LengthOverflow)?;
    if bytes > input.len() {
        return Err(StateSnapshotError::Truncated);
    }
    for _ in 0..count {
        let entry: &[u8; SPAN_BYTES] = layout::record(input, cursor)?;
        let source = match (entry.u8_at::<24>(), entry.u64_at::<32>()) {
            (0, 0) => ProducerResultSource::Stored,
            (1, operation) if operation != 0 => ProducerResultSource::Duplicate { operation },
            (2, operation) if operation != 0 => ProducerResultSource::Scheduled { operation },
            _ => return Err(StateSnapshotError::InvalidPartition),
        };
        if entry.array::<25, 7>() != [0; 7] {
            return Err(StateSnapshotError::UnsupportedFields);
        }
        producer.results.push_back(ProducerResultSpan {
            first_sequence: ProducerSequence::new(entry.u64_at::<0>()),
            first_offset: Offset::new(entry.u64_at::<8>()),
            records: entry.u64_at::<16>(),
            source,
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
    revision: u64,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(), StateSnapshotError> {
    require_nonzero("partition", partition.as_bytes())?;
    if value.owner_epoch.get() == 0
        || value.policy_revision == 0
        || value.retained_from > value.next_offset
        || (value.producers.is_empty()
            && value.next_offset != Offset::ZERO
            && value.retired_epoch == 0)
    {
        return Err(StateSnapshotError::InvalidPartition);
    }
    let mut occupied = Vec::new();
    for (id, producer) in &value.producers {
        require_nonzero("producer", id.as_bytes())?;
        validate_producer(producer, value, revision, &mut occupied, step).await?;
    }
    ozzy_journal::work::sort_by(&mut occupied, <(u64, u64)>::cmp, step).await;
    for pair in occupied.windows(2) {
        if let [left, right] = pair
            && left.1 > right.0
        {
            return Err(StateSnapshotError::InvalidPartition);
        }
        step(size_of::<(u64, u64)>()).await;
    }
    if let Some(delay) = &value.delay {
        validate_delay(value, delay)?;
    }
    step(PARTITION_BYTES + value.address.stream.len() + value.address.topic.len()).await;
    Ok(())
}

/// Held totals fit the issued schedule sequences, and a record always has
/// encoded bytes. Delay excludes deduplication.
fn validate_delay(
    partition: &CanonicalPartition,
    delay: &DelayState,
) -> Result<(), StateSnapshotError> {
    let records = delay.held_records();
    if partition.dedup_window_millis.is_some()
        || records > delay.next_schedule
        || (records == 0) != (delay.held_bytes() == 0)
        || delay.released().is_some_and(|released| {
            released.schedule >= delay.next_schedule || released.millis > delay.time
        })
    {
        return Err(StateSnapshotError::InvalidPartition);
    }
    Ok(())
}

async fn validate_producer(
    producer: &CanonicalProducer,
    partition: &CanonicalPartition,
    revision: u64,
    occupied: &mut Vec<(u64, u64)>,
    step: &mut impl AsyncFnMut(usize),
) -> Result<(), StateSnapshotError> {
    if has_invalid_fields(producer) {
        return Err(StateSnapshotError::InvalidPartition);
    }
    let mut sequence = producer.producer_result_floor.get();
    let mut previous_stored_end = None;
    let mut previous: Option<ProducerResultSpan> = None;
    for span in &producer.results {
        let end = span
            .first_offset
            .get()
            .checked_add(span.records)
            .ok_or(StateSnapshotError::InvalidPartition)?;
        if span.records == 0
            || span.first_sequence.get() != sequence
            || end > span_bound(span, partition)?
            || previous.is_some_and(|previous| previous.can_extend(*span))
        {
            return Err(StateSnapshotError::InvalidPartition);
        }
        sequence = sequence
            .checked_add(span.records)
            .ok_or(StateSnapshotError::InvalidPartition)?;
        match span.source {
            ProducerResultSource::Stored => {
                if span.first_offset < partition.retained_from
                    || previous_stored_end
                        .is_some_and(|previous| previous > span.first_offset.get())
                    || end > producer.last_stored_end.get()
                {
                    return Err(StateSnapshotError::InvalidPartition);
                }
                previous_stored_end = Some(end);
                occupied.push((span.first_offset.get(), end));
            }
            ProducerResultSource::Duplicate { operation }
            | ProducerResultSource::Scheduled { operation } => {
                if operation == 0 || operation > revision {
                    return Err(StateSnapshotError::InvalidPartition);
                }
            }
        }
        previous = Some(*span);
        step(SPAN_BYTES).await;
    }
    if spans_miss_producer_end(producer, partition, sequence, previous_stored_end) {
        return Err(StateSnapshotError::InvalidPartition);
    }
    step(PRODUCER_BYTES).await;
    Ok(())
}

/// Exclusive end of a span's second coordinate: the next offset, or the next
/// schedule sequence for a scheduled span.
fn span_bound(
    span: &ProducerResultSpan,
    partition: &CanonicalPartition,
) -> Result<u64, StateSnapshotError> {
    match span.source {
        ProducerResultSource::Scheduled { .. } => partition
            .delay
            .as_ref()
            .map(|delay| delay.next_schedule)
            .ok_or(StateSnapshotError::InvalidPartition),
        ProducerResultSource::Stored | ProducerResultSource::Duplicate { .. } => {
            Ok(partition.next_offset.get())
        }
    }
}

/// A producer needs a session epoch, a result floor not past its next
/// sequence, and a transition naming an operation and an earlier epoch.
fn has_invalid_fields(producer: &CanonicalProducer) -> bool {
    producer.producer_epoch.get() == 0
        || producer.producer_result_floor > producer.next_producer_sequence
        || producer.transition.is_some_and(|transition| {
            transition.operation_id.as_bytes() == &[0; 16]
                || transition
                    .expected_epoch
                    .is_some_and(|epoch| epoch.get() == 0 || epoch >= producer.producer_epoch)
        })
}

/// Retry spans must end at the producer's next sequence, and its last stored
/// span at its stored end inside the partition's records. A producer with no
/// stored span and no expired results has stored nothing.
fn spans_miss_producer_end(
    producer: &CanonicalProducer,
    partition: &CanonicalPartition,
    sequence: u64,
    stored_end: Option<u64>,
) -> bool {
    sequence != producer.next_producer_sequence.get()
        || producer.last_stored_end > partition.next_offset
        || stored_end.is_some_and(|end| end != producer.last_stored_end.get())
        || (producer.producer_result_floor == ProducerSequence::ZERO
            && stored_end.is_none()
            && producer.last_stored_end != Offset::ZERO)
}
