use ozzy_bench::{automation::Result, workload};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, OperationLimits, encode_operation_body,
};
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};

#[derive(Clone, Copy)]
pub(super) struct Shape {
    pub record_bytes: usize,
    pub target: usize,
    pub random: bool,
}

pub(super) fn body(shape: Shape, count: usize, first: u64) -> Result<Vec<u8>> {
    let payloads: Vec<_> = (0..count)
        .map(|i| payload(shape, first + i as u64))
        .collect();
    Ok(encode_operation_body(
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: PartitionIncarnation::from_bytes([2; 16]),
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([3; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(first),
                first_offset: Offset::new(first),
                append_timestamp_millis: 1_789_000_000_000 + first,
                records: payloads
                    .iter()
                    .enumerate()
                    .map(|(i, p)| AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes(
                            ((0x9e37_79b9_7f4a_7c15_u128 << 64) | u128::from(first + i as u64))
                                .to_be_bytes(),
                        ),
                        parts: [p.as_slice()].into_iter().collect(),
                    })
                    .collect(),
            }],
        }),
        OperationLimits::default(),
    )?)
}

fn payload(shape: Shape, number: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(shape.record_bytes);
    bytes.extend_from_slice(&number.wrapping_mul(1000).to_be_bytes());
    if shape.random {
        let mut seed = number;
        while bytes.len() < shape.record_bytes {
            seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut value = seed;
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            bytes.extend_from_slice(&(value ^ (value >> 31)).to_le_bytes());
        }
        bytes.truncate(shape.record_bytes);
    } else {
        workload::append_json_record(&mut bytes, shape.record_bytes - 8, number);
    }
    bytes
}

pub(super) fn record_count(shape: Shape) -> Result<Option<usize>> {
    let minimum = body(shape, 1, 0)?.len();
    if minimum > shape.target {
        return Ok(None);
    }
    let step = body(shape, 2, 0)?.len() - minimum;
    Ok(Some(1 + (shape.target - minimum) / step))
}

pub(super) fn bodies(
    shape: Shape,
    records: usize,
    samples: usize,
    first: u64,
) -> Result<Vec<Vec<u8>>> {
    (0..samples)
        .map(|i| body(shape, records, first + (i * records) as u64))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn whole_records_fit_target_and_training_never_uses_test_records() {
        for record_bytes in [128, 1024, 8192] {
            for target in [256, 512, 1024, 2048, 4096, 8192, 16384, 65536] {
                let shape = Shape {
                    record_bytes,
                    target,
                    random: false,
                };
                if let Some(count) = record_count(shape).unwrap() {
                    let train = bodies(shape, count, 16, 0).unwrap();
                    let test = bodies(shape, count, 16, 1 << 40).unwrap();
                    assert!(test.iter().all(|b| b.len() <= target));
                    assert!(body(shape, count + 1, 0).unwrap().len() > target);
                    assert!(train.iter().all(|b| !test.contains(b)));
                    assert_ne!(test[0], test[1]);
                }
            }
        }
    }
}
