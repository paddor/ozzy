use lz4rip::block::{Compressor, Decompressor, DictCompressor, DictTrainer};
use ozzy_bench::automation::Result;
use ozzy_journal_segment::{ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, WRITE_GROUP_ALIGNMENT};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

pub(super) struct Dictionary {
    pub bytes: Vec<u8>,
    pub requested: usize,
    pub training_ns: u128,
    pub samples: usize,
    pub sample_bytes: usize,
}

pub(super) fn train(bodies: &[Vec<u8>], capacity: usize) -> Result<Dictionary> {
    if capacity == 0 {
        return Ok(Dictionary {
            bytes: vec![],
            requested: 0,
            training_ns: 0,
            samples: 0,
            sample_bytes: 0,
        });
    }
    let start = Instant::now();
    let mut trainer = DictTrainer::new(capacity);
    // The trainer rejects samples larger than its dictionary capacity.
    // Feed opaque body chunks; its own eight-dictionary-size budget stays active.
    for body in bodies {
        for chunk in body.chunks(capacity) {
            trainer.add_sample(chunk);
        }
    }
    let samples = trainer.sample_count();
    let sample_bytes = trainer.total_bytes();
    let bytes = trainer.train();
    if bytes.is_empty() || bytes.len() > capacity {
        return Err("invalid trained dictionary".into());
    }
    Ok(Dictionary {
        bytes,
        requested: capacity,
        training_ns: start.elapsed().as_nanos(),
        samples,
        sample_bytes,
    })
}

pub(super) fn aligned_bytes(lengths: impl Iterator<Item = usize>) -> usize {
    let entries: usize = lengths
        .map(|n| (ENTRY_HEADER_BYTES + n).div_ceil(8) * 8)
        .sum();
    (entries + GROUP_SEAL_BYTES).div_ceil(WRITE_GROUP_ALIGNMENT) * WRITE_GROUP_ALIGNMENT
}

fn timed(
    duration: Duration,
    count: usize,
    mut operation: impl FnMut(usize) -> Result<()>,
) -> Result<f64> {
    let start = Instant::now();
    let mut completed = 0_u64;
    while start.elapsed() < duration {
        for i in 0..count {
            operation(i)?;
        }
        completed += count as u64;
    }
    Ok(start.elapsed().as_secs_f64() * 1e9 / completed as f64)
}

pub(super) fn measure(
    bodies: &[Vec<u8>],
    dict: &Dictionary,
    warmup: Duration,
    duration: Duration,
) -> Result<Value> {
    let start = Instant::now();
    if dict.bytes.is_empty() {
        let mut compressor = Compressor::new();
        let setup = start.elapsed().as_nanos();
        measure_with(bodies, dict, warmup, duration, setup, |input, output| {
            compressor.compress_into(input, output)
        })
    } else {
        let mut compressor = DictCompressor::new(&dict.bytes);
        let setup = start.elapsed().as_nanos();
        measure_with(bodies, dict, warmup, duration, setup, |input, output| {
            compressor.compress_into(input, output)
        })
    }
}

fn measure_with(
    bodies: &[Vec<u8>],
    dict: &Dictionary,
    warmup: Duration,
    duration: Duration,
    setup: u128,
    mut compress: impl FnMut(
        &[u8],
        &mut [u8],
    ) -> std::result::Result<usize, lz4rip::block::CompressError>,
) -> Result<Value> {
    let maximum = bodies.iter().map(Vec::len).max().ok_or("empty corpus")?;
    let mut output = vec![0; lz4rip::get_maximum_output_size(maximum)];
    let mut restored = vec![0; maximum];
    let decoder = Decompressor::with_dict(&dict.bytes);
    let mut encoded = Vec::with_capacity(bodies.len());
    for body in bodies {
        let n = compress(body, &mut output)?;
        let restored_len = decoder.decompress_into(&output[..n], &mut restored)?;
        if &restored[..restored_len] != body {
            return Err("dictionary roundtrip mismatch".into());
        }
        encoded.push(output[..n].to_vec());
    }
    timed(warmup, bodies.len(), |i| {
        let n = compress(black_box(&bodies[i]), black_box(&mut output))?;
        black_box(&output[..n]);
        Ok(())
    })?;
    // Epoch-based table reuse may legitimately vary compressed bytes. Measure
    // their actual lengths instead of requiring a deterministic representation.
    let mut lengths = vec![(usize::MAX, 0, 0_u64, 0_u64); bodies.len()];
    let encode_ns = timed(duration, bodies.len(), |i| {
        let n = compress(black_box(&bodies[i]), black_box(&mut output))?;
        let stats = &mut lengths[i];
        stats.0 = stats.0.min(n);
        stats.1 = stats.1.max(n);
        stats.2 += n as u64;
        stats.3 += 1;
        black_box(&output[..n]);
        Ok(())
    })?;
    // Use the warmed encoder's output for decompression and size modeling.
    for (body, encoded) in bodies.iter().zip(&mut encoded) {
        let n = compress(body, &mut output)?;
        let restored_len = decoder.decompress_into(&output[..n], &mut restored)?;
        if &restored[..restored_len] != body {
            return Err("post-encode roundtrip mismatch".into());
        }
        encoded.clear();
        encoded.extend_from_slice(&output[..n]);
    }
    let mut decode_one = |i: usize| -> Result<()> {
        let n = decoder.decompress_into(black_box(&encoded[i]), black_box(&mut restored))?;
        if n != bodies[i].len() {
            return Err("decoded length mismatch".into());
        }
        black_box(&restored[..n]);
        Ok(())
    };
    timed(warmup, bodies.len(), &mut decode_one)?;
    let decode_ns = timed(duration, bodies.len(), decode_one)?;
    // Recheck full bytes after all context reuse and measured iterations.
    for body in bodies {
        let n = compress(body, &mut output)?;
        let n = decoder.decompress_into(&output[..n], &mut restored)?;
        if &restored[..n] != body {
            return Err("post-timing roundtrip mismatch".into());
        }
    }
    let body_bytes: usize = bodies.iter().map(Vec::len).sum();
    let encoded_mean =
        lengths.iter().map(|s| s.2 as f64 / s.3 as f64).sum::<f64>() / bodies.len() as f64;
    let groups = observed_write_sizes(bodies, &encoded, &lengths);
    Ok(
        json!({"dict_capacity":dict.requested,"dict_bytes":dict.bytes.len(),
        "dict_sha256":format!("{:x}",Sha256::digest(&dict.bytes)),
        "training_ns":dict.training_ns,"training_samples_retained":dict.samples,"training_bytes_retained":dict.sample_bytes,
        "compressor_setup_ns":setup,"body_bytes":bodies[0].len(),"samples":bodies.len(),
        "lz4_bytes_mean":encoded_mean,
        "encoded_fraction":encoded_mean * bodies.len() as f64 / body_bytes as f64,
        "samples_with_encoded_size_variation":lengths.iter().filter(|s| s.0 != s.1).count(),
        "encode_ns_per_operation":encode_ns,"decode_ns_per_operation":decode_ns,
        "write_groups":groups,"verified_roundtrips":bodies.len()*3}),
    )
}

fn observed_write_sizes(
    bodies: &[Vec<u8>],
    encoded: &[Vec<u8>],
    lengths: &[(usize, usize, u64, u64)],
) -> Vec<Value> {
    let mut groups = write_sizes(bodies, &encoded.iter().map(Vec::len).collect::<Vec<_>>());
    let minimum = write_sizes(bodies, &lengths.iter().map(|s| s.0).collect::<Vec<_>>());
    let maximum = write_sizes(bodies, &lengths.iter().map(|s| s.1).collect::<Vec<_>>());
    for ((group, minimum), maximum) in groups.iter_mut().zip(minimum).zip(maximum) {
        group["observed_min_selected_bytes_mean"] = minimum["selected_bytes_mean"].clone();
        group["observed_max_selected_bytes_mean"] = maximum["selected_bytes_mean"].clone();
    }
    groups
}

fn write_sizes(bodies: &[Vec<u8>], lengths: &[usize]) -> Vec<Value> {
    [1, 4, 16].into_iter().map(|count| {
        let mut raw = 0;
        let mut selected = 0;
        let mut smaller = 0;
        for (body, compressed) in bodies.chunks(count).zip(lengths.chunks(count)) {
            let raw_group = aligned_bytes(body.iter().map(Vec::len));
            let compressed_group = aligned_bytes(body.iter().zip(compressed).map(|(b,c)|
                if c + 32 <= b.len() { *c } else { b.len() }));
            raw += raw_group;
            selected += compressed_group;
            smaller += usize::from(compressed_group < raw_group);
        }
        let groups = bodies.len().div_ceil(count);
        json!({"operations_per_write":count,"raw_bytes_mean":raw as f64 / groups as f64,
            "selected_bytes_mean":selected as f64 / groups as f64,"groups_saving_blocks":smaller,"groups":groups})
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::super::corpus::{self, Shape};
    use super::*;
    use ozzy_journal_segment::{
        BodyEncoding, CanonicalOperation, ChainPosition, Digest as JournalDigest, OperationKind,
        SEGMENT_HEADER_BYTES, SegmentHeader, encode_group_with_body_encoding,
    };
    use ozzy_proto::GroupId;

    #[test]
    fn small_bodies_survive_repeated_context_reuse() {
        let shape = Shape {
            record_bytes: 128,
            target: 256,
            random: false,
        };
        let count = corpus::record_count(shape).unwrap().unwrap();
        let bodies = corpus::bodies(shape, count, 128, 1 << 40).unwrap();
        let pristine = bodies.clone();
        let mut compressor = Compressor::new();
        let decoder = Decompressor::new();
        let mut output = vec![0; lz4rip::get_maximum_output_size(bodies[0].len())];
        let mut restored = vec![0; bodies[0].len()];
        for _ in 0..600 {
            for body in &bodies {
                let n = compressor.compress_into(body, &mut output).unwrap();
                let n = decoder
                    .decompress_into(&output[..n], &mut restored)
                    .unwrap();
                assert_eq!(&restored[..n], body);
            }
        }
        assert_eq!(bodies, pristine);
        let dict = train(&[], 0).unwrap();
        measure(
            &bodies,
            &dict,
            Duration::from_millis(50),
            Duration::from_millis(50),
        )
        .unwrap();
    }

    #[test]
    fn size_model_matches_production_group_framing_and_raw_fallback() {
        let header = SegmentHeader::new(
            GroupId::from_bytes([1; 16]),
            1,
            None,
            JournalDigest::ZERO,
            1024 * 1024,
        )
        .unwrap();
        for random in [false, true] {
            for target in [512, 4096, 8192, 16384] {
                let shape = Shape {
                    record_bytes: 128,
                    target,
                    random,
                };
                let count = corpus::record_count(shape).unwrap().unwrap();
                let bodies = corpus::bodies(shape, count, 16, 1 << 40).unwrap();
                let mut c = Compressor::new();
                let encoded: Vec<_> = bodies.iter().map(|b| c.compress(b).len()).collect();
                let predicted = write_sizes(&bodies, &encoded);
                for operations in [1, 4, 16] {
                    let mut total = 0;
                    for chunk in bodies.chunks(operations) {
                        let mut chain = ChainPosition::new(1, JournalDigest::ZERO);
                        let descriptions: Vec<_> = chunk
                            .iter()
                            .map(|body| {
                                let op = CanonicalOperation {
                                    group_id: header.group_id(),
                                    configuration_epoch: 1,
                                    original_view: 1,
                                    op_number: chain.next_op_number(),
                                    previous_digest: chain.previous_digest(),
                                    kind: OperationKind::Append,
                                    body,
                                };
                                chain = ChainPosition::new(
                                    op.op_number + 1,
                                    ozzy_journal_segment::logical_operation_digest(&op),
                                );
                                op
                            })
                            .collect();
                        let group = encode_group_with_body_encoding(
                            &header,
                            1,
                            SEGMENT_HEADER_BYTES as u64,
                            ChainPosition::new(1, JournalDigest::ZERO),
                            &descriptions,
                            BodyEncoding::Lz4 {
                                min_savings_bytes: 32,
                            },
                        )
                        .unwrap();
                        total += group.as_bytes().len();
                    }
                    let row = predicted
                        .iter()
                        .find(|r| r["operations_per_write"] == operations)
                        .unwrap();
                    assert_eq!(
                        row["selected_bytes_mean"],
                        total as f64 / bodies.len().div_ceil(operations) as f64
                    );
                }
            }
        }
    }

    #[test]
    fn dictionaries_are_bounded_and_held_out_bodies_roundtrip() {
        let shape = Shape {
            record_bytes: 128,
            target: 8192,
            random: false,
        };
        let count = corpus::record_count(shape).unwrap().unwrap();
        let training = corpus::bodies(shape, count, 64, 0).unwrap();
        let test = corpus::bodies(shape, count, 16, 1 << 40).unwrap();
        for capacity in [0, 2048, 8192] {
            let dict = train(&training, capacity).unwrap();
            assert!(dict.bytes.len() <= capacity);
            assert!(dict.sample_bytes <= capacity * 8);
            let result = measure(
                &test,
                &dict,
                Duration::from_millis(1),
                Duration::from_millis(1),
            )
            .unwrap();
            assert_eq!(result["verified_roundtrips"], 48);
        }
    }
}
