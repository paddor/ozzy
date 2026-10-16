use lz4rip::block::{CompressError, Compressor, Decompressor, DictCompressor, DictTrainer};
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

/// Warm-up and measured time for each codec phase.
#[derive(Clone, Copy, Debug)]
pub(super) struct Timing {
    pub warmup: Duration,
    pub duration: Duration,
}

pub(super) fn measure(bodies: &[Vec<u8>], dict: &Dictionary, timing: Timing) -> Result<Value> {
    let start = Instant::now();
    if dict.bytes.is_empty() {
        let mut compressor = Compressor::new();
        let setup = start.elapsed().as_nanos();
        measure_with(bodies, dict, timing, setup, |input, output| {
            compressor.compress_into(input, output)
        })
    } else {
        let mut compressor = DictCompressor::new(&dict.bytes);
        let setup = start.elapsed().as_nanos();
        measure_with(bodies, dict, timing, setup, |input, output| {
            compressor.compress_into(input, output)
        })
    }
}

fn measure_with(
    bodies: &[Vec<u8>],
    dict: &Dictionary,
    timing: Timing,
    setup: u128,
    compress: impl FnMut(&[u8], &mut [u8]) -> std::result::Result<usize, CompressError>,
) -> Result<Value> {
    let maximum = bodies.iter().map(Vec::len).max().ok_or("empty corpus")?;
    let mut codec = Codec {
        compress,
        decoder: Decompressor::with_dict(&dict.bytes),
        output: vec![0; lz4rip::get_maximum_output_size(maximum)],
        restored: vec![0; maximum],
    };
    let mut encoded = bodies
        .iter()
        .map(|body| {
            Ok(codec
                .roundtrip(body, "dictionary roundtrip mismatch")?
                .to_vec())
        })
        .collect::<Result<Vec<_>>>()?;
    let (encode_ns, sizes) = codec.time_encode(bodies, timing)?;
    // Use the warmed encoder's output for decompression and size modeling.
    for (body, encoded) in bodies.iter().zip(&mut encoded) {
        let fresh = codec.roundtrip(body, "post-encode roundtrip mismatch")?;
        encoded.clear();
        encoded.extend_from_slice(fresh);
    }
    let decode_ns = codec.time_decode(bodies, &encoded, timing)?;
    // Recheck full bytes after all context reuse and measured iterations.
    for body in bodies {
        codec.roundtrip(body, "post-timing roundtrip mismatch")?;
    }
    let body_bytes: usize = bodies.iter().map(Vec::len).sum();
    let encoded_mean = sizes.iter().map(|size| size.mean()).sum::<f64>() / bodies.len() as f64;
    let groups = observed_write_sizes(bodies, &encoded, &sizes);
    Ok(
        json!({"dict_capacity":dict.requested,"dict_bytes":dict.bytes.len(),
        "dict_sha256":format!("{:x}",Sha256::digest(&dict.bytes)),
        "training_ns":dict.training_ns,"training_samples_retained":dict.samples,"training_bytes_retained":dict.sample_bytes,
        "compressor_setup_ns":setup,"body_bytes":bodies[0].len(),"samples":bodies.len(),
        "lz4_bytes_mean":encoded_mean,
        "encoded_fraction":encoded_mean * bodies.len() as f64 / body_bytes as f64,
        "samples_with_encoded_size_variation":sizes.iter().filter(|size| size.varied()).count(),
        "encode_ns_per_operation":encode_ns,"decode_ns_per_operation":decode_ns,
        "write_groups":groups,"verified_roundtrips":bodies.len()*3}),
    )
}

/// One compressor, the matching dictionary decoder, and their scratch buffers.
struct Codec<C> {
    compress: C,
    decoder: Decompressor,
    output: Vec<u8>,
    restored: Vec<u8>,
}

impl<C> Codec<C>
where
    C: FnMut(&[u8], &mut [u8]) -> std::result::Result<usize, CompressError>,
{
    /// Compress `body`, require it to decode back exactly, and return the
    /// encoded bytes.
    fn roundtrip(&mut self, body: &[u8], mismatch: &'static str) -> Result<&[u8]> {
        let n = (self.compress)(body, &mut self.output)?;
        let restored = self
            .decoder
            .decompress_into(&self.output[..n], &mut self.restored)?;
        if &self.restored[..restored] != body {
            return Err(mismatch.into());
        }
        Ok(&self.output[..n])
    }

    /// Warm the encoder, then measure its mean time per body and the sizes
    /// each body compressed to while measured.
    fn time_encode(
        &mut self,
        bodies: &[Vec<u8>],
        timing: Timing,
    ) -> Result<(f64, Vec<EncodedSizes>)> {
        timed(timing.warmup, bodies.len(), |i| {
            let n = (self.compress)(black_box(&bodies[i]), black_box(&mut self.output))?;
            black_box(&self.output[..n]);
            Ok(())
        })?;
        // Epoch-based table reuse may legitimately vary compressed bytes. Measure
        // their actual lengths instead of requiring a deterministic representation.
        let mut sizes = vec![EncodedSizes::NONE; bodies.len()];
        let encode_ns = timed(timing.duration, bodies.len(), |i| {
            let n = (self.compress)(black_box(&bodies[i]), black_box(&mut self.output))?;
            sizes[i].record(n);
            black_box(&self.output[..n]);
            Ok(())
        })?;
        Ok((encode_ns, sizes))
    }

    /// Warm the decoder, then measure its mean time per encoded body.
    fn time_decode(
        &mut self,
        bodies: &[Vec<u8>],
        encoded: &[Vec<u8>],
        timing: Timing,
    ) -> Result<f64> {
        let mut decode_one = |i: usize| -> Result<()> {
            let n = self
                .decoder
                .decompress_into(black_box(&encoded[i]), black_box(&mut self.restored))?;
            if n != bodies[i].len() {
                return Err("decoded length mismatch".into());
            }
            black_box(&self.restored[..n]);
            Ok(())
        };
        timed(timing.warmup, bodies.len(), &mut decode_one)?;
        timed(timing.duration, bodies.len(), decode_one)
    }
}

/// The compressed sizes one body produced over the measured iterations.
#[derive(Clone, Copy, Debug)]
struct EncodedSizes {
    min: usize,
    max: usize,
    total: u64,
    count: u64,
}

impl EncodedSizes {
    const NONE: Self = Self {
        min: usize::MAX,
        max: 0,
        total: 0,
        count: 0,
    };

    fn record(&mut self, bytes: usize) {
        self.min = self.min.min(bytes);
        self.max = self.max.max(bytes);
        self.total += bytes as u64;
        self.count += 1;
    }

    fn mean(self) -> f64 {
        self.total as f64 / self.count as f64
    }

    fn varied(self) -> bool {
        self.min != self.max
    }
}

fn observed_write_sizes(
    bodies: &[Vec<u8>],
    encoded: &[Vec<u8>],
    sizes: &[EncodedSizes],
) -> Vec<Value> {
    let mut groups = write_sizes(bodies, &encoded.iter().map(Vec::len).collect::<Vec<_>>());
    let minimum = write_sizes(
        bodies,
        &sizes.iter().map(|size| size.min).collect::<Vec<_>>(),
    );
    let maximum = write_sizes(
        bodies,
        &sizes.iter().map(|size| size.max).collect::<Vec<_>>(),
    );
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
        BodyEncoding, CanonicalOperation, ChainPosition, Digest as JournalDigest, GroupStart,
        OperationKind, SEGMENT_HEADER_BYTES, SegmentHeader, encode_group_with_body_encoding,
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
            Timing {
                warmup: Duration::from_millis(50),
                duration: Duration::from_millis(50),
            },
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
                            GroupStart::new(
                                1,
                                SEGMENT_HEADER_BYTES as u64,
                                ChainPosition::new(1, JournalDigest::ZERO),
                            ),
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
                Timing {
                    warmup: Duration::from_millis(1),
                    duration: Duration::from_millis(1),
                },
            )
            .unwrap();
            assert_eq!(result["verified_roundtrips"], 48);
        }
    }
}
