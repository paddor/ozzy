//! CPU-only benchmark of the production segment group's decode/verify path.
//! Encoding, corpus construction, and a byte-for-byte preflight are untimed.
//! No disk I/O, network, replication, or ACK latency is measured here.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, OperationLimits, encode_operation_body,
};
use ozzy_journal_segment::{
    BodyEncoding, CanonicalOperation, ChainPosition, CodecError, DecodeLimits, DecodedGroup,
    Digest, EncodedGroup, GroupStart, OperationKind, SEGMENT_HEADER_BYTES, SegmentHeader,
    decode_group, encode_group_with_body_encoding,
};
use ozzy_proto::{
    GroupId, MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};

use ozzy_bench::{automation::Result, workload};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Codec {
    Raw,
    Lz4,
}

impl Codec {
    fn encoding(self) -> BodyEncoding {
        match self {
            Codec::Raw => BodyEncoding::Raw,
            Codec::Lz4 => BodyEncoding::Lz4 {
                min_savings_bytes: 64,
            },
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    about = "In-memory segment-group decode + integrity verification (not broker throughput)"
)]
struct Args {
    #[arg(long, value_enum, default_value = "raw")]
    codec: Codec,
    #[arg(long, default_value_t = 1024)]
    record_bytes: usize,
    #[arg(long, default_value_t = 1000)]
    batch: usize,
    #[arg(long, default_value_t = workload::THROUGHPUT_CORPUS_BATCHES * 8)]
    corpus_batches: usize,
    #[arg(long, default_value_t = 3)]
    seconds: u64,
    #[arg(long, default_value_t = 1)]
    warmup: u64,
}

#[derive(Debug)]
struct Sample {
    group: EncodedGroup,
    before: ChainPosition,
    body: Vec<u8>,
}

impl Args {
    fn validate(&self) -> Result<()> {
        let batch_bytes = self.record_bytes.saturating_mul(self.batch);
        let corpus_bytes = batch_bytes.saturating_mul(self.corpus_batches);
        let bounded = (1..=65536).contains(&self.record_bytes)
            && (1..=10000).contains(&self.batch)
            && (1..=256).contains(&self.corpus_batches)
            && self.seconds != 0
            && batch_bytes <= 16 * 1024 * 1024
            && corpus_bytes <= 256 * 1024 * 1024;
        if !bounded {
            return Err("invalid or excessive corpus bounds".into());
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    args.validate()?;
    let (header, samples) = corpus(&args)?;
    // Byte-for-byte validation is deliberately outside the timing loop. Every
    // timed decode still verifies all normal header/body/chain/seal digests.
    preflight(&header, &samples)?;
    run(&header, &samples, Duration::from_secs(args.warmup))?;
    let (groups, elapsed) = run(&header, &samples, Duration::from_secs(args.seconds))?;
    println!("{}", report(&args, &samples, groups, elapsed));
    Ok(())
}

/// Each sample decodes to exactly its one operation and next chain.
fn preflight(header: &SegmentHeader, samples: &[Sample]) -> Result<()> {
    for sample in samples {
        let decoded = decode(header, sample)?;
        let exact = decoded.operations.len() == 1
            && decoded.operations[0].body.as_ref() == sample.body
            && decoded.next_chain == sample.group.next_chain();
        if !exact {
            return Err("corpus group does not decode to its operation".into());
        }
    }
    Ok(())
}

fn report(args: &Args, samples: &[Sample], groups: u64, elapsed: Duration) -> serde_json::Value {
    let body_bytes: usize = samples.iter().map(|sample| sample.body.len()).sum();
    let encoded_bytes: usize = samples
        .iter()
        .map(|sample| sample.group.as_bytes().len())
        .sum();
    let mean_body = body_bytes as f64 / samples.len() as f64;
    serde_json::json!({
        "measurement": "in_memory_segment_group_decode_and_verify",
        "workload": workload::NAME,
        "codec": format!("{:?}", args.codec),
        "record_bytes": args.record_bytes,
        "records_per_group": args.batch,
        "corpus_groups": samples.len(),
        "corpus_body_bytes": body_bytes,
        "corpus_encoded_bytes": encoded_bytes,
        "groups": groups,
        "seconds": elapsed.as_secs_f64(),
        "records_per_second": groups as f64 * args.batch as f64 / elapsed.as_secs_f64(),
        "body_gib_per_second": groups as f64 * mean_body / elapsed.as_secs_f64() / 1_073_741_824.0,
        "mean_group_us": elapsed.as_secs_f64() * 1e6 / groups as f64,
    })
}

fn corpus(args: &Args) -> Result<(SegmentHeader, Vec<Sample>)> {
    let header = SegmentHeader::new(
        GroupId::from_bytes([1; 16]),
        1,
        None,
        Digest::ZERO,
        1024 * 1024 * 1024,
    )?;
    let mut chain = ChainPosition::GENESIS;
    let mut offset = SEGMENT_HEADER_BYTES as u64;
    let mut samples = Vec::with_capacity(args.corpus_batches);
    for index in 0..args.corpus_batches {
        let body = append_body(args, (index * args.batch) as u64)?;
        let operation = CanonicalOperation {
            group_id: header.group_id(),
            configuration_epoch: 1,
            original_view: 1,
            op_number: chain.next_op_number(),
            previous_digest: chain.previous_digest(),
            kind: OperationKind::Append,
            body: &body,
        };
        let group = encode_group_with_body_encoding(
            &header,
            GroupStart::new(index as u64 + 1, offset, chain),
            &[operation],
            args.codec.encoding(),
        )?;
        let before = chain;
        chain = group.next_chain();
        offset = group.end_offset();
        samples.push(Sample {
            group,
            before,
            body,
        });
    }
    Ok((header, samples))
}

/// The canonical APPEND body of one batch of packed workload records.
fn append_body(args: &Args, sequence: u64) -> Result<Vec<u8>> {
    let payload = workload::packed_batch(args.record_bytes, args.batch, sequence);
    let records = payload
        .chunks_exact(args.record_bytes)
        .enumerate()
        .map(|(i, part)| AppendRecord {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes((u128::from(sequence) + i as u128 + 1).to_be_bytes()),
            parts: [part].into_iter().collect(),
        })
        .collect();
    let body = encode_operation_body(
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: PartitionIncarnation::from_bytes([2; 16]),
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([3; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(sequence),
                first_offset: Offset::new(sequence),
                append_timestamp_millis: 1_789_000_000_000,
                records,
            }],
        }),
        OperationLimits::default(),
    )?;
    Ok(body)
}

fn decode<'a>(
    header: &SegmentHeader,
    sample: &'a Sample,
) -> std::result::Result<DecodedGroup<'a>, CodecError> {
    decode_group(
        header,
        GroupStart::new(
            sample.group.group_number(),
            sample.group.start_offset(),
            sample.before,
        ),
        black_box(sample.group.as_bytes()),
        DecodeLimits::default(),
    )
}

fn run(header: &SegmentHeader, samples: &[Sample], duration: Duration) -> Result<(u64, Duration)> {
    let start = Instant::now();
    let mut groups = 0;
    while start.elapsed() < duration {
        // Bound each drain by the prevalidated corpus count and byte limits.
        for sample in samples {
            black_box(decode(header, sample)?);
            groups += 1;
        }
    }
    Ok((groups, start.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_exercises_integrity_and_compression() {
        for codec in [Codec::Raw, Codec::Lz4] {
            let args = Args {
                codec,
                record_bytes: 128,
                batch: 1000,
                corpus_batches: 2,
                seconds: 1,
                warmup: 0,
            };
            let (header, samples) = corpus(&args).unwrap();
            for sample in &samples {
                assert_eq!(
                    decode(&header, sample).unwrap().operations[0].body.as_ref(),
                    sample.body
                );
                if matches!(codec, Codec::Lz4) {
                    assert!(sample.group.as_bytes().len() < sample.body.len());
                }
                let mut corrupt = sample.group.as_bytes().to_vec();
                // First encoded body byte, never padding or a stored checksum.
                corrupt[ozzy_journal_segment::ENTRY_HEADER_BYTES] ^= 1;
                assert!(
                    decode_group(
                        &header,
                        GroupStart::new(
                            sample.group.group_number(),
                            sample.group.start_offset(),
                            sample.before
                        ),
                        &corrupt,
                        DecodeLimits::default()
                    )
                    .is_err()
                );
            }
        }
    }

    #[test]
    fn corpus_uses_multiple_distinct_batches() {
        let args = Args {
            codec: Codec::Raw,
            record_bytes: 128,
            batch: 1000,
            corpus_batches: workload::THROUGHPUT_CORPUS_BATCHES,
            seconds: 1,
            warmup: 0,
        };
        let (_, samples) = corpus(&args).unwrap();
        assert_ne!(samples[0].body, samples[1].body);
        assert_eq!(samples[0].group.next_chain(), samples[1].before);
    }
}
