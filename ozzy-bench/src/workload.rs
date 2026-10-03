use bytes::Bytes;

use crate::{BenchResult, bench_error};

mod json;
pub use json::append_json_record;
/// Result-row label of the JSON corpus. Rows labeled "JSON log events" used an
/// earlier generator whose events ended in random text and barely compressed.
pub const JSON_EVENT_CORPUS: &str =
    "8-byte monotonic submission clock followed by OMQ compression-benchmark JSON events";
mod binary;
pub use binary::{BINARY_EVENT_CORPUS, binary_event};

pub const NAME: &str = "structured-v1";
pub const THROUGHPUT_CORPUS_BATCHES: usize = 8;

const DATA: &[u8] =
    b"sku=AX42;region=eu-central-1;channel=web;campaign=autumn;warehouse=zh-3;priority=normal;";
const STATUSES: [&str; 4] = ["paid", "packed", "shipped", "returned"];

pub fn record(size: usize, sequence: u64) -> Bytes {
    let mut record = Vec::with_capacity(size);
    write_record(&mut record, size, sequence);
    Bytes::from(record)
}

fn write_record(record: &mut Vec<u8>, size: usize, sequence: u64) {
    record.clear();
    append_record(record, size, sequence);
}

/// Append one deterministic record directly into its final payload allocation.
pub fn append_record(record: &mut Vec<u8>, size: usize, sequence: u64) {
    let start = record.len();
    if size >= 96 {
        append_header(record, sequence);
        let suffix = b"\"}";
        if record.len() - start + suffix.len() <= size {
            let remaining = size - (record.len() - start) - suffix.len();
            append_data(record, remaining, sequence);
            record.extend_from_slice(suffix);
            return;
        }
        record.truncate(start);
    }

    append_data(record, size, sequence);
    let sequence_bytes = sequence.to_le_bytes();
    let copied = size.min(sequence_bytes.len());
    record[start..start + copied].copy_from_slice(&sequence_bytes[..copied]);
}

fn append_header(record: &mut Vec<u8>, sequence: u64) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    record.extend_from_slice(b"{\"type\":\"order\",\"seq\":\"");
    let mut hex = [0; 16];
    for (index, byte) in sequence.to_be_bytes().into_iter().enumerate() {
        hex[2 * index] = HEX[usize::from(byte >> 4)];
        hex[2 * index + 1] = HEX[usize::from(byte & 15)];
    }
    record.extend_from_slice(&hex);
    record.extend_from_slice(b"\",\"tenant\":");
    let mut number = itoa::Buffer::new();
    record.extend_from_slice(number.format(sequence % 64).as_bytes());
    record.extend_from_slice(b",\"amount\":");
    record.extend_from_slice(
        number
            .format(1_000 + sequence.wrapping_mul(7919) % 900_000)
            .as_bytes(),
    );
    record.extend_from_slice(b",\"status\":\"");
    record.extend_from_slice(STATUSES[sequence as usize % STATUSES.len()].as_bytes());
    record.extend_from_slice(b"\",\"data\":\"");
}

pub fn packed_batch(size: usize, count: usize, first_sequence: u64) -> Bytes {
    let mut packed = Vec::with_capacity(size.saturating_mul(count));
    for index in 0..count {
        append_record(&mut packed, size, first_sequence + index as u64);
    }
    Bytes::from(packed)
}

/// Full payload verification with bounded per-batch identity receipts.
///
/// Each submission owns a unique batch ID, starting at zero. Its record sequences
/// are `base + batch_id * batch_records + position`. Delivery may reorder whole batches,
/// but records within one atomic batch must stay ordered. After draining, `finish`
/// joins the observed IDs to writer confirmations in broker offset order. It does
/// not assert that no additional records can arrive after the checked cohort.
#[derive(Debug)]
pub struct VerifiedBatches {
    base: u64,
    size: usize,
    batch: u64,
    capacity: usize,
    position: u64,
    first: u64,
    ids: Vec<u64>,
    scratch: Vec<u8>,
}

impl VerifiedBatches {
    pub fn new(size: usize, batch: usize, capacity: usize, base: u64) -> BenchResult<Self> {
        if size < 8 || batch == 0 || capacity == 0 {
            return Err(bench_error(
                "verification requires size >= 8 and nonzero batch/capacity",
            ));
        }
        let batch = u64::try_from(batch)?;
        batch
            .checked_mul(u64::try_from(capacity)?)
            .and_then(|count| base.checked_add(count))
            .ok_or_else(|| bench_error("verified sequence range overflow"))?;
        Ok(Self {
            base,
            size,
            batch,
            capacity,
            position: 0,
            first: 0,
            ids: Vec::with_capacity(capacity),
            // A small record can fall back to binary after attempting the JSON
            // header. Reserve its maximum size before the measurement starts.
            scratch: Vec::with_capacity(size.max(128)),
        })
    }

    pub fn record(&mut self, payload: &[u8]) -> BenchResult<()> {
        if self.ids.len() == self.capacity || payload.len() != self.size {
            return Err(bench_error(
                "verified payload size or batch capacity mismatch",
            ));
        }
        let first = if self.position == 0 {
            let first = sequence(payload)?
                .checked_sub(self.base)
                .ok_or_else(|| bench_error("record sequence outside this run"))?;
            if !first.is_multiple_of(self.batch) || first / self.batch >= self.capacity as u64 {
                return Err(bench_error("invalid verified batch identity"));
            }
            first
        } else {
            self.first
        };
        let expected = self
            .base
            .checked_add(first)
            .and_then(|first| first.checked_add(self.position))
            .ok_or_else(|| bench_error("verified sequence overflow"))?;
        write_record(&mut self.scratch, self.size, expected);
        if payload != self.scratch {
            return Err(bench_error(format!(
                "verified record bytes differ at sequence {expected}"
            )));
        }
        self.first = first;
        self.position += 1;
        if self.position == self.batch {
            self.ids.push(first / self.batch);
            self.position = 0;
        }
        Ok(())
    }

    /// Confirm that payload identities match the submitted batches at their
    /// acknowledged offsets. The caller supplies IDs in delivery/offset order.
    pub fn finish(&self, confirmed_ids: &[u64]) -> BenchResult<()> {
        self.finish_for_workers(confirmed_ids, 1)
    }

    /// Each worker submits a contiguous prefix of its fixed share of the total
    /// capacity. Unequal worker throughput may leave unused gaps between shares.
    pub fn finish_for_workers(&self, confirmed_ids: &[u64], workers: usize) -> BenchResult<()> {
        if self.position != 0 || self.ids != confirmed_ids {
            return Err(bench_error(
                "delivered record identities do not match writer confirmations",
            ));
        }
        let shares = (0..workers)
            .map(|index| worker_share(self.capacity, workers, index))
            .collect::<BenchResult<Vec<_>>>()?;
        if shares.is_empty() {
            return Err(bench_error("verification requires at least one worker"));
        }
        let mut ordered = confirmed_ids.to_vec();
        ordered.sort_unstable();
        let mut remaining = ordered.as_slice();
        for share in shares {
            let count = remaining.partition_point(|&id| id < share.end as u64);
            if remaining[..count]
                .iter()
                .copied()
                .ne((share.start as u64..).take(count))
            {
                return Err(bench_error("missing or duplicate confirmed batch identity"));
            }
            remaining = &remaining[count..];
        }
        if !remaining.is_empty() {
            return Err(bench_error(
                "confirmed batch identity exceeds worker budget",
            ));
        }
        Ok(())
    }
}

/// Split one total budget into nonempty, disjoint worker shares, with at most
/// one unit of skew. Caller keeps the total unchanged when adding workers.
pub fn worker_share(
    total: usize,
    workers: usize,
    index: usize,
) -> BenchResult<std::ops::Range<usize>> {
    if workers == 0 || workers > total || index >= workers {
        return Err(bench_error("invalid worker budget division"));
    }
    let width = total / workers;
    let extra = total % workers;
    let start = index * width + index.min(extra);
    Ok(start..start + width + usize::from(index < extra))
}

/// Select a deterministic, run-specific sequence range without wrapping. The
/// run token must identify this execution, not a reusable chart/run label.
pub fn sequence_base(token: &str, batch: usize, capacity: usize) -> BenchResult<u64> {
    let records = u64::try_from(batch)?
        .checked_mul(u64::try_from(capacity)?)
        .filter(|count| *count > 0)
        .ok_or_else(|| bench_error("invalid verified sequence range"))?;
    let available = u64::MAX
        .checked_sub(records)
        .filter(|count| *count > 0)
        .ok_or_else(|| bench_error("verified sequence range too large"))?;
    let hash = ozzy_journal::integrity::hash("ozzy benchmark sequence range v1", token.as_bytes());
    Ok(u64::from_le_bytes(hash.as_bytes()[..8].try_into()?) % available)
}

fn sequence(payload: &[u8]) -> BenchResult<u64> {
    const PREFIX: &[u8] = b"{\"type\":\"order\",\"seq\":\"";
    if payload.len() >= 96 && payload.starts_with(PREFIX) {
        Ok(u64::from_str_radix(
            std::str::from_utf8(&payload[PREFIX.len()..PREFIX.len() + 16])?,
            16,
        )?)
    } else {
        Ok(u64::from_le_bytes(
            payload
                .get(..8)
                .ok_or_else(|| bench_error("record too short for sequence"))?
                .try_into()?,
        ))
    }
}

fn append_data(output: &mut Vec<u8>, count: usize, sequence: u64) {
    let mut start = sequence.wrapping_mul(17) as usize % DATA.len();
    let mut remaining = count;
    output.reserve(count);
    while remaining != 0 {
        let take = remaining.min(DATA.len() - start);
        output.extend_from_slice(&DATA[start..start + take]);
        remaining -= take;
        start = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stack_header_matches_original_json_format_for_boundaries_and_wraparound() {
        let mut actual = Vec::with_capacity(128);
        for sequence in (0..4096).chain([u64::MAX, u64::MAX / 7919, u64::MAX / 7919 + 1]) {
            actual.clear();
            append_header(&mut actual, sequence);
            let expected = format!(
                "{{\"type\":\"order\",\"seq\":\"{sequence:016x}\",\"tenant\":{},\"amount\":{},\"status\":\"{}\",\"data\":\"",
                sequence % 64,
                1_000 + sequence.wrapping_mul(7919) % 900_000,
                STATUSES[sequence as usize % STATUSES.len()],
            );
            assert_eq!(actual, expected.as_bytes());
        }
    }

    #[test]
    fn chunked_data_is_identical_to_the_original_byte_generator() {
        for sequence in [0, 1, 42, u64::MAX / 17, u64::MAX] {
            for count in [
                0,
                1,
                128,
                1024,
                DATA.len() - 1,
                DATA.len(),
                DATA.len() * 3 + 7,
            ] {
                let mut actual = vec![0xab; 9];
                append_data(&mut actual, count, sequence);
                let start = sequence.wrapping_mul(17) as usize % DATA.len();
                let mut expected = vec![0xab; 9];
                expected.extend((0..count).map(|index| DATA[(start + index) % DATA.len()]));
                assert_eq!(actual, expected, "sequence {sequence}, count {count}");
            }
        }
    }

    #[test]
    fn direct_payload_append_preserves_prefix_and_exact_record_bytes() {
        for size in [0, 4, 8, 95, 96, 120, 1016] {
            let mut bytes = vec![23; 8];
            append_record(&mut bytes, size, 42);
            assert_eq!(&bytes[..8], &[23; 8]);
            assert_eq!(&bytes[8..], record(size, 42).as_ref());
            let packed = packed_batch(size, 3, 42);
            assert_eq!(packed.len(), size * 3);
            for index in 0..3 {
                assert_eq!(
                    &packed[index * size..(index + 1) * size],
                    record(size, 42 + index as u64).as_ref()
                );
            }
        }
    }

    #[test]
    fn records_have_exact_requested_size() {
        for size in [1, 16, 64, 96, 128, 1_024] {
            assert_eq!(record(size, 42).len(), size);
        }
    }

    #[test]
    fn json_sized_records_are_valid_and_vary() {
        let first = record(128, 1);
        let second = record(128, 2);
        serde_json::from_slice::<serde_json::Value>(&first).unwrap();
        serde_json::from_slice::<serde_json::Value>(&second).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn packed_batches_are_deterministic() {
        let first = packed_batch(128, 8, 100);
        assert_eq!(first, packed_batch(128, 8, 100));
        assert_ne!(first, packed_batch(128, 8, 108));
        assert_eq!(first.len(), 1_024);
    }

    fn deliver(verifier: &mut VerifiedBatches, ids: &[u64]) {
        for &id in ids {
            for position in 0..verifier.batch {
                verifier
                    .record(&record(
                        verifier.size,
                        verifier.base + id * verifier.batch + position,
                    ))
                    .unwrap();
            }
        }
    }

    #[test]
    fn verified_batches_accept_reordered_submissions_only_at_confirmed_offsets() {
        for size in [8, 16, 64, 96, 97, 128, 1_024] {
            let base = sequence_base("one-execution", 3, 4).unwrap();
            let mut verifier = VerifiedBatches::new(size, 3, 4, base).unwrap();
            let buffers = (verifier.ids.as_ptr(), verifier.scratch.as_ptr());
            deliver(&mut verifier, &[2, 0, 3, 1]);
            verifier.finish(&[2, 0, 3, 1]).unwrap();
            assert!(verifier.finish(&[0, 1, 2, 3]).is_err());
            assert!(verifier.record(&record(size, base)).is_err());
            assert_eq!(buffers, (verifier.ids.as_ptr(), verifier.scratch.as_ptr()));
        }
    }

    #[test]
    fn verification_rejects_every_same_sized_byte_corruption() {
        for size in [8, 16, 96, 128, 1_024] {
            let base = sequence_base("integrity-execution", 2, 4).unwrap();
            let payload = record(size, base);
            for byte in 0..size {
                let mut verifier = VerifiedBatches::new(size, 2, 4, base).unwrap();
                let mut corrupt = payload.to_vec();
                corrupt[byte] ^= 0xff;
                // Corruption of the sequence may describe another valid batch.
                // It still cannot match the actual writer confirmation identity.
                let result = verifier.record(&corrupt).and_then(|()| {
                    verifier.record(&record(size, base + 1))?;
                    verifier.finish(&[0])
                });
                assert!(result.is_err(), "accepted corrupt size={size} byte={byte}");
            }
        }
    }

    #[test]
    fn verification_rejects_skipped_repeated_partial_and_foreign_records() {
        let base = sequence_base("this-execution", 3, 4).unwrap();
        let other = sequence_base("different-execution", 3, 4).unwrap();
        for size in [16, 96, 128] {
            for second in [base, base + 2, other] {
                let mut verifier = VerifiedBatches::new(size, 3, 4, base).unwrap();
                verifier.record(&record(size, base)).unwrap();
                assert!(verifier.record(&record(size, second)).is_err());
                assert!(verifier.finish(&[0]).is_err());
            }
            let mut verifier = VerifiedBatches::new(size, 3, 4, base).unwrap();
            assert!(verifier.record(&record(size, base + 1)).is_err());
            assert!(verifier.record(&record(size, base + 12)).is_err());
            assert!(verifier.record(&record(size, other)).is_err());
            assert!(verifier.record(&record(size + 1, base)).is_err());
        }
    }

    #[test]
    fn valid_bytes_alone_cannot_hide_missing_duplicate_or_substituted_batches() {
        for observed in [&[0, 0][..], &[0, 2], &[1, 0]] {
            let mut verifier = VerifiedBatches::new(128, 2, 4, 100).unwrap();
            deliver(&mut verifier, observed);
            assert!(verifier.finish(&[0, 1]).is_err());
        }
        for invalid_confirmations in [&[0, 0][..], &[0, 2]] {
            let mut verifier = VerifiedBatches::new(128, 2, 4, 100).unwrap();
            deliver(&mut verifier, invalid_confirmations);
            assert!(verifier.finish(invalid_confirmations).is_err());
        }
    }

    #[test]
    fn verified_ranges_reject_invalid_sizes_capacities_and_overflow() {
        for (size, batch, capacity, base) in [
            (7, 1, 1, 0),
            (8, 0, 1, 0),
            (8, 1, 0, 0),
            (8, 2, 1, u64::MAX - 1),
            (8, usize::MAX, 2, 0),
        ] {
            assert!(VerifiedBatches::new(size, batch, capacity, base).is_err());
        }
        assert!(sequence_base("run", 0, 1).is_err());
        assert!(sequence_base("run", usize::MAX, 2).is_err());
        let base = sequence_base("run", 1_000, 1_000_000).unwrap();
        assert!(base.checked_add(1_000_000_000).is_some());
        assert_eq!(base, sequence_base("run", 1_000, 1_000_000).unwrap());
    }

    #[test]
    fn worker_budgets_partition_the_total_without_growth_or_overlap() {
        for total in [1, 2, 3, 17, 1_000_000, usize::MAX] {
            for workers in 1..=total.min(16) {
                let mut end = 0;
                for index in 0..workers {
                    let share = worker_share(total, workers, index).unwrap();
                    assert_eq!(share.start, end);
                    assert!(!share.is_empty());
                    assert!((total / workers..=total.div_ceil(workers)).contains(&share.len()));
                    end = share.end;
                }
                assert_eq!(end, total);
            }
        }
        assert!(worker_share(1, 0, 0).is_err());
        assert!(worker_share(1, 2, 0).is_err());
        assert!(worker_share(2, 2, 2).is_err());
    }

    #[test]
    fn unequal_worker_prefixes_are_valid_but_holes_inside_one_worker_are_not() {
        for valid in [&[4, 0, 1][..], &[0, 4, 5, 6], &[4], &[]] {
            let mut verifier = VerifiedBatches::new(128, 2, 8, 100).unwrap();
            deliver(&mut verifier, valid);
            verifier.finish_for_workers(valid, 2).unwrap();
        }
        for invalid in [&[1, 4][..], &[0, 5], &[0, 4, 4]] {
            let mut verifier = VerifiedBatches::new(128, 2, 8, 100).unwrap();
            deliver(&mut verifier, invalid);
            assert!(verifier.finish_for_workers(invalid, 2).is_err());
        }
    }
}
