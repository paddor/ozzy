//! Durable attempt outcomes, separate from the newly stored record body.

use crate::operation::codec::encode_atomic;
use ozzy_proto::append::{
    AppendDisposition, AppendOutcome, OUTCOME_RUN_BYTES, OutcomeRun, OutcomeRuns,
};
use ozzy_proto::{Offset, ProducerSequence};

use super::{
    Encoder, OperationCodecError, OperationLimits, OperationOutput, Reader, RetryFingerprint,
    enforce_limit, validate_limits,
};

#[derive(Debug, Clone, Copy)]
enum Runs<'a> {
    Classified(&'a [AppendOutcome]),
    Encoded(&'a [[u8; OUTCOME_RUN_BYTES]]),
}

#[derive(Debug, Clone, Copy)]
enum Proofs<'a> {
    Classified(&'a [RetryFingerprint]),
    Encoded(&'a [[u8; 32]]),
}

/// Validated, borrowed results for one producer's attempted sequences.
/// Duplicate payloads are absent; their exact fingerprints remain in order.
#[derive(Debug, Clone, Copy)]
pub struct AppendResults<'a> {
    logical_time: u64,
    attempts: u32,
    stored: u32,
    run_count: u32,
    runs: Runs<'a>,
    proofs: Proofs<'a>,
}

impl PartialEq for AppendResults<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.logical_time == other.logical_time
            && self.attempts == other.attempts
            && self.runs().eq(other.runs())
            && self
                .duplicate_fingerprints()
                .eq(other.duplicate_fingerprints())
    }
}

impl Eq for AppendResults<'_> {}

impl<'a> AppendResults<'a> {
    /// Validate fresh classification before any canonical bytes become visible.
    pub fn classified(
        logical_time: u64,
        outcomes: &'a [AppendOutcome],
        duplicate_fingerprints: &'a [RetryFingerprint],
        limits: OperationLimits,
    ) -> Result<Self, OperationCodecError> {
        validate_limits(limits)?;
        enforce_limit("append attempt count", outcomes.len(), limits.max_records)?;
        if outcomes.is_empty() {
            return Err(OperationCodecError::InvalidAppendResults);
        }
        let mut stored = 0u32;
        let mut duplicates = 0usize;
        let mut run_count = 0u32;
        for run in OutcomeRuns::new(outcomes) {
            run.encode()
                .map_err(|_| OperationCodecError::InvalidAppendResults)?;
            run_count = run_count
                .checked_add(1)
                .ok_or(OperationCodecError::LengthOverflow)?;
            match run.disposition {
                AppendDisposition::Stored => {
                    stored = stored
                        .checked_add(run.records)
                        .ok_or(OperationCodecError::LengthOverflow)?;
                }
                AppendDisposition::Duplicate => {
                    duplicates = duplicates
                        .checked_add(run.records as usize)
                        .ok_or(OperationCodecError::LengthOverflow)?;
                }
                _ => return Err(OperationCodecError::InvalidAppendResults),
            }
        }
        if duplicates != duplicate_fingerprints.len() {
            return Err(OperationCodecError::InvalidAppendResults);
        }
        let results = Self {
            logical_time,
            attempts: u32::try_from(outcomes.len())
                .map_err(|_| OperationCodecError::LengthOverflow)?,
            stored,
            run_count,
            runs: Runs::Classified(outcomes),
            proofs: Proofs::Classified(duplicate_fingerprints),
        };
        enforce_limit(
            "append result bytes",
            results.encoded_bytes()?,
            limits.max_body_bytes,
        )?;
        Ok(results)
    }

    /// Replicated logical broker-log time at acceptance.
    pub const fn logical_time(self) -> u64 {
        self.logical_time
    }

    /// Producer sequences advance by attempts, even when no record is stored.
    pub const fn attempts(self) -> u32 {
        self.attempts
    }

    /// Partition offsets advance only by stored outcomes.
    pub const fn stored(self) -> u32 {
        self.stored
    }

    /// Validate producer and record boundaries against the accompanying batch.
    /// Duplicate references must precede their attempt; they consume no offsets.
    pub fn validate_positions(
        self,
        first_sequence: ProducerSequence,
        first_offset: Offset,
        stored_records: usize,
    ) -> Result<(ProducerSequence, Offset), OperationCodecError> {
        if stored_records != self.stored as usize {
            return Err(OperationCodecError::InvalidAppendResults);
        }
        let sequence = first_sequence
            .get()
            .checked_add(u64::from(self.attempts))
            .ok_or(OperationCodecError::AppendPositionOverflow)?;
        let mut next = first_offset.get();
        for run in self.runs() {
            let end = run
                .first_offset
                .get()
                .checked_add(u64::from(run.records))
                .ok_or(OperationCodecError::AppendPositionOverflow)?;
            match run.disposition {
                AppendDisposition::Stored if run.first_offset.get() == next => {
                    next = end;
                }
                AppendDisposition::Duplicate if end <= next => {}
                _ => return Err(OperationCodecError::InvalidAppendResults),
            }
        }
        Ok((ProducerSequence::new(sequence), Offset::new(next)))
    }

    /// Walk attempted records and their optional discarded-payload evidence once.
    /// Ordinary stored records retain their byte-for-byte journal retry evidence.
    pub fn attempts_with_fingerprints(
        self,
    ) -> impl Iterator<Item = (AppendOutcome, Option<RetryFingerprint>)> + Clone + 'a {
        let mut proofs = self.duplicate_fingerprints();
        self.runs()
            .flat_map(OutcomeRun::outcomes)
            .map(move |outcome| {
                let proof = if outcome.disposition == AppendDisposition::Duplicate {
                    proofs.next()
                } else {
                    None
                };
                (outcome, proof)
            })
    }

    /// Iterate the shared compact mapping without allocating or copying payloads.
    #[expect(
        clippy::expect_used,
        reason = "read_results validated every encoded run"
    )]
    pub fn runs(self) -> impl Iterator<Item = OutcomeRun> + Clone + 'a {
        let (classified, encoded) = match self.runs {
            Runs::Classified(rows) => (Some(OutcomeRuns::new(rows)), None),
            Runs::Encoded(rows) => (None, Some(rows.iter())),
        };
        classified.into_iter().flatten().chain(
            encoded
                .into_iter()
                .flatten()
                .map(|bytes| OutcomeRun::decode(bytes).expect("validated append result run")),
        )
    }

    /// Fingerprints correspond only to duplicate attempts, in attempt order.
    pub fn duplicate_fingerprints(self) -> impl Iterator<Item = RetryFingerprint> + Clone + 'a {
        let (classified, encoded) = match self.proofs {
            Proofs::Classified(rows) => (Some(rows.iter().copied()), None),
            Proofs::Encoded(rows) => (
                None,
                Some(
                    rows.iter()
                        .map(|bytes| RetryFingerprint::from_bytes(*bytes)),
                ),
            ),
        };
        classified
            .into_iter()
            .flatten()
            .chain(encoded.into_iter().flatten())
    }

    pub(super) fn encoded_bytes(self) -> Result<usize, OperationCodecError> {
        (self.run_count as usize)
            .checked_mul(OUTCOME_RUN_BYTES)
            .and_then(|bytes| {
                ((self.attempts - self.stored) as usize)
                    .checked_mul(32)?
                    .checked_add(bytes)
            })
            .and_then(|bytes| bytes.checked_add(16))
            .ok_or(OperationCodecError::LengthOverflow)
    }
}

/// Append time, counts, shared outcome runs, and duplicate fingerprints.
/// Failure restores the output length; previously appended bytes stay intact.
pub fn append_results(
    output: &mut impl OperationOutput,
    results: AppendResults<'_>,
    limits: OperationLimits,
) -> Result<(), OperationCodecError> {
    encode_atomic(output, limits, |encoder| {
        write_results(encoder, results, limits)
    })
    .map(|_| ())
}

pub(super) fn write_results(
    encoder: &mut Encoder<'_, impl OperationOutput>,
    results: AppendResults<'_>,
    limits: OperationLimits,
) -> Result<(), OperationCodecError> {
    enforce_limit(
        "append attempt count",
        results.attempts as usize,
        limits.max_records,
    )?;
    encoder.check_length(results.encoded_bytes()?)?;
    encoder.u64(results.logical_time)?;
    encoder.u32(results.attempts)?;
    encoder.u32(results.run_count)?;
    for run in results.runs() {
        encoder.bytes(
            &run.encode()
                .map_err(|_| OperationCodecError::InvalidAppendResults)?,
        )?;
    }
    for proof in results.duplicate_fingerprints() {
        encoder.bytes(proof.as_bytes())?;
    }
    Ok(())
}

/// Borrow canonical result bytes after validating every run and proof boundary.
/// The caller must still check sequences, stored-record count, and partition end.
pub fn decode_append_results(
    input: &[u8],
    limits: OperationLimits,
) -> Result<AppendResults<'_>, OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("append result bytes", input.len(), limits.max_body_bytes)?;
    let mut decoder = Reader::new(input);
    let results = read_results(&mut decoder, limits)?;
    decoder.finish()?;
    Ok(results)
}

pub(super) fn read_results<'a>(
    decoder: &mut Reader<'a>,
    limits: OperationLimits,
) -> Result<AppendResults<'a>, OperationCodecError> {
    let logical_time = decoder.u64()?;
    let attempts = decoder.u32()?;
    let run_count = decoder.u32()?;
    enforce_limit(
        "append attempt count",
        attempts as usize,
        limits.max_records,
    )?;
    if attempts == 0 || run_count == 0 || run_count > attempts {
        return Err(OperationCodecError::InvalidAppendResults);
    }
    let bytes = (run_count as usize)
        .checked_mul(OUTCOME_RUN_BYTES)
        .ok_or(OperationCodecError::LengthOverflow)?;
    let runs = decoder.take(bytes)?.as_chunks::<OUTCOME_RUN_BYTES>().0;
    let (mut counted, mut stored) = (0u32, 0u32);
    for bytes in runs {
        let run =
            OutcomeRun::decode(bytes).map_err(|_| OperationCodecError::InvalidAppendResults)?;
        counted = counted
            .checked_add(run.records)
            .ok_or(OperationCodecError::LengthOverflow)?;
        if run.disposition == AppendDisposition::Stored {
            stored = stored
                .checked_add(run.records)
                .ok_or(OperationCodecError::LengthOverflow)?;
        }
    }
    if counted != attempts {
        return Err(OperationCodecError::InvalidAppendResults);
    }
    let bytes = ((attempts - stored) as usize)
        .checked_mul(32)
        .ok_or(OperationCodecError::LengthOverflow)?;
    let proofs = decoder.take(bytes)?.as_chunks::<32>().0;
    Ok(AppendResults {
        logical_time,
        attempts,
        stored,
        run_count,
        runs: Runs::Encoded(runs),
        proofs: Proofs::Encoded(proofs),
    })
}

#[cfg(test)]
mod tests;
