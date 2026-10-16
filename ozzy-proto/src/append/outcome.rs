use super::CodecError;
use crate::{Offset, layout::Fixed};

/// Bytes in one contiguous outcome run: disposition, first offset, record count.
pub const OUTCOME_RUN_BYTES: usize = 13;

/// Result of one producer sequence, independent of its partition offset.
/// An outcome proves no confirmation until its containing operation commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendOutcome {
    /// Whether this attempt appended a record or reused an existing event.
    pub disposition: AppendDisposition,
    /// Newly assigned offset or the original event's offset.
    pub offset: Offset,
}

/// Disposition of one accepted producer record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(u8)]
pub enum AppendDisposition {
    /// This occurrence creates a new stored record.
    Stored = 0,
    /// An earlier occurrence wins; no additional user record is stored.
    Duplicate = 1,
}

impl TryFrom<u8> for AppendDisposition {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Stored),
            1 => Ok(Self::Duplicate),
            _ => Err(CodecError::Profile),
        }
    }
}

/// Consecutive producer sequences mapped to consecutive offsets of one kind.
/// Repeated or reordered duplicate offsets start another run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutcomeRun {
    /// Stored or duplicate for every attempt in this run.
    pub disposition: AppendDisposition,
    /// Partition offset mapped to the first attempt.
    pub first_offset: Offset,
    /// Nonzero number of consecutive attempts and offsets.
    pub records: u32,
}

impl OutcomeRun {
    /// Resolve one attempt without inferring a confirmation boundary.
    pub fn outcome(self, index: u32) -> Option<AppendOutcome> {
        (index < self.records)
            .then(|| self.first_offset.get().checked_add(u64::from(index)))
            .flatten()
            .map(|offset| AppendOutcome {
                disposition: self.disposition,
                offset: Offset::new(offset),
            })
    }

    /// Every attempt of this run, in order.
    pub fn outcomes(self) -> impl Iterator<Item = AppendOutcome> + Clone {
        let first = self.first_offset.get();
        let end = first.saturating_add(u64::from(self.records));
        (first..end).map(move |offset| AppendOutcome {
            disposition: self.disposition,
            offset: Offset::new(offset),
        })
    }

    /// Encode one nonempty run, rejecting an unrepresentable exclusive end.
    pub fn encode(self) -> Result<[u8; OUTCOME_RUN_BYTES], CodecError> {
        self.validate()?;
        let mut bytes = [0; OUTCOME_RUN_BYTES];
        bytes[0] = self.disposition as u8;
        bytes[1..9].copy_from_slice(&self.first_offset.get().to_be_bytes());
        bytes[9..].copy_from_slice(&self.records.to_be_bytes());
        Ok(bytes)
    }

    /// Decode the shared journal/confirmation run representation.
    /// Session, sequence, and partition fences remain the caller's obligation.
    pub fn decode(bytes: &[u8; OUTCOME_RUN_BYTES]) -> Result<Self, CodecError> {
        let run = Self {
            disposition: AppendDisposition::try_from(bytes.u8_at::<0>())?,
            first_offset: Offset::new(bytes.u64_at::<1>()),
            records: bytes.u32_at::<9>(),
        };
        run.validate()?;
        Ok(run)
    }

    fn validate(self) -> Result<(), CodecError> {
        if self.records == 0
            || self
                .first_offset
                .get()
                .checked_add(u64::from(self.records))
                .is_none()
        {
            return Err(CodecError::Length);
        }
        Ok(())
    }
}

/// Allocation-free grouping of already classified attempts in submission order.
#[derive(Debug, Clone)]
pub struct OutcomeRuns<'a> {
    remaining: &'a [AppendOutcome],
}

impl<'a> OutcomeRuns<'a> {
    /// Keep sparse confirmations prompt; this iterator adds no batching delay.
    pub const fn new(outcomes: &'a [AppendOutcome]) -> Self {
        Self {
            remaining: outcomes,
        }
    }
}

impl Iterator for OutcomeRuns<'_> {
    type Item = OutcomeRun;

    fn next(&mut self) -> Option<Self::Item> {
        let (&first, rest) = self.remaining.split_first()?;
        let mut run = OutcomeRun {
            disposition: first.disposition,
            first_offset: first.offset,
            records: 1,
        };
        for outcome in rest {
            let Some(records) = run.records.checked_add(1) else {
                break;
            };
            if outcome.disposition != run.disposition
                || run.first_offset.get().checked_add(u64::from(run.records))
                    != Some(outcome.offset.get())
            {
                break;
            }
            run.records = records;
        }
        // The run counts outcomes it took from `remaining`.
        self.remaining = self
            .remaining
            .get(run.records as usize..)
            .unwrap_or_default();
        Some(run)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            usize::from(!self.remaining.is_empty()),
            Some(self.remaining.len()),
        )
    }
}

impl std::iter::FusedIterator for OutcomeRuns<'_> {}

#[cfg(test)]
mod tests;
