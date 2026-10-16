//! Retry verification: a resent APPEND is checked record by record against
//! the stored originals or the duplicate proofs its results name.

use super::super::{AppendBuffer, JournalError, OwnedJournal, prefix};
use crate::replica_journal::producer::{self, Assignment, Original, Plan};
use ozzy_core::state::ProducerResultSource;
use ozzy_journal::operation::{AppendRecordList, OperationBody, RecordIter, decode_operation_body};
use ozzy_journal_segment::{DecodedBatches, JournalIndexError, JournalIndexSnapshot};
use ozzy_replication::Prefix;

// A protocol APPEND is one canonical operation. Read all its records together
// so a cold retry does not reread and decode that operation for each small
// verification chunk. ReadLimits still bounds the returned payload bytes.
const RETRY_READ_RECORDS: usize = 2048;

#[derive(Clone, Copy)]
struct DuplicateEvidence {
    number: u64,
    through: Prefix,
    proof: producer::DuplicateProof,
}

/// What checking a retry's retained results has established so far.
struct Verification<'a, 'data> {
    /// Resent records not checked yet.
    expected: RecordIter<'a, 'data>,
    snapshot: Option<JournalIndexSnapshot>,
    decoded: DecodedBatches,
    timestamp: Option<u64>,
    through: Prefix,
    duplicate: Option<DuplicateEvidence>,
}

impl Verification<'_, '_> {
    fn observe(&mut self, timestamp: u64, through: Prefix) {
        self.timestamp.get_or_insert(timestamp);
        self.through = through;
    }
}

impl OwnedJournal {
    /// Assign one producer batch a fresh offset, or verify a retry against
    /// what its retained results name.
    pub(super) async fn assign_producer(
        &mut self,
        buffer: &AppendBuffer,
        index: usize,
        now: u64,
    ) -> Result<Assignment, JournalError> {
        let plan = producer::plan(
            self.images()?.speculative(),
            buffer,
            index,
            self.buffer_generation,
            self.limits.operations,
        )?;
        let retry = match plan {
            Plan::Retry(retry) => retry,
            Plan::Fresh(offset) => return Ok(Assignment::fresh(offset, now)),
        };
        let operation = buffer
            .operations()
            .nth(index)
            .ok_or(JournalError::AppendMismatch)?;
        let OperationBody::Append(body) =
            decode_operation_body(operation.kind, operation.body, self.limits.operations)?
        else {
            return Err(JournalError::AppendMismatch);
        };
        let [expected] = body.batches.as_slice() else {
            return Err(JournalError::AppendMismatch);
        };
        let mut verification = Verification {
            expected: expected.records.iter(),
            snapshot: None,
            decoded: DecodedBatches::default(),
            timestamp: None,
            through: Prefix::GENESIS,
            duplicate: None,
        };
        let mut index = 0;
        while index < retry.retained {
            index += match retry.source(index)? {
                ProducerResultSource::Stored => {
                    self.verify_stored(&retry, index, &mut verification).await?
                }
                ProducerResultSource::Duplicate { operation } => {
                    let duplicate = (operation, &expected.records);
                    self.verify_duplicate(&retry, index, duplicate, &mut verification)
                        .await?;
                    1
                }
                _ => return Err(JournalError::AppendMismatch),
            };
        }
        let timestamp = verification.timestamp.ok_or(JournalError::AppendMismatch)?;
        Ok(retry.finish(timestamp, verification.through, now))
    }

    /// Check resent records whose originals were stored, from writeback or
    /// cold reads. Returns how many records it checked.
    async fn verify_stored(
        &mut self,
        retry: &producer::Retry,
        index: usize,
        verification: &mut Verification<'_, '_>,
    ) -> Result<usize, JournalError> {
        let offset = retry.offset(index)?;
        if let Some((actual, position)) =
            self.writeback
                .record(retry.batch.partition, offset, &mut verification.decoded)
        {
            let expected = verification
                .expected
                .next()
                .ok_or(JournalError::AppendMismatch)?;
            retry.verify(index, &actual, expected)?;
            let timestamp = actual.append_timestamp_millis;
            verification.observe(timestamp, position);
            return Ok(1);
        }
        let records = self
            .cold_retry_records(
                retry,
                index,
                &mut verification.decoded,
                &mut verification.snapshot,
            )
            .await?;
        let count = records.len();
        for (checked, (actual, position)) in records.into_iter().enumerate() {
            let expected = verification
                .expected
                .next()
                .ok_or(JournalError::AppendMismatch)?;
            retry.verify(index + checked, &actual, expected)?;
            verification.observe(actual.append_timestamp_millis, prefix(position));
        }
        Ok(count)
    }

    /// Check one resent record against the duplicate proof of operation
    /// `number`, reusing the previous record's proof when it names the same.
    async fn verify_duplicate(
        &mut self,
        retry: &producer::Retry,
        index: usize,
        (number, expected): (u64, &AppendRecordList<'_>),
        verification: &mut Verification<'_, '_>,
    ) -> Result<(), JournalError> {
        let evidence = match verification
            .duplicate
            .filter(|evidence| evidence.number == number)
        {
            Some(evidence) => evidence,
            None => {
                self.duplicate_evidence(retry, number, expected, &mut verification.snapshot)
                    .await?
            }
        };
        if !evidence
            .proof
            .contains(retry.batch.first_sequence.get() + index as u64)
        {
            return Err(JournalIndexError::Read(
                ozzy_journal_segment::IndexedReadError::RecordMismatch,
            )
            .into());
        }
        verification.duplicate = Some(evidence);
        verification
            .expected
            .next()
            .ok_or(JournalError::AppendMismatch)?;
        verification.observe(evidence.proof.timestamp, evidence.through);
        Ok(())
    }

    async fn duplicate_evidence(
        &mut self,
        retry: &producer::Retry,
        number: u64,
        expected: &ozzy_journal::operation::AppendRecordList<'_>,
        snapshot: &mut Option<ozzy_journal_segment::JournalIndexSnapshot>,
    ) -> Result<DuplicateEvidence, JournalError> {
        if let Some((operation, through)) = self.writeback.operation(number) {
            return Ok(DuplicateEvidence {
                number,
                through,
                proof: retry
                    .verify_duplicates(
                        Original {
                            number,
                            kind: operation.kind,
                            body: operation.body,
                        },
                        expected,
                        self.limits.operations,
                    )
                    .await?,
            });
        }
        let operation = self
            .retry_snapshot(snapshot)
            .await?
            .read_operation_number(number)
            .await?
            .ok_or(JournalIndexError::MissingOperationNumber(number))?;
        Ok(DuplicateEvidence {
            number,
            through: Prefix {
                op: ozzy_replication::OpNumber(number),
                digest: operation.digest,
            },
            proof: retry
                .verify_duplicates(
                    Original {
                        number,
                        kind: operation.kind,
                        body: operation.body.as_ref(),
                    },
                    expected,
                    self.limits.operations,
                )
                .await?,
        })
    }

    async fn cold_retry_records(
        &mut self,
        retry: &producer::Retry,
        index: usize,
        decoded: &mut DecodedBatches,
        snapshot: &mut Option<ozzy_journal_segment::JournalIndexSnapshot>,
    ) -> Result<
        Vec<(
            ozzy_journal_segment::IndexedRecord,
            ozzy_journal_segment::LogPosition,
        )>,
        JournalError,
    > {
        let end = (index + RETRY_READ_RECORDS).min(retry.retained);
        let mut offsets = Vec::with_capacity(end - index);
        for next in index..end {
            if retry.source(next)? != ProducerResultSource::Stored {
                break;
            }
            let offset = retry.offset(next)?;
            // A later retry range can still be queued for its physical write.
            // Its authoritative bytes remain in writeback, outside this snapshot.
            if next != index
                && self
                    .writeback
                    .record(retry.batch.partition, offset, decoded)
                    .is_some()
            {
                break;
            }
            offsets.push(offset);
        }
        let limits = ozzy_journal::ReadLimits {
            max_records: RETRY_READ_RECORDS,
            max_bytes: self.limits.operations.max_payload_bytes,
        };
        let recent = self
            .reader
            .as_ref()
            .ok_or(JournalError::AppendMismatch)?
            .read_offsets_with_positions(
                self.journal.readable()?,
                retry.batch.partition,
                &offsets,
                limits,
            )
            .await?;
        let records = if let Some(records) = recent {
            records
        } else {
            self.retry_snapshot(snapshot)
                .await?
                .read_offsets_with_positions(retry.batch.partition, &offsets, limits)
                .await?
        };
        if records.is_empty() {
            return Err(JournalIndexError::MissingOffset(retry.offset(index)?).into());
        }
        Ok(records)
    }
}

impl OwnedJournal {
    /// The journal index snapshot a retry check reads, captured on first use.
    async fn retry_snapshot<'s>(
        &mut self,
        slot: &'s mut Option<ozzy_journal_segment::JournalIndexSnapshot>,
    ) -> Result<&'s ozzy_journal_segment::JournalIndexSnapshot, JournalError> {
        let snapshot = match slot.take() {
            Some(snapshot) => snapshot,
            None => self.journal.retry_snapshot(self.recovery.index).await?,
        };
        Ok(slot.insert(snapshot))
    }
}
