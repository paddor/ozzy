//! A copy's check of `Release` and `Carry` operations against its held
//! index. Validation of a buffer waits for the reads the check needs.

use ozzy_core::state::PendingSchedule;
use ozzy_journal::operation::{
    CanonicalOperation, Carry, HeldPosition, OperationBody, OperationKind, OperationLimits,
    Release, ReleaseKey, decode_operation_body,
};

use super::carry::Span;
use super::{Held, JournalError, Rows, merge, rows};
use crate::replica_journal::owned::OwnedJournal;

/// A buffer's releases and carries against the held index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::replica_journal::owned) enum Checked {
    /// Every release copies exactly the held records through its key, and
    /// every carry the held records before its end.
    Valid,
    /// A release passes the loaded rows; load more and check again.
    Load,
}

impl OwnedJournal {
    /// Check every `Release` and `Carry` in `buffer` against the held
    /// index, first reading the held rows the carries cover and loading the
    /// sealed rows a release passes. Validation of `buffer` waits for those
    /// reads.
    pub(in crate::replica_journal::owned) async fn prepare_held(
        &mut self,
        buffer: &crate::replica_journal::AppendBuffer,
    ) -> Result<(), JournalError> {
        if !buffer.operations().any(|operation| {
            matches!(
                operation.kind,
                OperationKind::Release | OperationKind::Carry
            )
        }) {
            return Ok(());
        }
        self.observe_held()?;
        let limits = self.limits.operations;
        let covered = match (covered(buffer.operations(), limits)?, &self.held) {
            (Some((range, limit)), Some(held)) => {
                let snapshot = self.journal.retry_snapshot(self.recovery.index).await?;
                held.between(snapshot, range, limit).read().await?.0
            }
            _ => Vec::new(),
        };
        while check(self.held.as_ref(), buffer.operations(), limits, &covered)? == Checked::Load {
            let held = self.held.as_ref().ok_or(JournalError::AppendMismatch)?;
            let before = held.loaded;
            let snapshot = self.journal.retry_snapshot(self.recovery.index).await?;
            let load = held.refill(snapshot).read().await?;
            let held = self.held.as_mut().ok_or(JournalError::AppendMismatch)?;
            held.extend(load);
            // Each load reads past the loaded rows or loads every one.
            if held.loaded == before {
                return Err(JournalError::AppendMismatch);
            }
        }
        Ok(())
    }
}

/// The position range the carries in `operations` cover, and how many held
/// rows to read there: every carried record, every record a release may
/// take first, and one more to show nothing else is left.
fn covered<'a>(
    operations: impl Iterator<Item = CanonicalOperation<'a>>,
    limits: OperationLimits,
) -> Result<Option<(Span, usize)>, JournalError> {
    let mut range: Option<Span> = None;
    let mut limit = 1_usize;
    for operation in operations {
        match decode(&operation, limits)? {
            Some(OperationBody::Carry(carry)) => {
                limit = limit.saturating_add(carry.batch.records.len());
                range = Some(
                    range.map_or((carry.from, carry.through), |(from, through)| {
                        (from.min(carry.from), through.max(carry.through))
                    }),
                );
            }
            Some(OperationBody::Release(release)) => {
                limit = limit.saturating_add(release.batch.records.len());
            }
            _ => {}
        }
    }
    Ok(range.map(|range| (range, limit)))
}

/// Check each `Release` and `Carry` in `operations` against the held
/// records, the records of `Schedule` and `Carry` operations before it in
/// the same buffer, and `covered`: the held rows the carries cover, in
/// position order.
pub(in crate::replica_journal::owned) fn check<'a>(
    held: Option<&Held>,
    operations: impl Iterator<Item = CanonicalOperation<'a>>,
    limits: OperationLimits,
    covered: &[(ReleaseKey, PendingSchedule)],
) -> Result<Checked, JournalError> {
    let empty = Rows::new();
    let (recent, sealed) = held.map_or((&empty, &empty), |held| (&held.recent, &held.sealed));
    let mut released = held.and_then(|held| held.released);
    let mut loaded = held.and_then(|held| held.loaded);
    let mut carried = held.map_or(HeldPosition::default(), |held| held.carried);
    let partition = held.map(Held::partition);
    let mut added = Rows::new();
    // Rows of carries earlier in the buffer; their keys stay in `added` or
    // the held index for releases.
    let mut moved = Vec::new();
    for operation in operations {
        match decode(&operation, limits)? {
            Some(OperationBody::Schedule(schedule)) => {
                if partition.is_some_and(|partition| partition != schedule.batch.partition) {
                    return Err(JournalError::AppendMismatch);
                }
                let entries = ozzy_core::state::schedule_entries(operation.op_number, &schedule)
                    .map_err(|_| JournalError::AppendMismatch)?;
                added.extend(entries);
            }
            Some(OperationBody::Release(release)) => {
                let through = release.through;
                if loaded.is_some_and(|loaded| through > loaded) {
                    return Ok(Checked::Load);
                }
                if partition.is_some_and(|partition| partition != release.batch.partition) {
                    return Err(mismatch());
                }
                check_release(
                    &release,
                    merge(merge(rows(recent), rows(sealed)), rows(&added))
                        .skip_while(|(key, _)| released.is_some_and(|released| *key <= released)),
                )?;
                released = Some(through);
                loaded = loaded.map(|loaded| loaded.max(through));
            }
            Some(OperationBody::Carry(carry)) => {
                if partition.is_some_and(|partition| partition != carry.batch.partition)
                    || carry.from != carried
                {
                    return Err(carry_mismatch());
                }
                check_carry(
                    &carry,
                    carried_rows(covered, &added, &moved, &carry, released),
                )?;
                moved.extend(
                    ozzy_core::state::carry_entries(operation.op_number, &carry)
                        .map_err(|_| carry_mismatch())?,
                );
                carried = carry.through;
            }
            _ => {}
        }
    }
    Ok(Checked::Valid)
}

/// A release copies exactly the held records from `rows` through its key.
fn check_release<'r>(
    release: &Release<'_>,
    rows: impl Iterator<Item = (ReleaseKey, &'r PendingSchedule)>,
) -> Result<(), JournalError> {
    let mut rows = rows.take_while(|(key, _)| *key <= release.through);
    let mut last = None;
    for record in &release.batch.records {
        let (key, row) = rows.next().ok_or_else(mismatch)?;
        if !same(record, row) {
            return Err(mismatch());
        }
        last = Some(key);
    }
    // The release ends at its last record's key.
    if rows.next().is_some() || last != Some(release.through) {
        return Err(mismatch());
    }
    Ok(())
}

/// Held rows `carry` must copy, in position order: those in its range
/// after the last release.
fn carried_rows<'r>(
    covered: &'r [(ReleaseKey, PendingSchedule)],
    added: &'r Rows,
    moved: &'r [(ReleaseKey, PendingSchedule)],
    carry: &Carry<'_>,
    released: Option<ReleaseKey>,
) -> Vec<(ReleaseKey, &'r PendingSchedule)> {
    let range = carry.from..carry.through;
    let mut rows: Vec<_> = covered
        .iter()
        .chain(moved)
        .map(|(key, row)| (*key, row))
        .chain(rows(added))
        .filter(|(key, row)| {
            range.contains(&row.position()) && released.is_none_or(|released| *key > released)
        })
        .collect();
    rows.sort_unstable_by_key(|(_, row)| row.position());
    rows
}

/// A carry copies exactly `rows`, with their keys.
fn check_carry(
    carry: &Carry<'_>,
    rows: Vec<(ReleaseKey, &PendingSchedule)>,
) -> Result<(), JournalError> {
    if rows.len() != carry.batch.records.len() {
        return Err(carry_mismatch());
    }
    for (((key, row), record), carried) in
        rows.into_iter().zip(&carry.batch.records).zip(&carry.keys)
    {
        if key != *carried || !same(record, row) {
            return Err(carry_mismatch());
        }
    }
    Ok(())
}

/// Whether a copied record matches its held row.
fn same(record: ozzy_journal::operation::RecordRef<'_>, row: &PendingSchedule) -> bool {
    let bytes = ozzy_journal::operation::encoded_record_bytes(record.encoding, record.parts.iter());
    record.message_id == row.message_id && u32::try_from(bytes).ok() == Some(row.bytes)
}

/// The body of a `Schedule`, `Release`, or `Carry`.
fn decode<'a>(
    operation: &CanonicalOperation<'a>,
    limits: OperationLimits,
) -> Result<Option<OperationBody<'a>>, JournalError> {
    if !matches!(
        operation.kind,
        OperationKind::Schedule | OperationKind::Release | OperationKind::Carry
    ) {
        return Ok(None);
    }
    Ok(Some(decode_operation_body(
        operation.kind,
        operation.body,
        limits,
    )?))
}

fn mismatch() -> JournalError {
    state_error(ozzy_core::state::StateError::ReleaseMismatch)
}

fn carry_mismatch() -> JournalError {
    state_error(ozzy_core::state::StateError::CarryMismatch)
}

fn state_error(error: ozzy_core::state::StateError) -> JournalError {
    JournalError::Images(ozzy_core::state::CanonicalImagesError::State(error))
}
