//! A copy's check of `Release` operations against its held index.

use ozzy_journal::operation::{
    CanonicalOperation, OperationBody, OperationKind, OperationLimits, decode_operation_body,
};

use super::{Held, JournalError, Rows, merge, rows};

/// A buffer's releases against the held index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::replica_journal::owned) enum Checked {
    /// Every release copies exactly the held records through its key.
    Valid,
    /// A release passes the loaded rows; load more and check again.
    Load,
}

/// Check each `Release` in `operations` against the held records and the
/// records of `Schedule` operations before it in the same buffer.
pub(in crate::replica_journal::owned) fn check<'a>(
    held: Option<&Held>,
    operations: impl Iterator<Item = CanonicalOperation<'a>>,
    limits: OperationLimits,
) -> Result<Checked, JournalError> {
    let empty = Rows::new();
    let (recent, sealed, mut released, mut loaded) = held
        .map_or((&empty, &empty, None, None), |held| {
            (&held.recent, &held.sealed, held.released, held.loaded)
        });
    let partition = held.map(Held::partition);
    let mut added = Rows::new();
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
                let mut rows = merge(merge(rows(recent), rows(sealed)), rows(&added))
                    .skip_while(|(key, _)| released.is_some_and(|released| *key <= released))
                    .take_while(|(key, _)| *key <= through);
                let mut last = None;
                for record in &release.batch.records {
                    let (key, row) = rows.next().ok_or_else(mismatch)?;
                    let bytes = ozzy_journal::operation::encoded_record_bytes(
                        record.encoding,
                        record.parts.iter(),
                    );
                    if record.message_id != row.message_id
                        || u32::try_from(bytes).ok() != Some(row.bytes)
                    {
                        return Err(mismatch());
                    }
                    last = Some(key);
                }
                // The release ends at its last record's key.
                if rows.next().is_some() || last != Some(through) {
                    return Err(mismatch());
                }
                released = Some(through);
                loaded = loaded.map(|loaded| loaded.max(through));
            }
            _ => {}
        }
    }
    Ok(Checked::Valid)
}

/// The body of a `Schedule` or `Release`.
fn decode<'a>(
    operation: &CanonicalOperation<'a>,
    limits: OperationLimits,
) -> Result<Option<OperationBody<'a>>, JournalError> {
    if !matches!(
        operation.kind,
        OperationKind::Schedule | OperationKind::Release
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
    JournalError::Images(ozzy_core::state::CanonicalImagesError::State(
        ozzy_core::state::StateError::ReleaseMismatch,
    ))
}
