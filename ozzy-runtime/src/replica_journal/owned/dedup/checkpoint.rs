//! Confirmed identity rows share the canonical checkpoint's exact source.

use super::{Image, JournalError, State};
pub(in crate::replica_journal::owned) mod recovery;
use ozzy_core::{
    checkpoint::Identities,
    dedup::{Limits, SNAPSHOT_ROW_BYTES},
    state::canonical_state_schema_digest,
};
use ozzy_io::signal::yield_once;
use ozzy_journal_segment::{CanonicalCheckpointError, CanonicalRecoveryLimits, CheckpointFiles};

/// Live confirmed identity rows captured for a checkpoint written later.
#[derive(Debug)]
pub(in crate::replica_journal::owned) struct CheckpointRows {
    pub(in crate::replica_journal::owned) identities: Identities,
    pub(in crate::replica_journal::owned) bytes: Vec<u8>,
    _charge: Option<crate::memory::Charge>,
}

impl State {
    /// Copy every live confirmed identity row at the committed boundary.
    /// `charge` admits the rows before they are allocated. Admission continues
    /// afterward; the checkpoint is encoded and written beside later appends.
    pub(in crate::replica_journal::owned) fn checkpoint_rows(
        &mut self,
        charge: impl FnOnce(usize) -> Result<Option<crate::memory::Charge>, JournalError>,
    ) -> Result<CheckpointRows, JournalError> {
        let image = self.image.as_mut().ok_or(JournalError::AppendMismatch)?;
        let partition = image.partition.ok_or(JournalError::AppendMismatch)?;
        let mut snapshot = image.index.begin_snapshot()?;
        let rows = copy_rows(image, &mut snapshot, charge);
        if rows.is_err() {
            image.index.abort_snapshot(&snapshot)?;
        }
        let (bytes, charge) = rows?;
        Ok(CheckpointRows {
            identities: Identities {
                partition,
                header: *snapshot.header(),
            },
            bytes,
            _charge: charge,
        })
    }
}

fn copy_rows(
    image: &mut Image,
    snapshot: &mut ozzy_core::dedup::Snapshot,
    charge: impl FnOnce(usize) -> Result<Option<crate::memory::Charge>, JournalError>,
) -> Result<(Vec<u8>, Option<crate::memory::Charge>), JournalError> {
    let total = usize::try_from(snapshot.header().rows())
        .ok()
        .and_then(|rows| rows.checked_mul(SNAPSHOT_ROW_BYTES))
        .ok_or(JournalError::Configuration)?;
    let charge = charge(total)?;
    let mut output = vec![0; total];
    let mut written = 0;
    while !snapshot.is_complete() {
        written += image
            .index
            .write_snapshot_rows(snapshot, &mut output[written..], usize::MAX)?;
    }
    image.index.finish_snapshot(snapshot)?;
    if written != total {
        return Err(JournalError::CompletionMismatch);
    }
    Ok((output, charge))
}

pub(super) async fn restore(
    files: &CheckpointFiles,
    limits: Limits,
    recovery: CanonicalRecoveryLimits,
) -> Result<Image, JournalError> {
    if files.manifest().state_schema_digest != canonical_state_schema_digest() {
        return Err(CanonicalCheckpointError::SchemaMismatch.into());
    }
    let mut decoded =
        recovery::Restoring::new(files.manifest().state_bytes, recovery, Some(limits), None);
    let mut stream = files.stream();
    while let Some(bytes) = stream.next(64 * 1024).await? {
        decoded.push(&bytes)?;
        yield_once().await;
    }
    if !stream.is_verified() {
        return Err(JournalError::HistorySourceMismatch);
    }
    decoded
        .finish(files.manifest().position.op_number)
        .await?
        .identities
        .ok_or(JournalError::UnsupportedHistory)
}
