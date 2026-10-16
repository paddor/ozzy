//! The same private metadata and identity validation for local and transferred images.

use super::{Image, JournalError, yield_once};
use crate::memory::{Charge, Owner};
use ozzy_core::{
    checkpoint::{Decoder, Error},
    dedup::{Limits, RestoringIndex},
    state::CanonicalState,
};
use ozzy_journal_segment::{CanonicalCheckpointError, CanonicalRecoveryLimits};

#[derive(Debug)]
pub(in crate::replica_journal::owned) struct Restoring {
    decoder: Decoder,
    index: Option<RestoringIndex>,
    limits: Option<Limits>,
    recovery: CanonicalRecoveryLimits,
    memory: Option<Owner>,
    charge: Option<Charge>,
}

pub(in crate::replica_journal::owned) struct Decoded {
    pub state: CanonicalState,
    pub identities: Option<Image>,
    pub charge: Option<Charge>,
}

impl Restoring {
    pub(in crate::replica_journal::owned) fn new(
        bytes: u64,
        recovery: CanonicalRecoveryLimits,
        limits: Option<Limits>,
        memory: Option<Owner>,
    ) -> Self {
        Self {
            decoder: Decoder::new(bytes, recovery.snapshot.max_snapshot_bytes),
            index: None,
            limits,
            recovery,
            memory,
            charge: None,
        }
    }

    pub(in crate::replica_journal::owned) fn push(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), JournalError> {
        let Self {
            decoder,
            index,
            limits,
            memory,
            charge,
            ..
        } = self;
        decoder
            .push(
                bytes,
                |layout| {
                    match (layout.identities(), *limits) {
                        (None, None) => {}
                        (Some(image), Some(limits))
                            if image.header.window_millis() == limits.window_millis
                                && image.header.rows() <= limits.entries as u64 => {}
                        _ => return Err(Error::Identity(ozzy_core::dedup::Error::Snapshot)),
                    }
                    // Covers retained bytes, private core construction, and bounded codec scratch.
                    let bytes = usize::try_from(layout.core_bytes())
                        .ok()
                        .and_then(|bytes| bytes.checked_mul(4))
                        .ok_or(Error::Length)?;
                    *charge = memory
                        .as_ref()
                        .map(|owner| owner.try_charge(bytes))
                        .transpose()
                        .map_err(|_| Error::Allocation)?;
                    Ok(())
                },
                |image, row| {
                    let restoring = match index.take() {
                        Some(restoring) => restoring,
                        None => RestoringIndex::new(
                            limits.ok_or(ozzy_core::dedup::Error::Snapshot)?,
                            image.header,
                        )?,
                    };
                    index.insert(restoring).push_rows(row)
                },
            )
            .map_err(CanonicalCheckpointError::from)?;
        Ok(())
    }

    pub(in crate::replica_journal::owned) async fn finish(
        self,
        operation: u64,
    ) -> Result<Decoded, JournalError> {
        let (layout, bytes) = self
            .decoder
            .finish()
            .map_err(CanonicalCheckpointError::from)?;
        let state = CanonicalState::decode_snapshot_cooperative(
            &bytes,
            self.recovery.state,
            self.recovery.snapshot,
            async |_| yield_once().await,
        )
        .await
        .map_err(CanonicalCheckpointError::from)?;
        layout
            .validate_state(&state)
            .map_err(CanonicalCheckpointError::from)?;
        if state.revision() != operation {
            return Err(JournalError::CompletionMismatch);
        }
        let identities = match (layout.identities(), self.limits) {
            (Some(image), Some(limits)) => Some(Image {
                index: match self.index {
                    Some(candidate) => candidate.finish()?,
                    None => RestoringIndex::new(limits, image.header)?.finish()?,
                },
                partition: Some(image.partition),
                time: image.header.logical_time(),
                end: image.header.next_offset(),
                reservation: None,
            }),
            (None, None)
                if !state
                    .partitions()
                    .any(|(_, partition)| partition.dedup_window_millis.is_some()) =>
            {
                None
            }
            _ => return Err(JournalError::Configuration),
        };
        Ok(Decoded {
            state,
            identities,
            charge: self.charge,
        })
    }
}
