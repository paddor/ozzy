use super::{DirectoryError, Journal, evidence, position_before};
use crate::directory::{position_regresses, validate_metadata_successor_fields};
use crate::{CurrentReference, Manifest};

impl Journal {
    pub(super) fn next_manifest(&self) -> Result<Manifest, DirectoryError> {
        let mut next = self.manifest.clone();
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(DirectoryError::ManifestGeneration)?;
        next.parent_generation = self.manifest.generation;
        Ok(next)
    }

    pub(super) async fn install_selected(&mut self, next: Manifest) -> Result<(), DirectoryError> {
        self.interrupted = true;
        super::opening::validate_headers(&self.access, self.root(), &next, self.limits).await?;
        let (next, digest) = self
            .directory
            .publish_manifest(&self.manifest, next, self.limits.metadata)
            .await?;
        let current = CurrentReference {
            group_id: next.identity.group_id,
            store_id: next.identity.store_id,
            generation: next.generation,
            manifest_digest: digest,
        };
        self.directory.select_current(current).await?;
        self.manifest = next;
        self.current = current;
        self.interrupted = false;
        Ok(())
    }

    /// Freeze synchronized progress in a manifest for maintenance or backup.
    pub async fn publish_progress(&mut self) -> Result<(), DirectoryError> {
        self.healthy()?;
        let accepted = self.accepted_position()?;
        let committed = self.committed_position()?;
        let durable = position_before(self.writer.durable_position().next_chain())?;
        if accepted.op_number > durable.op_number {
            return Err(DirectoryError::PositionNotDurable(accepted.op_number));
        }
        if self.manifest.accepted == accepted && self.manifest.committed == committed {
            return Ok(());
        }
        let mut next = self.next_manifest()?;
        next.accepted = accepted;
        next.committed = committed;
        validate_metadata_successor_fields(&self.manifest, &next)?;
        self.install_selected(next).await
    }

    /// Publish only the exact synchronized prefix to fixed durability evidence.
    /// Physical write completion alone never completes this journal operation.
    pub async fn publish_durable_progress(&mut self) -> Result<(), DirectoryError> {
        self.healthy()?;
        if !self.manifest.durable_evidence {
            return self.publish_progress().await;
        }
        let accepted = self.accepted_position()?;
        let durable = position_before(self.writer.durable_position().next_chain())?;
        if accepted.op_number > durable.op_number {
            return Err(DirectoryError::PositionNotDurable(accepted.op_number));
        }
        let evidence = self
            .evidence
            .as_ref()
            .ok_or(DirectoryError::CurrentMismatch)?;
        let protected = evidence.protected(&self.manifest)?;
        if position_regresses(protected, accepted) {
            return Err(DirectoryError::HardStateRegression);
        }
        if protected == accepted {
            return Ok(());
        }
        let (record, first) = evidence.next(&self.manifest, accepted)?;
        self.interrupted = true;
        self.directory.overwrite_evidence(first, &record).await?;
        self.evidence = Some(evidence::Copies::new(&[record; 2], &self.manifest)?);
        self.interrupted = false;
        Ok(())
    }

    /// Persist hard state without changing segment/checkpoint ownership.
    pub async fn install_metadata(&mut self, next: Manifest) -> Result<(), DirectoryError> {
        self.healthy()?;
        if next.segments != self.manifest.segments {
            return Err(DirectoryError::ActiveSegmentChangeRequiresRoll);
        }
        validate_metadata_successor_fields(&self.manifest, &next)?;
        self.validate_positions([next.accepted, next.committed])
            .await?;
        self.install_selected(next).await
    }

    pub(super) async fn validate_positions(
        &self,
        positions: [crate::LogPosition; 2],
    ) -> Result<(), DirectoryError> {
        let durable = position_before(self.writer.durable_position().next_chain())?;
        // The writer already validated and synchronized this exact boundary.
        // Publishing its metadata is not another whole-history scrub. Cold
        // positions still require the physical lineage validation below.
        if positions.iter().all(|position| *position == durable) {
            return Ok(());
        }
        if let Some(position) = positions
            .iter()
            .find(|position| position.op_number > durable.op_number)
        {
            return Err(DirectoryError::PositionNotDurable(position.op_number));
        }
        let mut seen = positions.map(|position| position == crate::LogPosition::GENESIS);
        for reference in &self.manifest.segments {
            for (found, position) in seen.iter_mut().zip(positions) {
                *found |= reference.first_chain == position.following_chain()?;
            }
            let mut groups = self.segment_groups(*reference).await?;
            while let Some(group) = groups.next().await? {
                for operation in &group.operations {
                    for (found, position) in seen.iter_mut().zip(positions) {
                        *found |= operation.op_number == position.op_number
                            && operation.digest == position.digest;
                    }
                }
            }
            groups.finish().await?;
        }
        for (position, found) in positions.into_iter().zip(seen) {
            if !found {
                return Err(DirectoryError::PositionMismatch(position.op_number));
            }
        }
        Ok(())
    }
}
