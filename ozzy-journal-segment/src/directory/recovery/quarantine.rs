//! Explicit transition from damaged payloads to full nonvoting recovery.

use super::super::{
    Arc, ChainPosition, DecodeLimits, DirectoryError, GroupDirectory, JournalGeneration,
    LogPosition, NoopObserver, OpenGroupJournal, OpenOptions, OperationLimits, PersistenceObserver,
    PinRegistry, SegmentHeader, SegmentReference, SegmentWriter, allocate_segment, publication,
    segment_name, sync_directory,
};
use super::recovery_marker;
use std::io;

impl GroupDirectory {
    /// Explicitly quarantine a configured store whose authority metadata is intact.
    ///
    /// Preserves selected metadata and every payload file. Persistently removes
    /// voting eligibility before replacement work can begin. Missing/corrupt
    /// identity, configuration, CURRENT, or durability evidence is not repaired.
    /// Obtain fresh responses from both other brokers through the recovery core;
    /// local metadata or this operation never supplies replacement authority.
    pub fn quarantine_for_recovery(self, configuration: &[u8]) -> Result<Self, DirectoryError> {
        self.quarantine_observing(configuration, &mut NoopObserver)
    }

    fn quarantine_observing(
        mut self,
        configuration: &[u8],
        observer: &mut impl PersistenceObserver,
    ) -> Result<Self, DirectoryError> {
        if self.configuration() != Some(configuration)
            || self.manifest.commit_mode != crate::CommitMode::External
            || !self.manifest.durable_evidence
        {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        super::super::require_exact_contents(
            &self.root.join(super::super::CONFIGURATION_FILE),
            configuration,
        )?;
        super::super::evidence::protected(&self.root, &self.manifest)?;
        let marker = recovery_marker(configuration)?;
        publication::replace_reusable(
            &mut publication::Filesystem(&self.root),
            super::super::CONFIGURATION_FILE,
            ".CONFIGURATION.quarantine.tmp",
            &marker,
            observer,
        )?;
        self.configuration = Some(marker.into());
        Ok(self)
    }

    /// Start a fresh private staging generation in an explicitly nonvoting store.
    ///
    /// Never scans, truncates, or copies damaged old payloads. Old selected files
    /// remain for inspection. New segment IDs and bounded orphan probes prevent
    /// replacement from reusing a prior attempt's file. Only a complete fresh
    /// recovery exchange may later publish a valid configured image.
    pub fn recover_nonvoting(
        mut self,
        configuration: &[u8],
        generation: JournalGeneration,
        decode_limits: DecodeLimits,
        operation_limits: OperationLimits,
        max_orphan_probes: usize,
    ) -> Result<OpenGroupJournal, DirectoryError> {
        self.require_recovering(configuration)?;
        if self.manifest.checkpoint.is_some()
            || self.manifest.segments[0].first_chain != ChainPosition::GENESIS
        {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        let previous = self
            .manifest
            .segments
            .last()
            .ok_or(DirectoryError::MissingActiveSegment)?;
        let capacity = previous.capacity;
        let mut segment_id = previous.segment_id;
        for _ in 0..max_orphan_probes {
            segment_id = segment_id
                .checked_add(1)
                .ok_or(DirectoryError::SegmentIdExhausted)?;
            let file = match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(self.root.join(segment_name(segment_id)))
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            };
            allocate_segment(&file, capacity)?;
            let header = SegmentHeader::new(
                self.identity.group_id,
                segment_id,
                None,
                crate::Digest::ZERO,
                capacity,
            )?;
            let mut writer = SegmentWriter::initialize_allocated_at(
                std::sync::Arc::new(file),
                header,
                generation,
                1,
                ChainPosition::GENESIS,
                true,
            )?;
            sync_directory(&self.root.join("segments"))?;
            let mut next = self.manifest.clone();
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(DirectoryError::ManifestGeneration)?;
            next.parent_generation = self.manifest.generation;
            next.accepted = LogPosition::GENESIS;
            next.committed = LogPosition::GENESIS;
            next.segments = vec![SegmentReference {
                segment_id,
                file_generation: 0,
                first_group_number: 1,
                first_chain: ChainPosition::GENESIS,
                capacity,
                sealed: None,
            }];
            self.install_manifest(next, &mut NoopObserver)?;
            if self.data_sync {
                writer.set_write_mode(
                    &self.segment_path(segment_id)?,
                    crate::SegmentWriteMode::DataSync,
                )?;
            }
            let evidence = super::super::evidence::Evidence::open(&self.root, &self.manifest)?;
            return Ok(OpenGroupJournal {
                directory: self,
                writer,
                evidence,
                decode_limits,
                operation_limits,
                pins: Arc::new(PinRegistry::default()),
            });
        }
        Err(DirectoryError::RollProbeLimit {
            limit: max_orphan_probes,
        })
    }
}

#[cfg(test)]
mod tests;
