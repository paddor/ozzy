use super::{
    AsyncGroupJournal, AsyncJournalFormat, CommitMode, Digest, JournalError, JournalGeneration,
    Local, Mode, OwnedConfig, OwnedJournal, SegmentHeader, prefix,
};
use ozzy_journal_segment::SegmentWriteMode;
use ozzy_replication::local::{Configuration, Driver};

impl OwnedConfig<Configuration> {
    fn validate_local(&self, generation: JournalGeneration) -> Result<(), JournalError> {
        if self.identity.group_id != self.configuration.scope().group_id
            || self.identity.replica_node_id != self.configuration.broker()
        {
            return Err(JournalError::Configuration);
        }
        self.validate_storage(generation, SegmentWriteMode::DataSync)
    }
}

impl OwnedJournal {
    pub(crate) fn check_local_driver(&self, driver: &Driver) -> Result<(), JournalError> {
        if !matches!(self.configuration, Mode::Local(_)) {
            return Err(JournalError::Configuration);
        }
        self.validate_image(driver.begin_validation()?)
    }

    /// Explicitly create one locally durable partition. Uses the same segment
    /// format, canonical images, retries, asynchronous jobs and bounded writeback
    /// as replicated ownership. Configuration binds exactly one broker.
    pub async fn format_local(
        config: OwnedConfig<Configuration>,
        io: Local,
        generation: JournalGeneration,
        capacity: u64,
    ) -> Result<(Self, Driver), JournalError> {
        config.validate_local(generation)?;
        let first_segment =
            SegmentHeader::new(config.identity.group_id, 1, None, Digest::ZERO, capacity)
                .map_err(ozzy_journal_segment::DirectoryError::from)?;
        let journal = AsyncGroupJournal::format(
            config.root.clone(),
            io,
            AsyncJournalFormat {
                identity: config.identity,
                configuration_epoch: config.configuration.scope().configuration_epoch,
                commit_mode: CommitMode::LocalDurable,
                first_segment,
                configuration: config.configuration.encode().to_vec(),
            },
            generation,
            config.limits,
        )
        .await?;
        Box::pin(Self::start_local(config, journal, generation)).await
    }

    /// Recover exact local identity and mode before allowing admission. Missing
    /// stores and replicated configurations fail closed. Recovery verifies the
    /// protected durable prefix, repairs only an unprotected torn suffix, and
    /// synchronizes retained history before application becomes visible.
    pub async fn open_local(
        config: OwnedConfig<Configuration>,
        io: Local,
        generation: JournalGeneration,
    ) -> Result<(Self, Driver), JournalError> {
        config.validate_local(generation)?;
        let opening = AsyncGroupJournal::prepare_open(
            config.root.clone(),
            io,
            config.identity,
            Some(&config.configuration.encode()),
            config.limits,
        )
        .await?;
        let manifest = opening.manifest();
        super::super::authority::validate_storage_profile(
            manifest,
            CommitMode::LocalDurable,
            config.configuration.scope().configuration_epoch,
            config.limits.io.max_segment_bytes,
        )?;
        if manifest.promised_view != 0 || manifest.last_normal_view != 0 {
            return Err(JournalError::UnsupportedHistory);
        }
        let journal = opening.recover(generation).await?;
        Box::pin(Self::start_local(config, journal, generation)).await
    }

    async fn start_local(
        config: OwnedConfig<Configuration>,
        mut journal: AsyncGroupJournal,
        generation: JournalGeneration,
    ) -> Result<(Self, Driver), JournalError> {
        let candidate = journal.recover_canonical_candidate(config.recovery).await?;
        let applied = prefix(journal.committed_position()?);
        if prefix(journal.accepted_position()?) != applied {
            return Err(JournalError::CompletionMismatch);
        }
        let images = candidate.activate(&journal)?;
        let reader =
            ozzy_journal_segment::AsyncJournalPartitionIndex::open(&journal, config.reads).await?;
        let driver = Driver::recover(config.configuration, generation, applied, config.writeback)?;
        let mode = Mode::Local(config.configuration);
        Ok((
            Self::from_open(&config, mode, journal, Some(images), Some(reader))?,
            driver,
        ))
    }
}
