use super::{Arc, DirectoryError, Pipeline, WriterError, WriterPosition};
use crate::{
    CurrentReference,
    async_files::Access,
    async_metadata,
    directory::{evidence, position_before, position_regresses},
};
use ozzy_io::Operation;

/// Captured data barrier and fixed evidence publication for installed writes.
/// New writes may proceed; authority and a second publication remain frozen.
#[derive(Debug)]
#[must_use = "execute and install before releasing the pipeline"]
pub struct PreparedSync {
    owner: Arc<()>,
    key: Arc<()>,
    current: CurrentReference,
    through: WriterPosition,
    access: Access,
    directory: async_metadata::Directory,
    barrier: Option<Operation>,
    evidence: Option<([u8; evidence::RECORD_BYTES], usize)>,
}

/// Physical evidence only. The exact pipeline must observe success before
/// advancing logical durability or returning a replication completion.
#[derive(Debug)]
#[must_use = "install on the originating pipeline"]
pub struct CompletedSync {
    work: PreparedSync,
    result: Result<(), DirectoryError>,
}

impl PreparedSync {
    /// Execute the prepared metadata publication and return its fenced completion.
    pub async fn publish(mut self) -> CompletedSync {
        let result = async {
            if let Some(barrier) = self.barrier.take() {
                self.access.done(barrier).await?;
            }
            if let Some((record, first)) = &self.evidence {
                self.directory.overwrite_evidence(*first, record).await?;
            }
            Ok(())
        }
        .await;
        CompletedSync { work: self, result }
    }
}

impl Pipeline {
    pub(super) fn require_no_sync(&self) -> Result<(), DirectoryError> {
        if self.sync.is_some() {
            Err(WriterError::InvalidSyncPosition.into())
        } else {
            Ok(())
        }
    }

    /// Capture only the installed prefix, excluding reserved/incomplete writes.
    /// One outstanding job protects both evidence copies from competing writers.
    pub fn prepare_sync(&mut self) -> Result<PreparedSync, DirectoryError> {
        self.healthy()?;
        self.require_no_sync()?;
        let journal = &self.journal;
        if !journal.manifest.durable_evidence {
            return Err(DirectoryError::CurrentMismatch);
        }
        let through = journal.writer.written_position();
        let accepted = position_before(through.next_chain())?;
        let copies = journal
            .evidence
            .as_ref()
            .ok_or(DirectoryError::CurrentMismatch)?;
        let protected = copies.protected(&journal.manifest)?;
        if position_regresses(protected, accepted) {
            return Err(DirectoryError::HardStateRegression);
        }
        let evidence = if accepted == protected {
            None
        } else {
            Some(copies.next(&journal.manifest, accepted)?)
        };
        let key = Arc::new(());
        let prepared = PreparedSync {
            owner: self.key.clone(),
            key: key.clone(),
            current: journal.current,
            through,
            access: journal.access.clone(),
            directory: journal.directory.detached_publication()?,
            barrier: journal.writer.detached_sync(through)?,
            evidence,
        };
        self.sync = Some(key);
        Ok(prepared)
    }

    /// Observe one exact publication. Later writes cannot enlarge its returned
    /// prefix. Any error leaves this pipeline fenced, even after physical success.
    pub fn complete_sync(&mut self, done: CompletedSync) -> Result<WriterPosition, DirectoryError> {
        self.healthy()?;
        self.faulted = true;
        if !Arc::ptr_eq(&done.work.owner, &self.key)
            || self
                .sync
                .as_ref()
                .is_none_or(|key| !Arc::ptr_eq(key, &done.work.key))
            || self.journal.current != done.work.current
        {
            return Err(DirectoryError::CurrentMismatch);
        }
        done.result?;
        let through = self
            .journal
            .writer
            .complete_detached_sync(done.work.through)?;
        if let Some((record, _)) = done.work.evidence {
            self.journal.evidence =
                Some(evidence::Copies::new(&[record; 2], &self.journal.manifest)?);
        }
        self.sync = None;
        self.faulted = false;
        Ok(through)
    }
}
