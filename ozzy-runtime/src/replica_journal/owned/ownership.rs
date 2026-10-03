//! Exclusive journal ownership across consuming asynchronous transitions.

use super::{AsyncGroupJournal, JournalError, install::PendingInstallation};

#[derive(Debug)]
pub(super) enum JournalOwner {
    Ready(Box<AsyncGroupJournal>),
    Writing(Box<ozzy_journal_segment::AsyncJournalWritePipeline>),
    Rolling(Box<ozzy_journal_segment::AsyncPendingJournalRoll>),
    Installing(Box<PendingInstallation>),
    Fenced,
}

impl JournalOwner {
    pub(super) fn is_faulted(&self) -> bool {
        match self {
            Self::Ready(journal) => journal.is_faulted(),
            Self::Writing(pipeline) => pipeline.is_faulted(),
            Self::Rolling(pending) => pending.journal().is_faulted(),
            Self::Installing(_) => false,
            Self::Fenced => true,
        }
    }

    pub(super) fn readable(&self) -> Result<&AsyncGroupJournal, JournalError> {
        match self {
            Self::Ready(journal) => Ok(journal),
            Self::Writing(pipeline) => Ok(pipeline.journal()),
            Self::Rolling(pending) => Ok(pending.journal()),
            _ => Err(JournalError::InstallationMismatch),
        }
    }

    pub(super) async fn retry_snapshot(
        &mut self,
        limits: ozzy_journal_segment::IndexBuildLimits,
    ) -> Result<ozzy_journal_segment::AsyncJournalIndexSnapshot, JournalError> {
        use ozzy_journal_segment::JournalIndexBoundary::Written;
        Ok(match self {
            Self::Ready(journal) => journal.build_index_snapshot(Written, limits).await?,
            Self::Writing(pipeline) => pipeline.build_index_snapshot(Written, limits).await?,
            Self::Rolling(pending) => pending.build_index_snapshot(Written, limits).await?,
            _ => return Err(JournalError::InstallationMismatch),
        })
    }

    pub(super) fn pipeline(
        &mut self,
        capacity: usize,
    ) -> Result<&mut ozzy_journal_segment::AsyncJournalWritePipeline, JournalError> {
        if matches!(self, Self::Ready(_)) {
            *self = Self::Writing(Box::new(self.take_ready()?.begin_write_pipeline(capacity)?));
        }
        match self {
            Self::Writing(pipeline) => Ok(pipeline),
            _ => Err(JournalError::CompletionMismatch),
        }
    }

    pub(super) fn finish_writes(&mut self) -> Result<(), JournalError> {
        if let Self::Writing(pipeline) = self {
            if pipeline.pending() != 0 || pipeline.sync_pending() {
                return Err(JournalError::CompletionMismatch);
            }
            let Self::Writing(pipeline) = std::mem::replace(self, Self::Fenced) else {
                unreachable!()
            };
            *self = Self::Ready(Box::new(pipeline.finish()?));
        }
        Ok(())
    }

    pub(super) fn take_rolling(
        &mut self,
    ) -> Result<ozzy_journal_segment::AsyncPendingJournalRoll, JournalError> {
        match std::mem::replace(self, Self::Fenced) {
            Self::Rolling(pending) => Ok(*pending),
            _ => Err(JournalError::CompletionMismatch),
        }
    }

    pub(super) fn ready(&self) -> Result<&AsyncGroupJournal, JournalError> {
        match self {
            Self::Ready(journal) => Ok(journal),
            _ => Err(JournalError::InstallationMismatch),
        }
    }

    pub(super) fn ready_mut(&mut self) -> Result<&mut AsyncGroupJournal, JournalError> {
        match self {
            Self::Ready(journal) => Ok(journal),
            _ => Err(JournalError::InstallationMismatch),
        }
    }

    pub(super) fn take_ready(&mut self) -> Result<AsyncGroupJournal, JournalError> {
        self.ready()?;
        let Self::Ready(journal) = std::mem::replace(self, Self::Fenced) else {
            unreachable!("ready owner checked")
        };
        Ok(*journal)
    }

    pub(super) fn installing_mut(&mut self) -> Result<&mut PendingInstallation, JournalError> {
        match self {
            Self::Installing(pending) => Ok(pending),
            _ => Err(JournalError::InstallationMismatch),
        }
    }

    pub(super) fn take_installing(&mut self) -> Result<PendingInstallation, JournalError> {
        self.installing_mut()?;
        let Self::Installing(pending) = std::mem::replace(self, Self::Fenced) else {
            unreachable!("pending installation checked")
        };
        Ok(*pending)
    }
}
