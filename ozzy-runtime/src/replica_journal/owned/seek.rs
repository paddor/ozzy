//! Captured seek execution cannot install visibility or move a live cursor.

use super::{JournalError, OwnedJournal};
use crate::replica_journal::PartitionReadCursor;
use ozzy_core::reader::seek::Selection;
use ozzy_journal_segment::{AsyncJournalIndexSnapshot, PreparedOperationRecords, SeekQuery};
use ozzy_proto::{PartitionIncarnation, reader::Start};
use ozzy_replication::driver::ValidationTicket;
use std::rc::Rc;

#[derive(Debug)]
pub(in crate::replica_journal) struct PreparedSeek {
    key: Rc<()>,
    cursor: PartitionReadCursor,
    query: SeekQuery,
    stored: Option<AsyncJournalIndexSnapshot>,
    memory: Vec<PreparedOperationRecords>,
}

#[derive(Debug)]
pub(in crate::replica_journal) struct CompletedSeek {
    key: Rc<()>,
    cursor: PartitionReadCursor,
    result: Result<ozzy_proto::Offset, JournalError>,
}

impl PreparedSeek {
    pub(in crate::replica_journal) async fn execute(self) -> CompletedSeek {
        let mut selection = Selection::new(
            self.query.start,
            self.query.retained_from,
            self.query.confirmed_end,
        );
        let result = async {
            if let Some(stored) = self.stored {
                stored.seek(self.query, &mut selection).await?;
            }
            for records in self.memory {
                records.observe_seek(self.query, &mut selection).await;
            }
            Ok(selection.finish()?)
        }
        .await;
        CompletedSeek {
            key: self.key,
            cursor: self.cursor,
            result,
        }
    }
}

impl OwnedJournal {
    pub(in crate::replica_journal) async fn prepare_seek(
        &mut self,
        ticket: ValidationTicket,
        partition: PartitionIncarnation,
        start: Start,
    ) -> Result<PreparedSeek, JournalError> {
        let cursor = self.open_reader(ticket, partition, None)?;
        let query = SeekQuery {
            partition,
            start,
            retained_from: cursor.retained_from(),
            confirmed_end: cursor.committed_end(),
            through: ticket.applied().op.0,
        };
        let history = matches!(start, Start::Timestamp(_) | Start::RecordId { .. });
        let memory = if history {
            self.writeback.seek_records()
        } else {
            Vec::new()
        };
        let stored = if history {
            Some(self.journal.retry_snapshot(self.recovery.index).await?)
        } else {
            None
        };
        Ok(PreparedSeek {
            key: self.read_key.clone(),
            cursor,
            query,
            stored,
            memory,
        })
    }

    pub(in crate::replica_journal) fn complete_seek(
        &mut self,
        completed: CompletedSeek,
    ) -> Result<PartitionReadCursor, JournalError> {
        if !Rc::ptr_eq(&self.read_key, &completed.key) {
            return Err(crate::replica_journal::PartitionReadError::Fenced.into());
        }
        let mut cursor = completed.cursor;
        cursor.next = completed.result?;
        self.check_reader(cursor)?;
        Ok(cursor)
    }
}
