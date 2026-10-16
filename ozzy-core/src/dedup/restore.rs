use super::{Entry, Error, Index, Limits, SNAPSHOT_ROW_BYTES, SnapshotHeader, SnapshotRows};

/// Private streaming recovery image. A failed chunk poisons the whole candidate;
/// only a complete image can expose an index for admission.
#[derive(Debug)]
pub struct RestoringIndex {
    header: SnapshotHeader,
    index: Option<Index>,
    rows: SnapshotRows,
}

impl RestoringIndex {
    /// Reserve within local limits after validating the captured window and size.
    /// The enclosing checkpoint must independently verify partition and integrity.
    pub fn new(limits: Limits, header: SnapshotHeader) -> Result<Self, Error> {
        if header.window_millis() != limits.window_millis || header.rows() > limits.entries as u64 {
            return Err(Error::Snapshot);
        }
        let index = Index::new(limits, header.next_offset(), header.logical_time())?;
        Ok(Self {
            header,
            index: Some(index),
            rows: SnapshotRows::new(header),
        })
    }

    /// Restore whole rows in batch-bounded chunks. Any invalid row or chunk makes
    /// subsequent restore and completion fail, even after a valid prefix.
    pub fn push_rows(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if let Err(error) = self.push_inner(bytes) {
            self.index = None;
            return Err(error);
        }
        Ok(())
    }

    fn push_inner(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let index = self.index.as_mut().ok_or(Error::Snapshot)?;
        let count = bytes.len() / SNAPSHOT_ROW_BYTES;
        if !bytes.len().is_multiple_of(SNAPSHOT_ROW_BYTES)
            || count > index.limits.batch_records
            || count as u64 > self.header.rows() - self.rows.consumed_rows()
        {
            return Err(Error::Snapshot);
        }
        for bytes in bytes.as_chunks::<SNAPSHOT_ROW_BYTES>().0 {
            let decoded = self.rows.push(bytes)?;
            let row = Entry {
                id: decoded.id,
                offset: decoded.offset,
                time: decoded.time,
            };
            let hash = index.hash.hash_one(row.id);
            if index
                .committed
                .find(hash, |entry| entry.id == row.id)
                .is_some()
            {
                return Err(Error::Snapshot);
            }
            index
                .committed
                .insert_unique(hash, row, |entry| index.hash.hash_one(entry.id));
            index.expiry.push_back(row);
        }
        Ok(())
    }

    /// Publish only after the exact declared row count and verified final chunk.
    pub fn finish(mut self) -> Result<Index, Error> {
        if !self.rows.is_complete() {
            return Err(Error::Snapshot);
        }
        let mut index = self.index.take().ok_or(Error::Snapshot)?;
        index.accepted_operation = self.header.operation();
        index.committed_operation = self.header.operation();
        Ok(index)
    }
}
