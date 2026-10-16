use super::{Error, MessageId, Offset, SNAPSHOT_ROW_BYTES, SnapshotHeader};
use ozzy_proto::layout::Fixed;

/// One checked identity row from a private snapshot stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRow {
    /// Original event identity; never zero.
    pub id: MessageId,
    /// Original stored partition offset.
    pub offset: Offset,
    /// Logical acceptance time, within the captured window.
    pub time: u64,
}

/// Bounded row-format validation shared by checkpoint scanning and index restore.
/// It does not prove outer integrity or uniqueness of identities across rows.
#[derive(Debug)]
pub struct SnapshotRows {
    header: SnapshotHeader,
    rows: u64,
    previous_time: Option<u64>,
    failed: bool,
}

impl SnapshotRows {
    /// Start at the first row of an already validated fixed header.
    pub const fn new(header: SnapshotHeader) -> Self {
        Self {
            header,
            rows: 0,
            previous_time: None,
            failed: false,
        }
    }

    /// All declared rows have passed format validation and no row failed.
    pub const fn is_complete(&self) -> bool {
        !self.failed && self.rows == self.header.rows()
    }

    /// Valid rows consumed so far. A failed cursor cannot subsequently complete.
    pub const fn consumed_rows(&self) -> u64 {
        self.rows
    }

    /// Validate one complete row. Any malformed or extra row poisons the cursor.
    pub fn push(&mut self, bytes: &[u8]) -> Result<SnapshotRow, Error> {
        let result = self.decode(bytes);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn decode(&mut self, bytes: &[u8]) -> Result<SnapshotRow, Error> {
        if self.failed || self.rows == self.header.rows() {
            return Err(Error::Snapshot);
        }
        let bytes: &[u8; SNAPSHOT_ROW_BYTES] = bytes.try_into().map_err(|_| Error::Snapshot)?;
        let row = SnapshotRow {
            id: MessageId::from_bytes(bytes.array::<0, 16>()),
            offset: Offset::new(bytes.u64_at::<16>()),
            time: bytes.u64_at::<24>(),
        };
        if *row.id.as_bytes() == [0; 16]
            || row.offset.get() != self.header.first_offset().get() + self.rows
            || row.time > self.header.logical_time()
            || self.header.logical_time() - row.time >= self.header.window_millis()
            || self.previous_time.is_some_and(|time| row.time < time)
        {
            return Err(Error::Snapshot);
        }
        self.rows += 1;
        self.previous_time = Some(row.time);
        Ok(row)
    }
}
