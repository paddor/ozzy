use std::rc::{Rc, Weak};

use super::{Error, Index, Offset};

/// Fixed header bytes; the enclosing checkpoint supplies integrity and authority.
pub const SNAPSHOT_HEADER_BYTES: usize = 64;
/// One identity, partition offset, and logical acceptance time.
pub const SNAPSHOT_ROW_BYTES: usize = 32;
const MAGIC: &[u8; 8] = b"OZYDIDX\0";
const VERSION: u16 = 1;

/// Confirmed coordinates of a bounded, separately streamed index image.
/// Decoding this header supplies no checksum or partition authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotHeader {
    operation: u64,
    time: u64,
    window: u64,
    first: Offset,
    end: Offset,
    rows: u64,
}

impl SnapshotHeader {
    /// Original canonical commit boundary.
    pub const fn operation(&self) -> u64 {
        self.operation
    }

    /// Replicated logical time at capture.
    pub const fn logical_time(&self) -> u64 {
        self.time
    }

    /// Fixed configured deduplication window.
    pub const fn window_millis(&self) -> u64 {
        self.window
    }

    /// First live occurrence, or the record end for an empty image.
    pub const fn first_offset(&self) -> Offset {
        self.first
    }

    /// Exclusive confirmed record end, independent of producer attempts.
    pub const fn next_offset(&self) -> Offset {
        self.end
    }

    /// Number of live confirmed identities to stream.
    pub const fn rows(&self) -> u64 {
        self.rows
    }

    /// Exact section length, including the header.
    pub fn encoded_bytes(&self) -> u64 {
        // Construction and decoding validate that the section fits u64.
        SNAPSHOT_HEADER_BYTES as u64 + self.rows * SNAPSHOT_ROW_BYTES as u64
    }

    /// Encode only the fixed header. Rows follow separately in offset order.
    pub fn encode(&self) -> [u8; SNAPSHOT_HEADER_BYTES] {
        let mut bytes = [0; SNAPSHOT_HEADER_BYTES];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..10].copy_from_slice(&VERSION.to_be_bytes());
        for (target, value) in bytes[16..].as_chunks_mut::<8>().0.iter_mut().zip([
            self.operation,
            self.time,
            self.window,
            self.first.get(),
            self.end.get(),
            self.rows,
        ]) {
            target.copy_from_slice(&value.to_be_bytes());
        }
        bytes
    }

    /// Reject unknown versions, reserved fields, and inconsistent row coordinates.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != SNAPSHOT_HEADER_BYTES
            || &bytes[..8] != MAGIC
            || bytes[8..10] != VERSION.to_be_bytes()
            || bytes[10..16] != [0; 6]
        {
            return Err(Error::Snapshot);
        }
        Self {
            operation: read_u64(&bytes[16..24]),
            time: read_u64(&bytes[24..32]),
            window: read_u64(&bytes[32..40]),
            first: Offset::new(read_u64(&bytes[40..48])),
            end: Offset::new(read_u64(&bytes[48..56])),
            rows: read_u64(&bytes[56..64]),
        }
        .validated()
    }

    fn validated(self) -> Result<Self, Error> {
        if self.window == 0
            || self.end.get().checked_sub(self.first.get()) != Some(self.rows)
            || (self.rows != 0 && self.operation == 0)
            || self
                .rows
                .checked_mul(SNAPSHOT_ROW_BYTES as u64)
                .and_then(|bytes| bytes.checked_add(SNAPSHOT_HEADER_BYTES as u64))
                .is_none()
        {
            return Err(Error::Snapshot);
        }
        Ok(self)
    }
}

pub(super) fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("fixed snapshot integer"))
}

#[derive(Debug)]
pub(super) struct ActiveSnapshot {
    key: Weak<()>,
    pub(super) baseline: usize,
}

/// Owner-local streaming cursor, without a copy of the retained index.
/// Dropping it releases its expiry hold and delta limit on the next owner call.
#[derive(Debug)]
pub struct Snapshot {
    key: Rc<()>,
    header: SnapshotHeader,
    first: usize,
    next: usize,
    end: usize,
}

impl Snapshot {
    /// Confirmed boundary captured before any subsequently admitted operations.
    pub const fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    /// Rows already encoded; the caller owns and charges the output buffers.
    pub fn written_rows(&self) -> usize {
        self.next - self.first
    }

    /// Whether every captured row has been encoded.
    pub fn is_complete(&self) -> bool {
        self.next == self.end
    }
}

impl Index {
    pub(super) fn snapshot_active(&self) -> Option<&ActiveSnapshot> {
        self.snapshot
            .as_ref()
            .filter(|snapshot| snapshot.key.strong_count() != 0)
    }

    /// Capture only live confirmed identities. Pending rows are not checkpointed.
    /// Time lookup is logarithmic; rows are encoded on subsequent bounded calls.
    /// Admissions continue within a pending-record-sized delta allowance.
    pub fn begin_snapshot(&mut self) -> Result<Snapshot, Error> {
        if self.snapshot_active().is_some() {
            return Err(Error::SnapshotBusy);
        }
        let first = self.expiry.partition_point(|row| {
            self.committed_time.saturating_sub(row.time) >= self.limits.window_millis
        });
        let end = self.expiry.len();
        let header = SnapshotHeader {
            operation: self.committed_operation,
            time: self.committed_time,
            window: self.limits.window_millis,
            first: self
                .expiry
                .get(first)
                .map_or(self.committed_end, |row| row.offset),
            end: self.committed_end,
            rows: (end - first) as u64,
        }
        .validated()?;
        let key = Rc::new(());
        self.snapshot = Some(ActiveSnapshot {
            key: Rc::downgrade(&key),
            baseline: end,
        });
        Ok(Snapshot {
            key,
            header,
            first,
            next: first,
            end,
        })
    }

    fn validate_snapshot(&self, snapshot: &Snapshot) -> Result<(), Error> {
        if self
            .snapshot_active()
            .is_none_or(|active| !Weak::ptr_eq(&active.key, &Rc::downgrade(&snapshot.key)))
        {
            return Err(Error::Snapshot);
        }
        Ok(())
    }

    /// Fill complete fixed-width rows within output, caller, and batch limits.
    /// Captured values stay unchanged across commit, expiry time, and branch cuts.
    pub fn write_snapshot_rows(
        &self,
        snapshot: &mut Snapshot,
        output: &mut [u8],
        maximum_records: usize,
    ) -> Result<usize, Error> {
        self.validate_snapshot(snapshot)?;
        if snapshot.is_complete() {
            return Ok(0);
        }
        let count = (output.len() / SNAPSHOT_ROW_BYTES)
            .min(maximum_records)
            .min(self.limits.batch_records)
            .min(snapshot.end - snapshot.next);
        if count == 0 {
            return Err(Error::Snapshot);
        }
        let bytes = count * SNAPSHOT_ROW_BYTES;
        for (position, target) in output[..bytes]
            .as_chunks_mut::<SNAPSHOT_ROW_BYTES>()
            .0
            .iter_mut()
            .enumerate()
        {
            let row = self.expiry[snapshot.next + position];
            target[..16].copy_from_slice(row.id.as_bytes());
            target[16..24].copy_from_slice(&row.offset.get().to_be_bytes());
            target[24..32].copy_from_slice(&row.time.to_be_bytes());
        }
        snapshot.next += count;
        Ok(bytes)
    }

    /// Release a fully encoded capture after its caller has retained the buffers.
    /// Incomplete or stale cursors leave the active capture unchanged.
    pub fn finish_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Error> {
        self.validate_snapshot(snapshot)?;
        if !snapshot.is_complete() {
            return Err(Error::Snapshot);
        }
        self.snapshot = None;
        Ok(())
    }

    /// Abandon a capture without changing confirmed identities or time.
    pub fn abort_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Error> {
        self.validate_snapshot(snapshot)?;
        self.snapshot = None;
        Ok(())
    }
}
