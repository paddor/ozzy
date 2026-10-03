//! Schedule completed ranges before roll. This never advances durable history.

use std::{fs::File, io, ops::Range, sync::Arc};

use super::{PreparedJournalWrite, StoreLock};

/// Owned writeback hint. Retains its file and store lock, never payload bytes.
#[derive(Debug)]
pub struct JournalWriteback {
    file: Arc<File>,
    lock: Arc<StoreLock>,
    segment: u64,
    range: Range<u64>,
}

impl JournalWriteback {
    pub(super) fn capture(work: &PreparedJournalWrite, end: u64) -> Option<Self> {
        if !work.buffered {
            return None;
        }
        let range = completed_range(work.plan.before.end_offset(), end)?;
        Some(Self {
            file: Arc::clone(&work.file),
            lock: Arc::clone(&work.store_lock),
            segment: work.locations[0].segment_id,
            range,
        })
    }

    /// Coalesce consecutive ranges from the same open store and segment.
    pub fn merge(&mut self, next: Self) -> io::Result<()> {
        if !Arc::ptr_eq(&self.lock, &next.lock)
            || self.segment != next.segment
            || self.range.end != next.range.start
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "nonconsecutive writeback ranges",
            ));
        }
        self.range.end = next.range.end;
        drop(next);
        Ok(())
    }

    /// Start writeback; success does not establish durable history.
    pub fn start(self) -> io::Result<()> {
        start_range(&self.file, self.range)
    }
}

const RANGE_BYTES: u64 = 4 * 1024 * 1024;

pub(crate) fn completed_range(before: u64, after: u64) -> Option<Range<u64>> {
    let start = before / RANGE_BYTES * RANGE_BYTES;
    let end = after / RANGE_BYTES * RANGE_BYTES;
    (end > start).then_some(start..end)
}

pub(super) fn start(file: &File, before: u64, after: u64) -> io::Result<()> {
    let Some(range) = completed_range(before, after) else {
        return Ok(());
    };
    start_range(file, range)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[allow(unsafe_code)]
pub(crate) fn start_range(file: &File, range: Range<u64>) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let offset = i64::try_from(range.start).map_err(|_| io::ErrorKind::InvalidInput)?;
    let length = i64::try_from(range.end - range.start).map_err(|_| io::ErrorKind::InvalidInput)?;
    loop {
        // SAFETY: file stays borrowed for the syscall; offsets are representable,
        // length is positive, and the kernel receives no userspace pointers.
        let result = unsafe {
            libc::sync_file_range(
                file.as_raw_fd(),
                offset,
                length,
                libc::SYNC_FILE_RANGE_WRITE,
            )
        };
        if result == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn start_range(_file: &File, _range: Range<u64>) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_complete_ranges_are_submitted_once() {
        let b = RANGE_BYTES;
        assert_eq!(completed_range(4096, b - 1), None);
        assert_eq!(completed_range(b - 1, b), Some(0..b));
        assert_eq!(completed_range(b, b + 1), None);
        assert_eq!(completed_range(b + 1, 3 * b + 17), Some(b..3 * b));
        // A new segment restarts offsets; no state leaks from its predecessor.
        assert_eq!(completed_range(4096, b + 17), Some(0..b));
    }

    #[test]
    fn schedules_written_file_ranges_without_changing_contents() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&vec![0x5a; RANGE_BYTES as usize + 17])
            .unwrap();
        start(&file, 0, RANGE_BYTES + 17).unwrap();
        file.sync_data().unwrap();
        file.seek(SeekFrom::Start(RANGE_BYTES - 1)).unwrap();
        let mut tail = [0; 18];
        file.read_exact(&mut tail).unwrap();
        assert_eq!(tail, [0x5a; 18]);
    }
}

#[cfg(test)]
mod owned_tests {
    use super::*;
    #[test]
    fn merge_requires_same_store_segment_and_consecutive_range() {
        let (_dir, journal) = crate::directory::progress_tests::journal_mode(true);
        let (_other_dir, other) = crate::directory::progress_tests::journal_mode(true);
        let hint = |lock: Arc<StoreLock>, segment, range| JournalWriteback {
            file: Arc::new(tempfile::tempfile().unwrap()),
            lock,
            segment,
            range,
        };
        let lock = journal.directory.lock.clone();
        let mut first = hint(lock.clone(), 1, 0..4);
        first.merge(hint(lock.clone(), 1, 4..8)).unwrap();
        assert_eq!(first.range, 0..8);
        assert!(first.merge(hint(lock.clone(), 1, 9..12)).is_err());
        assert!(first.merge(hint(lock.clone(), 2, 8..12)).is_err());
        assert!(
            first
                .merge(hint(other.directory.lock.clone(), 1, 8..12))
                .is_err()
        );
        assert_eq!(first.range, 0..8);
        drop(journal);
        assert_eq!(Arc::strong_count(&lock), 2);
        drop(first);
        assert_eq!(Arc::strong_count(&lock), 1);
    }
}
