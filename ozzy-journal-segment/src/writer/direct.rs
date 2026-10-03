//! `O_DIRECT` segment writes from 4 KiB-aligned staging memory.
//!
//! Groups already start and end on 4 KiB boundaries in the file. Their bytes
//! live in unaligned buffers, so each direct write first copies them into one
//! aligned staging buffer per writer thread.

use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

/// Required alignment of every direct write's file offset, length and memory.
pub(crate) const ALIGNMENT: usize = crate::codec::WRITE_GROUP_ALIGNMENT;

/// Open `path` for `O_DIRECT` writes, with `O_DSYNC` when every write must be
/// durable. Fails when the file system cannot do direct I/O at `ALIGNMENT`.
pub(crate) fn open(path: &Path, data_sync: bool) -> io::Result<Arc<File>> {
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{AtFlags, StatxFlags, statx};
        use std::os::unix::fs::OpenOptionsExt;
        let flags = libc::O_DIRECT | if data_sync { libc::O_DSYNC } else { 0 };
        let file = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(flags)
            .open(path)?;
        let stat = statx(&file, "", AtFlags::EMPTY_PATH, StatxFlags::DIOALIGN)?;
        if stat.stx_mask & StatxFlags::DIOALIGN.bits() != 0 {
            let memory = stat.stx_dio_mem_align as usize;
            let offset = stat.stx_dio_offset_align as usize;
            if memory == 0
                || offset == 0
                || !ALIGNMENT.is_multiple_of(memory)
                || !ALIGNMENT.is_multiple_of(offset)
            {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("direct I/O needs {memory} B memory and {offset} B offset alignment"),
                ));
            }
        }
        Ok(Arc::new(file))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (path, data_sync);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "direct I/O requires Linux",
        ))
    }
}

thread_local! {
    /// Reused per writer thread; grows to the largest write it staged.
    static STAGING: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Copy exactly `total` bytes from `slices` into aligned staging and write them
/// at `offset`, in calls of at most `limit` bytes rounded down to `ALIGNMENT`.
pub(crate) fn write<'a>(
    file: &File,
    offset: u64,
    total: usize,
    slices: impl Iterator<Item = &'a [u8]>,
    limit: std::num::NonZeroUsize,
) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    if !offset.is_multiple_of(ALIGNMENT as u64) || !total.is_multiple_of(ALIGNMENT) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unaligned direct write",
        ));
    }
    STAGING.with_borrow_mut(|buffer| {
        buffer.clear();
        buffer.reserve(total + ALIGNMENT);
        // Capacity already covers the padding, so the start stays aligned.
        let start = buffer.as_ptr().align_offset(ALIGNMENT);
        buffer.resize(start, 0);
        for slice in slices {
            buffer.extend_from_slice(slice);
        }
        let bytes = &buffer[start..];
        if bytes.len() != total {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "direct write length differs from its plan",
            ));
        }
        let step = (limit.get() / ALIGNMENT).max(1) * ALIGNMENT;
        let mut written = 0;
        while written < total {
            let end = (written + step).min(total);
            let count = file.write_at(&bytes[written..end], offset + written as u64)?;
            if count == 0 || !count.is_multiple_of(ALIGNMENT) {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "short direct write",
                ));
            }
            written += count;
        }
        Ok(())
    })
}
