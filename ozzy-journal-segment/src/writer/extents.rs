//! Bounded scatter/gather output. Bodies stay borrowed until all bytes, including
//! the group seal, have been written. No asynchronous operation outlives a borrow.

use std::io::{self, IoSlice};

use super::SegmentIo;

// A bound on syscall descriptors, not records or storage-group size.
// Linux permits 1024 iovecs. Small packing blocks must not artificially split
// an ordinary group, especially when each O_DSYNC write is a durability wait.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(super) const WRITE_EXTENTS: usize = 1024;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub(super) const WRITE_EXTENTS: usize = 64;

pub(crate) fn write_extents<'a>(
    io: &mut impl SegmentIo,
    offset: u64,
    extents: impl Iterator<Item = &'a [u8]>,
) -> io::Result<()> {
    write_extents_bounded(io, offset, extents, std::num::NonZeroUsize::MAX)
}

pub(crate) fn write_extents_bounded<'a>(
    io: &mut impl SegmentIo,
    mut offset: u64,
    mut extents: impl Iterator<Item = &'a [u8]>,
    limit: std::num::NonZeroUsize,
) -> io::Result<()> {
    let mut slices = [IoSlice::new(&[]); WRITE_EXTENTS];
    let mut remainder = None;
    loop {
        let mut count = 0;
        let mut budget = limit.get();
        while count < WRITE_EXTENTS && budget != 0 {
            let Some(bytes) = remainder.take().or_else(|| extents.next()) else {
                break;
            };
            if bytes.is_empty() {
                continue;
            }
            let length = bytes.len().min(budget);
            slices[count] = IoSlice::new(&bytes[..length]);
            if length != bytes.len() {
                remainder = Some(&bytes[length..]);
            }
            budget -= length;
            count += 1;
        }
        if count == 0 {
            return Ok(());
        }
        let mut remaining = slices[..count].iter().try_fold(0_usize, |total, slice| {
            total
                .checked_add(slice.len())
                .ok_or(io::ErrorKind::InvalidInput)
        })?;
        offset
            .checked_add(remaining as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let mut pending = &mut slices[..count];
        while remaining != 0 {
            match io.write_vectored_at(offset, pending) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(written) if written > remaining => return Err(io::ErrorKind::InvalidData.into()),
                Ok(written) => {
                    offset += written as u64;
                    remaining -= written;
                    IoSlice::advance_slices(&mut pending, written);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}
