//! Backend-thread primitive for Linux kernel AIO (`io_submit`).
//!
//! The owning thread submits page-aligned writes and reaps completions without
//! blocking; an eventfd tells its event loop when to reap. Every buffer and job
//! stays owned by the context until the kernel reports the write finished.
//! Dropping the context waits for writes still in flight.
#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::sync::Arc;

/// File offset, length and memory alignment of every submitted write.
pub const ALIGNMENT: usize = 4096;

const IOCB_CMD_PWRITE: u16 = 1;
const IOCB_FLAG_RESFD: u32 = 1;

/// Kernel `struct io_event`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct IoEvent {
    data: u64,
    obj: u64,
    res: i64,
    res2: i64,
}

/// Heap bytes starting on an `ALIGNMENT` boundary, reusable across writes.
#[derive(Debug, Default)]
pub struct AlignedBuf {
    bytes: Vec<u8>,
    start: usize,
}

impl AlignedBuf {
    /// Replace the contents with exactly `total` bytes copied from `slices`.
    pub fn fill<'a>(
        &mut self,
        total: usize,
        slices: impl Iterator<Item = &'a [u8]>,
    ) -> io::Result<()> {
        self.bytes.clear();
        self.start = 0;
        let capacity = total
            .checked_add(ALIGNMENT)
            .ok_or(io::ErrorKind::InvalidInput)?;
        self.bytes
            .try_reserve_exact(capacity)
            .map_err(io::Error::other)?;
        // Capacity already covers the padding, so the start stays aligned.
        self.start = self.bytes.as_ptr().align_offset(ALIGNMENT);
        self.bytes.resize(self.start, 0);
        for slice in slices {
            if slice.len() > total - (self.bytes.len() - self.start) {
                self.bytes.clear();
                self.start = 0;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "aligned buffer exceeds its plan",
                ));
            }
            self.bytes.extend_from_slice(slice);
        }
        if self.bytes.len() - self.start == total {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "aligned buffer length differs from its plan",
            ))
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[self.start..]
    }
}

#[derive(Debug)]
struct Request<T, F> {
    buffer: AlignedBuf,
    file: Arc<F>,
    // Drop payload and descriptor before a job's admission guard.
    job: T,
}

/// One kernel AIO context and the requests it owns.
#[derive(Debug)]
pub struct AioContext<T, F = File> {
    context: u64,
    event: OwnedFd,
    slots: Vec<Option<Request<T, F>>>,
    in_flight: usize,
    events: Vec<IoEvent>,
    /// `events.len()` as the syscall argument type.
    capacity: libc::c_long,
}

fn last_error() -> io::Error {
    io::Error::last_os_error()
}

impl<T, F: AsFd> AioContext<T, F> {
    /// A context for at most `depth` writes in flight.
    pub fn new(depth: usize) -> io::Result<Self> {
        if !(1..=65).contains(&depth) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AIO depth must be 1..=65",
            ));
        }
        let requests = libc::c_long::try_from(depth)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let mut context = 0_u64;
        // SAFETY: io_setup writes one aio_context_t into `context`.
        let result = unsafe {
            libc::syscall(
                libc::SYS_io_setup,
                requests,
                std::ptr::from_mut(&mut context),
            )
        };
        if result != 0 {
            return Err(last_error());
        }
        // SAFETY: eventfd takes no pointers.
        let raw = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if raw < 0 {
            let error = last_error();
            // SAFETY: the context exists and has no requests.
            unsafe { libc::syscall(libc::SYS_io_destroy, context) };
            return Err(error);
        }
        // SAFETY: `raw` is a fresh descriptor owned by nothing else.
        let event = unsafe { OwnedFd::from_raw_fd(raw) };
        Ok(Self {
            context,
            event,
            slots: (0..depth).map(|_| None).collect(),
            in_flight: 0,
            events: vec![IoEvent::default(); depth],
            capacity: requests,
        })
    }

    pub fn depth(&self) -> usize {
        self.slots.len()
    }

    pub const fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// Submit one write of `buffer` at `offset`. On failure the job and buffer
    /// come back, including for a full context (`WouldBlock`), a descriptor
    /// without `O_DIRECT`, or an unaligned offset or length.
    pub fn submit_write(
        &mut self,
        file: Arc<F>,
        offset: u64,
        buffer: AlignedBuf,
        job: T,
    ) -> Result<(), (T, AlignedBuf, io::Error)> {
        let offset = match check(file.as_ref(), offset, &buffer) {
            Ok(offset) => offset,
            Err(error) => return Err((job, buffer, error)),
        };
        let Some(slot) = self.slots.iter().position(Option::is_none) else {
            return Err((job, buffer, io::ErrorKind::WouldBlock.into()));
        };
        let bytes = buffer.as_slice();
        // SAFETY: all-zero is a valid iocb; the fields set below complete it.
        let mut iocb: libc::iocb = unsafe { std::mem::zeroed() };
        iocb.aio_data = slot as u64;
        iocb.aio_lio_opcode = IOCB_CMD_PWRITE;
        iocb.aio_fildes = file.as_fd().as_raw_fd() as u32;
        iocb.aio_buf = bytes.as_ptr() as u64;
        iocb.aio_nbytes = bytes.len() as u64;
        iocb.aio_offset = offset;
        iocb.aio_flags = IOCB_FLAG_RESFD;
        iocb.aio_resfd = self.event.as_raw_fd() as u32;
        // The slot owns the buffer from here on. Moving `AlignedBuf` moves only
        // its `Vec` header, so the heap bytes the kernel reads stay in place.
        self.slots[slot] = Some(Request { buffer, file, job });
        let mut list = [std::ptr::from_mut(&mut iocb)];
        // SAFETY: the iocb is read during the call. Its buffer and descriptor
        // stay alive in `slots` until io_getevents or io_destroy reports it.
        let result =
            unsafe { libc::syscall(libc::SYS_io_submit, self.context, 1_i64, list.as_mut_ptr()) };
        if result == 1 {
            self.in_flight += 1;
            return Ok(());
        }
        let error = if result < 0 {
            let error = last_error();
            if error.raw_os_error() == Some(libc::EAGAIN) {
                io::ErrorKind::WouldBlock.into()
            } else {
                error
            }
        } else {
            io::Error::other("io_submit accepted no request")
        };
        let request = self.slots[slot].take().expect("slot filled above");
        drop(request.file);
        Err((request.job, request.buffer, error))
    }

    /// Hand every finished write to `done` without blocking. Returns how many.
    pub fn reap(
        &mut self,
        done: impl FnMut(T, AlignedBuf, io::Result<usize>),
    ) -> io::Result<usize> {
        self.drain_event()?;
        self.collect(
            0,
            Some(libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            }),
            done,
        )
    }

    /// Wait for at least one write in flight to finish, then reap. For shutdown
    /// paths that cannot run the event loop.
    pub fn wait(
        &mut self,
        done: impl FnMut(T, AlignedBuf, io::Result<usize>),
    ) -> io::Result<usize> {
        if self.in_flight == 0 {
            return Ok(0);
        }
        self.drain_event()?;
        self.collect(1, None, done)
    }

    fn drain_event(&self) -> io::Result<()> {
        let mut counter = [0_u8; 8];
        // SAFETY: reads at most 8 bytes into `counter`.
        loop {
            let result = unsafe {
                libc::read(
                    self.event.as_raw_fd(),
                    counter.as_mut_ptr().cast(),
                    counter.len(),
                )
            };
            if result < 0 {
                let error = last_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if error.raw_os_error() != Some(libc::EAGAIN) {
                    return Err(error);
                }
            }
            return Ok(());
        }
    }

    fn collect(
        &mut self,
        minimum: libc::c_long,
        timeout: Option<libc::timespec>,
        mut done: impl FnMut(T, AlignedBuf, io::Result<usize>),
    ) -> io::Result<usize> {
        let mut timeout = timeout;
        let timeout = timeout
            .as_mut()
            .map_or(std::ptr::null_mut(), std::ptr::from_mut);
        let count = loop {
            // SAFETY: `events` holds `depth` entries; the kernel writes at most
            // that many. `timeout` is null or points to a live timespec.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_io_getevents,
                    self.context,
                    minimum,
                    self.capacity,
                    self.events.as_mut_ptr(),
                    timeout,
                )
            };
            if result >= 0 {
                break result as usize;
            }
            let error = last_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        };
        for index in 0..count {
            let event = self.events[index];
            let request = usize::try_from(event.data)
                .ok()
                .and_then(|slot| self.slots.get_mut(slot))
                .and_then(Option::take)
                .ok_or_else(|| io::Error::other("completion for an unknown AIO request"))?;
            self.in_flight -= 1;
            let result = if event.res < 0 {
                Err(io::Error::from_raw_os_error(-event.res as i32))
            } else if event.res as usize > request.buffer.as_slice().len() {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "AIO completion exceeds submitted length",
                ))
            } else {
                Ok(event.res as usize)
            };
            drop(request.file);
            done(request.job, request.buffer, result);
        }
        Ok(count)
    }
}

/// The kernel offset of a write that direct I/O accepts.
fn check(file: &impl AsFd, offset: u64, buffer: &AlignedBuf) -> io::Result<i64> {
    let flags = rustix::fs::fcntl_getfl(file)?;
    if !flags.contains(rustix::fs::OFlags::DIRECT) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AIO writes need an O_DIRECT descriptor",
        ));
    }
    let bytes = buffer.as_slice();
    if bytes.is_empty()
        || !offset.is_multiple_of(ALIGNMENT as u64)
        || !bytes.len().is_multiple_of(ALIGNMENT)
        || !(bytes.as_ptr() as usize).is_multiple_of(ALIGNMENT)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unaligned AIO write",
        ));
    }
    offset
        .checked_add(bytes.len() as u64)
        .filter(|end| i64::try_from(*end).is_ok())
        .ok_or(io::ErrorKind::InvalidInput)?;
    i64::try_from(offset).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
}

impl<T, F> AsFd for AioContext<T, F> {
    /// Readable after at least one write finished since the last reap.
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.event.as_fd()
    }
}

impl<T, F> Drop for AioContext<T, F> {
    fn drop(&mut self) {
        // SAFETY: io_destroy waits for requests still in flight, so no buffer
        // in `slots` is freed while the kernel may still read it.
        let result = unsafe { libc::syscall(libc::SYS_io_destroy, self.context) };
        if result != 0 {
            // The kernel may still read these buffers; never free them.
            std::mem::forget(std::mem::take(&mut self.slots));
        }
    }
}

#[cfg(test)]
mod tests;
