//! Journal write batches submitted through kernel AIO by the journal owner.

use super::{CompletedJournalWrite, PreparedJournalWrite};
use crate::aio::{AioContext, AlignedBuf};
use std::io;
use std::os::fd::{AsFd, BorrowedFd};
use std::sync::Arc;

const _: () = assert!(crate::codec::WRITE_GROUP_ALIGNMENT == crate::aio::ALIGNMENT);

/// Submits consecutive `O_DIRECT` write batches without blocking the journal
/// owner, and turns finished batches into completions for it to install.
/// Each batch carries a caller `tag` that comes back with its completions.
#[derive(Debug)]
pub struct JournalAio<T> {
    context: AioContext<(T, Vec<PreparedJournalWrite>)>,
    /// Staging buffers of finished batches, reused by later ones.
    spare: Vec<AlignedBuf>,
}

impl<T> JournalAio<T> {
    /// At most `depth` batches in flight.
    pub fn new(depth: usize) -> io::Result<Self> {
        Ok(Self {
            context: AioContext::new(depth)?,
            spare: Vec::new(),
        })
    }

    pub fn depth(&self) -> usize {
        self.context.depth()
    }

    pub const fn in_flight(&self) -> usize {
        self.context.in_flight()
    }

    /// Submit consecutive direct chunks as one write. If they cannot be
    /// submitted, `tag` comes back and every chunk completes with the error.
    pub fn submit(
        &mut self,
        chunks: Vec<PreparedJournalWrite>,
        tag: T,
    ) -> Result<(), (T, Vec<CompletedJournalWrite>)> {
        let valid = !chunks.is_empty()
            && chunks.iter().all(|chunk| chunk.direct)
            && chunks.windows(2).all(|pair| {
                Arc::ptr_eq(&pair[0].key, &pair[1].key) && pair[0].plan.after == pair[1].plan.before
            });
        if !valid {
            return Err((
                tag,
                fail(
                    chunks,
                    &io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "AIO needs consecutive direct journal writes",
                    ),
                ),
            ));
        }
        let first = &chunks[0];
        let last = chunks.last().expect("nonempty batch");
        let offset = first.plan.before.end_offset();
        let total = usize::try_from(last.plan.after.end_offset() - offset).unwrap_or(usize::MAX);
        let file = Arc::clone(&first.file);
        let mut buffer = self.spare.pop().unwrap_or_default();
        if let Err(error) = buffer.fill(total, chunks.iter().flat_map(|chunk| chunk.bytes.slices()))
        {
            self.spare.push(buffer);
            return Err((tag, fail(chunks, &error)));
        }
        self.context
            .submit_write(file, offset, buffer, (tag, chunks))
            .map_err(|((tag, chunks), buffer, error)| {
                self.spare.push(buffer);
                (tag, fail(chunks, &error))
            })
    }

    /// Hand every finished batch's tag and completions to `done`, without
    /// blocking. Batches may finish in any order; chunks keep theirs.
    pub fn reap(&mut self, mut done: impl FnMut(T, Vec<CompletedJournalWrite>)) -> io::Result<()> {
        let spare = &mut self.spare;
        self.context.reap(|(tag, chunks), buffer, result| {
            let result = full_write(result, buffer.as_slice().len());
            spare.push(buffer);
            done(tag, complete(chunks, &result));
        })?;
        Ok(())
    }

    /// Block until at least one batch in flight finishes, then reap. For
    /// shutdown paths that cannot run the event loop.
    pub fn wait(&mut self, mut done: impl FnMut(T, Vec<CompletedJournalWrite>)) -> io::Result<()> {
        let spare = &mut self.spare;
        self.context.wait(|(tag, chunks), buffer, result| {
            let result = full_write(result, buffer.as_slice().len());
            spare.push(buffer);
            done(tag, complete(chunks, &result));
        })?;
        Ok(())
    }
}

impl<T> AsFd for JournalAio<T> {
    /// Readable after a batch finished since the last reap.
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.context.as_fd()
    }
}

fn full_write(result: io::Result<usize>, expected: usize) -> io::Result<()> {
    result.and_then(|count| {
        if count == expected {
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::WriteZero, "short AIO write"))
        }
    })
}

fn fail(chunks: Vec<PreparedJournalWrite>, error: &io::Error) -> Vec<CompletedJournalWrite> {
    complete(chunks, &Err(copy(error)))
}

fn copy(error: &io::Error) -> io::Error {
    error.raw_os_error().map_or_else(
        || io::Error::new(error.kind(), error.to_string()),
        io::Error::from_raw_os_error,
    )
}

fn complete(
    chunks: Vec<PreparedJournalWrite>,
    result: &io::Result<()>,
) -> Vec<CompletedJournalWrite> {
    chunks
        .into_iter()
        .map(|work| CompletedJournalWrite {
            work,
            result: result.as_ref().copied().map_err(copy),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_kernel_results_never_complete_journal_groups() {
        assert!(full_write(Ok(4096), 4096).is_ok());
        assert_eq!(
            full_write(Ok(0), 4096).unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        assert_eq!(
            full_write(Ok(2048), 4096).unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        assert_eq!(
            full_write(Err(io::ErrorKind::PermissionDenied.into()), 4096)
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }
}
