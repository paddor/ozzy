use crate::AsyncSegmentOptions as Options;
use crate::WriterError;
use ozzy_io::{
    Class, Completed, Handle, Local, OpenMode, Operation, Outcome, SyncMode, WriteBuffer,
};
use std::{io, ops::Range, path::PathBuf};

#[derive(Debug, Clone)]
pub(crate) struct Access {
    pub(crate) io: Local,
    pub(crate) protection: Option<Handle>,
}

impl Access {
    fn protect(&self, operation: Operation) -> Operation {
        match &self.protection {
            Some(handle) => Operation::Protected {
                operation: Box::new(operation),
                handles: vec![handle.clone()],
            },
            None => operation,
        }
    }

    pub(crate) fn check_data(&self, operation: &Operation) -> io::Result<()> {
        let extra = if self.protection.is_some() {
            size_of::<Operation>() + size_of::<Handle>()
        } else {
            0
        };
        let charge = operation
            .retained_bytes()?
            .checked_add(extra)
            .ok_or(io::ErrorKind::InvalidInput)?;
        if charge
            > self
                .io
                .admission()
                .limits()
                .share(self.io.shard(), Class::Data)
                .bytes
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment append exceeds I/O byte share",
            ));
        }
        Ok(())
    }

    pub(crate) async fn execute(
        &self,
        class: Class,
        operation: Operation,
    ) -> io::Result<Completed> {
        self.io.execute(class, self.protect(operation)).await
    }

    pub(crate) async fn done(&self, operation: Operation) -> io::Result<()> {
        match *self.execute(Class::Progress, operation).await? {
            Outcome::Done => Ok(()),
            _ => Err(unexpected()),
        }
    }

    pub(crate) async fn open(
        &self,
        path: PathBuf,
        mode: OpenMode,
        direct: bool,
        data_sync: bool,
    ) -> io::Result<Handle> {
        let result = self
            .execute(
                Class::Progress,
                Operation::Open {
                    path,
                    mode,
                    direct,
                    data_sync,
                },
            )
            .await?;
        match &*result {
            Outcome::Opened(handle) => Ok(handle.clone()),
            _ => Err(unexpected()),
        }
    }

    pub(crate) async fn sync(&self, handle: &Handle) -> io::Result<()> {
        self.done(Operation::Sync {
            handle: handle.clone(),
            mode: SyncMode::Data,
        })
        .await
    }

    pub(crate) async fn open_directory(&self, path: PathBuf) -> io::Result<Handle> {
        let result = self
            .execute(Class::Progress, Operation::OpenDirectory { path })
            .await?;
        match &*result {
            Outcome::Opened(handle) => Ok(handle.clone()),
            _ => Err(unexpected()),
        }
    }

    pub(crate) async fn sync_directory(&self, path: PathBuf) -> io::Result<()> {
        let handle = self.open_directory(path).await?;
        self.done(Operation::Sync {
            handle: handle.clone(),
            mode: SyncMode::All,
        })
        .await?;
        self.done(Operation::Close { handle }).await
    }

    pub(crate) async fn length(&self, handle: &Handle) -> io::Result<u64> {
        match *self
            .execute(
                Class::Progress,
                Operation::Metadata {
                    handle: handle.clone(),
                },
            )
            .await?
        {
            Outcome::Metadata(metadata) => Ok(metadata.length),
            _ => Err(unexpected()),
        }
    }

    pub(crate) async fn read_range(
        &self,
        handle: &Handle,
        offset: u64,
        length: usize,
        chunk: usize,
    ) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.read_append(handle, offset, length, chunk, &mut bytes)
            .await?;
        Ok(bytes)
    }

    /// Append bounded transfers into caller-owned storage. Completed backend
    /// buffers stay charged until copied; cancellation exposes no success.
    pub(crate) async fn read_append(
        &self,
        handle: &Handle,
        offset: u64,
        length: usize,
        chunk: usize,
        bytes: &mut Vec<u8>,
    ) -> io::Result<()> {
        if chunk == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        offset
            .checked_add(length as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let start = bytes.len();
        let end = start
            .checked_add(length)
            .ok_or(io::ErrorKind::InvalidInput)?;
        bytes
            .try_reserve_exact(length)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        while bytes.len() < end {
            let result = self
                .execute(
                    Class::Progress,
                    Operation::Read {
                        handle: handle.clone(),
                        offset: offset + (bytes.len() - start) as u64,
                        length: (end - bytes.len()).min(chunk),
                    },
                )
                .await?;
            let Outcome::Read(buffer) = &*result else {
                return Err(unexpected());
            };
            if buffer.as_slice().is_empty() {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            if buffer.as_slice().len() > (end - bytes.len()).min(chunk) {
                return Err(unexpected());
            }
            bytes.extend_from_slice(buffer.as_slice());
        }
        Ok(())
    }

    pub(crate) async fn read_file(
        &self,
        path: PathBuf,
        limit: usize,
        chunk: usize,
    ) -> io::Result<Vec<u8>> {
        let handle = self.open(path, OpenMode::Read, false, false).await?;
        let result = async {
            let length = self.length(&handle).await?;
            if length > limit as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "file exceeds read limit",
                ));
            }
            self.read_range(&handle, 0, length as usize, chunk).await
        }
        .await;
        let closed = self.done(Operation::Close { handle }).await;
        let bytes = result?;
        closed?;
        Ok(bytes)
    }

    pub(crate) async fn list(
        &self,
        path: PathBuf,
        entries: usize,
        name_bytes: usize,
    ) -> io::Result<Vec<ozzy_io::Entry>> {
        let directory = self.open_directory(path.clone()).await?;
        let mut result = self
            .execute(
                Class::Progress,
                Operation::ReadDirectory {
                    path,
                    max_entries: entries,
                    max_name_bytes: name_bytes,
                },
            )
            .await?;
        // Metadata leaves the backend charge only after the caller has bounded
        // the complete listing above. It becomes caller-owned pending state.
        let entries = result.take_directory().ok_or_else(unexpected)?;
        drop(result);
        self.done(Operation::Close { handle: directory }).await?;
        Ok(entries)
    }

    pub(crate) async fn read_image(
        &self,
        handle: &Handle,
        options: Options,
    ) -> Result<Vec<u8>, WriterError> {
        let length = match *self
            .execute(
                Class::Progress,
                Operation::Metadata {
                    handle: handle.clone(),
                },
            )
            .await?
        {
            Outcome::Metadata(metadata) => metadata.length,
            _ => return Err(unexpected().into()),
        };
        if length > options.max_segment_bytes {
            return Err(WriterError::RecoveryLimit {
                actual: length,
                limit: options.max_segment_bytes,
            });
        }
        let length =
            usize::try_from(length).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let mut image = Vec::new();
        image
            .try_reserve_exact(length)
            .map_err(|_| WriterError::EncodeBufferAllocation)?;
        while image.len() < length {
            let result = self
                .execute(
                    Class::Progress,
                    Operation::Read {
                        handle: handle.clone(),
                        offset: image.len() as u64,
                        length: (length - image.len()).min(options.chunk_bytes),
                    },
                )
                .await?;
            let Outcome::Read(buffer) = &*result else {
                return Err(unexpected().into());
            };
            if buffer.as_slice().is_empty() {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
            }
            image.extend_from_slice(buffer.as_slice());
        }
        Ok(image)
    }

    pub(crate) async fn write(
        &self,
        class: Class,
        operation: Operation,
        length: usize,
    ) -> io::Result<()> {
        match *self.execute(class, operation).await? {
            Outcome::Written(count) if count == length => Ok(()),
            Outcome::Written(count) if count < length => Err(io::ErrorKind::WriteZero.into()),
            _ => Err(unexpected()),
        }
    }

    pub(crate) async fn write_all(
        &self,
        handle: &Handle,
        mut offset: u64,
        mut bytes: &[u8],
    ) -> io::Result<()> {
        while !bytes.is_empty() {
            let result = self
                .execute(
                    Class::Progress,
                    Operation::Write {
                        handle: handle.clone(),
                        offset,
                        data: WriteBuffer::from_vec(bytes.to_vec()),
                    },
                )
                .await?;
            let count = match *result {
                Outcome::Written(count) if count != 0 && count <= bytes.len() => count,
                Outcome::Written(0) => return Err(io::ErrorKind::WriteZero.into()),
                _ => return Err(unexpected()),
            };
            offset += count as u64;
            bytes = &bytes[count..];
        }
        Ok(())
    }

    pub(crate) async fn zero(
        &self,
        handle: &Handle,
        range: Range<u64>,
        chunk: usize,
    ) -> io::Result<()> {
        if range.is_empty() {
            return Ok(());
        }
        let zeros =
            vec![0; chunk.min(usize::try_from(range.end - range.start).unwrap_or(usize::MAX))];
        let mut offset = range.start;
        while offset < range.end {
            let count = zeros
                .len()
                .min(usize::try_from(range.end - offset).unwrap_or(usize::MAX));
            self.write_all(handle, offset, &zeros[..count]).await?;
            offset += count as u64;
        }
        Ok(())
    }
}

fn unexpected() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "unexpected segment I/O result")
}
