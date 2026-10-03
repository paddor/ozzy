use super::Limits;
use crate::{
    DirectoryError,
    directory::{evidence, publication::algorithm},
};
use ozzy_io::{
    Class, Completed, Handle, Local, OpenMode, Operation, Outcome, SyncMode, WriteBuffer,
};
use std::{
    io,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Clone)]
pub(super) struct Files {
    pub(super) root: PathBuf,
    pub(super) io: Local,
    directory: Handle,
    pub(super) lock: Handle,
    evidence: Option<Handle>,
    pub(super) limits: Limits,
    pub(super) readers: crate::async_files::ReadHandles,
}

impl Files {
    pub(super) async fn open(
        root: PathBuf,
        io: Local,
        limits: Limits,
        create_lock: bool,
    ) -> Result<Self, DirectoryError> {
        let needed = limits.chunk_bytes.checked_mul(2).and_then(|n| {
            n.checked_add(
                4096 + size_of::<bytes::Bytes>() + size_of::<Operation>() + size_of::<Handle>(),
            )
        });
        if limits.chunk_bytes < crate::directory::evidence::RECORD_BYTES
            || limits.chunk_bytes > limits.max_file_bytes
            || needed.is_none_or(|n| {
                n > io
                    .admission()
                    .limits()
                    .share(io.shard(), Class::Progress)
                    .bytes
            })
        {
            return Err(
                io::Error::new(io::ErrorKind::InvalidInput, "invalid metadata I/O limits").into(),
            );
        }
        let directory = opened(
            io.execute(
                Class::Progress,
                Operation::OpenDirectory { path: root.clone() },
            )
            .await?,
        )?;
        let open = |mode| Operation::Open {
            path: root.join("group.lock"),
            mode,
            direct: false,
            data_sync: false,
        };
        let lock = match io.execute(Class::Progress, open(OpenMode::ReadWrite)).await {
            Ok(result) => opened(result)?,
            Err(error) if create_lock && error.kind() == io::ErrorKind::NotFound => {
                match io.execute(Class::Progress, open(OpenMode::CreateNew)).await {
                    Ok(result) => opened(result)?,
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => opened(
                        io.execute(Class::Progress, open(OpenMode::ReadWrite))
                            .await?,
                    )?,
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        };
        done(
            io.execute(
                Class::Progress,
                Operation::LockExclusive {
                    handle: lock.clone(),
                },
            )
            .await?,
        )?;
        Ok(Self {
            root,
            io,
            directory,
            lock,
            evidence: None,
            limits,
            readers: std::rc::Rc::default(),
        })
    }

    pub(super) async fn close_all(self) -> Result<(), DirectoryError> {
        drop(self.readers);
        let mut result = Ok(());
        for handle in self.evidence.into_iter().chain([self.directory, self.lock]) {
            let closed = self
                .io
                .execute(Class::Progress, Operation::Close { handle })
                .await
                .map_err(DirectoryError::from)
                .and_then(done);
            if result.is_ok() {
                result = closed;
            }
        }
        result
    }

    pub(super) async fn read(
        &self,
        name: &str,
        exact: Option<usize>,
        limit: usize,
    ) -> Result<Vec<u8>, DirectoryError> {
        self.check(name, limit)?;
        let handle = self.open_file(name, OpenMode::Read).await?;
        let result = async {
            let actual = self.length(&handle).await?;
            if let Some(length) = exact
                && actual != length as u64
            {
                return Err(DirectoryError::WrongFileSize {
                    object: "metadata",
                    actual,
                    expected: length as u64,
                });
            }
            if actual > limit as u64 {
                return Err(DirectoryError::FileLimit {
                    object: "metadata",
                    actual,
                    limit,
                });
            }
            self.read_range(&handle, 0, actual as usize).await
        }
        .await;
        self.close(handle, result).await
    }

    async fn read_range(
        &self,
        handle: &Handle,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, DirectoryError> {
        let mut bytes = Vec::with_capacity(length);
        while bytes.len() < length {
            let result = self
                .execute(Operation::Read {
                    handle: handle.clone(),
                    offset: offset + bytes.len() as u64,
                    length: (length - bytes.len()).min(self.limits.chunk_bytes),
                })
                .await?;
            let Outcome::Read(buffer) = &*result else {
                return Err(unexpected());
            };
            if buffer.as_slice().is_empty() {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
            }
            bytes.extend_from_slice(buffer.as_slice());
        }
        Ok(bytes)
    }

    async fn evidence_handle(&mut self) -> Result<Handle, DirectoryError> {
        if let Some(handle) = &self.evidence {
            return Ok(handle.clone());
        }
        let handle = self.open_file(evidence::NAME, OpenMode::ReadWrite).await?;
        let actual = self.length(&handle).await?;
        if actual != evidence::FILE_BYTES as u64 {
            return Err(DirectoryError::WrongFileSize {
                object: evidence::NAME,
                actual,
                expected: evidence::FILE_BYTES as u64,
            });
        }
        self.evidence = Some(handle.clone());
        Ok(handle)
    }

    pub(super) async fn evidence_records(
        &mut self,
    ) -> Result<[[u8; evidence::RECORD_BYTES]; 2], DirectoryError> {
        let handle = self.evidence_handle().await?;
        let mut records = [[0; evidence::RECORD_BYTES]; 2];
        for (copy, record) in records.iter_mut().enumerate() {
            record.copy_from_slice(
                &self
                    .read_range(
                        &handle,
                        (copy * evidence::COPY_STRIDE) as u64,
                        evidence::RECORD_BYTES,
                    )
                    .await?,
            );
        }
        Ok(records)
    }

    async fn invalidate_evidence(&mut self, name: &str) -> Result<(), DirectoryError> {
        if name == evidence::NAME
            && let Some(handle) = self.evidence.take()
        {
            self.close(handle, Ok(())).await?;
        }
        Ok(())
    }

    pub(super) fn check(&self, name: &str, bytes: usize) -> Result<(), DirectoryError> {
        let mut components = Path::new(name).components();
        if !matches!(components.next(), Some(Component::Normal(_)))
            || components.next().is_some()
            || name.contains(['/', '\\'])
            || name == "group.lock"
            || bytes > self.limits.max_file_bytes
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid metadata name or size",
            )
            .into());
        }
        Ok(())
    }

    fn path(&self, name: &str) -> Result<PathBuf, DirectoryError> {
        self.check(name, 0)?;
        Ok(self.root.join(name))
    }

    async fn execute(&self, operation: Operation) -> io::Result<Completed> {
        self.io
            .execute(
                Class::Progress,
                Operation::Protected {
                    operation: Box::new(operation),
                    handles: vec![self.lock.clone()],
                },
            )
            .await
    }

    async fn open_file(&self, name: &str, mode: OpenMode) -> Result<Handle, DirectoryError> {
        opened(
            self.execute(Operation::Open {
                path: self.path(name)?,
                mode,
                direct: false,
                data_sync: false,
            })
            .await?,
        )
    }

    async fn close<T>(
        &self,
        handle: Handle,
        result: Result<T, DirectoryError>,
    ) -> Result<T, DirectoryError> {
        let closed = self
            .execute(Operation::Close { handle })
            .await
            .map_err(DirectoryError::from)
            .and_then(done);
        match result {
            Ok(value) => {
                closed?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    async fn length(&self, handle: &Handle) -> Result<u64, DirectoryError> {
        let result = self
            .execute(Operation::Metadata {
                handle: handle.clone(),
            })
            .await?;
        match &*result {
            Outcome::Metadata(metadata) => Ok(metadata.length),
            _ => Err(unexpected()),
        }
    }

    async fn write_once(
        &self,
        handle: &Handle,
        offset: u64,
        bytes: &[u8],
    ) -> Result<usize, DirectoryError> {
        let result = self
            .execute(Operation::Write {
                handle: handle.clone(),
                offset,
                data: WriteBuffer::from_vec(bytes.to_vec()),
            })
            .await?;
        match *result {
            Outcome::Written(0) => Err(io::Error::from(io::ErrorKind::WriteZero).into()),
            Outcome::Written(count) if count <= bytes.len() => Ok(count),
            _ => Err(unexpected()),
        }
    }
}

fn opened(result: Completed) -> Result<Handle, DirectoryError> {
    let handle = match &*result {
        Outcome::Opened(handle) => Ok(handle.clone()),
        _ => Err(unexpected()),
    };
    drop(result); // Release this operation's charge before admitting more work.
    handle
}

fn done(result: Completed) -> Result<(), DirectoryError> {
    let outcome = match *result {
        Outcome::Done => Ok(()),
        _ => Err(unexpected()),
    };
    drop(result);
    outcome
}

fn unexpected() -> DirectoryError {
    io::Error::new(io::ErrorKind::InvalidData, "unexpected metadata I/O result").into()
}

impl algorithm::Io for Files {
    async fn exists(&mut self, name: &str) -> Result<bool, DirectoryError> {
        match self.open_file(name, OpenMode::Read).await {
            Ok(handle) => self.close(handle, Ok(true)).await,
            Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn read_exact(&mut self, name: &str, length: usize) -> Result<Vec<u8>, DirectoryError> {
        self.read(name, Some(length), length).await
    }

    async fn write_new(&mut self, name: &str, bytes: &[u8]) -> Result<(), DirectoryError> {
        self.check(name, bytes.len())?;
        let handle = self.open_file(name, OpenMode::CreateNew).await?;
        let result = async {
            let mut offset = 0;
            while offset < bytes.len() {
                let end = bytes.len().min(offset + self.limits.chunk_bytes);
                offset += self
                    .write_once(&handle, offset as u64, &bytes[offset..end])
                    .await?;
            }
            Ok(())
        }
        .await;
        self.close(handle, result).await
    }

    async fn sync_file(&mut self, name: &str) -> Result<(), DirectoryError> {
        let handle = self.open_file(name, OpenMode::Read).await?;
        let result = self
            .execute(Operation::Sync {
                handle: handle.clone(),
                mode: SyncMode::All,
            })
            .await
            .map_err(DirectoryError::from)
            .and_then(done);
        self.close(handle, result).await
    }

    async fn link(&mut self, source: &str, target: &str) -> Result<(), DirectoryError> {
        done(
            self.execute(Operation::HardLink {
                source: self.path(source)?,
                destination: self.path(target)?,
            })
            .await?,
        )
    }
    async fn remove(&mut self, name: &str) -> Result<(), DirectoryError> {
        self.invalidate_evidence(name).await?;
        done(
            self.execute(Operation::RemoveFile {
                path: self.path(name)?,
            })
            .await?,
        )
    }
    async fn rename(&mut self, source: &str, target: &str) -> Result<(), DirectoryError> {
        self.invalidate_evidence(source).await?;
        self.invalidate_evidence(target).await?;
        done(
            self.execute(Operation::Rename {
                source: self.path(source)?,
                destination: self.path(target)?,
            })
            .await?,
        )
    }
    async fn sync_directory(&mut self) -> Result<(), DirectoryError> {
        done(
            self.execute(Operation::Sync {
                handle: self.directory.clone(),
                mode: SyncMode::All,
            })
            .await?,
        )
    }
}

impl algorithm::Overwrite for Files {
    async fn write_synced(
        &mut self,
        name: &str,
        offsets: &[usize],
        bytes: &[u8],
    ) -> Result<(), DirectoryError> {
        self.check(name, bytes.len())?;
        if name != evidence::NAME || bytes.len() != evidence::RECORD_BYTES {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        let handle = self.evidence_handle().await?;
        for &offset in offsets {
            let end = offset
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?
                as u64;
            if (evidence::FILE_BYTES as u64) < end {
                return Err(DirectoryError::WrongFileSize {
                    object: "metadata",
                    actual: evidence::FILE_BYTES as u64,
                    expected: end,
                });
            }
        }
        for &offset in offsets {
            // Do not retry partial evidence records: that could persist
            // intermediate torn contents in both redundant copies.
            if self.write_once(&handle, offset as u64, bytes).await? != bytes.len() {
                return Err(io::Error::from(io::ErrorKind::WriteZero).into());
            }
        }
        done(
            self.execute(Operation::Sync {
                handle: handle.clone(),
                mode: SyncMode::Data,
            })
            .await?,
        )
    }
}
