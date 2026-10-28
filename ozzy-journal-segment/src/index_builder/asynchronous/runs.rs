use super::{Access, IndexBuildError, IndexFileError, PendingRuns, RunKind, Workspace};
use crate::index_file::{
    compare_locations, compare_message_entries, compare_offset_entries, compare_operation_entries,
    compare_schedule_entries, encode_message_entry, encode_offset_entry, encode_operation_entry,
    encode_operation_location, encode_schedule_entry,
};
use crate::{
    MESSAGE_INDEX_ENTRY_BYTES, MessageIndexEntry, OFFSET_INDEX_ENTRY_BYTES,
    OPERATION_INDEX_ENTRY_BYTES, OPERATION_LOCATION_BYTES, OffsetIndexEntry, OperationIndexEntry,
    OperationLocation, SCHEDULE_INDEX_ENTRY_BYTES, ScheduleIndexEntry,
};
use ozzy_io::{Handle, OpenMode, Operation};
use std::{cmp::Ordering, path::PathBuf};

pub(super) struct Output<'a> {
    access: &'a Access,
    file: &'a Handle,
    chunk: usize,
    buffer: Vec<u8>,
    offset: u64,
}

impl<'a> Output<'a> {
    pub(super) fn new(access: &'a Access, file: &'a Handle, chunk: usize) -> Self {
        Self {
            access,
            file,
            chunk,
            buffer: Vec::with_capacity(chunk),
            offset: 0,
        }
    }
    pub(super) const fn position(&self) -> u64 {
        self.offset
    }
    pub(super) async fn push(&mut self, mut bytes: &[u8]) -> Result<(), IndexBuildError> {
        while !bytes.is_empty() {
            let count = bytes.len().min(self.chunk - self.buffer.len());
            self.buffer.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.buffer.len() == self.chunk {
                self.flush().await?;
            }
        }
        Ok(())
    }
    pub(super) async fn flush(&mut self) -> Result<(), IndexBuildError> {
        if !self.buffer.is_empty() {
            self.access
                .write_all(self.file, self.offset, &self.buffer)
                .await?;
            self.offset = self
                .offset
                .checked_add(self.buffer.len() as u64)
                .ok_or(IndexFileError::LengthOverflow)?;
            self.buffer.clear();
        }
        Ok(())
    }
}

/// One staging-run section: its kind, entry order, and fixed-width encoding.
struct RunFormat<T, const W: usize> {
    kind: RunKind,
    compare: fn(&T, &T) -> Ordering,
    encode: fn(&T, &mut [u8; W]),
}

const OFFSETS: RunFormat<OffsetIndexEntry, OFFSET_INDEX_ENTRY_BYTES> = RunFormat {
    kind: RunKind::Offset,
    compare: compare_offset_entries,
    encode: encode_offset_entry,
};
const MESSAGES: RunFormat<MessageIndexEntry, MESSAGE_INDEX_ENTRY_BYTES> = RunFormat {
    kind: RunKind::Message,
    compare: compare_message_entries,
    encode: encode_message_entry,
};
const OPERATIONS: RunFormat<OperationIndexEntry, OPERATION_INDEX_ENTRY_BYTES> = RunFormat {
    kind: RunKind::Operation,
    compare: compare_operation_entries,
    encode: encode_operation_entry,
};
const LOCATIONS: RunFormat<OperationLocation, OPERATION_LOCATION_BYTES> = RunFormat {
    kind: RunKind::Location,
    compare: compare_locations,
    encode: encode_operation_location,
};
const SCHEDULES: RunFormat<ScheduleIndexEntry, SCHEDULE_INDEX_ENTRY_BYTES> = RunFormat {
    kind: RunKind::Schedule,
    compare: compare_schedule_entries,
    encode: encode_schedule_entry,
};

impl PendingRuns {
    pub(super) async fn flush_async(
        &mut self,
        workspace: &mut Workspace,
    ) -> Result<(), IndexBuildError> {
        write_typed_run(
            &mut self.offsets,
            OFFSETS,
            workspace,
            &mut self.offset_runs,
            self.max_runs,
        )
        .await?;
        write_typed_run(
            &mut self.messages,
            MESSAGES,
            workspace,
            &mut self.message_runs,
            self.max_runs,
        )
        .await?;
        write_typed_run(
            &mut self.operations,
            OPERATIONS,
            workspace,
            &mut self.operation_runs,
            self.max_runs,
        )
        .await?;
        write_typed_run(
            &mut self.locations,
            LOCATIONS,
            workspace,
            &mut self.location_runs,
            self.max_runs,
        )
        .await?;
        write_typed_run(
            &mut self.schedules,
            SCHEDULES,
            workspace,
            &mut self.schedule_runs,
            self.max_runs,
        )
        .await?;
        self.buffered_bytes = 0;
        Ok(())
    }
}

async fn write_typed_run<T, const W: usize>(
    entries: &mut Vec<T>,
    format: RunFormat<T, W>,
    workspace: &mut Workspace,
    runs: &mut Vec<PathBuf>,
    max_runs: usize,
) -> Result<(), IndexBuildError> {
    let RunFormat {
        kind,
        compare,
        encode,
    } = format;
    if entries.is_empty() {
        return Ok(());
    }
    if runs.len() >= max_runs {
        return Err(IndexBuildError::RunLimitExceeded(max_runs));
    }
    crate::cooperative::sort_by(entries, compare).await;
    let mut budget = crate::cooperative::Budget::default();
    for pair in entries.windows(2) {
        if compare(&pair[0], &pair[1]) != Ordering::Less {
            return Err(IndexFileError::UnsortedOrDuplicate(kind.name()).into());
        }
        budget.charge(size_of::<T>()).await;
    }
    let path = workspace.next_run(kind)?;
    let file = workspace
        .access
        .open(path.clone(), OpenMode::CreateNew, false, false)
        .await?;
    let mut output = Output::new(&workspace.access, &file, workspace.limits.chunk_bytes);
    debug_assert_eq!(kind.width(), W);
    let mut buffer = [0; W];
    for entry in entries.iter() {
        buffer.fill(0);
        encode(entry, &mut buffer);
        output.push(&buffer).await?;
        budget.charge(buffer.len()).await;
    }
    output.flush().await?;
    workspace
        .access
        .done(Operation::Close { handle: file })
        .await?;
    runs.push(path);
    entries.clear();
    Ok(())
}

pub(super) async fn merge_all(
    mut runs: Vec<PathBuf>,
    kind: RunKind,
    fan_in: usize,
    workspace: &mut Workspace,
) -> Result<Option<PathBuf>, IndexBuildError> {
    while runs.len() > 1 {
        let mut next = Vec::with_capacity(runs.len().div_ceil(fan_in));
        for chunk in runs.chunks(fan_in) {
            if chunk.len() == 1 {
                next.push(chunk[0].clone());
            } else {
                let path = workspace.next_run(kind)?;
                merge_runs(chunk, path.clone(), kind, workspace).await?;
                for input in chunk {
                    workspace.remove(input).await?;
                }
                next.push(path);
            }
        }
        runs = next;
    }
    Ok(runs.pop())
}

async fn merge_runs(
    inputs: &[PathBuf],
    path: PathBuf,
    kind: RunKind,
    workspace: &Workspace,
) -> Result<(), IndexBuildError> {
    let mut readers = Vec::with_capacity(inputs.len());
    for input in inputs {
        readers.push(
            Reader::open(
                &workspace.access,
                input.clone(),
                kind.width(),
                workspace.limits.chunk_bytes,
            )
            .await?,
        );
    }
    let file = workspace
        .access
        .open(path, OpenMode::CreateNew, false, false)
        .await?;
    let mut output = Output::new(&workspace.access, &file, workspace.limits.chunk_bytes);
    let mut previous = Vec::new();
    let mut budget = crate::cooperative::Budget::default();
    while let Some((selected, entry)) = readers
        .iter()
        .enumerate()
        .filter_map(|(index, reader)| reader.head().map(|entry| (index, entry)))
        .min_by(|(_, a), (_, b)| a[..kind.key_bytes()].cmp(&b[..kind.key_bytes()]))
    {
        let key = &entry[..kind.key_bytes()];
        if !previous.is_empty() && previous.as_slice() >= key {
            return Err(IndexFileError::UnsortedOrDuplicate(kind.name()).into());
        }
        output.push(entry).await?;
        previous.clear();
        previous.extend_from_slice(key);
        readers[selected].advance(&workspace.access).await?;
        budget
            .charge(kind.width().saturating_mul(readers.len()))
            .await;
    }
    output.flush().await?;
    workspace
        .access
        .done(Operation::Close { handle: file })
        .await?;
    for reader in readers {
        workspace
            .access
            .done(Operation::Close {
                handle: reader.file,
            })
            .await?;
    }
    Ok(())
}

struct Reader {
    file: Handle,
    length: usize,
    offset: usize,
    chunk: usize,
    width: usize,
    buffer: Vec<u8>,
    at: usize,
}

impl Reader {
    async fn open(
        access: &Access,
        path: PathBuf,
        width: usize,
        chunk: usize,
    ) -> Result<Self, IndexBuildError> {
        let file = access.open(path, OpenMode::Read, false, false).await?;
        let length = usize::try_from(access.length(&file).await?)
            .map_err(|_| IndexFileError::LengthOverflow)?;
        if length == 0 || !length.is_multiple_of(width) {
            return Err(IndexBuildError::InvalidRun);
        }
        let mut reader = Self {
            file,
            length,
            offset: 0,
            chunk: (chunk / width) * width,
            width,
            buffer: Vec::new(),
            at: 0,
        };
        reader.refill(access).await?;
        Ok(reader)
    }
    fn head(&self) -> Option<&[u8]> {
        self.buffer.get(self.at..self.at + self.width)
    }
    async fn advance(&mut self, access: &Access) -> Result<(), IndexBuildError> {
        self.at += self.width;
        if self.at == self.buffer.len() {
            self.refill(access).await?;
        }
        Ok(())
    }
    async fn refill(&mut self, access: &Access) -> Result<(), IndexBuildError> {
        let count = (self.length - self.offset).min(self.chunk);
        self.buffer = access
            .read_range(&self.file, self.offset as u64, count, self.chunk)
            .await?;
        self.offset += count;
        self.at = 0;
        Ok(())
    }
}
