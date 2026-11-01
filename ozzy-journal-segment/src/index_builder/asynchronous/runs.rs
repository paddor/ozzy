use super::{
    Access, IndexBuildError, IndexFileError, OrderedRun, PendingRuns, RowBase, RunKind,
    SectionRuns, Workspace,
};
use crate::index_file::{
    compare_message_rows, compare_operation_entries, compare_schedule_entries, encode_batch_entry,
    encode_message_row, encode_operation_entry, encode_operation_location, encode_schedule_entry,
};
use crate::{
    BATCH_INDEX_ENTRY_BYTES, BatchIndexEntry, MESSAGE_INDEX_ENTRY_BYTES, MessageRow,
    OPERATION_INDEX_ENTRY_BYTES, OPERATION_LOCATION_BYTES, OperationIndexEntry, OperationLocation,
    SCHEDULE_INDEX_ENTRY_BYTES, ScheduleIndexEntry,
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
        Self::at(access, file, chunk, 0)
    }
    fn at(access: &'a Access, file: &'a Handle, chunk: usize, offset: u64) -> Self {
        Self {
            access,
            file,
            chunk,
            buffer: Vec::with_capacity(chunk),
            offset,
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

/// One staged section's kind and fixed-width encoding.
struct RowFormat<T, const W: usize> {
    kind: RunKind,
    encode: fn(&T, RowBase, &mut [u8; W]),
}

/// A sorted section's format and entry order.
struct RunFormat<T, const W: usize> {
    rows: RowFormat<T, W>,
    compare: fn(&T, &T) -> Ordering,
}

const BATCHES: RowFormat<BatchIndexEntry, BATCH_INDEX_ENTRY_BYTES> = RowFormat {
    kind: RunKind::Batch,
    encode: encode_batch_entry,
};
const LOCATIONS: RowFormat<OperationLocation, OPERATION_LOCATION_BYTES> = RowFormat {
    kind: RunKind::Location,
    encode: |entry, _, row| encode_operation_location(entry, row),
};
const MESSAGES: RunFormat<MessageRow, MESSAGE_INDEX_ENTRY_BYTES> = RunFormat {
    rows: RowFormat {
        kind: RunKind::Message,
        encode: encode_message_row,
    },
    compare: compare_message_rows,
};
const OPERATIONS: RunFormat<OperationIndexEntry, OPERATION_INDEX_ENTRY_BYTES> = RunFormat {
    rows: RowFormat {
        kind: RunKind::Operation,
        encode: |entry, _, row| encode_operation_entry(entry, row),
    },
    compare: compare_operation_entries,
};
const SCHEDULES: RunFormat<ScheduleIndexEntry, SCHEDULE_INDEX_ENTRY_BYTES> = RunFormat {
    rows: RowFormat {
        kind: RunKind::Schedule,
        encode: |entry, _, row| encode_schedule_entry(entry, row),
    },
    compare: compare_schedule_entries,
};

#[derive(Clone, Copy)]
struct RunBounds {
    max_runs: usize,
    fan_in: usize,
    base: RowBase,
}

impl PendingRuns {
    pub(super) async fn flush_async(
        &mut self,
        workspace: &mut Workspace,
    ) -> Result<(), IndexBuildError> {
        let bounds = RunBounds {
            max_runs: self.max_runs,
            fan_in: self.fan_in,
            base: self.base,
        };
        self.batch_run
            .extend(&mut self.batches, BATCHES, self.base, workspace)
            .await?;
        write_typed_run(
            &mut self.messages,
            MESSAGES,
            workspace,
            &mut self.message_runs,
            bounds,
        )
        .await?;
        write_typed_run(
            &mut self.operations,
            OPERATIONS,
            workspace,
            &mut self.operation_runs,
            bounds,
        )
        .await?;
        self.location_run
            .extend(&mut self.locations, LOCATIONS, self.base, workspace)
            .await?;
        write_typed_run(
            &mut self.schedules,
            SCHEDULES,
            workspace,
            &mut self.schedule_runs,
            bounds,
        )
        .await?;
        self.buffered_bytes = 0;
        Ok(())
    }
}

impl OrderedRun {
    /// Append `entries`, which continue this section's order.
    async fn extend<T, const W: usize>(
        &mut self,
        entries: &mut Vec<T>,
        format: RowFormat<T, W>,
        base: RowBase,
        workspace: &mut Workspace,
    ) -> Result<(), IndexBuildError> {
        let RowFormat { kind, encode } = format;
        if entries.is_empty() {
            return Ok(());
        }
        if self.file.is_none() {
            let path = workspace.next_run(kind)?;
            self.file = Some(
                workspace
                    .access
                    .open(path.clone(), OpenMode::CreateNew, false, false)
                    .await?,
            );
            self.path = Some(path);
        }
        let file = self.file.as_ref().ok_or(IndexBuildError::InvalidRun)?;
        let chunk = workspace.limits.chunk_bytes;
        let mut output = Output::at(&workspace.access, file, chunk, self.written);
        debug_assert_eq!(kind.width(), W);
        let mut buffer = [0; W];
        let mut budget = crate::cooperative::Budget::default();
        for entry in entries.iter() {
            buffer.fill(0);
            encode(entry, base, &mut buffer);
            let key = &buffer[..kind.key_bytes()];
            if !self.last_key.is_empty() && self.last_key.as_slice() >= key {
                return Err(IndexFileError::UnsortedOrDuplicate(kind.name()).into());
            }
            self.last_key.clear();
            self.last_key.extend_from_slice(key);
            output.push(&buffer).await?;
            budget.charge_row(W).await;
        }
        output.flush().await?;
        self.written = output.position();
        entries.clear();
        Ok(())
    }

    /// Close the section's file. `None` when it had no rows.
    pub(super) async fn finish(self, access: &Access) -> Result<Option<PathBuf>, IndexBuildError> {
        if let Some(handle) = self.file {
            access.done(Operation::Close { handle }).await?;
        }
        Ok(self.path)
    }
}

async fn write_typed_run<T, const W: usize>(
    entries: &mut Vec<T>,
    format: RunFormat<T, W>,
    workspace: &mut Workspace,
    runs: &mut SectionRuns,
    bounds: RunBounds,
) -> Result<(), IndexBuildError> {
    let RunFormat {
        rows: RowFormat { kind, encode },
        compare,
    } = format;
    if entries.is_empty() {
        return Ok(());
    }
    if runs.paths.len() >= bounds.max_runs {
        return Err(IndexBuildError::RunLimitExceeded(bounds.max_runs));
    }
    crate::cooperative::sort_by(entries, compare).await;
    let mut budget = crate::cooperative::Budget::default();
    for pair in entries.windows(2) {
        if compare(&pair[0], &pair[1]) != Ordering::Less {
            return Err(IndexFileError::UnsortedOrDuplicate(kind.name()).into());
        }
        budget.charge_row(size_of::<T>()).await;
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
        encode(entry, bounds.base, &mut buffer);
        output.push(&buffer).await?;
        budget.charge_row(buffer.len()).await;
    }
    output.flush().await?;
    workspace
        .access
        .done(Operation::Close { handle: file })
        .await?;
    entries.clear();
    runs.add(path, kind, bounds.fan_in, workspace).await
}

impl SectionRuns {
    /// Keep a written run and merge each tier that fills, like carrying digits
    /// in base `fan_in`. A section then holds a few runs per tier, so a dense
    /// segment stays within the workspace bound.
    async fn add(
        &mut self,
        path: PathBuf,
        kind: RunKind,
        fan_in: usize,
        workspace: &mut Workspace,
    ) -> Result<(), IndexBuildError> {
        self.paths.push(path);
        self.written += 1;
        let mut carry = self.written;
        while carry.is_multiple_of(fan_in as u64) {
            let tier = self.paths.split_off(self.paths.len() - fan_in);
            self.paths
                .extend(merge_all(tier, kind, fan_in, workspace).await?);
            carry /= fan_in as u64;
        }
        Ok(())
    }
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
            .charge_row(kind.width().saturating_mul(readers.len()))
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
