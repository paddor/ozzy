use bytes::Bytes;
use std::{ffi::OsString, io, path::PathBuf};

use crate::Handle;

/// Owned scatter/gather data. `retained_bytes` charges the full backing
/// allocations, not just short slices into larger buffers. It excludes the
/// descriptor vector, which is charged separately.
#[derive(Debug)]
pub struct WriteBuffer {
    parts: Vec<Bytes>,
    length: usize,
    retained_bytes: usize,
}

impl WriteBuffer {
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        let retained_bytes = bytes.capacity();
        let length = bytes.len();
        Self {
            parts: vec![bytes.into()],
            length,
            retained_bytes,
        }
    }

    /// The caller owns backing-allocation accounting for shared `Bytes`.
    /// Repeated references may be conservatively charged more than once.
    pub fn shared(parts: Vec<Bytes>, retained_bytes: usize) -> io::Result<Self> {
        let length = parts
            .iter()
            .try_fold(0_usize, |sum, bytes| sum.checked_add(bytes.len()))
            .ok_or(io::ErrorKind::InvalidInput)?;
        if retained_bytes < length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write backing charge below payload length",
            ));
        }
        Ok(Self {
            parts,
            length,
            retained_bytes,
        })
    }

    pub fn parts(&self) -> &[Bytes] {
        &self.parts
    }
    pub const fn len(&self) -> usize {
        self.length
    }
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    fn charge(&self) -> Option<usize> {
        // Include bounded direct-I/O staging for interchangeable backends.
        self.parts
            .capacity()
            .checked_mul(size_of::<Bytes>())?
            .checked_add(self.retained_bytes)?
            .checked_add(self.length)?
            .checked_add(4096)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenMode {
    Read,
    ReadWrite,
    CreateNew,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    Data,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    File,
    Directory,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Metadata {
    pub kind: FileKind,
    pub length: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Entry {
    pub name: OsString,
    pub kind: FileKind,
}

/// No operation implies a journal commit. Short reads and writes are explicit;
/// errors may follow partial physical changes. There is no rollback promise.
#[derive(Debug)]
pub enum Operation {
    /// Keep locks or other opaque handle lifetimes until physical execution
    /// settles, even when both the observer and original owner are dropped.
    /// No nested wrappers; at most 64 handles from this backend. Explicit close requires
    /// settling dependent work first. This does not acquire an OS file lock.
    Protected {
        operation: Box<Operation>,
        handles: Vec<Handle>,
    },
    Open {
        path: PathBuf,
        mode: OpenMode,
        direct: bool,
        data_sync: bool,
    },
    OpenDirectory {
        path: PathBuf,
    },
    Read {
        handle: Handle,
        offset: u64,
        length: usize,
    },
    Write {
        handle: Handle,
        offset: u64,
        data: WriteBuffer,
    },
    Metadata {
        handle: Handle,
    },
    SetLength {
        handle: Handle,
        length: u64,
    },
    Allocate {
        handle: Handle,
        offset: u64,
        length: u64,
    },
    Sync {
        handle: Handle,
        mode: SyncMode,
    },
    LockExclusive {
        handle: Handle,
    },
    /// Invalidates every clone. Caller must first settle dependent operations.
    Close {
        handle: Handle,
    },
    /// Refuse oversized listings instead of allocating without a bound.
    ReadDirectory {
        path: PathBuf,
        max_entries: usize,
        max_name_bytes: usize,
    },
    CreateDirectory {
        path: PathBuf,
    },
    Rename {
        source: PathBuf,
        destination: PathBuf,
    },
    HardLink {
        source: PathBuf,
        destination: PathBuf,
    },
    RemoveFile {
        path: PathBuf,
    },
    RemoveDirectory {
        path: PathBuf,
    },
}

impl Operation {
    /// The physical operation, excluding additional lifetime protection.
    pub fn unprotected(&self) -> &Self {
        match self {
            Self::Protected { operation, .. } => operation,
            operation => operation,
        }
    }

    /// Retained payload, paths, result space and backend scratch. Fixed-size
    /// per-operation bookkeeping is bounded separately by operation count.
    pub fn retained_bytes(&self) -> io::Result<usize> {
        let bytes = match self {
            Self::Protected { operation, handles } => {
                if handles.len() > 64 || matches!(**operation, Self::Protected { .. }) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid file-operation protection",
                    ));
                }
                operation
                    .retained_bytes()?
                    .checked_add(size_of::<Self>())
                    .and_then(|bytes| {
                        handles
                            .capacity()
                            .checked_mul(size_of::<Handle>())
                            .and_then(|protection| bytes.checked_add(protection))
                    })
            }
            Self::Open { path, .. }
            | Self::OpenDirectory { path }
            | Self::CreateDirectory { path }
            | Self::RemoveFile { path }
            | Self::RemoveDirectory { path } => Some(path.capacity()),
            Self::Read { offset, length, .. } => {
                end_offset(*offset, *length as u64).map(|_| *length)
            }
            Self::Write { offset, data, .. } => {
                end_offset(*offset, data.len() as u64).and_then(|_| data.charge())
            }
            Self::ReadDirectory {
                path,
                max_entries,
                max_name_bytes,
            } => max_entries
                .checked_mul(size_of::<Entry>())
                .and_then(|bytes| bytes.checked_add(*max_name_bytes))
                .and_then(|bytes| bytes.checked_add(path.capacity())),
            Self::Rename {
                source,
                destination,
            }
            | Self::HardLink {
                source,
                destination,
            } => source.capacity().checked_add(destination.capacity()),
            Self::Allocate { offset, length, .. } => end_offset(*offset, *length).map(|_| 0),
            Self::SetLength { length, .. } => end_offset(0, *length).map(|_| 0),
            _ => Some(0),
        };
        bytes.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "file operation size overflow")
        })
    }
}

// File positions must fit the signed 64-bit syscall interface on every backend,
// including zero-length operations. The wire/storage counters remain unsigned.
fn end_offset(offset: u64, length: u64) -> Option<u64> {
    offset
        .checked_add(length)
        .filter(|end| i64::try_from(*end).is_ok())
}

#[derive(Debug)]
pub enum Outcome {
    Opened(Handle),
    Read(ReadBuffer),
    Written(usize),
    Metadata(Metadata),
    Directory(Vec<Entry>),
    Done,
}

/// Short reads retain their original admitted allocation without allocating
/// another buffer to shrink it. The completion holds its byte charge.
#[derive(Debug)]
pub struct ReadBuffer {
    bytes: Box<[u8]>,
    length: usize,
}

impl ReadBuffer {
    pub fn new(bytes: Box<[u8]>, length: usize) -> io::Result<Self> {
        if length > bytes.len() {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(Self { bytes, length })
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
}
