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
    /// Own one buffer and charge its full allocated capacity.
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

    /// Ordered payload views for scatter/gather writes.
    pub fn parts(&self) -> &[Bytes] {
        &self.parts
    }
    /// Total visible bytes across all payload parts.
    pub const fn len(&self) -> usize {
        self.length
    }
    /// Whether the write contains no visible payload bytes.
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

/// Access and creation policy for a file handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenMode {
    /// Open an existing file for reads.
    Read,
    /// Open an existing file for reads and writes.
    ReadWrite,
    /// Create a new file, refusing an existing path.
    CreateNew,
}

/// Physical barrier scope; neither form establishes journal authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    /// Persist file data and metadata needed to retrieve it.
    Data,
    /// Persist file data and all file metadata.
    All,
}

/// Backend-neutral directory entry classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    /// Regular file.
    File,
    /// Directory.
    Directory,
    /// Entry with another filesystem type.
    Other,
}

/// File type and current logical byte length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Metadata {
    /// Entry type reported by the backend.
    pub kind: FileKind,
    /// Logical length in bytes.
    pub length: u64,
}

/// One owned directory name and its backend-reported type.
#[derive(Debug, PartialEq, Eq)]
pub struct Entry {
    /// Name relative to the listed directory.
    pub name: OsString,
    /// Entry type reported by the backend.
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
        /// Physical operation whose additional handle lifetimes are protected.
        operation: Box<Operation>,
        /// Handles retained until physical execution settles.
        handles: Vec<Handle>,
    },
    /// Open a file under explicit access, direct-I/O, and write-sync policies.
    Open {
        /// Backend path to the target entry.
        path: PathBuf,
        /// Selected access or barrier policy.
        mode: OpenMode,
        /// Request direct I/O, subject to backend alignment checks.
        direct: bool,
        /// Request synchronous file-data writes when opening the file.
        data_sync: bool,
    },
    /// Open a directory handle for metadata barriers.
    OpenDirectory {
        /// Backend path to the target entry.
        path: PathBuf,
    },
    /// Read a bounded range; a short result is allowed.
    Read {
        /// Backend-owned target handle.
        handle: Handle,
        /// Starting byte offset in the file.
        offset: u64,
        /// Requested byte count or logical file length.
        length: usize,
    },
    /// Write owned scatter/gather bytes at an explicit file offset.
    Write {
        /// Backend-owned target handle.
        handle: Handle,
        /// Starting byte offset in the file.
        offset: u64,
        /// Owned payload and full backing-allocation charge.
        data: WriteBuffer,
    },
    /// Inspect an open handle without reopening its path.
    Metadata {
        /// Backend-owned target handle.
        handle: Handle,
    },
    /// Set logical file length; shrinking discards the suffix.
    SetLength {
        /// Backend-owned target handle.
        handle: Handle,
        /// Requested byte count or logical file length.
        length: u64,
    },
    /// Reserve physical space for a file range.
    Allocate {
        /// Backend-owned target handle.
        handle: Handle,
        /// Starting byte offset in the file.
        offset: u64,
        /// Requested byte count or logical file length.
        length: u64,
    },
    /// Run the selected physical barrier on an open handle.
    Sync {
        /// Backend-owned target handle.
        handle: Handle,
        /// Selected access or barrier policy.
        mode: SyncMode,
    },
    /// Acquire the backend-supported exclusive file lock.
    LockExclusive {
        /// Backend-owned target handle.
        handle: Handle,
    },
    /// Invalidates every clone. Caller must first settle dependent operations.
    Close {
        /// Backend-owned target handle.
        handle: Handle,
    },
    /// Refuse oversized listings instead of allocating without a bound.
    ReadDirectory {
        /// Backend path to the target entry.
        path: PathBuf,
        /// Maximum entries retained by this listing.
        max_entries: usize,
        /// Maximum aggregate name storage retained by this listing.
        max_name_bytes: usize,
    },
    /// Create one directory, refusing an existing entry.
    CreateDirectory {
        /// Backend path to the target entry.
        path: PathBuf,
    },
    /// Rename an entry; directory durability requires a separate barrier.
    Rename {
        /// Existing entry path.
        source: PathBuf,
        /// New entry path.
        destination: PathBuf,
    },
    /// Create another name for an existing file.
    HardLink {
        /// Existing entry path.
        source: PathBuf,
        /// New entry path.
        destination: PathBuf,
    },
    /// Remove a file name; existing handles retain their file lifetime.
    RemoveFile {
        /// Backend path to the target entry.
        path: PathBuf,
    },
    /// Remove an empty directory.
    RemoveDirectory {
        /// Backend path to the target entry.
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

/// Physical operation result; short transfers remain explicit.
#[derive(Debug)]
pub enum Outcome {
    /// New backend-owned file or directory handle.
    Opened(Handle),
    /// Read bytes, which may be shorter than requested.
    Read(ReadBuffer),
    /// Number of bytes physically written; callers must check for short writes.
    Written(usize),
    /// Observed handle type and logical length.
    Metadata(Metadata),
    /// Owned entries within the requested listing bounds.
    Directory(Vec<Entry>),
    /// Successful operation with no additional result data.
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
    /// Retain the admitted allocation and expose only its completed prefix.
    pub fn new(bytes: Box<[u8]>, length: usize) -> io::Result<Self> {
        if length > bytes.len() {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(Self { bytes, length })
    }

    /// Completed read bytes, excluding unused admitted capacity.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
}
