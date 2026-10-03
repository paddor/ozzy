//! Shared bounded byte and directory-persistence model.

use crate::directory::publication::{MetadataIo, SyncedOverwrite};
use crate::{DirectoryError, SegmentIo};
use std::collections::BTreeMap;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex};

const FILE_LIMIT: usize = 4096;
const BYTE_LIMIT: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub(crate) struct Inode {
    pub(crate) pending: Vec<u8>,
    pub(crate) stable: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Disk {
    pub(crate) names: BTreeMap<String, usize>,
    pub(crate) stable_names: BTreeMap<String, usize>,
    pub(crate) files: Vec<Inode>,
    pub(crate) trace: Vec<String>,
    pub(crate) fail_at: Option<usize>,
    pub(crate) max_write: usize,
    pub(crate) omit_directory_sync: bool,
    pub(crate) omit_in_place_sync: bool,
}

impl Disk {
    pub(crate) fn event(&mut self, operation: &str) -> io::Result<()> {
        assert!(self.trace.len() < 100_000);
        self.trace.push(operation.to_owned());
        if self.fail_at == Some(self.trace.len()) {
            Err(io::Error::other("injected storage completion failure"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn inode(&self, name: &str) -> io::Result<usize> {
        self.names
            .get(name)
            .copied()
            .ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    pub(crate) fn crash(&mut self) {
        self.names.clone_from(&self.stable_names);
        for inode in &mut self.files {
            inode.pending.clone_from(&inode.stable);
        }
        self.fail_at = None;
    }

    /// Model selected bytes reaching stable storage before a file barrier.
    /// Also preserves the length needed to expose them, but no directory entry.
    /// This is a crash-image choice, not the guarantee of a writeback syscall.
    pub(crate) fn persist_range(&mut self, name: &str, range: Range<usize>) -> io::Result<()> {
        let inode = self.inode(name)?;
        let file = &mut self.files[inode];
        if range.start > range.end || range.end > file.pending.len() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if !range.is_empty() {
            file.stable.resize(file.stable.len().max(range.end), 0);
            file.stable[range.clone()].copy_from_slice(&file.pending[range]);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Memory(pub(crate) Arc<Mutex<Disk>>);

impl Memory {
    pub(crate) fn sync_parent(&self, parent: &str) -> Result<(), DirectoryError> {
        let mut disk = self.0.lock().unwrap();
        disk.event("directory sync")?;
        if !disk.omit_directory_sync {
            let belongs =
                |name: &str| name.rsplit_once('/').map_or("", |(parent, _)| parent) == parent;
            let selected: Vec<_> = disk
                .names
                .iter()
                .filter(|(name, _)| belongs(name))
                .map(|(name, inode)| (name.clone(), *inode))
                .collect();
            disk.stable_names.retain(|name, _| !belongs(name));
            disk.stable_names.extend(selected);
        }
        Ok(())
    }

    pub(crate) fn inspect(&self, name: &str) -> io::Result<Vec<u8>> {
        let disk = self.0.lock().unwrap();
        Ok(disk.files[disk.inode(name)?].pending.clone())
    }

    pub(crate) fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        let mut disk = self.0.lock().unwrap();
        disk.event("read bounded image")?;
        Ok(disk.files[disk.inode(name)?].pending.clone())
    }
}

impl MetadataIo for Memory {
    fn exists(&mut self, name: &str) -> Result<bool, DirectoryError> {
        let mut disk = self.0.lock().unwrap();
        disk.event("exists")?;
        Ok(disk.names.contains_key(name))
    }

    fn read_exact(&mut self, name: &str, bytes: usize) -> Result<Vec<u8>, DirectoryError> {
        let mut disk = self.0.lock().unwrap();
        disk.event("read")?;
        let inode = disk.inode(name)?;
        let value = &disk.files[inode].pending;
        if value.len() != bytes {
            return Err(DirectoryError::WrongFileSize {
                object: "simulated file",
                actual: value.len() as u64,
                expected: bytes as u64,
            });
        }
        Ok(value.clone())
    }

    fn write_new(&mut self, name: &str, bytes: &[u8]) -> Result<(), DirectoryError> {
        let mut disk = self.0.lock().unwrap();
        disk.event("create/write")?;
        if disk.names.contains_key(name) {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists).into());
        }
        assert!(disk.files.len() < FILE_LIMIT && bytes.len() <= BYTE_LIMIT);
        let id = disk.files.len();
        disk.files.push(Inode {
            pending: bytes.to_vec(),
            stable: Vec::new(),
        });
        disk.names.insert(name.to_owned(), id);
        Ok(())
    }

    fn sync_file(&mut self, name: &str) -> Result<(), DirectoryError> {
        let mut disk = self.0.lock().unwrap();
        disk.event("file sync")?;
        let inode = disk.inode(name)?;
        let value = &mut disk.files[inode];
        value.stable.clone_from(&value.pending);
        Ok(())
    }

    fn link(&mut self, source: &str, target: &str) -> Result<(), DirectoryError> {
        let mut disk = self.0.lock().unwrap();
        disk.event("link")?;
        let inode = disk.inode(source)?;
        if disk.names.contains_key(target) {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists).into());
        }
        disk.names.insert(target.to_owned(), inode);
        Ok(())
    }

    fn remove(&mut self, name: &str) -> Result<(), DirectoryError> {
        let mut disk = self.0.lock().unwrap();
        disk.event("remove")?;
        disk.names
            .remove(name)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        Ok(())
    }

    fn rename(&mut self, source: &str, target: &str) -> Result<(), DirectoryError> {
        let mut disk = self.0.lock().unwrap();
        disk.event("rename")?;
        let inode = disk
            .names
            .remove(source)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        disk.names.insert(target.to_owned(), inode);
        Ok(())
    }

    fn sync_directory(&mut self) -> Result<(), DirectoryError> {
        self.sync_parent("")
    }
}

impl SyncedOverwrite for Memory {
    /// Chunks follow `max_write`; a failure cut can leave any prefix of the
    /// new bytes pending. Only the final barrier makes the file stable.
    fn write_synced(
        &mut self,
        name: &str,
        offsets: &[usize],
        bytes: &[u8],
    ) -> Result<(), DirectoryError> {
        for offset in offsets {
            let mut written = 0;
            while written < bytes.len() {
                let mut disk = self.0.lock().unwrap();
                disk.event("in-place write")?;
                let count = (bytes.len() - written).min(disk.max_write.max(1));
                let index = disk.inode(name)?;
                let inode = &mut disk.files[index];
                let start = offset + written;
                if start + count > inode.pending.len() {
                    return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
                }
                inode.pending[start..start + count].copy_from_slice(&bytes[written..][..count]);
                written += count;
            }
        }
        if self.0.lock().unwrap().omit_in_place_sync {
            return self
                .0
                .lock()
                .unwrap()
                .event("omitted in-place barrier")
                .map_err(Into::into);
        }
        self.sync_file(name)
    }
}

#[derive(Debug)]
pub(crate) struct Segment(pub(crate) Memory, pub(crate) String);

impl SegmentIo for Segment {
    fn file_len(&mut self) -> io::Result<u64> {
        let disk = self.0.0.lock().unwrap();
        Ok(disk.files[disk.inode(&self.1)?].pending.len() as u64)
    }

    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        let mut disk = self.0.0.lock().unwrap();
        disk.event("segment read")?;
        Ok(disk.files[disk.inode(&self.1)?].pending.clone())
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        let mut disk = self.0.0.lock().unwrap();
        disk.event("segment write")?;
        let count = bytes.len().min(disk.max_write.max(1));
        let offset = usize::try_from(offset).unwrap();
        assert!(offset + count <= BYTE_LIMIT);
        let index = disk.inode(&self.1)?;
        let inode = &mut disk.files[index];
        inode
            .pending
            .resize(inode.pending.len().max(offset + count), 0);
        inode.pending[offset..offset + count].copy_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn set_len(&mut self, length: u64) -> io::Result<()> {
        let mut disk = self.0.0.lock().unwrap();
        disk.event("segment truncate")?;
        let length = usize::try_from(length).unwrap();
        assert!(length <= BYTE_LIMIT);
        let index = disk.inode(&self.1)?;
        disk.files[index].pending.resize(length, 0);
        Ok(())
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.0
            .sync_file(&self.1)
            .map_err(|error| io::Error::other(error.to_string()))
    }
}
