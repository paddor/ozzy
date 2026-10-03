use crate::{Entry, FileKind};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::io;
use std::ops::Range;
use std::path::{Component, Path};

type Inode = u64;

/// Bounds the simulated media image separately from admitted I/O buffers.
/// Totals include both dirty and durable copies. Temporary copies are bounded
/// by one file/directory operation; no unbounded trace or orphan list is kept.
#[derive(Clone, Copy, Debug)]
pub struct ImageLimits {
    /// Maximum file and directory inodes in the simulated image.
    pub nodes: usize,
    /// Maximum aggregate dirty and durable directory entries.
    pub directory_entries: usize,
    /// Maximum logical bytes in any one file image.
    pub file_bytes: usize,
    /// Maximum aggregate dirty and durable file storage.
    pub total_bytes: usize,
}

impl ImageLimits {
    pub(super) fn validate(self) -> io::Result<()> {
        if self.nodes == 0
            || self.directory_entries == 0
            || self.file_bytes == 0
            || self.total_bytes < self.file_bytes
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
enum Contents {
    File(Box<[u8]>),
    Directory(BTreeMap<OsString, Inode>),
}

impl Contents {
    fn kind(&self) -> FileKind {
        match self {
            Self::File(_) => FileKind::File,
            Self::Directory(_) => FileKind::Directory,
        }
    }
    fn bytes(&self) -> usize {
        match self {
            Self::File(bytes) => bytes.len(),
            Self::Directory(_) => 0,
        }
    }
    fn entries(&self) -> usize {
        match self {
            Self::Directory(entries) => entries.len(),
            Self::File(_) => 0,
        }
    }
}

#[derive(Clone, Debug)]
struct Node {
    dirty: Contents,
    durable: Contents,
}

/// Byte and namespace state shared across simulated process incarnations.
/// Directory entries reference inodes, so open handles survive rename/unlink
/// and hard links share bytes. All model paths are absolute and reject `..`.
#[derive(Clone, Debug)]
pub struct Image {
    nodes: BTreeMap<Inode, Node>,
    next_inode: Inode,
    pub(super) next_boot: u64,
}

impl Default for Image {
    fn default() -> Self {
        let root = Contents::Directory(BTreeMap::new());
        Self {
            nodes: BTreeMap::from([(
                0,
                Node {
                    dirty: root.clone(),
                    durable: root,
                },
            )]),
            next_inode: 1,
            next_boot: 0,
        }
    }
}

impl Image {
    /// Borrow file bytes from either the dirty image or durable media.
    pub fn bytes(&self, path: &Path, durable: bool) -> io::Result<&[u8]> {
        self.file(self.resolve(path, durable)?, durable)
    }

    /// Test namespace reachability in the selected dirty or durable image.
    pub fn exists(&self, path: &Path, durable: bool) -> bool {
        self.resolve(path, durable).is_ok()
    }

    pub(super) fn validate(&self, limits: ImageLimits) -> io::Result<()> {
        let mut bytes = 0_usize;
        let mut entries = 0_usize;
        for node in self.nodes.values() {
            for value in [&node.dirty, &node.durable] {
                if value.bytes() > limits.file_bytes {
                    return Err(io::ErrorKind::StorageFull.into());
                }
                bytes = bytes
                    .checked_add(value.bytes())
                    .ok_or(io::ErrorKind::StorageFull)?;
                entries = entries
                    .checked_add(value.entries())
                    .ok_or(io::ErrorKind::StorageFull)?;
            }
        }
        if self.nodes.len() > limits.nodes
            || bytes > limits.total_bytes
            || entries > limits.directory_entries
        {
            return Err(io::ErrorKind::StorageFull.into());
        }
        Ok(())
    }

    fn byte_capacity(&self, removed: usize, added: usize, limits: ImageLimits) -> io::Result<()> {
        let total: usize = self
            .nodes
            .values()
            .map(|n| n.dirty.bytes() + n.durable.bytes())
            .sum();
        if added > limits.file_bytes
            || total
                .checked_sub(removed)
                .and_then(|n| n.checked_add(added))
                .is_none_or(|n| n > limits.total_bytes)
        {
            return Err(io::ErrorKind::StorageFull.into());
        }
        Ok(())
    }

    fn entry_capacity(&self, removed: usize, added: usize, limits: ImageLimits) -> io::Result<()> {
        let total: usize = self
            .nodes
            .values()
            .map(|n| n.dirty.entries() + n.durable.entries())
            .sum();
        if total
            .checked_sub(removed)
            .and_then(|n| n.checked_add(added))
            .is_none_or(|n| n > limits.directory_entries)
        {
            return Err(io::ErrorKind::StorageFull.into());
        }
        Ok(())
    }

    fn contents(&self, inode: Inode, durable: bool) -> io::Result<&Contents> {
        let node = self.nodes.get(&inode).ok_or(io::ErrorKind::NotFound)?;
        Ok(if durable { &node.durable } else { &node.dirty })
    }

    pub(super) fn kind(&self, inode: Inode) -> io::Result<FileKind> {
        Ok(self.contents(inode, false)?.kind())
    }

    pub(super) fn file(&self, inode: Inode, durable: bool) -> io::Result<&[u8]> {
        match self.contents(inode, durable)? {
            Contents::File(bytes) => Ok(bytes),
            Contents::Directory(_) => Err(io::ErrorKind::IsADirectory.into()),
        }
    }

    fn directory(&self, inode: Inode, durable: bool) -> io::Result<&BTreeMap<OsString, Inode>> {
        match self.contents(inode, durable)? {
            Contents::Directory(entries) => Ok(entries),
            Contents::File(_) => Err(io::ErrorKind::NotADirectory.into()),
        }
    }

    fn directory_mut(&mut self, inode: Inode) -> &mut BTreeMap<OsString, Inode> {
        let Contents::Directory(entries) = &mut self
            .nodes
            .get_mut(&inode)
            .expect("resolved directory")
            .dirty
        else {
            unreachable!()
        };
        entries
    }

    pub(super) fn resolve(&self, path: &Path, durable: bool) -> io::Result<Inode> {
        Ok(*self.walk(path, durable)?.last().expect("root inode"))
    }

    fn walk(&self, path: &Path, durable: bool) -> io::Result<Vec<Inode>> {
        if !path.is_absolute() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut result = vec![0];
        let mut inode = 0;
        for component in path.components() {
            match component {
                Component::RootDir | Component::CurDir => {}
                Component::Normal(name) if name.len() <= 255 => {
                    inode = *self
                        .directory(inode, durable)?
                        .get(name)
                        .ok_or(io::ErrorKind::NotFound)?;
                    result.push(inode);
                }
                _ => return Err(io::ErrorKind::InvalidInput.into()),
            }
        }
        Ok(result)
    }

    fn parent<'a>(&self, path: &'a Path) -> io::Result<(Inode, &'a OsStr)> {
        let name = path
            .file_name()
            .filter(|name| name.len() <= 255)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let parent = self.resolve(path.parent().ok_or(io::ErrorKind::InvalidInput)?, false)?;
        self.directory(parent, false)?;
        Ok((parent, name))
    }

    pub(super) fn create(
        &mut self,
        path: &Path,
        directory: bool,
        limits: ImageLimits,
    ) -> io::Result<Inode> {
        let (parent, name) = self.parent(path)?;
        if self.directory(parent, false)?.contains_key(name) {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        if self.nodes.len() == limits.nodes {
            return Err(io::ErrorKind::StorageFull.into());
        }
        self.entry_capacity(0, 1, limits)?;
        let inode = self.next_inode;
        self.next_inode = inode.checked_add(1).ok_or(io::ErrorKind::StorageFull)?;
        let empty = if directory {
            Contents::Directory(BTreeMap::new())
        } else {
            Contents::File(Box::default())
        };
        self.nodes.insert(
            inode,
            Node {
                dirty: empty.clone(),
                durable: empty,
            },
        );
        self.directory_mut(parent).insert(name.into(), inode);
        Ok(inode)
    }

    pub(super) fn set_length(
        &mut self,
        inode: Inode,
        length: usize,
        durable: bool,
        limits: ImageLimits,
    ) -> io::Result<()> {
        let old = self.file(inode, durable)?;
        self.byte_capacity(old.len(), length, limits)?;
        let mut resized = vec![0; length].into_boxed_slice();
        let keep = old.len().min(length);
        resized[..keep].copy_from_slice(&old[..keep]);
        let node = self.nodes.get_mut(&inode).expect("existing file");
        *if durable {
            &mut node.durable
        } else {
            &mut node.dirty
        } = Contents::File(resized);
        Ok(())
    }

    pub(super) fn write(
        &mut self,
        inode: Inode,
        offset: usize,
        bytes: &[u8],
        limits: ImageLimits,
    ) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let end = offset
            .checked_add(bytes.len())
            .ok_or(io::ErrorKind::InvalidInput)?;
        let old = self.file(inode, false)?.len();
        if end > old {
            self.set_length(inode, end, false, limits)?;
        }
        let Contents::File(file) = &mut self.nodes.get_mut(&inode).expect("existing file").dirty
        else {
            unreachable!()
        };
        file[offset..end].copy_from_slice(bytes);
        Ok(())
    }

    pub(super) fn sync(&mut self, inode: Inode, limits: ImageLimits) -> io::Result<()> {
        let node = self.nodes.get(&inode).ok_or(io::ErrorKind::NotFound)?;
        match &node.dirty {
            Contents::File(bytes) => {
                self.byte_capacity(node.durable.bytes(), bytes.len(), limits)?;
            }
            Contents::Directory(entries) => {
                self.entry_capacity(node.durable.entries(), entries.len(), limits)?;
            }
        }
        let node = self.nodes.get_mut(&inode).expect("existing node");
        node.durable.clone_from(&node.dirty);
        Ok(())
    }

    pub(super) fn list(
        &self,
        path: &Path,
        max_entries: usize,
        max_bytes: usize,
    ) -> io::Result<Vec<Entry>> {
        let entries = self.directory(self.resolve(path, false)?, false)?;
        if entries.len() > max_entries
            || entries.keys().map(|name| name.len()).sum::<usize>() > max_bytes
        {
            return Err(io::ErrorKind::InvalidData.into());
        }
        entries
            .iter()
            .map(|(name, inode)| {
                Ok(Entry {
                    name: name.clone(),
                    kind: self.kind(*inode)?,
                })
            })
            .collect()
    }

    pub(super) fn link(
        &mut self,
        source: &Path,
        destination: &Path,
        limits: ImageLimits,
    ) -> io::Result<()> {
        let inode = self.resolve(source, false)?;
        if self.kind(inode)? != FileKind::File {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        let (parent, name) = self.parent(destination)?;
        if self.directory(parent, false)?.contains_key(name) {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        self.entry_capacity(0, 1, limits)?;
        self.directory_mut(parent).insert(name.into(), inode);
        Ok(())
    }

    pub(super) fn remove(&mut self, path: &Path, directory: bool) -> io::Result<()> {
        let inode = self.resolve(path, false)?;
        if directory {
            if !self.directory(inode, false)?.is_empty() {
                return Err(io::ErrorKind::DirectoryNotEmpty.into());
            }
        } else {
            self.file(inode, false)?;
        }
        let (parent, name) = self.parent(path)?;
        self.directory_mut(parent).remove(name);
        Ok(())
    }

    pub(super) fn rename(&mut self, source: &Path, destination: &Path) -> io::Result<()> {
        let inode = self.resolve(source, false)?;
        let (old_parent, old_name) = self.parent(source)?;
        let (new_parent, new_name) = self.parent(destination)?;
        if let Some(&target) = self.directory(new_parent, false)?.get(new_name) {
            if target == inode {
                return Ok(());
            }
            if self.kind(target)? != self.kind(inode)? {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            if self.kind(target)? == FileKind::Directory
                && !self.directory(target, false)?.is_empty()
            {
                return Err(io::ErrorKind::DirectoryNotEmpty.into());
            }
        }
        if self
            .walk(
                destination.parent().ok_or(io::ErrorKind::InvalidInput)?,
                false,
            )?
            .contains(&inode)
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.directory_mut(old_parent).remove(old_name);
        self.directory_mut(new_parent)
            .insert(new_name.into(), inode);
        Ok(())
    }

    pub(super) fn check_range(
        &self,
        path: &Path,
        range: &Range<usize>,
        limits: ImageLimits,
    ) -> io::Result<()> {
        let inode = self.resolve(path, false)?;
        let old = self.file(inode, true)?.len();
        if range.start > range.end || range.end > self.file(inode, false)?.len() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.byte_capacity(old, old.max(range.end), limits)
    }

    pub(super) fn persist_range(
        &mut self,
        path: &Path,
        range: Range<usize>,
        limits: ImageLimits,
    ) -> io::Result<()> {
        self.check_range(path, &range, limits)?;
        self.persist_inode_range(self.resolve(path, false)?, range, limits)
    }

    pub(super) fn persist_inode_range(
        &mut self,
        inode: Inode,
        range: Range<usize>,
        limits: ImageLimits,
    ) -> io::Result<()> {
        if range.is_empty() {
            return Ok(());
        }
        let old = self.file(inode, true)?.len();
        if range.end > old {
            self.set_length(inode, range.end, true, limits)?;
        }
        let Node {
            dirty: Contents::File(dirty),
            durable: Contents::File(durable),
        } = self.nodes.get_mut(&inode).expect("existing file")
        else {
            unreachable!()
        };
        durable[range.clone()].copy_from_slice(&dirty[range]);
        Ok(())
    }

    pub(super) fn check_length(&self, path: &Path, limits: ImageLimits) -> io::Result<()> {
        let inode = self.resolve(path, false)?;
        self.byte_capacity(
            self.file(inode, true)?.len(),
            self.file(inode, false)?.len(),
            limits,
        )
    }

    pub(super) fn persist_length(&mut self, path: &Path, limits: ImageLimits) -> io::Result<()> {
        let inode = self.resolve(path, false)?;
        self.set_length(inode, self.file(inode, false)?.len(), true, limits)
    }

    pub(super) fn power_loss(&mut self) {
        for node in self.nodes.values_mut() {
            node.dirty.clone_from(&node.durable);
        }
        self.reclaim(std::iter::empty());
    }

    pub(super) fn reclaim(&mut self, handles: impl Iterator<Item = Inode>) {
        let mut pending: Vec<_> = std::iter::once(0).chain(handles).collect();
        let mut reachable = BTreeSet::new();
        while let Some(inode) = pending.pop() {
            if !reachable.insert(inode) {
                continue;
            }
            if let Some(node) = self.nodes.get(&inode) {
                for contents in [&node.dirty, &node.durable] {
                    if let Contents::Directory(entries) = contents {
                        pending.extend(entries.values().copied());
                    }
                }
            }
        }
        self.nodes.retain(|inode, _| reachable.contains(inode));
    }
}
