//! The spare: one segment file per partition, kept for the next new segment.
//! A retired one keeps its written blocks, and appends into them skip the
//! metadata commit that the first write into an unwritten extent costs.
//! Preparation allocates the spare ahead, so a roll needs no new space.

use super::Journal;
use crate::async_files::Access;
use crate::directory::{DirectoryError, segment_name, spare_segment_name};
use ozzy_io::{Handle, OpenMode, Operation};
use std::{
    io,
    path::{Path, PathBuf},
};

/// Captured spare preparation; it borrows no journal, so it runs beside
/// writes. It must not overlap a roll, an installation, or cleanup.
#[derive(Debug)]
pub struct SparePreparation {
    access: Access,
    root: PathBuf,
    capacity: u64,
}

impl SparePreparation {
    /// Make sure the spare exists and has the active segment's capacity
    /// allocated. A spare this call created and could not allocate is removed
    /// again; an existing one stays. `StorageFull` means the device lacks room
    /// for another segment.
    pub async fn run(self) -> Result<(), DirectoryError> {
        let path = self.root.join(spare_segment_name());
        let created = self
            .access
            .open(path.clone(), OpenMode::CreateNew, false, false)
            .await;
        let (file, created) = match created {
            Ok(file) => (file, true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (
                self.access
                    .open(path.clone(), OpenMode::ReadWrite, false, false)
                    .await?,
                false,
            ),
            Err(error) => return Err(error.into()),
        };
        let allocated = async {
            self.access
                .done(Operation::Allocate {
                    handle: file.clone(),
                    offset: 0,
                    length: self.capacity,
                })
                .await?;
            self.access.sync(&file).await
        }
        .await;
        self.access.done(Operation::Close { handle: file }).await?;
        if let Err(error) = allocated {
            if created {
                self.access.done(Operation::RemoveFile { path }).await?;
            }
            return Err(error.into());
        }
        if created {
            self.access
                .sync_directory(self.root.join("segments"))
                .await?;
        }
        Ok(())
    }
}

/// Create segment file `id` under `root`, refusing an existing one, move the
/// spare over it when there is one, and reserve `capacity` bytes. The spare's
/// old bytes stay unused capacity: groups of an earlier use never validate
/// against the new header.
pub(super) async fn create_segment_file(
    access: &Access,
    root: &Path,
    id: u64,
    capacity: u64,
) -> io::Result<Handle> {
    let path = root.join(segment_name(id));
    let claimed = access
        .open(path.clone(), OpenMode::CreateNew, false, false)
        .await?;
    let moved = access
        .done(Operation::Rename {
            source: root.join(spare_segment_name()),
            destination: path.clone(),
        })
        .await;
    let file = match moved {
        Err(error) if error.kind() == io::ErrorKind::NotFound => claimed,
        moved => {
            moved?;
            access.done(Operation::Close { handle: claimed }).await?;
            let file = access.open(path, OpenMode::ReadWrite, false, false).await?;
            if access.length(&file).await? > capacity {
                let handle = file.clone();
                let length = capacity;
                access.done(Operation::SetLength { handle, length }).await?;
            }
            file
        }
    };
    access
        .done(Operation::Allocate {
            handle: file.clone(),
            offset: 0,
            length: capacity,
        })
        .await?;
    Ok(file)
}

impl Journal {
    /// Capture preparation of a spare with the active segment's capacity.
    pub fn prepare_spare(&self) -> Result<SparePreparation, DirectoryError> {
        self.healthy()?;
        Ok(SparePreparation {
            access: self.access.clone(),
            root: self.root().to_path_buf(),
            capacity: self.writer.header().capacity(),
        })
    }

    /// Write a spare of up to `bytes` zeros, at most the active segment's
    /// capacity, so the next new segment starts on written blocks. For a new
    /// replicated store, whose first election installs its log into a new
    /// segment.
    pub async fn zero_spare(&mut self, bytes: u64) -> Result<(), DirectoryError> {
        self.healthy()?;
        let length = self.writer.header().capacity().min(bytes);
        let file = self
            .access
            .open(
                self.root().join(spare_segment_name()),
                OpenMode::CreateNew,
                false,
                false,
            )
            .await?;
        self.access
            .zero(&file, 0..length, self.limits.io.chunk_bytes)
            .await?;
        self.access.sync(&file).await?;
        self.access.done(Operation::Close { handle: file }).await?;
        self.access
            .sync_directory(self.root().join("segments"))
            .await?;
        Ok(())
    }
}
