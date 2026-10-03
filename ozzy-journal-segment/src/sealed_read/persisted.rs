//! Load derived indexes; rebuild only from an exact pinned and validated source.

use std::fs::{self, File};
use std::io::{self, Read};

use crate::index_builder::IndexPublication;
use crate::{
    DirectoryError, JournalIndexError, SegmentIndex, TailState, open_segment_index, scan_segment,
    segment_index_name,
};

use super::SealedReadScope;

pub(super) fn open_or_rebuild(
    scope: &SealedReadScope,
    position: usize,
) -> Result<SegmentIndex, JournalIndexError> {
    let source = scope.pin.sources()[position];
    let path = scope
        .pin
        .root()
        .join("indexes")
        .join(segment_index_name(source));
    // Published files are immutable. The normal open needs no builder lock.
    if let Ok(index) = open_segment_index(&path, source, scope.index_limits.file) {
        return Ok(index);
    }
    let _guard = scope
        .publication_lock
        .lock()
        .map_err(|_| io::Error::other("index publication poisoned"))?;
    // An owner refresh or another reader may have repaired it while we waited.
    // Reopen under the shared guard before considering any derived-file removal.
    let mut publication = IndexPublication::new(
        scope.pin.root().join("indexes"),
        scope.pin.root().join("staging"),
    );
    let index = match publication
        .reuse(source, scope.index_limits.file)
        .map_err(DirectoryError::from)?
    {
        Some(index) => index,
        None => rebuild(scope, position, &mut publication)?,
    };
    publication
        .finish(|path| Ok(File::open(path)?.sync_all()?))
        .map_err(DirectoryError::from)?;
    Ok(index)
}

fn rebuild(
    scope: &SealedReadScope,
    position: usize,
    publication: &mut IndexPublication,
) -> Result<SegmentIndex, JournalIndexError> {
    let source = scope.pin.sources()[position];
    let reference = &scope.pin.references()[position];
    let mismatch = || JournalIndexError::LineageSegmentMismatch(source.segment_id);
    let path = scope
        .pin
        .segment_path(source.segment_id)
        .ok_or_else(mismatch)?;
    if !fs::symlink_metadata(&path)?.file_type().is_file() {
        return Err(mismatch());
    }
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file()
        || metadata.len() < source.valid_bytes
        || metadata.len() > reference.capacity
    {
        return Err(mismatch());
    }
    let length = usize::try_from(source.valid_bytes).map_err(|_| mismatch())?;
    if source.valid_bytes > reference.capacity {
        return Err(mismatch());
    }
    // Only an exceptional rebuild reads the whole sealed prefix. This working
    // image is released after publication, not retained as a per-store arena.
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(io::Error::other)?;
    bytes.resize(length, 0);
    file.read_exact(&mut bytes)?;
    let scan = scan_segment(
        &bytes,
        reference.first_group_number,
        reference.first_chain,
        scope.decode,
    )?;
    if scan.header.group_id() != source.group_id
        || scan.header.segment_id() != source.segment_id
        || scan.header.capacity() != reference.capacity
        || scan.valid_bytes != source.valid_bytes
        || scan.digest != source.segment_digest
        || scan.next_chain.next_op_number().checked_sub(1) != Some(source.last_op_number)
        || scan.next_chain.previous_digest() != source.last_operation_digest
        || scan.tail != TailState::Clean
        || scan
            .groups
            .iter()
            .flat_map(|group| &group.operations)
            .any(|operation| {
                operation.configuration_epoch != scope.configuration_epoch
                    || operation.original_view > scope.promised_view
            })
    {
        return Err(mismatch());
    }
    Ok(publication
        .build(&scan, source, scope.operations, scope.index_limits)
        .map_err(DirectoryError::from)?)
}
