use super::Journal;
use crate::{
    ActiveSegmentIndex, DirectoryError, IndexBuildLimits, IndexLimits, JournalIndexBoundary,
    JournalIndexError, index_catalog::asynchronous::Catalog, journal_index::asynchronous::Snapshot,
};

impl Journal {
    /// Capture a deletion-protected read view using already-published indexes.
    /// The active index and immutable file-image cache have explicit bounds.
    pub async fn open_index_snapshot(
        &self,
        boundary: JournalIndexBoundary,
        limits: IndexLimits,
    ) -> Result<Snapshot, JournalIndexError> {
        self.healthy()?;
        let sources = self
            .manifest
            .segments
            .windows(2)
            .map(|pair| {
                crate::directory::sealed_source(self.manifest.identity.group_id, &pair[0], &pair[1])
            })
            .collect::<Result<Vec<_>, _>>()?;
        let sealed = Catalog::open(
            self.access.clone(),
            self.root().join("indexes"),
            sources,
            limits,
            self.limits.io.chunk_bytes,
        )
        .await?;
        let through = match boundary {
            JournalIndexBoundary::Written => self.written_position()?,
            JournalIndexBoundary::Accepted => self.accepted_position()?,
            JournalIndexBoundary::Committed => self.committed_position()?,
        };
        let reference = self
            .manifest
            .segments
            .last()
            .expect("validated manifest has active segment");
        let active_bytes = self.writer.written_position().end_offset();
        let active_digest = self.writer.state().structural_digest();
        let groups = self.segment_groups(*reference).await?;
        let active = ActiveSegmentIndex::build_groups(
            groups,
            through.op_number,
            self.limits.operations,
            limits,
        )
        .await?;
        let leases = self
            .manifest
            .segments
            .iter()
            .map(|r| self.pins.protect_prepared_segment(r.segment_id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(DirectoryError::from)?;
        Ok(Snapshot {
            access: self.access.clone(),
            root: self.root().join("segments"),
            references: self.manifest.segments.clone(),
            identity: self.manifest.identity,
            sealed,
            active: active.map(std::rc::Rc::new),
            active_bytes,
            active_digest,
            through,
            decode: self.limits.decode,
            operations: self.limits.operations,
            chunk: self.limits.io.chunk_bytes,
            _leases: leases.into(),
        })
    }

    /// Build missing indexes without silently repairing corrupt derived files,
    /// then capture the exact requested prefix. No filesystem calls run here.
    pub async fn build_index_snapshot(
        &mut self,
        boundary: JournalIndexBoundary,
        limits: IndexBuildLimits,
    ) -> Result<Snapshot, JournalIndexError> {
        self.healthy()?;
        self.validated_indexes.retain(|id, _| {
            self.manifest
                .segments
                .binary_search_by_key(id, |reference| reference.segment_id)
                .is_ok()
        });
        for at in 0..self.manifest.segments.len() - 1 {
            let id = self.manifest.segments[at].segment_id;
            let (_, source) = self.index_source(id)?;
            if self.validated_indexes.get(&id) == Some(&source) {
                // This owner already validated the immutable physical source.
                // Recheck the published index's checksum and complete source
                // binding; explicit repair/scrub still validate physical bytes.
                match self.open_sealed_index(id, limits.file).await {
                    Ok(_) => continue,
                    Err(DirectoryError::Index(crate::IndexBuildError::Io(error)))
                        if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            self.validated_indexes.remove(&id);
            self.build_sealed_index(id, limits).await?;
            self.validated_indexes.insert(id, source);
        }
        self.open_index_snapshot(boundary, limits.file).await
    }

    pub(super) async fn build_recovery_index_snapshots(
        &mut self,
        limits: IndexBuildLimits,
    ) -> Result<(Snapshot, Snapshot), JournalIndexError> {
        let accepted = self
            .build_index_snapshot(JournalIndexBoundary::Accepted, limits)
            .await?;
        let mut committed = accepted.clone();
        // Both positions belong to the same exclusively borrowed journal.
        // Every lookup filters its result through this exact logical boundary.
        committed.through = self.committed_position()?;
        Ok((committed, accepted))
    }
}
