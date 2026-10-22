use super::{DirectoryError, Journal};
use crate::directory::segment_name;
use crate::{
    ChainPosition, CodecError, Digest, LogPosition, Manifest, SEGMENT_HEADER_BYTES, SealedSegment,
    SegmentHeader, SegmentReference, SegmentStart, SegmentWriter, WriterError,
    encode_manifest_with_limits,
};
use ozzy_journal::progress::JournalGeneration;
use ozzy_proto::GroupId;
use std::io;

#[derive(Debug, Clone, Copy)]
pub(super) struct RollBoundary {
    pub(super) active: SegmentReference,
    pub(super) valid_bytes: u64,
    pub(super) digest: Digest,
    pub(super) next_group_number: u64,
    pub(super) next_chain: ChainPosition,
    pub(super) writer_generation: JournalGeneration,
}

impl RollBoundary {
    /// The header of successor `segment`, linked to the active segment end.
    pub(super) fn successor_header(
        &self,
        group_id: GroupId,
        segment: u64,
        capacity: u64,
    ) -> Result<SegmentHeader, CodecError> {
        SegmentHeader::new(
            group_id,
            segment,
            Some(self.active.segment_id),
            self.next_chain.previous_digest(),
            capacity,
        )
    }

    /// Where the successor first group continues the active chain.
    pub(super) fn successor_start(&self) -> SegmentStart {
        SegmentStart {
            generation: self.writer_generation,
            first_group_number: self.next_group_number,
            initial_chain: self.next_chain,
        }
    }
}

pub(super) fn validate_buffered_roll_state(
    manifest: &Manifest,
    writer: &crate::SegmentState,
) -> Result<RollBoundary, DirectoryError> {
    if writer.is_faulted() {
        return Err(WriterError::Faulted.into());
    }
    let written = writer.written_position();
    let active = *manifest
        .segments
        .last()
        .ok_or(DirectoryError::MissingActiveSegment)?;
    if active.sealed.is_some()
        || active.segment_id != writer.header().segment_id()
        || active.capacity != writer.header().capacity()
    {
        return Err(DirectoryError::SegmentMismatch(active.segment_id));
    }

    if written.end_offset() == SEGMENT_HEADER_BYTES as u64 {
        return Err(DirectoryError::EmptyActiveSegment);
    }
    let next_group_number = written
        .group_number()
        .checked_add(1)
        .ok_or(WriterError::GroupNumberExhausted)?;
    next_group_number
        .checked_add(1)
        .ok_or(WriterError::GroupNumberExhausted)?;
    Ok(RollBoundary {
        active,
        valid_bytes: written.end_offset(),
        digest: writer.structural_digest(),
        next_group_number,
        next_chain: written.next_chain(),
        writer_generation: written.generation(),
    })
}

pub(super) fn roll_manifest_after(
    manifest: &Manifest,
    boundary: RollBoundary,
    committed: LogPosition,
    next_segment_id: u64,
    new_capacity: u64,
) -> Result<Manifest, DirectoryError> {
    let mut next = manifest.clone();
    next.generation = next
        .generation
        .checked_add(1)
        .ok_or(DirectoryError::ManifestGeneration)?;
    next.parent_generation = manifest.generation;
    // Accepted history covers the commit. The sealed bytes protect both.
    next.committed = committed;
    if committed.op_number > next.accepted.op_number {
        next.accepted = committed;
    }
    next.segments
        .last_mut()
        .ok_or(DirectoryError::MissingActiveSegment)?
        .sealed = Some(SealedSegment {
        valid_bytes: boundary.valid_bytes,
        digest: boundary.digest,
    });
    next.segments.push(SegmentReference {
        segment_id: next_segment_id,
        file_generation: 0,
        first_group_number: boundary.next_group_number,
        first_chain: boundary.next_chain,
        capacity: new_capacity,
        sealed: None,
    });
    Ok(next)
}

impl Journal {
    /// Select a durable successor without overwriting leftovers from canceled
    /// attempts. The probe bound limits abandoned-file work per roll attempt.
    pub async fn roll_active(
        &mut self,
        capacity: u64,
        max_probes: usize,
    ) -> Result<(), DirectoryError> {
        self.healthy()?;
        let boundary = validate_buffered_roll_state(&self.manifest, self.writer.state())?;
        if self.writer.written_position() != self.writer.durable_position() {
            return Err(DirectoryError::ActiveSegmentNotDurable);
        }
        crate::codec::validate_segment_capacity(capacity)?;
        let mut segment = boundary.active.segment_id;
        self.interrupted = true;
        for _ in 0..max_probes {
            segment = segment
                .checked_add(1)
                .ok_or(DirectoryError::SegmentIdExhausted)?;
            let header =
                boundary.successor_header(self.manifest.identity.group_id, segment, capacity)?;
            let next = roll_manifest_after(
                &self.manifest,
                boundary,
                self.manifest.committed,
                segment,
                capacity,
            )?;
            encode_manifest_with_limits(&next, self.limits.metadata)?;
            let writer = SegmentWriter::create(
                self.access
                    .segment_file(self.root().join(segment_name(segment))),
                header,
                boundary.successor_start(),
                self.limits.io,
            )
            .await;
            let mut writer = match writer {
                Ok(writer) => writer,
                Err(WriterError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            self.access
                .sync_directory(self.root().join("segments"))
                .await?;
            self.writer.transfer_buffers_to(&mut writer);
            self.install_selected(next).await?;
            self.writer = writer;
            self.active_prefix = None;
            self.interrupted = false;
            return Ok(());
        }
        Err(DirectoryError::RollProbeLimit { limit: max_probes })
    }
}
