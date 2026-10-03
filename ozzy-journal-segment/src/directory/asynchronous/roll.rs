use super::{DirectoryError, Journal};
use crate::directory::{roll_manifest_after, segment_name, validate_buffered_roll_state};
use crate::{
    AsyncSegmentStart, AsyncSegmentWriter, SegmentHeader, WriterError, encode_manifest_with_limits,
};
use std::io;

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
            let header = SegmentHeader::new(
                self.manifest.identity.group_id,
                segment,
                Some(boundary.active.segment_id),
                boundary.next_chain.previous_digest(),
                capacity,
            )?;
            let next = roll_manifest_after(&self.manifest, boundary, segment, capacity)?;
            encode_manifest_with_limits(&next, self.limits.metadata)?;
            let writer = AsyncSegmentWriter::create(
                self.root().join(segment_name(segment)),
                self.access.io.clone(),
                self.access.protection.clone(),
                header,
                AsyncSegmentStart {
                    generation: boundary.writer_generation,
                    first_group_number: boundary.next_group_number,
                    initial_chain: boundary.next_chain,
                },
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
            self.interrupted = false;
            return Ok(());
        }
        Err(DirectoryError::RollProbeLimit { limit: max_probes })
    }
}
