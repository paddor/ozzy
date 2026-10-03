//! Owned physical output. Preparation changes no written/durable boundary.

use super::{
    CanonicalOperation, CodecError, Digest, FinalizedGroup, PreparedGroupBodies, SegmentState,
    WriterError, WriterPosition, finalize_group_bodies, io,
};

#[derive(Debug)]
pub(crate) enum WriteBytes {
    Contiguous(Vec<u8>),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WritePlan {
    pub(crate) before: WriterPosition,
    pub(crate) after: WriterPosition,
    digest: Digest,
}

/// Immutable placement captured by the journal owner. Encoding may run elsewhere;
/// only `complete_write` can install the resulting plan on the original writer.
#[derive(Debug)]
pub(crate) struct WriteLayout {
    pub(crate) header: crate::SegmentHeader,
    pub(crate) before: WriterPosition,
    pub(crate) group_number: u64,
}

impl WriteLayout {
    pub(crate) fn plan(
        &self,
        finalized: FinalizedGroup,
        body_bytes: usize,
    ) -> Result<WritePlan, WriterError> {
        self.group_number
            .checked_add(1)
            .ok_or(WriterError::GroupNumberExhausted)?;
        let decoded_body_bytes = self
            .before
            .decoded_body_bytes
            .checked_add(body_bytes)
            .ok_or(CodecError::LengthOverflow)?;
        Ok(WritePlan {
            before: self.before,
            after: WriterPosition {
                generation: self.before.generation,
                segment_id: self.header.segment_id(),
                group_number: self.group_number,
                end_offset: finalized.end_offset,
                decoded_body_bytes,
                next_chain: finalized.next_chain,
            },
            digest: finalized.digest,
        })
    }
}

impl SegmentState {
    pub(crate) fn take_encode_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.encode_buffer)
    }

    pub(crate) fn preencode_owned_bodies<'a>(
        &mut self,
        bodies: impl Iterator<Item = &'a [u8]> + Clone,
        encoding: crate::BodyEncoding,
    ) -> Result<PreparedGroupBodies, WriterError> {
        self.require_healthy()?;
        Ok(crate::codec::prepare_group_bodies(
            bodies,
            encoding,
            std::mem::take(&mut self.encode_buffer),
            self.body_encode_scratch
                .as_mut()
                .ok_or(WriterError::InvalidSyncPosition)?,
        )?)
    }

    pub(crate) fn plan_write(
        &self,
        finalized: FinalizedGroup,
        body_bytes: usize,
    ) -> Result<WritePlan, WriterError> {
        self.require_healthy()?;
        self.write_layout().plan(finalized, body_bytes)
    }

    pub(crate) fn write_layout(&self) -> WriteLayout {
        WriteLayout {
            header: self.header.clone(),
            before: self.written,
            group_number: self.next_group_number,
        }
    }

    pub(crate) fn prepare_owned_encoded(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        digests: &[Digest],
        mut prepared: PreparedGroupBodies,
    ) -> Result<(WritePlan, WriteBytes), WriterError> {
        self.require_healthy()?;
        let result = finalize_group_bodies(
            &self.header,
            self.next_group_number,
            self.written.end_offset,
            self.written.next_chain,
            operations,
            digests,
            &mut prepared,
        )
        .map_err(WriterError::from)
        .and_then(|group| self.plan_write(group, prepared.decoded_body_bytes()));
        match result {
            Ok(plan) => Ok((plan, WriteBytes::Contiguous(prepared.into_owned_bytes()))),
            Err(error) => {
                self.restore_encode_buffer(prepared);
                Err(error)
            }
        }
    }

    pub(crate) fn complete_write(
        &mut self,
        plan: WritePlan,
        result: io::Result<()>,
    ) -> Result<WriterPosition, WriterError> {
        self.require_healthy()?;
        if self.written != plan.before {
            return Err(WriterError::InvalidSyncPosition);
        }
        if let Err(error) = result {
            self.faulted = true;
            return Err(error.into());
        }
        self.segment_digest.push(plan.digest);
        self.written = plan.after;
        self.next_group_number += 1;
        #[cfg(feature = "storage-metrics")]
        crate::write_metrics::ENCODED_GROUP_BYTES.fetch_add(
            plan.after.end_offset - plan.before.end_offset,
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(self.written)
    }
}
