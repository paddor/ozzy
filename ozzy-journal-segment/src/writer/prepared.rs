//! Owned physical output. Preparation changes no written/durable boundary.

use super::{
    CanonicalOperation, CodecError, Digest, File, FinalizedGroup, PreparedGroupBodies, SegmentIo,
    SegmentWriter, WriterError, WriterPosition, extents, finalize_group_bodies, io,
};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum WriteBytes {
    Contiguous(Vec<u8>),
    SharedRaw {
        framing: Vec<u8>,
        bodies: Vec<bytes::Bytes>,
    },
}

impl WriteBytes {
    pub(crate) fn write(&self, file: &mut impl SegmentIo, offset: u64) -> io::Result<()> {
        extents::write_extents(file, offset, self.slices())
    }

    pub(crate) fn slices(&self) -> impl Iterator<Item = &[u8]> {
        let contiguous = match self {
            Self::Contiguous(bytes) => Some(bytes.as_slice()),
            Self::SharedRaw { .. } => None,
        };
        let shared = match self {
            Self::SharedRaw { framing, bodies } => Some((framing, bodies)),
            Self::Contiguous(_) => None,
        };
        contiguous
            .into_iter()
            .chain(shared.into_iter().flat_map(|(framing, bodies)| {
                const PADDING: [u8; 8] = [0; 8];
                let entries = bodies.iter().enumerate().flat_map(|(index, body)| {
                    let start = index * crate::ENTRY_HEADER_BYTES;
                    [
                        &framing[start..start + crate::ENTRY_HEADER_BYTES],
                        body.as_ref(),
                        &PADDING[..(8 - body.len() % 8) % 8],
                    ]
                });
                entries.chain(std::iter::once(
                    &framing[bodies.len() * crate::ENTRY_HEADER_BYTES..],
                ))
            }))
            .filter(|bytes| !bytes.is_empty())
    }

    pub(crate) fn into_journal_scratch(self) -> Vec<u8> {
        match self {
            Self::Contiguous(bytes) | Self::SharedRaw { framing: bytes, .. } => bytes,
        }
    }
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

impl<I> SegmentWriter<I> {
    pub(crate) fn take_encode_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.encode_buffer)
    }

    pub(crate) fn preencode_owned_shared_bodies<'a>(
        &mut self,
        bodies: impl Iterator<Item = &'a [u8]> + Clone,
        encoding: crate::BodyEncoding,
    ) -> Result<PreparedGroupBodies, WriterError> {
        self.require_healthy()?;
        Ok(crate::codec::prepare_shared_group_bodies(
            bodies,
            encoding,
            std::mem::take(&mut self.encode_buffer),
            self.body_encode_scratch
                .as_mut()
                .ok_or(WriterError::InvalidSyncPosition)?,
        )?)
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

    pub(crate) fn prepare_owned_descriptors(
        &mut self,
        operations: &[crate::codec::PackedOperation],
        mut prepared: PreparedGroupBodies,
    ) -> Result<(WritePlan, WriteBytes), WriterError> {
        self.require_healthy()?;
        let result = crate::codec::finalize_group_descriptors(
            &self.header,
            self.next_group_number,
            self.written.end_offset,
            self.written.next_chain,
            operations.iter().copied(),
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

impl SegmentWriter<Arc<File>> {
    /// The shared open segment file for write jobs, direct when configured.
    /// No descriptor is duplicated.
    pub(crate) fn write_handle(&self) -> Arc<File> {
        Arc::clone(self.direct.as_ref().unwrap_or(&self.io))
    }

    pub(crate) fn restore_write_bytes(&mut self, bytes: Vec<u8>) {
        self.encode_buffer = bytes;
        self.encode_buffer.clear();
    }
}
