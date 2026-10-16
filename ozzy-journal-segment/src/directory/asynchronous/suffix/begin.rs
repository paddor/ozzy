//! Starting a suffix replacement from the selected journal: request checks,
//! the retained prefix, and the first staged segment.

use super::{
    CheckpointRecovery, Installer, Journal, Start, SuffixRequest, active_reference, create_segment,
};
use crate::SuffixReplacementError as Error;
use crate::directory::asynchronous::active_segment;
use crate::directory::recovery::recovery_marker;
use crate::metadata::position_before;
use crate::suffix::state::Source;
use crate::suffix::{
    OwnedOperation, enforce_limit, retained_image, rewrite_segment_index, streaming,
    validate_common_request,
};
use crate::{
    ChainPosition, Digest, ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, LogPosition, SegmentReference,
    SuffixReplacementLimits, SuffixStreamLimits, WRITE_GROUP_ALIGNMENT,
};

impl Journal {
    pub(super) fn suffix_source(&self) -> Source<'_> {
        Source {
            manifest: &self.manifest,
            current: self.current,
            header: self.writer.header(),
            written: self.writer.written_position(),
            durable: self.writer.durable_position(),
            healthy: !self.is_faulted(),
            decode: self.limits.decode,
            operations: self.limits.operations,
            max_segments: self.limits.metadata.max_segments,
        }
    }

    /// Preserve the exact protected prefix and stage a selected suffix. Copies
    /// at most one bounded source segment; older sealed predecessors stay shared.
    pub async fn begin_suffix_replacement(
        mut self,
        request: SuffixRequest,
    ) -> Result<Installer, Error> {
        let SuffixRequest {
            replacement,
            limits,
            ..
        } = request;
        streaming::validate_stream_request(&self.suffix_source(), &request)?;
        self.validate_authority_files().await?;
        let index = rewrite_segment_index(&self.manifest, replacement.protected_committed)?;
        let reference = self.manifest.segments[index];
        if reference.capacity > limits.max_source_segment_bytes {
            return Err(Error::InvalidLimits);
        }
        let image = self.segment_image(reference).await?;
        let retained = retained_image(
            &self.suffix_source(),
            reference,
            replacement.protected_committed,
            SuffixReplacementLimits {
                max_operations: usize::try_from(reference.capacity / ENTRY_HEADER_BYTES as u64)
                    .map_err(|_| Error::LengthOverflow)?,
                max_body_bytes: self.limits.decode.max_segment_decoded_body_bytes,
                max_segment_bytes: limits.max_source_segment_bytes,
            },
            &image,
        )?;
        drop(image);
        Box::pin(self.start_suffix(&request, index, reference, &retained)).await
    }

    /// Replace an unfinished nonvoting store from genesis under fresh external
    /// authority. Final selection leaves its marker intact. Private replay and
    /// configuration publication remain separate required gates.
    pub async fn begin_recovery_replacement(
        self,
        configuration: &[u8],
        request: SuffixRequest,
    ) -> Result<Installer, Error> {
        self.begin_anchored_recovery(configuration, &request, ChainPosition::GENESIS)
            .await
    }

    /// Stage retained operations from an externally authorized checkpoint's
    /// original predecessor. The target remains nonvoting until checkpoint state,
    /// complete retained chain and accepted suffix are privately validated.
    pub async fn begin_checkpoint_replacement(
        self,
        configuration: &[u8],
        request: SuffixRequest,
        anchor: CheckpointRecovery,
    ) -> Result<Installer, Error> {
        let replacement = request.replacement;
        let predecessor = anchor.predecessor;
        predecessor
            .validate()
            .map_err(crate::DirectoryError::from)?;
        anchor
            .position
            .validate()
            .map_err(crate::DirectoryError::from)?;
        if predecessor.op_number > replacement.committed.op_number
            || (predecessor.op_number == replacement.committed.op_number
                && predecessor != replacement.committed)
            || predecessor.op_number > anchor.position.op_number
            || anchor.position.op_number == 0
            || anchor.position.op_number > replacement.committed.op_number
            || (anchor.position.op_number == predecessor.op_number
                && anchor.position != predecessor)
            || (anchor.position.op_number == replacement.committed.op_number
                && anchor.position != replacement.committed)
        {
            return Err(Error::ProtectedCommitMismatch);
        }
        let next = predecessor
            .op_number
            .checked_add(1)
            .ok_or(Error::LengthOverflow)?;
        let mut installer = self
            .begin_anchored_recovery(
                configuration,
                &request,
                ChainPosition::new(next, predecessor.digest),
            )
            .await?;
        installer.checkpoint_position = Some(anchor.position);
        installer.checkpoint_seen = predecessor == anchor.position;
        Ok(installer)
    }

    async fn begin_anchored_recovery(
        mut self,
        configuration: &[u8],
        request: &SuffixRequest,
        first_chain: ChainPosition,
    ) -> Result<Installer, Error> {
        let SuffixRequest {
            replacement,
            limits,
            ..
        } = *request;
        let marker = recovery_marker(configuration)?;
        if self.configuration.as_deref() != Some(marker.as_slice()) {
            return Err(crate::DirectoryError::ConfigurationMismatch.into());
        }
        validate_common_request(
            &self.suffix_source(),
            replacement,
            SuffixReplacementLimits {
                max_operations: limits.max_group_operations,
                max_body_bytes: limits.max_group_body_bytes,
                max_segment_bytes: limits.max_staged_bytes,
            },
        )?;
        if replacement.protected_committed != LogPosition::GENESIS
            || self.manifest.checkpoint.is_some()
            || self.manifest.segments[0].first_chain != ChainPosition::GENESIS
        {
            return Err(Error::ProtectedCommitMismatch);
        }
        if replacement.promised_view != replacement.last_normal_view {
            return Err(Error::InvalidView);
        }
        streaming::validate_stream_limits(&self.suffix_source(), request)?;
        self.validate_authority_files().await?;
        let source = SegmentReference {
            first_group_number: 1,
            first_chain,
            ..self.manifest.segments[0]
        };
        Box::pin(self.start_suffix(request, 0, source, &[])).await
    }

    async fn start_suffix(
        self,
        request: &SuffixRequest,
        rewrite_index: usize,
        source: SegmentReference,
        retained: &[OwnedOperation],
    ) -> Result<Installer, Error> {
        let SuffixRequest {
            replacement,
            limits,
            ..
        } = *request;
        enforce_limit(
            "selected manifest segments",
            rewrite_index + 1,
            limits.max_segments,
        )?;
        if replacement.segment_capacity > self.limits.io.max_segment_bytes {
            return Err(Error::InvalidLimits);
        }
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(limits.max_segments)
            .map_err(|_| Error::Allocation)?;
        segments.extend_from_slice(&self.manifest.segments[..rewrite_index]);
        let start = self.suffix_start(segments.last(), source)?;
        let mut body_digests = Vec::new();
        body_digests
            .try_reserve_exact(limits.max_group_operations)
            .map_err(|_| Error::Allocation)?;
        let reserve = group_reserve(limits)?;
        let mut writer = create_segment(&self, replacement, limits, start).await?;
        writer.reserve_encode_buffer(reserve, replacement.body_encoding)?;
        segments.push(active_reference(&writer, start));
        let mut installer = Installer {
            checkpoint_position: None,
            checkpoint_seen: false,
            journal: self,
            request: *request,
            writer,
            segments,
            body_digests,
            staged_bytes: replacement.segment_capacity,
            committed_seen: replacement.committed == position_before(start.first_chain)?,
            faulted: false,
        };
        installer.copy_retained(retained).await?;
        if !retained.is_empty() || source.first_chain == ChainPosition::GENESIS {
            debug_assert_eq!(
                position_before(installer.writer.written_position().next_chain())?,
                replacement.protected_committed
            );
        }
        Ok(installer)
    }

    /// Where the first staged segment starts: after the newest selected id,
    /// linked to the retained `predecessor`, if any.
    fn suffix_start(
        &self,
        predecessor: Option<&SegmentReference>,
        source: SegmentReference,
    ) -> Result<Start, Error> {
        let (predecessor, predecessor_digest) =
            predecessor.map_or((None, Digest::ZERO), |segment| {
                (
                    Some(segment.segment_id),
                    source.first_chain.previous_digest(),
                )
            });
        Ok(Start {
            first_id: active_segment(&self.manifest.segments)?
                .segment_id
                .checked_add(1)
                .ok_or(Error::SegmentIdExhausted)?,
            predecessor,
            predecessor_digest,
            first_group: source.first_group_number,
            first_chain: source.first_chain,
        })
    }
}

/// Encode buffer bytes that hold one largest staged group.
fn group_reserve(limits: SuffixStreamLimits) -> Result<usize, Error> {
    limits
        .max_group_operations
        .checked_mul(ENTRY_HEADER_BYTES + 7)
        .and_then(|overhead| overhead.checked_add(limits.max_group_body_bytes))
        .and_then(|bytes| bytes.checked_add(GROUP_SEAL_BYTES + WRITE_GROUP_ALIGNMENT))
        .ok_or(Error::LengthOverflow)
}
