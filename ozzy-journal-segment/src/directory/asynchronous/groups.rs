//! Lending physical-group reader. Payloads expire before the next bounded read.

use super::{
    DirectoryError, Journal, Limits, OperationEnvelope, SegmentReference, validate_operation,
};
use crate::{
    ChainPosition, CodecError, DecodedGroup, Digest, SEGMENT_HEADER_BYTES, SealedSegment,
    SegmentHeader, WRITE_GROUP_ALIGNMENT, async_files::Access, codec::SegmentDigestBuilder,
};
use ozzy_io::{Handle, OpenMode, Operation};

pub(crate) struct Groups {
    resumed: bool,
    access: Access,
    handle: Handle,
    header: SegmentHeader,
    reference: SegmentReference,
    limits: Limits,
    expected: End,
    cursor: Cursor,
    digest: SegmentDigestBuilder,
    decoded: usize,
    bytes: Vec<u8>,
    configuration_epoch: u64,
    promised_view: u64,
    ended: bool,
    work: crate::cooperative::Budget,
}

#[derive(Debug, Clone, Copy)]
struct End {
    offset: u64,
    digest: Digest,
    chain: ChainPosition,
    group_number: u64,
}

struct Cursor {
    offset: u64,
    chain: ChainPosition,
    group_number: u64,
}

/// Validated reader state at the end of an active-segment scan. A later scan of
/// the same active segment resumes here and validates only appended groups.
#[derive(Clone)]
pub(crate) struct Resume {
    reference: SegmentReference,
    offset: u64,
    chain: ChainPosition,
    group_number: u64,
    digest: SegmentDigestBuilder,
    decoded: usize,
}

/// Validation inputs for one segment's groups, captured from the owner so the
/// scan itself borrows no journal. Deletion protection belongs to the caller.
#[derive(Debug, Clone)]
pub(crate) struct GroupScope {
    access: Access,
    path: std::path::PathBuf,
    reference: SegmentReference,
    expected: End,
    active_header: Option<SegmentHeader>,
    group_id: ozzy_proto::GroupId,
    limits: Limits,
    configuration_epoch: u64,
    promised_view: u64,
}

impl Journal {
    pub(crate) async fn segment_groups(
        &self,
        reference: SegmentReference,
    ) -> Result<Box<Groups>, DirectoryError> {
        self.segment_groups_from(reference, None).await
    }

    /// Resume only within the same active segment and its written prefix;
    /// otherwise scan from the header. `Groups::resumed` reports the choice.
    pub(crate) async fn segment_groups_from(
        &self,
        reference: SegmentReference,
        resume: Option<Resume>,
    ) -> Result<Box<Groups>, DirectoryError> {
        self.group_scope(reference)?.open(resume).await
    }

    /// Capture a selected segment's expected end without file access.
    pub(crate) fn group_scope(
        &self,
        reference: SegmentReference,
    ) -> Result<GroupScope, DirectoryError> {
        self.healthy()?;
        let (expected, active_header) = if let Some(sealed) = reference.sealed {
            let at = self
                .manifest
                .segments
                .iter()
                .position(|entry| *entry == reference)
                .ok_or(DirectoryError::SegmentMismatch(reference.segment_id))?;
            let next = self
                .manifest
                .segments
                .get(at + 1)
                .ok_or(DirectoryError::SegmentMismatch(reference.segment_id))?;
            let expected = End {
                offset: sealed.valid_bytes,
                digest: sealed.digest,
                chain: next.first_chain,
                group_number: next.first_group_number,
            };
            (expected, None)
        } else {
            let position = self.writer.written_position();
            let expected = End {
                offset: position.end_offset(),
                digest: self.writer.state().structural_digest(),
                chain: position.next_chain(),
                group_number: position
                    .group_number()
                    .checked_add(1)
                    .ok_or(CodecError::InvalidGroupNumber)?,
            };
            (expected, Some(self.writer.state().header().clone()))
        };
        Ok(GroupScope {
            access: self.access.clone(),
            path: self.root().join(super::segment_reference_name(reference)),
            reference,
            expected,
            active_header,
            group_id: self.manifest.identity.group_id,
            limits: self.limits,
            configuration_epoch: self.manifest.configuration_epoch,
            promised_view: self.manifest.promised_view,
        })
    }

    pub(crate) fn group_scan_bytes(&self) -> Result<usize, DirectoryError> {
        group_bytes(self.limits)
    }
}

impl GroupScope {
    pub(crate) const fn reference(&self) -> SegmentReference {
        self.reference
    }

    pub(crate) const fn limits(&self) -> Limits {
        self.limits
    }

    pub(crate) async fn open(self, resume: Option<Resume>) -> Result<Box<Groups>, DirectoryError> {
        let Self {
            access,
            path,
            reference,
            expected,
            active_header,
            group_id,
            limits,
            configuration_epoch,
            promised_view,
        } = self;
        let handle = access.open(path, OpenMode::Read, false, false).await?;
        let length = access.length(&handle).await?;
        let bytes = access
            .read_range(&handle, 0, SEGMENT_HEADER_BYTES, limits.io.chunk_bytes)
            .await?;
        let header = crate::decode_segment_header(&bytes)?;
        if active_header.is_some_and(|active| header != active)
            || length > reference.capacity
            || length < expected.offset
            || length > limits.io.max_segment_bytes
            || header.group_id() != group_id
            || header.segment_id() != reference.segment_id
            || header.capacity() != reference.capacity
        {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id));
        }
        let resume = resume.filter(|resume| {
            reference.sealed.is_none()
                && resume.reference == reference
                && resume.offset <= expected.offset
        });
        let resumed = resume.is_some();
        let (cursor, digest, decoded) = start(resume, reference, &header);
        Ok(Box::new(Groups {
            resumed,
            access,
            handle,
            header,
            reference,
            limits,
            expected,
            digest,
            decoded,
            cursor,
            bytes: Vec::new(),
            configuration_epoch,
            promised_view,
            ended: false,
            work: crate::cooperative::Budget::default(),
        }))
    }
}

fn start(
    resume: Option<Resume>,
    reference: SegmentReference,
    header: &SegmentHeader,
) -> (Cursor, SegmentDigestBuilder, usize) {
    match resume {
        Some(resume) => (
            Cursor {
                offset: resume.offset,
                chain: resume.chain,
                group_number: resume.group_number,
            },
            resume.digest,
            resume.decoded,
        ),
        None => (
            Cursor {
                offset: SEGMENT_HEADER_BYTES as u64,
                chain: reference.first_chain,
                group_number: reference.first_group_number,
            },
            SegmentDigestBuilder::new(header),
            0,
        ),
    }
}

fn group_bytes(limits: Limits) -> Result<usize, DirectoryError> {
    limits
        .decode
        .max_entries
        .checked_mul(crate::ENTRY_HEADER_BYTES + 7)
        .and_then(|bytes| bytes.checked_add(limits.decode.max_group_decoded_body_bytes))
        .and_then(|bytes| bytes.checked_add(2 * WRITE_GROUP_ALIGNMENT))
        .ok_or(DirectoryError::RetentionScanBudget)
}

impl Groups {
    pub(crate) fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// The validated prefix this reader must end at.
    pub(crate) fn boundary(&self) -> SealedSegment {
        SealedSegment {
            valid_bytes: self.expected.offset,
            digest: self.expected.digest,
        }
    }

    /// Whether this reader started after a previously validated prefix.
    pub(crate) const fn resumed(&self) -> bool {
        self.resumed
    }

    /// Validated end state, available once the reader reached its end.
    pub(crate) fn resume(&self) -> Option<Resume> {
        self.ended.then(|| Resume {
            reference: self.reference,
            offset: self.cursor.offset,
            chain: self.cursor.chain,
            group_number: self.cursor.group_number,
            digest: self.digest.clone(),
            decoded: self.decoded,
        })
    }

    /// Each yielded group has canonical metadata, payload and chain validation.
    /// End authority is checked before returning None; later bytes are unused
    /// capacity.
    pub(crate) async fn next(&mut self) -> Result<Option<DecodedGroup<'_>>, DirectoryError> {
        if self.ended {
            return Ok(None);
        }
        if self.cursor.offset == self.expected.offset {
            self.reach_end()?;
            return Ok(None);
        }
        if self.cursor.offset > self.expected.offset || self.group_budget_spent() {
            return Err(DirectoryError::RetentionScanBudget);
        }
        self.read_group().await?;
        let group = crate::decode_group(
            &self.header,
            crate::GroupStart::new(
                self.cursor.group_number,
                self.cursor.offset,
                self.cursor.chain,
            ),
            &self.bytes,
            self.limits.decode,
        )?;
        for operation in &group.operations {
            self.decoded = self
                .decoded
                .checked_add(operation.body.len())
                .ok_or(CodecError::LengthOverflow)?;
            if self.decoded > self.limits.decode.max_segment_decoded_body_bytes {
                return Err(CodecError::SegmentDecodedBodyLimit {
                    actual: self.decoded,
                    limit: self.limits.decode.max_segment_decoded_body_bytes,
                }
                .into());
            }
            validate_operation(
                OperationEnvelope {
                    kind: operation.kind,
                    body: &operation.body,
                    op_number: operation.op_number,
                    configuration_epoch: operation.configuration_epoch,
                    original_view: operation.original_view,
                },
                self.limits.operations,
                self.configuration_epoch,
                self.promised_view,
            )?;
            self.work.charge(operation.body.len()).await;
        }
        self.work.charge(group.consumed_bytes()).await;
        self.cursor.offset = group.end_offset;
        self.cursor.chain = group.next_chain;
        self.cursor.group_number = self
            .cursor
            .group_number
            .checked_add(1)
            .ok_or(CodecError::InvalidGroupNumber)?;
        self.digest.push(group.digest);
        Ok(Some(group))
    }

    /// At the expected end, what was read must match the end authority.
    fn reach_end(&mut self) -> Result<(), DirectoryError> {
        if !self.matches_end() {
            return Err(DirectoryError::SegmentMismatch(self.reference.segment_id));
        }
        self.ended = true;
        Ok(())
    }

    /// Whether the digest, chain, and group number read so far are the
    /// expected end authority.
    fn matches_end(&self) -> bool {
        self.digest.finish() == self.expected.digest
            && self.cursor.chain == self.expected.chain
            && self.cursor.group_number == self.expected.group_number
    }

    /// Whether this segment already yielded its decode limit of groups.
    fn group_budget_spent(&self) -> bool {
        self.cursor.group_number - self.reference.first_group_number
            >= self.limits.decode.max_groups as u64
    }

    async fn read_group(&mut self) -> Result<(), DirectoryError> {
        self.bytes.clear();
        let initial =
            ((self.expected.offset - self.cursor.offset) as usize).min(WRITE_GROUP_ALIGNMENT);
        self.access
            .read_append(
                &self.handle,
                crate::async_files::Span {
                    offset: self.cursor.offset,
                    length: initial,
                },
                self.limits.io.chunk_bytes,
                &mut self.bytes,
            )
            .await?;
        let maximum = group_bytes(self.limits)?;
        loop {
            match crate::codec::group_extent(
                &self.header,
                self.cursor.group_number,
                self.cursor.offset,
                &self.bytes,
                self.limits.decode,
            ) {
                Ok(_) => return Ok(()),
                Err(CodecError::Truncated {
                    needed, available, ..
                }) => {
                    let extra = needed
                        .checked_sub(available)
                        .filter(|bytes| *bytes > 0)
                        .ok_or(DirectoryError::RetentionScanBudget)?;
                    let total = self
                        .bytes
                        .len()
                        .checked_add(extra)
                        .ok_or(DirectoryError::RetentionScanBudget)?;
                    if total > maximum || total as u64 > self.expected.offset - self.cursor.offset {
                        return Err(DirectoryError::RetentionScanBudget);
                    }
                    self.access
                        .read_append(
                            &self.handle,
                            crate::async_files::Span {
                                offset: self.cursor.offset + self.bytes.len() as u64,
                                length: extra,
                            },
                            self.limits.io.chunk_bytes,
                            &mut self.bytes,
                        )
                        .await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    pub(crate) async fn finish(self: Box<Self>) -> Result<(), DirectoryError> {
        if !self.ended || self.cursor.offset != self.expected.offset || !self.matches_end() {
            return Err(DirectoryError::SegmentMismatch(self.reference.segment_id));
        }
        self.access
            .done(Operation::Close {
                handle: self.handle,
            })
            .await?;
        Ok(())
    }
}
