//! Lending physical-group reader. Payloads expire before the next bounded read.

use super::{
    DirectoryError, Journal, Limits, OperationEnvelope, SegmentReference, validate_operation,
};
use crate::{
    ChainPosition, CodecError, DecodedGroup, Digest, SEGMENT_HEADER_BYTES, SegmentHeader,
    WRITE_GROUP_ALIGNMENT, async_files::Access, codec::SegmentDigestBuilder,
};
use ozzy_io::{Handle, OpenMode, Operation};

pub(crate) struct Groups {
    access: Access,
    handle: Handle,
    header: SegmentHeader,
    reference: SegmentReference,
    limits: Limits,
    length: u64,
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

#[derive(Clone, Copy)]
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

impl Journal {
    pub(crate) async fn segment_groups(
        &self,
        reference: SegmentReference,
    ) -> Result<Box<Groups>, DirectoryError> {
        self.healthy()?;
        let handle = self
            .access
            .open(
                self.root().join(super::segment_reference_name(reference)),
                OpenMode::Read,
                false,
                false,
            )
            .await?;
        let length = self.access.length(&handle).await?;
        let bytes = self
            .access
            .read_range(&handle, 0, SEGMENT_HEADER_BYTES, self.limits.io.chunk_bytes)
            .await?;
        let header = crate::decode_segment_header(&bytes)?;
        let expected = if let Some(sealed) = reference.sealed {
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
            End {
                offset: sealed.valid_bytes,
                digest: sealed.digest,
                chain: next.first_chain,
                group_number: next.first_group_number,
            }
        } else {
            let position = self.writer.written_position();
            if header != *self.writer.state().header() {
                return Err(DirectoryError::SegmentMismatch(reference.segment_id));
            }
            End {
                offset: position.end_offset(),
                digest: self.writer.state().structural_digest(),
                chain: position.next_chain(),
                group_number: position
                    .group_number()
                    .checked_add(1)
                    .ok_or(CodecError::InvalidGroupNumber)?,
            }
        };
        if length > reference.capacity
            || length < expected.offset
            || length > self.limits.io.max_segment_bytes
            || header.group_id() != self.manifest.identity.group_id
            || header.segment_id() != reference.segment_id
            || header.capacity() != reference.capacity
        {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id));
        }
        let digest = SegmentDigestBuilder::new(&header);
        Ok(Box::new(Groups {
            access: self.access.clone(),
            handle,
            header,
            reference,
            limits: self.limits,
            length,
            expected,
            digest,
            decoded: 0,
            cursor: Cursor {
                offset: SEGMENT_HEADER_BYTES as u64,
                chain: reference.first_chain,
                group_number: reference.first_group_number,
            },
            bytes: Vec::new(),
            configuration_epoch: self.manifest.configuration_epoch,
            promised_view: self.manifest.promised_view,
            ended: false,
            work: crate::cooperative::Budget::default(),
        }))
    }

    pub(crate) fn group_scan_bytes(&self) -> Result<usize, DirectoryError> {
        group_bytes(self.limits)
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

    pub(crate) fn boundary(&self) -> (u64, Digest) {
        (self.expected.offset, self.expected.digest)
    }

    /// Each yielded group has canonical metadata, payload and chain validation.
    /// End authority and the remaining zero tail are checked before returning None.
    pub(crate) async fn next(&mut self) -> Result<Option<DecodedGroup<'_>>, DirectoryError> {
        if self.ended {
            return Ok(None);
        }
        if self.cursor.offset == self.expected.offset {
            self.zero_tail().await?;
            if self.digest.finish() != self.expected.digest
                || self.cursor.chain != self.expected.chain
                || self.cursor.group_number != self.expected.group_number
            {
                return Err(DirectoryError::SegmentMismatch(self.reference.segment_id));
            }
            self.ended = true;
            return Ok(None);
        }
        if self.cursor.offset > self.expected.offset
            || self.cursor.group_number - self.reference.first_group_number
                >= self.limits.decode.max_groups as u64
        {
            return Err(DirectoryError::RetentionScanBudget);
        }
        self.read_group().await?;
        let group = crate::decode_group(
            &self.header,
            self.cursor.group_number,
            self.cursor.offset,
            self.cursor.chain,
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

    async fn read_group(&mut self) -> Result<(), DirectoryError> {
        self.bytes.clear();
        let initial =
            ((self.expected.offset - self.cursor.offset) as usize).min(WRITE_GROUP_ALIGNMENT);
        self.access
            .read_append(
                &self.handle,
                self.cursor.offset,
                initial,
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
                            self.cursor.offset + self.bytes.len() as u64,
                            extra,
                            self.limits.io.chunk_bytes,
                            &mut self.bytes,
                        )
                        .await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn zero_tail(&mut self) -> Result<(), DirectoryError> {
        let mut offset = self.expected.offset;
        while offset < self.length {
            let bytes = self
                .access
                .read_range(
                    &self.handle,
                    offset,
                    ((self.length - offset) as usize).min(self.limits.io.chunk_bytes),
                    self.limits.io.chunk_bytes,
                )
                .await?;
            if bytes.iter().any(|byte| *byte != 0) {
                return Err(CodecError::NonZeroPadding.into());
            }
            offset += bytes.len() as u64;
            self.work.charge(bytes.len()).await;
        }
        Ok(())
    }

    pub(crate) async fn finish(self: Box<Self>) -> Result<(), DirectoryError> {
        if !self.ended
            || self.cursor.offset != self.expected.offset
            || self.digest.finish() != self.expected.digest
            || self.cursor.chain != self.expected.chain
            || self.cursor.group_number != self.expected.group_number
        {
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
