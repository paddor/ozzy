//! Owned body preparation; no file handle or mutable journal crosses the worker boundary.

use super::{CanonicalOperation, DirectoryError, PreencodedJournalGroup};
use crate::codec::PackedOperation;
use crate::directory::{OperationEnvelope, validate_operation, validate_operation_metadata};
use crate::{BodyEncoding, DecodeLimits, OperationLimits};
use ozzy_journal::operation::{
    Digest, canonical_body_digest, validate_operation_body_with_validated_payload,
};

/// Reusable compressor state owned by one CPU worker.
#[derive(Debug, Default)]
pub struct JournalGroupEncoder {
    scratch: crate::codec::BodyEncodeScratch,
}

/// Captured validation limits and reusable physical output for one encoding job.
/// Segment placement remains the journal owner's responsibility after encoding.
#[derive(Debug)]
pub struct JournalGroupEncoding {
    decode: DecodeLimits,
    operations: OperationLimits,
    configuration_epoch: u64,
    promised_view: u64,
    validated: Option<OperationLimits>,
    validated_payloads: bool,
    output: Vec<u8>,
}

impl JournalGroupEncoding {
    pub(crate) fn captured(
        decode: DecodeLimits,
        operations: OperationLimits,
        configuration_epoch: u64,
        promised_view: u64,
        output: Vec<u8>,
    ) -> Self {
        Self {
            decode,
            operations,
            configuration_epoch,
            promised_view,
            validated: None,
            validated_payloads: false,
            output,
        }
    }

    /// The owner already decoded every body of the group with `limits`. When
    /// they equal the journal's own limits, encoding skips only the body walk;
    /// envelope, digest, and storage bounds remain checked. Other limits
    /// keep full validation.
    #[must_use]
    pub const fn with_validated_bodies(mut self, limits: OperationLimits) -> Self {
        self.validated = Some(limits);
        self
    }

    /// Every body's whole-payload LZ4 block was validated before canonical
    /// construction, or produced by the local encoder. Encoding still walks
    /// descriptors and checks every limit.
    #[must_use]
    pub const fn with_validated_payloads(mut self) -> Self {
        self.validated_payloads = true;
        self
    }

    fn body_check(&self) -> BodyCheck {
        if self.validated == Some(self.operations) {
            BodyCheck::Metadata
        } else if self.validated_payloads {
            BodyCheck::Descriptors
        } else {
            BodyCheck::Full
        }
    }

    /// Validate one operation as far as `body` requires.
    fn check(
        &self,
        envelope: OperationEnvelope<'_>,
        body: BodyCheck,
    ) -> Result<(), DirectoryError> {
        match body {
            BodyCheck::Full => {
                validate_operation(
                    envelope,
                    self.operations,
                    self.configuration_epoch,
                    self.promised_view,
                )?;
            }
            BodyCheck::Descriptors => {
                validate_operation_body_with_validated_payload(
                    envelope.kind,
                    envelope.body,
                    self.operations,
                )?;
                validate_operation_metadata(
                    envelope,
                    self.configuration_epoch,
                    self.promised_view,
                )?;
            }
            BodyCheck::Metadata => {
                validate_operation_metadata(
                    envelope,
                    self.configuration_epoch,
                    self.promised_view,
                )?;
            }
        }
        Ok(())
    }

    /// Validate each operation under this job's limits and authority, then
    /// describe its header, body size, and body digest.
    fn describe<'a>(
        &self,
        operations: impl ExactSizeIterator<Item = (CanonicalOperation<'a>, Option<Digest>)>,
    ) -> Result<Vec<PackedOperation>, DirectoryError> {
        if operations.len() == 0 {
            return Err(crate::CodecError::EmptyGroup.into());
        }
        require_limit("group entries", operations.len(), self.decode.max_entries)?;
        let body_check = self.body_check();
        let mut body_bytes = 0usize;
        let mut descriptors = Vec::with_capacity(operations.len());
        for (operation, body_digest) in operations {
            let envelope = OperationEnvelope {
                kind: operation.kind,
                body: operation.body,
                op_number: operation.op_number,
                configuration_epoch: operation.configuration_epoch,
                original_view: operation.original_view,
            };
            self.check(envelope, body_check)?;
            body_bytes = body_bytes
                .checked_add(operation.body.len())
                .ok_or(crate::CodecError::LengthOverflow)?;
            require_limit(
                "decoded operation bytes",
                operation.body.len(),
                self.decode.max_decoded_body_bytes,
            )?;
            require_limit(
                "decoded group bytes",
                body_bytes,
                self.decode.max_group_decoded_body_bytes,
            )?;
            descriptors.push(PackedOperation {
                header: operation.header(),
                body_bytes: operation.body.len(),
                body_digest: body_digest.unwrap_or_else(|| canonical_body_digest(operation.body)),
            });
        }
        Ok(descriptors)
    }

    /// Keep canonical raw bodies shared; allocate only headers and group framing.
    pub fn encode_shared_raw(
        self,
        operations: Vec<super::SharedJournalOperation>,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        let descriptors = self.describe(
            operations
                .iter()
                .map(|op| (op.canonical(), Some(op.body_digest))),
        )?;
        let bodies = crate::codec::prepare_raw_group_extents(
            operations.iter().map(|op| op.body.as_ref()),
            self.output,
        )?;
        Ok(PreencodedJournalGroup {
            operations: descriptors,
            bodies,
            shared: operations.into_iter().map(|op| op.body).collect(),
        })
    }

    /// Validate and encode immutable canonical operations on a CPU worker.
    /// The caller retains their owners until this synchronous call returns.
    pub fn encode(
        self,
        encoder: &mut JournalGroupEncoder,
        operations: &[CanonicalOperation<'_>],
        encoding: BodyEncoding,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        self.encode_inner(
            encoder,
            operations.iter().copied().map(|op| (op, None)),
            encoding,
        )
    }

    /// Reuse owner-computed digests for these exact immutable canonical bodies.
    /// The caller must preserve that association, including across worker handoff.
    /// All ordinary schema, authority, and storage bounds remain checked.
    pub fn encode_verified(
        self,
        encoder: &mut JournalGroupEncoder,
        operations: &[(CanonicalOperation<'_>, ozzy_journal::operation::Digest)],
        encoding: BodyEncoding,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        self.encode_inner(
            encoder,
            operations.iter().map(|&(op, digest)| (op, Some(digest))),
            encoding,
        )
    }

    fn encode_inner<'a>(
        self,
        encoder: &mut JournalGroupEncoder,
        operations: impl ExactSizeIterator<
            Item = (
                CanonicalOperation<'a>,
                Option<ozzy_journal::operation::Digest>,
            ),
        > + Clone,
        encoding: BodyEncoding,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        let descriptors = self.describe(operations.clone())?;
        let bodies = crate::codec::prepare_shared_group_bodies(
            operations.map(|(operation, _)| operation.body),
            encoding,
            self.output,
            &mut encoder.scratch,
        )?;
        Ok(PreencodedJournalGroup {
            operations: descriptors,
            bodies,
            shared: Vec::new(),
        })
    }
}

/// How much of each body encoding walks again before describing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BodyCheck {
    /// Walk the whole body, including any whole-payload LZ4 block.
    Full,
    /// Walk descriptors and limits; the caller validated or produced each
    /// body's LZ4 block.
    Descriptors,
    /// The caller decoded every body with the journal's limits.
    Metadata,
}

fn require_limit(kind: &'static str, actual: usize, limit: usize) -> Result<(), DirectoryError> {
    if actual > limit {
        Err(crate::CodecError::LimitExceeded {
            kind,
            actual,
            limit,
        }
        .into())
    } else {
        Ok(())
    }
}
