//! Owned body preparation; no file handle or mutable journal crosses the worker boundary.

use super::{CanonicalOperation, DirectoryError, PreencodedJournalGroup};
use crate::{BodyEncoding, DecodeLimits, OperationLimits};

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

    fn body_check(&self) -> super::BodyCheck {
        if self.validated == Some(self.operations) {
            super::BodyCheck::Metadata
        } else if self.validated_payloads {
            super::BodyCheck::Descriptors
        } else {
            super::BodyCheck::Full
        }
    }

    /// Keep canonical raw bodies shared; allocate only headers and group framing.
    pub fn encode_shared_raw(
        self,
        operations: Vec<super::SharedJournalOperation>,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        let descriptors = super::describe_operations(
            operations
                .iter()
                .map(|op| (op.canonical(), Some(op.body_digest))),
            self.decode,
            self.operations,
            self.body_check(),
            self.configuration_epoch,
            self.promised_view,
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
        let descriptors = super::describe_operations(
            operations.clone(),
            self.decode,
            self.operations,
            self.body_check(),
            self.configuration_epoch,
            self.promised_view,
        )?;
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
