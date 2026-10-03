//! Owned canonical bodies for buffered vectored writes. Only framing is copied.

use bytes::Bytes;
use ozzy_journal::operation::{CanonicalOperation, Digest, OperationHeader};

use super::{DirectoryError, OpenGroupJournal, PreencodedJournalGroup};

/// An immutable body and its owner-computed digest. The caller must compute the
/// digest over these exact bytes before handing ownership to the write pipeline.
#[derive(Debug)]
pub struct SharedJournalOperation {
    pub header: OperationHeader,
    pub body: Bytes,
    pub body_digest: Digest,
}

impl SharedJournalOperation {
    pub(super) fn canonical(&self) -> CanonicalOperation<'_> {
        CanonicalOperation {
            group_id: self.header.group_id,
            configuration_epoch: self.header.configuration_epoch,
            original_view: self.header.original_view,
            op_number: self.header.op_number,
            previous_digest: self.header.previous_digest,
            kind: self.header.kind,
            body: &self.body,
        }
    }
}

pub(super) fn preencode(
    journal: &mut OpenGroupJournal,
    operations: Vec<SharedJournalOperation>,
) -> Result<PreencodedJournalGroup, DirectoryError> {
    journal
        .begin_group_encoding()?
        .encode_shared_raw(operations)
}
