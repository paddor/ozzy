use ozzy_journal::operation::OperationHeader;

use super::Digest;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PackedOperation {
    pub(crate) header: OperationHeader,
    pub(crate) body_bytes: usize,
    pub(crate) body_digest: Digest,
}
