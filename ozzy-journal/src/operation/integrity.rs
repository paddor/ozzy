//! Integrity of canonical bodies and logical operation identity.

use super::{CanonicalOperation, Digest, OperationHeader};
use crate::integrity::IntegrityHasher as Hasher;

const BODY_HASH_CONTEXT: &str = "ozzy journal canonical body v1";
const OPERATION_HASH_CONTEXT: &str = "ozzy journal logical operation v1";

/// Hash exact canonical body bytes independently of their disk representation.
pub fn canonical_body_digest(body: &[u8]) -> Digest {
    crate::integrity::hash(BODY_HASH_CONTEXT, body)
}

/// Start the canonical body's exact checksum domain for incremental processing.
/// Callers can bound each update and yield between chunks without changing bytes
/// or digest identity. An unfinished checksum is not validation evidence.
pub fn canonical_body_hasher() -> crate::integrity::IntegrityHasher {
    Hasher::new(BODY_HASH_CONTEXT)
}

/// Hash a body split across storage buffers, without concatenating it.
pub fn canonical_body_digest_parts<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> Digest {
    let mut hasher = canonical_body_hasher();
    for part in parts {
        hasher.update(part);
    }
    hasher.finish()
}

/// Hash one logical operation independently of disk padding and compression.
pub fn logical_operation_digest(operation: &CanonicalOperation<'_>) -> Digest {
    logical_operation_digest_with_body_digest(operation, canonical_body_digest(operation.body))
}

/// Hash one logical operation using an already verified canonical body digest.
pub fn logical_operation_digest_with_body_digest(
    operation: &CanonicalOperation<'_>,
    body_digest: Digest,
) -> Digest {
    operation.header().digest(body_digest)
}

impl OperationHeader {
    /// Hash logical identity and an already verified body digest.
    pub fn digest(&self, body_digest: Digest) -> Digest {
        let operation = self;
        let mut hasher = Hasher::new(OPERATION_HASH_CONTEXT);
        hasher.update(operation.group_id.as_bytes());
        hasher.update(&operation.configuration_epoch.to_be_bytes());
        hasher.update(&operation.original_view.to_be_bytes());
        hasher.update(&operation.op_number.to_be_bytes());
        hasher.update(&(operation.kind as u16).to_be_bytes());
        hasher.update(operation.previous_digest.as_bytes());
        hasher.update(body_digest.as_bytes());
        hasher.finish()
    }
}
