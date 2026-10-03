//! Output ownership is independent of the canonical byte layout.

use super::OperationCodecError;

/// Reusable byte output with rollback. Implementations must append bytes in
/// order and preserve the prefix when truncating a failed operation.
pub trait OperationOutput {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn truncate(&mut self, len: usize);
    fn extend(&mut self, bytes: &[u8]) -> Result<(), OperationCodecError>;
    /// Payload writes are distinct from framing/metadata writes. A storage
    /// adapter may specialize this without changing the canonical codec.
    fn payload(&mut self, bytes: &[u8]) -> Result<(), OperationCodecError> {
        self.extend(bytes)
    }
}

impl OperationOutput for Vec<u8> {
    fn len(&self) -> usize {
        self.len()
    }

    fn truncate(&mut self, len: usize) {
        self.truncate(len);
    }

    fn extend(&mut self, bytes: &[u8]) -> Result<(), OperationCodecError> {
        self.extend_from_slice(bytes);
        Ok(())
    }
}
