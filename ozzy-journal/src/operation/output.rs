//! Output ownership is independent of the canonical byte layout.

use super::OperationCodecError;

/// Reusable byte output with rollback. Implementations must append bytes in
/// order and preserve the prefix when truncating a failed operation.
pub trait OperationOutput {
    /// Number of bytes already appended to this output.
    fn len(&self) -> usize;
    /// Whether this output contains no bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Restore the output prefix to this byte length.
    fn truncate(&mut self, len: usize);
    /// Append framing or metadata bytes in order; report allocation failure.
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
