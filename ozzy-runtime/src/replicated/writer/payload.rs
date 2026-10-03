//! Payload ownership is independent of admission and confirmation credit.

use super::RecordInput;
#[cfg(test)]
use bytes::Bytes;
use omq_tokio::message::Payload;

#[derive(Debug)]
pub(super) struct Body(pub(super) Payload);

impl Body {
    pub(super) fn take(input: &mut RecordInput, bytes: usize) -> Self {
        Self(input.take_payload(bytes))
    }

    #[cfg(test)]
    pub(super) fn from_vec(bytes: Vec<u8>) -> Self {
        Self(Payload::from_bytes(Bytes::from(bytes)))
    }

    pub(super) fn bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub(super) fn len(&self) -> usize {
        self.bytes().len()
    }

    /// Large single-record requests share backing; tiny views remain inline.
    pub(super) fn payload(&self) -> &Payload {
        &self.0
    }
}
