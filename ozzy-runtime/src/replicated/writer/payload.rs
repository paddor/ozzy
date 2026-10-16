//! Payload ownership is independent of admission and confirmation capacity.

use super::{RecordInput, record::InlineBody};
#[cfg(test)]
use bytes::Bytes;
use omq_tokio::message::Payload;

#[derive(Debug)]
pub(super) enum Body {
    Inline(InlineBody),
    Packed(Payload),
}

impl Body {
    pub(super) fn take(input: &mut RecordInput, bytes: usize) -> Self {
        match input.take_inline() {
            Some(body) => Self::Inline(body),
            None => Self::Packed(input.take_payload(bytes)),
        }
    }

    #[cfg(test)]
    pub(super) fn from_vec(bytes: Vec<u8>) -> Self {
        Self::Packed(Payload::from_bytes(Bytes::from(bytes)))
    }

    pub(super) fn bytes(&self) -> &[u8] {
        match self {
            Self::Inline(body) => body.as_slice(),
            Self::Packed(body) => body.as_slice(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.bytes().len()
    }

    /// Pack a singleton for transport once and share it. Multi-record packing
    /// borrows inline bytes directly, without a transport payload per record.
    pub(super) fn payload(&mut self) -> Payload {
        match self {
            Self::Packed(body) => body.clone(),
            Self::Inline(body) => {
                let packed = Payload::from_slice(body.as_slice());
                *self = Self::Packed(packed.clone());
                packed
            }
        }
    }
}
