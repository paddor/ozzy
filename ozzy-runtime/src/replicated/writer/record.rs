//! Small application inputs stay inline; large owned buffers remain reusable.

use bytes::Bytes;
use ozzy_proto::MessageId;
use smallvec::SmallVec;

/// One application record, independent of transport or broker grouping.
///
/// Single payloads up to 62 bytes are stored in transport form at once, so
/// admission moves them without another copy; up to 128 bytes stay inline
/// through SDK intake and grouping.
/// Larger single buffers remain shared until admission can reuse their unique
/// storage. Multipart descriptors use inline storage for two parts and
/// preserve empty parts and their ordering.
#[derive(Debug, Clone)]
pub struct RecordInput {
    /// Stable application identity, unchanged by transport retries.
    pub message_id: MessageId,
    body: InputBody,
    due_millis: Option<u64>,
}

#[derive(Debug, Clone)]
enum InputBody {
    /// Already the queued entry's payload: one copy, made by the caller.
    Packed(omq_tokio::message::Payload),
    Inline(InlineBody),
    Single(Bytes),
    Shared(Bytes),
    Multipart(SmallVec<[Bytes; 2]>),
}

#[derive(Debug, Clone)]
pub(super) struct InlineBody {
    bytes: [u8; RecordInput::INLINE_BYTES],
    len: u8,
}

impl InlineBody {
    pub(super) fn empty() -> Self {
        Self {
            bytes: [0; RecordInput::INLINE_BYTES],
            len: 0,
        }
    }
    pub(super) fn from_slice(bytes: &[u8]) -> Self {
        if let Ok(bytes) = <&[u8; RecordInput::INLINE_BYTES]>::try_from(bytes) {
            return Self {
                bytes: *bytes,
                len: RecordInput::INLINE_BYTES as u8,
            };
        }
        let mut body = Self::empty();
        body.bytes[..bytes.len()].copy_from_slice(bytes);
        body.len = bytes.len() as u8;
        body
    }
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
    /// Concatenate parts totalling `bytes`, at most `INLINE_BYTES`.
    fn concat(parts: &[Bytes], bytes: usize) -> Self {
        debug_assert!(bytes <= RecordInput::INLINE_BYTES);
        let mut body = Self::empty();
        let mut offset = 0;
        for part in parts {
            body.bytes[offset..offset + part.len()].copy_from_slice(part);
            offset += part.len();
        }
        debug_assert_eq!(offset, bytes);
        body.len = bytes as u8;
        body
    }
}

impl RecordInput {
    /// Selected shared SDK owners charge normalized backing, never arbitrary
    /// caller allocations hidden behind a short immutable byte view.
    pub(super) fn detach_shared(&mut self) {
        if let InputBody::Shared(bytes) = &mut self.body {
            self.body = Self::single(self.message_id, std::mem::take(bytes)).body;
        }
    }
    /// Largest single payload held directly inside an application input.
    pub const INLINE_BYTES: usize = 128;

    /// One part. Small payloads are detached into inline storage immediately.
    pub fn single(message_id: MessageId, bytes: Bytes) -> Self {
        if bytes.len() <= Self::INLINE_BYTES {
            return Self::copy_from_slice(message_id, &bytes);
        }
        Self {
            message_id,
            body: InputBody::Single(bytes),
            due_millis: None,
        }
    }

    /// Copy one part, without a heap allocation for payloads up to 128 bytes.
    pub fn copy_from_slice(message_id: MessageId, bytes: &[u8]) -> Self {
        let body = if bytes.len() <= omq_tokio::message::MAX_INLINE_PAYLOAD {
            InputBody::Packed(omq_tokio::message::Payload::from_slice(bytes))
        } else if bytes.len() <= Self::INLINE_BYTES {
            InputBody::Inline(InlineBody::from_slice(bytes))
        } else {
            InputBody::Single(Bytes::copy_from_slice(bytes))
        };
        Self {
            message_id,
            body,
            due_millis: None,
        }
    }

    /// One shared part. Retains the supplied backing allocation even for a
    /// small payload, allowing repeated immutable payloads to cross SDK intake
    /// without another byte copy.
    pub fn shared(message_id: MessageId, bytes: Bytes) -> Self {
        Self {
            message_id,
            body: InputBody::Shared(bytes),
            due_millis: None,
        }
    }

    /// Preserve ordered parts. An empty collection is rejected by writer
    /// admission; a single empty part is a valid record and still uses capacity.
    pub fn multipart(message_id: MessageId, parts: impl IntoIterator<Item = Bytes>) -> Self {
        let mut parts: SmallVec<[Bytes; 2]> = parts.into_iter().collect();
        if parts.len() == 1
            && let Some(part) = parts.pop()
        {
            return Self::single(message_id, part);
        }
        Self {
            message_id,
            body: InputBody::Multipart(parts),
            due_millis: None,
        }
    }

    /// Hold this record until `due_millis`, an absolute Unix time in
    /// milliseconds. Readers see it only once due. The topic must enable
    /// delay; the receipt offset is then the record's schedule sequence.
    #[must_use]
    pub fn with_due_millis(mut self, due_millis: u64) -> Self {
        self.due_millis = Some(due_millis);
        self
    }

    /// The absolute Unix due time in milliseconds of a scheduled record.
    pub const fn due_millis(&self) -> Option<u64> {
        self.due_millis
    }

    /// Borrow opaque parts in their original order, without materializing a table.
    #[inline]
    pub fn parts(&self) -> impl ExactSizeIterator<Item = &[u8]> + Clone {
        match &self.body {
            InputBody::Packed(payload) => Parts::Single(Some(payload.as_slice())),
            InputBody::Inline(body) => Parts::Single(Some(body.as_slice())),
            InputBody::Single(body) | InputBody::Shared(body) => Parts::Single(Some(body)),
            InputBody::Multipart(parts) => Parts::Multipart(parts.iter()),
        }
    }

    /// Part count and total payload bytes in one pass. `None` on overflow.
    #[inline]
    pub(super) fn shape(&self) -> (usize, Option<usize>) {
        match &self.body {
            InputBody::Packed(payload) => (1, Some(payload.len())),
            InputBody::Inline(body) => (1, Some(body.as_slice().len())),
            InputBody::Single(body) | InputBody::Shared(body) => (1, Some(body.len())),
            InputBody::Multipart(parts) => (
                parts.len(),
                parts
                    .iter()
                    .try_fold(0_usize, |sum, part| sum.checked_add(part.len())),
            ),
        }
    }

    /// Move the caller's inline bytes into SDK intake without transport packing.
    pub(super) fn take_inline(&mut self) -> Option<InlineBody> {
        let InputBody::Inline(body) = &mut self.body else {
            return None;
        };
        let taken = body.clone();
        // Only the visible length needs clearing; the copied bytes remain
        // initialized, and the input exposes no payload after admission.
        body.len = 0;
        Some(taken)
    }

    /// Move the `bytes`-long payload out, leaving an empty body. Called only
    /// after validation and successful capacity admission. Dropping
    /// caller-owned parts here can return other slots to this writer;
    /// admission holds no shared lock while running these destructors.
    pub(super) fn take_payload(&mut self, bytes: usize) -> omq_tokio::message::Payload {
        use omq_tokio::message::Payload;
        match std::mem::replace(&mut self.body, InputBody::Inline(InlineBody::empty())) {
            InputBody::Packed(payload) => payload,
            InputBody::Inline(body) => Payload::from_slice(body.as_slice()),
            InputBody::Shared(body) => Payload::from_bytes(body),
            // Single inputs are larger than inline. Shared or custom-backed
            // slices are copied; unique storage is reused and trimmed so a
            // small slice cannot retain an unbounded backing allocation.
            InputBody::Single(body) => {
                let mut bytes = Vec::from(body);
                bytes.shrink_to_fit();
                Payload::from_bytes(Bytes::from(bytes))
            }
            // Never retain a large caller-owned backing for tiny parts.
            InputBody::Multipart(parts) if bytes <= Self::INLINE_BYTES => {
                Payload::from_slice(InlineBody::concat(&parts, bytes).as_slice())
            }
            InputBody::Multipart(parts) => {
                let mut body = Vec::with_capacity(bytes);
                for part in &parts {
                    body.extend_from_slice(part);
                }
                Payload::from_bytes(Bytes::from(body))
            }
        }
    }
}

#[derive(Clone)]
enum Parts<'a> {
    Single(Option<&'a [u8]>),
    Multipart(std::slice::Iter<'a, Bytes>),
}

impl<'a> Iterator for Parts<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(bytes) => bytes.take(),
            Self::Multipart(parts) => parts.next().map(Bytes::as_ref),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = match self {
            Self::Single(bytes) => usize::from(bytes.is_some()),
            Self::Multipart(parts) => parts.len(),
        };
        (size, Some(size))
    }
}
impl ExactSizeIterator for Parts<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_shared_writer_detaches_foreign_backing_without_changing_parts() {
        for size in [0, 2, 63, 128, 129, 256] {
            let owner = Bytes::from(vec![7; 1024 * 1024]);
            let pointer = owner.as_ptr();
            let mut record =
                RecordInput::shared(MessageId::from_bytes([1; 16]), owner.slice(..size));
            record.detach_shared();
            let payload = record.take_payload(size);
            assert_eq!(payload.as_slice(), vec![7; size]);
            if size != 0 {
                assert_ne!(payload.as_slice().as_ptr(), pointer);
            }
        }
    }

    #[test]
    fn tiny_payloads_stay_inline_and_inputs_fit_within_256_bytes() {
        assert!(std::mem::size_of::<RecordInput>() <= 256);
        for size in [0, 1, 55, 62, 63, 128] {
            let input = RecordInput::copy_from_slice(MessageId::new(), &vec![7; size]);
            let packed = size <= omq_tokio::message::MAX_INLINE_PAYLOAD;
            assert_eq!(matches!(input.body, InputBody::Packed(_)), packed);
            assert_eq!(matches!(input.body, InputBody::Inline(_)), !packed);
            let copied = input.clone();
            assert_eq!(copied.parts().next().unwrap(), vec![7; size]);
            let mut input = RecordInput::single(MessageId::new(), Bytes::from(vec![7; size]));
            assert_eq!(matches!(input.body, InputBody::Packed(_)), packed);
            let payload = input.take_payload(size);
            assert_eq!(payload.as_slice(), vec![7; size]);
            assert_eq!(input.parts().next().unwrap(), b"");
        }
    }

    #[test]
    fn multipart_shapes_preserve_empty_parts_across_inline_and_heap_packing() {
        for size in [0, 8, 128, 1024] {
            for count in [1, 2, 4, 17] {
                let mut input = RecordInput::multipart(
                    MessageId::new(),
                    (0..count)
                        .map(|i| Bytes::from(vec![i as u8; if i % 2 == 0 { size } else { 0 }])),
                );
                let expected: Vec<u8> = input.parts().flatten().copied().collect();
                assert_eq!(input.parts().len(), count);
                assert_eq!(input.take_payload(expected.len()).as_slice(), expected);
                assert_eq!(input.parts().flatten().count(), 0);
            }
        }
    }

    #[test]
    fn explicitly_shared_small_payload_keeps_its_backing() {
        let bytes = Bytes::from_static(b"same payload");
        let pointer = bytes.as_ptr();
        let mut input = RecordInput::shared(MessageId::new(), bytes);
        let payload = input.take_payload(12);
        assert_eq!(payload.as_slice(), b"same payload");
        assert_eq!(payload.as_slice().as_ptr(), pointer);
    }
}
