//! Record-level views of one validated append batch: descriptor-only
//! iteration, borrowed records, and their multipart payloads.

use std::iter::FusedIterator;

use ozzy_proto::MessageId;

use crate::operation::{
    AppendTotals, OperationCodecError, OperationLimits, ReadFields, Reader, TinyTable,
    enforce_limit, read_descriptor_head, validate_append_descriptors, validate_limits,
    validate_tiny_descriptors,
};

/// Descriptor-only iterator, independent of raw or whole-batch payload storage.
#[derive(Debug, Clone)]
pub struct AppendRecordDescriptors<'a> {
    descriptors: Reader<'a>,
    lengths: Option<Reader<'a>>,
    remaining: usize,
}

impl<'a> AppendRecordDescriptors<'a> {
    pub(in crate::operation) const fn new(
        descriptors: &'a [u8],
        lengths: Option<&'a [u8]>,
        count: usize,
    ) -> Self {
        Self {
            descriptors: Reader::new(descriptors),
            lengths: match lengths {
                Some(lengths) => Some(Reader::new(lengths)),
                None => None,
            },
            remaining: count,
        }
    }

    /// Remaining canonical descriptor bytes; compact one-byte lengths are separate.
    pub fn remaining_bytes(&self) -> (&'a [u8], Option<&'a [u8]>) {
        (
            self.descriptors.rest(),
            self.lengths.as_ref().map(Reader::rest),
        )
    }
}

/// One record identity and its logical multipart lengths.
#[derive(Debug, Clone)]
pub struct AppendRecordDescriptor<'a> {
    /// Payload representation preserved through persistence and replay.
    pub encoding: ozzy_proto::data::Encoding,
    /// Application record identity preserved through retry and replay.
    pub message_id: MessageId,
    /// Logical multipart lengths, available without payload decoding.
    pub part_lengths: AppendPartLengths<'a>,
}

/// Exact logical part lengths without payload access.
#[derive(Debug, Clone)]
pub struct AppendPartLengths<'a> {
    lengths: std::slice::Iter<'a, [u8; 4]>,
    single: Option<usize>,
}

impl Iterator for AppendPartLengths<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.single.take().or_else(|| {
            self.lengths
                .next()
                .map(|length| u32::from_be_bytes(*length) as usize)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let count = self.lengths.len() + usize::from(self.single.is_some());
        (count, Some(count))
    }
}

impl ExactSizeIterator for AppendPartLengths<'_> {}
impl FusedIterator for AppendPartLengths<'_> {}

impl<'a> Iterator for AppendRecordDescriptors<'a> {
    type Item = AppendRecordDescriptor<'a>;

    #[expect(
        clippy::expect_used,
        reason = "the validating walk accepted every descriptor"
    )]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let descriptor = Descriptor::read(&mut self.descriptors, self.lengths.as_mut())
            .expect("validated descriptor");
        self.remaining -= 1;
        Some(AppendRecordDescriptor {
            encoding: descriptor.encoding,
            message_id: descriptor.message_id,
            part_lengths: AppendPartLengths {
                lengths: descriptor.lengths.iter(),
                single: descriptor.single,
            },
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for AppendRecordDescriptors<'_> {}
impl FusedIterator for AppendRecordDescriptors<'_> {}

/// Borrowed record iterator. Cloning it copies cursor state, not payloads.
#[derive(Debug, Clone)]
pub struct AppendRecords<'a> {
    descriptors: Reader<'a>,
    lengths: Option<Reader<'a>>,
    payload: Reader<'a>,
    remaining: usize,
}

impl<'a> AppendRecords<'a> {
    pub(in crate::operation) const fn with_lengths(
        descriptors: &'a [u8],
        lengths: Option<&'a [u8]>,
        payload: &'a [u8],
        count: usize,
    ) -> Self {
        Self {
            lengths: match lengths {
                Some(lengths) => Some(Reader::new(lengths)),
                None => None,
            },
            descriptors: Reader::new(descriptors),
            payload: Reader::new(payload),
            remaining: count,
        }
    }

    /// Borrow a fragment whose spans an owner's read index recorded during the
    /// validating walk at admission. No descriptor is checked again, so the
    /// spans must come from that index, never from a client or a file.
    pub const fn indexed(
        descriptors: &'a [u8],
        lengths: Option<&'a [u8]>,
        payload: &'a [u8],
        count: usize,
    ) -> Self {
        Self::with_lengths(descriptors, lengths, payload, count)
    }

    /// Remaining canonical descriptor and payload bytes. These borrowed slices
    /// let an owner record byte positions while admitting immutable history.
    pub fn tiny_lengths(&self) -> Option<&'a [u8]> {
        self.lengths.as_ref().map(Reader::rest)
    }

    /// Remaining ID/descriptors and payload; compact lengths are in `tiny_lengths`.
    pub fn remaining_bytes(&self) -> (&'a [u8], &'a [u8]) {
        (self.descriptors.rest(), self.payload.rest())
    }
}

/// Decode exactly one indexed record fragment, without inspecting preceding or
/// following records. Owners must separately establish the source operation's
/// identity, confirmation and partition metadata. Index offsets are not proof:
/// all selected descriptors and the exact payload length are checked here.
pub fn decode_append_records<'a>(
    descriptors: &'a [u8],
    payload: &'a [u8],
    count: usize,
    limits: OperationLimits,
) -> Result<AppendRecords<'a>, OperationCodecError> {
    decode_packed_append_records(descriptors, None, payload, count, limits)
}

/// Validate an indexed fragment, including its optional compact length run.
pub fn decode_packed_append_records<'a>(
    descriptors: &'a [u8],
    lengths: Option<&'a [u8]>,
    payload: &'a [u8],
    count: usize,
    limits: OperationLimits,
) -> Result<AppendRecords<'a>, OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("append record count", count, limits.max_records)?;
    if count == 0 {
        return Err(OperationCodecError::EmptyAppend);
    }
    let bytes = descriptors
        .len()
        .checked_add(lengths.map_or(0, <[u8]>::len))
        .and_then(|n| n.checked_add(payload.len()))
        .ok_or(OperationCodecError::LengthOverflow)?;
    enforce_limit("operation body bytes", bytes, limits.max_body_bytes)?;
    let mut decoder = Reader::new(descriptors);
    let mut totals = AppendTotals::new(limits);
    let payload_bytes = if let Some(lengths) = lengths {
        let table = TinyTable::new(descriptors, lengths, count)?;
        validate_tiny_descriptors(table, &mut totals, None)?.0
    } else {
        let bytes = validate_append_descriptors(&mut decoder, count, &mut totals, None)?.0;
        decoder.finish()?;
        bytes
    };
    let mut data = Reader::new(payload);
    data.take(payload_bytes)?;
    data.finish()?;
    Ok(AppendRecords::with_lengths(
        descriptors,
        lengths,
        payload,
        count,
    ))
}

impl<'a> Iterator for AppendRecords<'a> {
    type Item = AppendRecordView<'a>;

    #[expect(
        clippy::expect_used,
        reason = "the validating walk accepted every descriptor and payload length"
    )]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let record = self.read_record().expect("validated record");
        self.remaining -= 1;
        Some(record)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for AppendRecords<'_> {}
impl FusedIterator for AppendRecords<'_> {}

impl<'a> AppendRecords<'a> {
    fn read_record(&mut self) -> Result<AppendRecordView<'a>, OperationCodecError> {
        let descriptor = Descriptor::read(&mut self.descriptors, self.lengths.as_mut())?;
        let payload = self.payload.take(descriptor.payload_bytes())?;
        Ok(AppendRecordView {
            encoding: descriptor.encoding,
            message_id: descriptor.message_id,
            parts: AppendParts {
                lengths: descriptor.lengths.iter(),
                single: descriptor.single.map(|_| payload),
                payload: Reader::new(payload),
            },
        })
    }
}

/// One record's identity, encoding, and part lengths.
struct Descriptor<'a> {
    message_id: MessageId,
    encoding: ozzy_proto::data::Encoding,
    /// The one raw part length of a compact table entry.
    single: Option<usize>,
    lengths: &'a [[u8; 4]],
}

impl<'a> Descriptor<'a> {
    /// Read the next entry of a general table, or of a compact table when its
    /// one-byte lengths are supplied.
    fn read(
        descriptors: &mut Reader<'a>,
        tiny_lengths: Option<&mut Reader<'a>>,
    ) -> Result<Self, OperationCodecError> {
        if let Some(lengths) = tiny_lengths {
            return Ok(Self {
                message_id: MessageId::from_bytes(descriptors.id()?),
                encoding: ozzy_proto::data::Encoding::Raw,
                single: Some(usize::from(lengths.u8()?)),
                lengths: &[],
            });
        }
        let head = read_descriptor_head(descriptors)?;
        let lengths = descriptors.take(head.parts * 4)?.as_chunks::<4>().0;
        Ok(Self {
            message_id: MessageId::from_bytes(head.id),
            encoding: head.encoding,
            single: None,
            lengths,
        })
    }

    fn payload_bytes(&self) -> usize {
        self.single.unwrap_or_else(|| {
            self.lengths
                .iter()
                .map(|length| u32::from_be_bytes(*length) as usize)
                .sum()
        })
    }
}

/// One record with independently iterable borrowed parts.
#[derive(Debug, Clone)]
pub struct AppendRecordView<'a> {
    /// Payload representation, preserved through persistence and replay.
    pub encoding: ozzy_proto::data::Encoding,
    /// Stable message identity, not proof of an applied operation.
    pub message_id: MessageId,
    /// At least one part; a part may be empty.
    pub parts: AppendParts<'a>,
}

/// Ordered multipart payload. No allocation, coalescing, or payload copy.
#[derive(Debug, Clone)]
pub struct AppendParts<'a> {
    lengths: std::slice::Iter<'a, [u8; 4]>,
    single: Option<&'a [u8]>,
    payload: Reader<'a>,
}

impl AppendParts<'_> {
    /// Sum of the remaining opaque part lengths, excluding all descriptors.
    pub fn payload_bytes(&self) -> usize {
        self.payload.rest().len()
    }
}

impl<'a> Iterator for AppendParts<'a> {
    type Item = &'a [u8];

    #[expect(
        clippy::expect_used,
        reason = "a record's payload is exactly the sum of its part lengths"
    )]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(payload) = self.single.take() {
            // The single part is the whole payload.
            self.payload = Reader::new(&[]);
            return Some(payload);
        }
        let length = u32::from_be_bytes(*self.lengths.next()?) as usize;
        Some(self.payload.take(length).expect("validated part payload"))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.lengths.len() + usize::from(self.single.is_some());
        (n, Some(n))
    }
}

impl ExactSizeIterator for AppendParts<'_> {}
impl FusedIterator for AppendParts<'_> {}
