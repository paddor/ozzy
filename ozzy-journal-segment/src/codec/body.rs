//! Entry body codecs: raw and LZ4 encoding and decoding.

use super::{CodecError, entry::ParsedEntry, span};
use std::borrow::Cow;

/// Body representation selected by one storage writer. Deferred writes collect
/// operation bodies into one shared block; synchronous writes encode separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyEncoding {
    /// Store canonical operation bodies without physical compression.
    Raw,
    /// Use physical LZ4 only when its encoded representation meets the savings bound.
    Lz4 {
        #[doc = "Minimum encoded byte savings required before physical compression is used."]
        min_savings_bytes: usize,
    },
}

impl BodyEncoding {
    /// Whether this build includes the requested encoder. This does not validate
    /// codec-specific settings or imply that existing stored bytes are readable.
    pub const fn is_supported(self) -> bool {
        match self {
            Self::Raw => true,
            Self::Lz4 { .. } => cfg!(feature = "lz4"),
        }
    }
}

pub(super) fn decode_entry_body<'a>(
    input: &'a [u8],
    entry: &ParsedEntry,
) -> Result<Cow<'a, [u8]>, CodecError> {
    let encoded = span(input, entry.body.clone(), "entry body")?;
    match entry.codec {
        0 => Ok(Cow::Borrowed(encoded)),
        1 => decode_lz4_body(encoded, entry.decoded_body_bytes).map(Cow::Owned),
        codec => Err(CodecError::UnsupportedCodec(codec)),
    }
}

#[cfg(feature = "lz4")]
pub(super) fn decode_lz4_body(input: &[u8], decoded_len: usize) -> Result<Vec<u8>, CodecError> {
    let mut output = vec![0_u8; decoded_len];
    let actual = lz4rip::decompress_into(input, &mut output)
        .map_err(|_| CodecError::DecompressionFailed("LZ4"))?;
    if actual != decoded_len {
        return Err(CodecError::DecompressionFailed("LZ4"));
    }
    Ok(output)
}

#[cfg(not(feature = "lz4"))]
pub(super) fn decode_lz4_body(_input: &[u8], _decoded_len: usize) -> Result<Vec<u8>, CodecError> {
    Err(CodecError::UnsupportedCodec(1))
}

#[derive(Debug)]
pub(super) struct EncodedBody<'a> {
    pub(super) bytes: Cow<'a, [u8]>,
    pub(super) codec: u16,
}

#[derive(Default)]
pub(crate) struct BodyEncodeScratch {
    #[cfg(feature = "lz4")]
    pub(super) joined: Vec<u8>,
    #[cfg(feature = "lz4")]
    lz4: Vec<u8>,
    #[cfg(feature = "lz4")]
    pub(super) lz4_compressor: lz4rip::block::Compressor,
}

impl std::fmt::Debug for BodyEncodeScratch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("BodyEncodeScratch");
        #[cfg(feature = "lz4")]
        debug
            .field("joined_capacity", &self.joined.capacity())
            .field("lz4_capacity", &self.lz4.capacity())
            .field("lz4_compressor", &self.lz4_compressor);
        debug.finish()
    }
}

impl BodyEncodeScratch {
    pub(crate) fn reserve(
        &mut self,
        body_capacity: usize,
        encoding: BodyEncoding,
    ) -> Result<(), CodecError> {
        match encoding {
            BodyEncoding::Raw => Ok(()),
            BodyEncoding::Lz4 { .. } => self.reserve_lz4(body_capacity),
        }
    }

    #[cfg(feature = "lz4")]
    fn reserve_lz4(&mut self, body_capacity: usize) -> Result<(), CodecError> {
        let output_capacity = lz4rip::get_maximum_output_size(body_capacity);
        if self.lz4.len() < output_capacity {
            self.lz4
                .try_reserve_exact(output_capacity - self.lz4.len())
                .map_err(|_| CodecError::CompressionScratchAllocation("LZ4"))?;
            self.lz4.resize(output_capacity, 0);
        }
        Ok(())
    }

    #[cfg(not(feature = "lz4"))]
    #[allow(
        clippy::unused_self,
        reason = "method call shape is shared with the feature-enabled implementation"
    )]
    fn reserve_lz4(&mut self, _body_capacity: usize) -> Result<(), CodecError> {
        Err(CodecError::UnsupportedCodec(1))
    }
}

pub(super) fn encode_body<'a>(
    input: &'a [u8],
    encoding: BodyEncoding,
    scratch: &'a mut BodyEncodeScratch,
) -> Result<EncodedBody<'a>, CodecError> {
    match encoding {
        BodyEncoding::Raw => Ok(EncodedBody {
            bytes: Cow::Borrowed(input),
            codec: 0,
        }),
        BodyEncoding::Lz4 { min_savings_bytes } => {
            encode_lz4_body(input, min_savings_bytes, scratch)
        }
    }
}

#[cfg(feature = "lz4")]
fn compressed_or_raw<'a>(
    input: &'a [u8],
    compressed: Cow<'a, [u8]>,
    codec: u16,
    min_savings_bytes: usize,
) -> EncodedBody<'a> {
    if compressed
        .len()
        .checked_add(min_savings_bytes)
        .is_some_and(|minimum| minimum <= input.len())
    {
        EncodedBody {
            bytes: compressed,
            codec,
        }
    } else {
        EncodedBody {
            bytes: Cow::Borrowed(input),
            codec: 0,
        }
    }
}

#[cfg(feature = "lz4")]
fn encode_lz4_body<'a>(
    input: &'a [u8],
    min_savings_bytes: usize,
    scratch: &'a mut BodyEncodeScratch,
) -> Result<EncodedBody<'a>, CodecError> {
    scratch.reserve_lz4(input.len())?;
    let length = scratch
        .lz4_compressor
        .compress_into(input, &mut scratch.lz4)
        .map_err(|_| CodecError::CompressionFailed("LZ4"))?;
    Ok(compressed_or_raw(
        input,
        Cow::Borrowed(
            scratch
                .lz4
                .get(..length)
                .ok_or(CodecError::CompressionFailed("LZ4"))?,
        ),
        1,
        min_savings_bytes,
    ))
}

#[cfg(not(feature = "lz4"))]
fn encode_lz4_body<'a>(
    _input: &'a [u8],
    _min_savings_bytes: usize,
    _scratch: &mut BodyEncodeScratch,
) -> Result<EncodedBody<'a>, CodecError> {
    Err(CodecError::UnsupportedCodec(1))
}
