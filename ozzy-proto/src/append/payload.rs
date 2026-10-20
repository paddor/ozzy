//! APPEND payload frame encoding: concatenated raw parts or one whole-group
//! LZ4 block, with block validation and bounded decoding.

use super::{CodecError, DataLimits};
use crate::layout::Reader;

/// Encoding of one complete APPEND payload frame.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum PayloadEncoding {
    /// Concatenated original record parts.
    #[default]
    Raw = 0,
    /// One raw LZ4 block over all concatenated record parts.
    Lz4 = 1,
}

/// Raw APPEND payload size where SDKs and brokers begin adaptive LZ4 packing.
/// Sparse writers are never delayed to reach this threshold.
pub const ADAPTIVE_LZ4_THRESHOLD: usize = 2 * 1024;

impl TryFrom<u8> for PayloadEncoding {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Lz4),
            _ => Err(CodecError::Profile),
        }
    }
}

/// Check LZ4 block syntax and exact decoded length without materializing bytes.
/// The fast path validates the common block layout. The reference validator
/// handles any valid form the fast path cannot recognize.
pub fn lz4_block_is_valid(input: &[u8], expected: usize) -> bool {
    fast_lz4_block_is_valid(input, expected)
        || lz4rip::block::validate_block(input, expected).is_ok()
}

fn fast_lz4_block_is_valid(input: &[u8], expected: usize) -> bool {
    lz4_sequences(input, expected).is_some()
}

/// Walk an LZ4 block's sequences. `Some` only when every literal run and
/// match fits and the block decodes to exactly `expected` bytes.
fn lz4_sequences(input: &[u8], expected: usize) -> Option<()> {
    let mut rest = input;
    let mut produced = 0_usize;
    loop {
        let (&token, tail) = rest.split_first()?;
        rest = tail;
        let literals = lz4_length(&mut rest, token >> 4)?;
        rest = rest.get(literals..)?;
        produced = produced.checked_add(literals).filter(|&n| n <= expected)?;
        if rest.is_empty() {
            return (produced == expected).then_some(());
        }
        let (offset, tail) = rest.split_first_chunk::<2>()?;
        rest = tail;
        let offset = usize::from(u16::from_le_bytes(*offset));
        if offset == 0 || offset > produced {
            return None;
        }
        // Every match copies at least four bytes beyond its nibble.
        let matched = lz4_length(&mut rest, token & 15)?.checked_add(4)?;
        produced = produced.checked_add(matched).filter(|&n| n <= expected)?;
    }
}

/// One 4-bit LZ4 length, extended by following bytes while they read 255.
fn lz4_length(rest: &mut &[u8], nibble: u8) -> Option<usize> {
    let mut length = usize::from(nibble);
    if nibble == 15 {
        loop {
            let (&extra, tail) = rest.split_first()?;
            *rest = tail;
            length = length.checked_add(usize::from(extra))?;
            if extra != 255 {
                break;
            }
        }
    }
    Some(length)
}

pub(crate) fn encode_payload_encoding(
    output: &mut Vec<u8>,
    encoding: PayloadEncoding,
    decoded_bytes: usize,
) -> Result<(), CodecError> {
    output.push(encoding as u8);
    output.extend_from_slice(
        &u32::try_from(decoded_bytes)
            .map_err(|_| CodecError::Limit)?
            .to_be_bytes(),
    );
    Ok(())
}

pub(crate) fn validate_payload_encoding(
    encoding: PayloadEncoding,
    decoded_bytes: usize,
    encoded_bytes: usize,
    limits: DataLimits,
) -> Result<(), CodecError> {
    if decoded_bytes > limits.envelope.max_payload_bytes {
        return Err(CodecError::Limit);
    }
    match encoding {
        PayloadEncoding::Raw if encoded_bytes == decoded_bytes => Ok(()),
        PayloadEncoding::Lz4 if encoded_bytes != 0 => Ok(()),
        _ => Err(CodecError::Length),
    }
}

/// Read the payload encoding and decoded byte count, checked against a payload
/// frame of `encoded_bytes`.
pub(crate) fn read_payload_encoding(
    cursor: &mut Reader<'_>,
    encoded_bytes: usize,
    limits: DataLimits,
) -> Result<(PayloadEncoding, usize), CodecError> {
    let encoding = PayloadEncoding::try_from(cursor.u8()?)?;
    let decoded_bytes = cursor.u32()? as usize;
    validate_payload_encoding(encoding, decoded_bytes, encoded_bytes, limits)?;
    Ok((encoding, decoded_bytes))
}

pub(crate) fn decode_payload_encoding<'a>(
    cursor: &mut Reader<'a>,
    encoded: &'a [u8],
    scratch: &'a mut Vec<u8>,
    limits: DataLimits,
) -> Result<(PayloadEncoding, &'a [u8]), CodecError> {
    let (encoding, decoded_bytes) = read_payload_encoding(cursor, encoded.len(), limits)?;
    if encoding == PayloadEncoding::Raw {
        return Ok((encoding, encoded));
    }
    scratch.clear();
    scratch
        .try_reserve(decoded_bytes)
        .map_err(|_| CodecError::Limit)?;
    let actual = lz4rip::block::decompress_append(encoded, scratch, decoded_bytes)
        .map_err(|_| CodecError::Length)?;
    if actual != decoded_bytes {
        return Err(CodecError::Length);
    }
    Ok((encoding, scratch))
}

/// Owned payload of a raw or whole-group LZ4 frame: the received `frame`, or
/// its bytes decompressed into a fresh allocation.
pub(crate) fn decode_owned_payload(
    cursor: &mut Reader<'_>,
    frame: &bytes::Bytes,
    limits: DataLimits,
) -> Result<(PayloadEncoding, bytes::Bytes), CodecError> {
    let (encoding, decoded_bytes) = read_payload_encoding(cursor, frame.len(), limits)?;
    if encoding == PayloadEncoding::Raw {
        return Ok((encoding, frame.clone()));
    }
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(decoded_bytes)
        .map_err(|_| CodecError::Limit)?;
    let actual = lz4rip::block::decompress_append(frame, &mut decoded, decoded_bytes)
        .map_err(|_| CodecError::Length)?;
    if actual != decoded_bytes {
        return Err(CodecError::Length);
    }
    Ok((encoding, bytes::Bytes::from(decoded)))
}

#[cfg(test)]
mod owned_payload_tests {
    use super::*;

    fn decode(
        body: &[u8],
        decoded_bytes: usize,
    ) -> Result<(PayloadEncoding, bytes::Bytes), CodecError> {
        let mut header = Vec::new();
        encode_payload_encoding(&mut header, PayloadEncoding::Lz4, decoded_bytes)?;
        let frame = bytes::Bytes::from(lz4rip::block::compress(body));
        decode_owned_payload(&mut Reader::new(&header), &frame, DataLimits::default())
    }

    #[test]
    fn retained_payloads_stay_intact_and_lengths_must_match() {
        let (encoding, first) = decode(&[1; 4096], 4096).unwrap();
        assert_eq!(encoding, PayloadEncoding::Lz4);
        let (_, second) = decode(&[2; 4096], 4096).unwrap();
        assert_ne!(first.as_ptr(), second.as_ptr());
        assert!(first.iter().all(|&byte| byte == 1));
        assert!(second.iter().all(|&byte| byte == 2));
        assert!(decode(&[4; 4096], 4097).is_err());
    }
}

#[cfg(test)]
mod lz4_validation_tests {
    use super::*;

    fn equivalent(input: &[u8], decoded_bytes: usize) {
        let reference = lz4rip::block::validate_block(input, decoded_bytes).is_ok();
        assert_eq!(fast_lz4_block_is_valid(input, decoded_bytes), reference);
        assert_eq!(lz4_block_is_valid(input, decoded_bytes), reference);
    }

    #[test]
    fn fast_validator_agrees_on_blocks_and_mutations() {
        let mut state = 0x85a3_08d3_2f6e_5c41_u64;
        for decoded_bytes in [0, 1, 15, 16, 256, 4096, 851_968] {
            let mut raw = Vec::with_capacity(decoded_bytes);
            for _ in 0..decoded_bytes {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                raw.push(b'a' + (state % 26) as u8);
            }
            let block = lz4rip::block::compress(&raw);
            equivalent(&block, decoded_bytes);
            equivalent(&block, decoded_bytes.saturating_add(1));
            if decoded_bytes > 0 {
                equivalent(&block, decoded_bytes - 1);
            }
            for _ in 0..100 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let mut changed = block.clone();
                let index = state as usize % changed.len();
                changed[index] ^= ((state >> 32) as u8) | 1;
                equivalent(&changed, decoded_bytes);
                equivalent(&changed, decoded_bytes.saturating_add(1));
            }
        }
    }

    #[test]
    fn fast_validator_agrees_on_arbitrary_input() {
        let mut state = 0x636e_38a2_9d71_4b52_u64;
        let mut bytes = [0_u8; 96];
        for _ in 0..10_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let length = state as usize % (bytes.len() + 1);
            for byte in &mut bytes[..length] {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            equivalent(&bytes[..length], (state >> 32) as usize % 256);
        }
        equivalent(&[0x10, b'a', 1, 0, 0], usize::MAX);
    }

    #[test]
    fn fast_validator_agrees_on_short_blocks_and_long_matches() {
        for first in 0..=u8::MAX {
            for second in 0..=u8::MAX {
                let input = [first, second];
                for expected in [0, 1, 4, 19, 20, 255, 256, usize::MAX] {
                    equivalent(&input, expected);
                }
            }
        }
        let block = lz4rip::block::compress(&vec![b'x'; 851_968]);
        equivalent(&block, 851_968);
        for cut in (0..block.len()).step_by(17) {
            equivalent(&block[..cut], 851_968);
        }

        let mut literals = vec![0xf0, 255, 255, 255, 244];
        literals.extend_from_slice(&[b'x'; 1024]);
        equivalent(&literals, 1024);
        equivalent(&literals[..literals.len() - 1], 1024);
        equivalent(&literals, 1025);
    }
}
