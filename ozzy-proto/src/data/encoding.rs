//! Payload encoding is record metadata, never inferred from user bytes.

/// Representation stored and replicated unchanged by brokers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Encoding {
    /// Original multipart bytes.
    #[default]
    Raw,
    /// An uncompressed original-part table followed by one LZ4 block.
    Lz4 {
        /// Exact sum of original payload part lengths.
        decoded_bytes: u32,
    },
}

impl Encoding {
    /// Extra descriptor bytes following the tagged part count.
    pub const fn metadata_bytes(self) -> usize {
        match self {
            Self::Raw => 0,
            Self::Lz4 { .. } => 4,
        }
    }

    /// Upper byte of the descriptor part count identifies the encoding.
    pub const fn tag(self) -> u32 {
        match self {
            Self::Raw => 0,
            Self::Lz4 { .. } => 1 << 24,
        }
    }

    /// Validate metadata without decoding payload bytes. Encoded records have
    /// one transport part; both claimed output and input remain bounded.
    pub fn validate(self, parts: usize, max_parts: usize, max_bytes: usize) -> bool {
        match self {
            Self::Raw => parts > 0 && parts <= max_parts && parts < (1 << 24),
            Self::Lz4 { decoded_bytes } => {
                parts == 1
                    && max_parts > 0
                    && decoded_bytes > 0
                    && (decoded_bytes as usize) <= max_bytes
            }
        }
    }
}

impl Encoding {
    /// Validate the original part table without decompressing. Returns the
    /// compressed frame's offset. Raw records have no such table.
    pub fn frame_offset(self, body: &[u8], max_parts: usize, max_bytes: usize) -> Option<usize> {
        let Self::Lz4 { decoded_bytes } = self else {
            return Some(0);
        };
        if decoded_bytes as usize > max_bytes {
            return None;
        }
        let count = u32::from_be_bytes(body.get(..4)?.try_into().ok()?) as usize;
        if count == 0 || count > max_parts {
            return None;
        }
        let end = count.checked_mul(4)?.checked_add(4)?;
        let table = body.get(4..end)?;
        let total = table.as_chunks::<4>().0.iter().try_fold(0usize, |n, p| {
            n.checked_add(u32::from_be_bytes(*p) as usize)
        })?;
        (total == decoded_bytes as usize && body.len() > end).then_some(end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MessageId;
    use crate::data::{DataLimits, Record, decode_records, encode_records, records_size};

    #[test]
    fn explicit_tag_roundtrips_and_broker_bounds_claim_without_decompressing() {
        // Deliberately invalid LZ4 block: broker validates metadata only.
        let mut body = Vec::new();
        body.extend_from_slice(&3u32.to_be_bytes());
        for size in [0u32, 4096, 0] {
            body.extend_from_slice(&size.to_be_bytes());
        }
        body.push(0xff);
        let encoding = Encoding::Lz4 {
            decoded_bytes: 4096,
        };
        let parts = [body.as_slice()];
        let record = Record {
            encoding,
            message_id: MessageId::new(),
            parts: &parts,
        };
        let limits = DataLimits::default();
        let mut metadata = Vec::new();
        let mut payload = Vec::new();
        assert!(records_size(&[record], 0, limits).is_ok());
        encode_records(&[record], &mut metadata, Some(&mut payload));
        let decoded = decode_records(&metadata, &payload, 0, limits)
            .unwrap()
            .iter()
            .next()
            .unwrap();
        assert_eq!(decoded.encoding, encoding);
        assert_eq!(decoded.parts.collect::<Vec<_>>(), parts);
        for smaller in [
            DataLimits {
                max_parts: 2,
                ..limits
            },
            DataLimits {
                max_record_bytes: 4095,
                ..limits
            },
        ] {
            assert!(records_size(&[record], 0, smaller).is_err());
            assert!(decode_records(&metadata, &payload, 0, smaller).is_err());
        }
        metadata[20] = 2; // Unknown codec in tagged part count.
        assert!(decode_records(&metadata, &payload, 0, limits).is_err());
        metadata[20] = 1;
        payload[4..8].copy_from_slice(&1u32.to_be_bytes()); // Part sum no longer matches claim.
        assert!(decode_records(&metadata, &payload, 0, limits).is_err());
    }
}
