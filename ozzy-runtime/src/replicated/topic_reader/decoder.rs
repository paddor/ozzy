//! Bounded record decompression at the reader boundary.

use super::ReaderError;
use bytes::Bytes;
#[cfg(test)]
use ozzy_proto::MessageId;
use ozzy_proto::data::{DataLimits, Encoding, OwnedRecord};

#[derive(Default)]
pub(crate) struct Decoder(lz4rip::block::Decompressor);
impl Decoder {
    pub(crate) fn decode(
        &mut self,
        record: OwnedRecord,
        limits: DataLimits,
    ) -> Result<OwnedRecord, ReaderError> {
        let Encoding::Lz4 { decoded_bytes } = record.encoding else {
            return Ok(record);
        };
        if !record.encoding.validate(
            record.payload.len(),
            limits.max_parts,
            limits.max_record_bytes,
        ) || record.payload[0].len() > limits.max_record_bytes
        {
            return Err(ReaderError::Response);
        }
        let body = &record.payload[0];
        let table_end = record
            .encoding
            .frame_offset(body, limits.max_parts, limits.max_record_bytes)
            .ok_or(ReaderError::Response)?;
        let mut decoded = vec![0; decoded_bytes as usize];
        let length = self
            .0
            .decompress_into(&body[table_end..], &mut decoded)
            .map_err(|_| ReaderError::Response)?;
        if length != decoded_bytes as usize {
            return Err(ReaderError::Response);
        }
        let decoded = Bytes::from(decoded);
        let mut offset = 0;
        let payload = body[4..table_end]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|n| {
                let start = offset;
                offset += u32::from_be_bytes(*n) as usize;
                decoded.slice(start..offset)
            })
            .collect();
        Ok(OwnedRecord {
            encoding: Encoding::Raw,
            message_id: record.message_id,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compressed(body: &[u8], claimed: u32) -> OwnedRecord {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&claimed.to_be_bytes());
        payload.extend_from_slice(&lz4rip::block::compress(body));
        OwnedRecord {
            encoding: Encoding::Lz4 {
                decoded_bytes: claimed,
            },
            message_id: MessageId::new(),
            payload: smallvec::smallvec![Bytes::from(payload)],
        }
    }

    #[test]
    fn bounded_decoder_rejects_length_lies_and_malformed_part_tables() {
        let limits = DataLimits {
            max_record_bytes: 4096,
            max_parts: 4,
            ..DataLimits::default()
        };
        let mut decoder = Decoder::default();
        assert!(decoder.decode(compressed(&[7; 4096], 32), limits).is_err());
        assert!(decoder.decode(compressed(&[7; 32], 4096), limits).is_err());
        assert!(
            decoder
                .decode(compressed(&[7; 4097], 4097), limits)
                .is_err()
        );
        for count in [0, 5, u32::MAX] {
            let mut record = compressed(&[7; 1024], 1024);
            let mut body = record.payload[0].to_vec();
            body[..4].copy_from_slice(&count.to_be_bytes());
            record.payload[0] = Bytes::from(body);
            assert!(decoder.decode(record, limits).is_err());
        }
        let mut record = compressed(&[7; 1024], 1024);
        record.payload[0] = record.payload[0].slice(..9);
        assert!(decoder.decode(record, limits).is_err());
        let restored = decoder
            .decode(compressed(&[7; 4096], 4096), limits)
            .unwrap();
        assert_eq!(restored.encoding, Encoding::Raw);
        assert_eq!(restored.payload[0].as_ref(), &[7; 4096]);
    }
}
