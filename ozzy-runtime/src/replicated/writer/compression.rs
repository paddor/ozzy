//! Adaptive whole-APPEND compression. Encoded bytes enter retry ownership once.

use std::sync::Arc;

use omq_tokio::message::{Payload, PayloadOwner};

use super::WriterError;

const CODEC_METADATA_BYTES: usize = 9;

pub(super) struct Compressor {
    codec: lz4rip::block::Compressor,
    slots: Vec<Arc<Slot>>,
}

struct Slot(Vec<u8>);

impl AsRef<[u8]> for Slot {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl PayloadOwner for Slot {
    fn release(self: Arc<Self>) {
        drop(self);
    }
}

impl Compressor {
    pub(super) fn new(slots: usize) -> Self {
        Self {
            codec: lz4rip::block::Compressor::new(),
            slots: (0..slots).map(|_| Arc::new(Slot(Vec::new()))).collect(),
        }
    }

    /// Return compressed group only when block plus codec metadata is smaller.
    pub(super) fn prepare(&mut self, body: &[u8]) -> Result<Option<Payload>, WriterError> {
        if body.len() < ozzy_proto::append::ADAPTIVE_LZ4_THRESHOLD {
            return Ok(None);
        }
        let started = crate::profiling::start();
        let maximum = lz4rip::get_maximum_output_size(body.len());
        let reusable = self
            .slots
            .iter_mut()
            .position(|slot| Arc::get_mut(slot).is_some());
        let mut fallback = Vec::new();
        let output = match reusable {
            Some(index) => {
                &mut Arc::get_mut(&mut self.slots[index])
                    .expect("unique compression slot")
                    .0
            }
            None => &mut fallback,
        };
        output.resize(maximum, 0);
        let Ok(encoded_bytes) = self.codec.compress_into(body, output) else {
            crate::profiling::finish(crate::profiling::Stage::SdkCompression, started);
            return Err(WriterError::Compression);
        };
        let packed = encoded_bytes
            .checked_add(CODEC_METADATA_BYTES)
            .is_some_and(|bytes| bytes < body.len());
        crate::profiling::compression(
            crate::profiling::CompressionSite::Sdk,
            packed,
            body.len(),
            encoded_bytes,
        );
        crate::profiling::finish(crate::profiling::Stage::SdkCompression, started);
        if !packed {
            output.clear();
            return Ok(None);
        }
        output.truncate(encoded_bytes);
        match reusable {
            Some(index) => Ok(Some(Payload::from_shared_owner(self.slots[index].clone()))),
            None => Ok(Some(Payload::from_bytes(bytes::Bytes::from(fallback)))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_and_incompressible_groups_fall_back_to_raw() {
        let mut compressor = Compressor::new(2);
        let threshold = ozzy_proto::append::ADAPTIVE_LZ4_THRESHOLD;
        let small = vec![7; threshold - 1];
        assert!(compressor.prepare(&small).unwrap().is_none());
        for size in [threshold, threshold + 1, 2 * threshold] {
            let encoded = compressor.prepare(&vec![7; size]).unwrap().unwrap();
            assert!(encoded.len() < size);
            assert_eq!(
                lz4rip::block::decompress(encoded.as_slice(), size).unwrap(),
                vec![7; size]
            );
        }
        let mut rng = 0x0917_5348_abcd_ef12_u64;
        let noise: Vec<u8> = (0..threshold * 2)
            .map(|_| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng as u8
            })
            .collect();
        assert!(compressor.prepare(&noise).unwrap().is_none());
    }
}
