//! Noncryptographic integrity profile shared by storage and replication.
//!
//! Each domain uses XXH3-64 of its UTF-8 context as the XXH3-128 seed.
//! Results occupy the first 16 bytes of a fixed 32-byte digest slot in network
//! byte order; the remaining bytes are zero. This is 128-bit error detection,
//! not authentication or protection against adversarial collision construction.

use std::fmt;

use xxhash_rust::xxh3::{Xxh3, xxh3_64, xxh3_128_with_seed};

use crate::operation::Digest;

/// Immutable configuration identifier for the XXH3-128 integrity profile.
pub const PROFILE: u8 = 2;

/// Allocation-free streaming checksum with an explicit object domain.
#[derive(Clone)]
pub struct IntegrityHasher(Xxh3);

impl fmt::Debug for IntegrityHasher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IntegrityHasher")
            .finish_non_exhaustive()
    }
}

impl IntegrityHasher {
    /// Start a checksum using the exact format-specific context string.
    pub fn new(context: &str) -> Self {
        Self(Xxh3::with_seed(xxh3_64(context.as_bytes())))
    }

    /// Add the next bytes. Chunk boundaries do not affect the result.
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// Read the checksum without consuming or resetting the stream.
    pub fn finish(&self) -> Digest {
        digest(self.0.digest128())
    }
}

/// Checksum one complete object without allocating a streaming state.
pub fn hash(context: &str, bytes: &[u8]) -> Digest {
    digest(xxh3_128_with_seed(bytes, xxh3_64(context.as_bytes())))
}

fn digest(value: u128) -> Digest {
    let mut bytes = [0; 32];
    bytes[..16].copy_from_slice(&value.to_be_bytes());
    Digest::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_shot_and_chunked_hashes_match_at_algorithm_boundaries() {
        let input: Vec<_> = (0..1024 * 1024).map(|index| index as u8).collect();
        for size in [0, 1, 15, 16, 127, 128, 239, 240, 241, 1024, input.len()] {
            let expected = hash("test domain", &input[..size]);
            let mut stream = IntegrityHasher::new("test domain");
            for chunk in input[..size].chunks(137) {
                stream.update(chunk);
            }
            assert_eq!(stream.finish(), expected, "size {size}");
            assert_eq!(&expected.as_bytes()[16..], &[0; 16]);
            assert_ne!(expected, hash("different domain", &input[..size]));
            let mut continued = stream.clone();
            continued.update(b"suffix");
            assert_ne!(continued.finish(), expected);
            assert_eq!(stream.finish(), expected);
        }
    }

    #[test]
    fn both_halves_of_the_128_bit_result_are_retained() {
        let expected = xxh3_128_with_seed(b"payload", xxh3_64(b"domain"));
        let actual = hash("domain", b"payload");
        assert_eq!(&actual.as_bytes()[..16], &expected.to_be_bytes());
        assert_ne!(&actual.as_bytes()[..8], &[0; 8]);
        assert_ne!(&actual.as_bytes()[8..16], &[0; 8]);
    }
}
