//! Fixed-size binary events for measuring tiny individual records.

/// Result-row label describing the exact sixteen-byte binary event representation.
pub const BINARY_EVENT_CORPUS: &str = "16-byte binary event: u64 submission clock, u8 kind, u8 flags, u8 writer, u8 status, u32 value; integers big-endian";

/// The high byte of `number` identifies the writer; lower 56 bits are its sequence.
/// Returns a stack value, independent of JSON formatting or heap allocation.
pub fn binary_event(submitted: u64, number: u64) -> [u8; 16] {
    let mut event = [0; 16];
    event[..8].copy_from_slice(&submitted.to_be_bytes());
    event[8] = (number & 7) as u8;
    event[9] = ((number >> 3) & 15) as u8;
    event[10] = (number >> 56) as u8;
    event[11] = ((number >> 7) & 15) as u8;
    let value = (number as u32).wrapping_mul(0x9e37_79b9).rotate_left(13) ^ (number >> 32) as u32;
    event[12..].copy_from_slice(&value.to_be_bytes());
    event
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_event_preserves_clock_writer_and_varied_numeric_fields() {
        let clock = 0x0102_0304_0506_0708;
        let event = binary_event(clock, (3 << 56) | 0x81);
        assert_eq!(&event[..8], &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(&event[8..12], &[1, 0, 3, 1]);
        let mut values = std::collections::HashSet::new();
        for writer in 0..32 {
            for sequence in 0..128 {
                let number = (writer << 56) | sequence;
                let event = binary_event(clock, number);
                assert_eq!(event, binary_event(clock, number));
                assert_eq!(event[10], writer as u8);
                assert!(values.insert(event));
                assert_ne!(event, binary_event(clock + 1, number));
            }
        }
    }
}
