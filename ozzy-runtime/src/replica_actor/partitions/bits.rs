//! A fixed set of small numbers, one bit each.

#[derive(Debug)]
pub(super) struct Bits(Vec<u64>);

impl Bits {
    /// Room for numbers below `len`, none set.
    pub(super) fn new(len: usize) -> Self {
        Self(vec![0; len.div_ceil(64)])
    }

    pub(super) fn words(&mut self) -> &mut [u64] {
        &mut self.0
    }

    pub(super) fn set(&mut self, bit: usize) {
        self.0[bit / 64] |= 1 << (bit % 64);
    }

    pub(super) fn clear(&mut self, bit: usize) {
        self.0[bit / 64] &= !(1 << (bit % 64));
    }

    pub(super) fn test(&self, bit: usize) -> bool {
        self.0[bit / 64] & (1 << (bit % 64)) != 0
    }

    /// The first set number at or after `from`.
    pub(super) fn next(&self, from: usize) -> Option<usize> {
        let mut word = from / 64;
        let mut bits = *self.0.get(word)? & (u64::MAX << (from % 64));
        while bits == 0 {
            word += 1;
            bits = *self.0.get(word)?;
        }
        Some(word * 64 + bits.trailing_zeros() as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::Bits;

    #[test]
    fn next_finds_set_numbers_in_order_across_words() {
        let mut bits = Bits::new(200);
        assert_eq!(bits.next(0), None);
        for bit in [3, 63, 64, 199] {
            bits.set(bit);
        }
        assert!(bits.test(64) && !bits.test(65));
        assert_eq!(bits.next(0), Some(3));
        assert_eq!(bits.next(4), Some(63));
        assert_eq!(bits.next(64), Some(64));
        assert_eq!(bits.next(65), Some(199));
        assert_eq!(bits.next(200), None);
        bits.clear(64);
        assert_eq!(bits.next(64), Some(199));
        bits.words()[0] = 0;
        assert_eq!(bits.next(0), Some(199));
    }
}
