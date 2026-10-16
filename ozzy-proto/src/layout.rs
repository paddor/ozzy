//! Bounds-checked big-endian fields for wire and file layouts.
//!
//! [`Reader`] and [`Writer`] walk counted layouts and fail with a
//! [`LayoutError`] where the bytes run out; each codec converts that into its
//! own error. [`Fixed`] reads and writes the fields of a fixed-size record at
//! offsets checked when the code is built, so a record whose length was checked
//! once needs no further checks.

/// A layout that does not fit its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LayoutError {
    /// The input ends before a field.
    #[error("layout is truncated: need {needed} bytes, have {available}")]
    Truncated {
        /// Input length the field needs.
        needed: usize,
        /// Input length.
        available: usize,
    },
    /// Bytes remain after the last field.
    #[error("layout has {0} trailing bytes")]
    Trailing(usize),
    /// The output cannot hold a field.
    #[error("layout overflows its output: need {needed} bytes, have {capacity}")]
    Overflow {
        /// Output length the field needs.
        needed: usize,
        /// Output length.
        capacity: usize,
    },
}

/// Sequential reader over borrowed bytes.
#[derive(Debug, Clone, Copy)]
pub struct Reader<'a> {
    rest: &'a [u8],
    len: usize,
}

impl<'a> Reader<'a> {
    /// Read `input` from its start.
    #[must_use]
    pub const fn new(input: &'a [u8]) -> Self {
        Self {
            rest: input,
            len: input.len(),
        }
    }

    /// Bytes consumed so far.
    #[must_use]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "rest is a suffix of the original input"
    )]
    pub const fn offset(&self) -> usize {
        self.len - self.rest.len()
    }

    /// Bytes not yet consumed.
    #[must_use]
    pub const fn rest(&self) -> &'a [u8] {
        self.rest
    }

    /// The next `count` bytes.
    pub fn take(&mut self, count: usize) -> Result<&'a [u8], LayoutError> {
        let (bytes, rest) = self
            .rest
            .split_at_checked(count)
            .ok_or_else(|| self.truncated(count))?;
        self.rest = rest;
        Ok(bytes)
    }

    /// The next `N` bytes, borrowed.
    pub fn chunk<const N: usize>(&mut self) -> Result<&'a [u8; N], LayoutError> {
        let (bytes, rest) = self
            .rest
            .split_first_chunk()
            .ok_or_else(|| self.truncated(N))?;
        self.rest = rest;
        Ok(bytes)
    }

    /// The next `N` bytes, copied.
    pub fn array<const N: usize>(&mut self) -> Result<[u8; N], LayoutError> {
        self.chunk().copied()
    }

    /// The next byte.
    pub fn u8(&mut self) -> Result<u8, LayoutError> {
        let [byte] = self.array()?;
        Ok(byte)
    }

    /// The next big-endian `u16`.
    pub fn u16(&mut self) -> Result<u16, LayoutError> {
        self.array().map(u16::from_be_bytes)
    }

    /// The next big-endian `u32`.
    pub fn u32(&mut self) -> Result<u32, LayoutError> {
        self.array().map(u32::from_be_bytes)
    }

    /// The next big-endian `u64`.
    pub fn u64(&mut self) -> Result<u64, LayoutError> {
        self.array().map(u64::from_be_bytes)
    }

    /// Require that every byte was consumed.
    pub const fn finish(self) -> Result<(), LayoutError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(LayoutError::Trailing(self.rest.len()))
        }
    }

    fn truncated(&self, count: usize) -> LayoutError {
        LayoutError::Truncated {
            needed: self.offset().saturating_add(count),
            available: self.len,
        }
    }
}

/// Sequential writer into a caller-sized buffer.
#[derive(Debug)]
pub struct Writer<'a> {
    output: &'a mut [u8],
    offset: usize,
}

impl<'a> Writer<'a> {
    /// Write `output` from its start.
    pub const fn new(output: &'a mut [u8]) -> Self {
        Self { output, offset: 0 }
    }

    /// Write the first `len` bytes of `output`, or fail when it is shorter.
    pub fn with_len(output: &'a mut [u8], len: usize) -> Result<Self, LayoutError> {
        let capacity = output.len();
        let output = output.get_mut(..len).ok_or(LayoutError::Overflow {
            needed: len,
            capacity,
        })?;
        Ok(Self::new(output))
    }

    /// Bytes written so far.
    #[must_use]
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// Write `bytes` next, or nothing when they do not fit.
    pub fn bytes(&mut self, bytes: &[u8]) -> Result<(), LayoutError> {
        let overflow = LayoutError::Overflow {
            needed: self.offset.saturating_add(bytes.len()),
            capacity: self.output.len(),
        };
        let end = self.offset.checked_add(bytes.len()).ok_or(overflow)?;
        let field = self.output.get_mut(self.offset..end).ok_or(overflow)?;
        field.copy_from_slice(bytes);
        self.offset = end;
        Ok(())
    }

    /// Write one byte.
    pub fn u8(&mut self, value: u8) -> Result<(), LayoutError> {
        self.bytes(&[value])
    }

    /// Write a big-endian `u16`.
    pub fn u16(&mut self, value: u16) -> Result<(), LayoutError> {
        self.bytes(&value.to_be_bytes())
    }

    /// Write a big-endian `u32`.
    pub fn u32(&mut self, value: u32) -> Result<(), LayoutError> {
        self.bytes(&value.to_be_bytes())
    }

    /// Write a big-endian `u64`.
    pub fn u64(&mut self, value: u64) -> Result<(), LayoutError> {
        self.bytes(&value.to_be_bytes())
    }
}

/// The `N`-byte record at `at`.
pub fn record<const N: usize>(input: &[u8], at: usize) -> Result<&[u8; N], LayoutError> {
    input
        .get(at..)
        .and_then(<[u8]>::first_chunk)
        .ok_or(LayoutError::Truncated {
            needed: at.saturating_add(N),
            available: input.len(),
        })
}

/// The mutable `N`-byte record at `at`.
pub fn record_mut<const N: usize>(
    output: &mut [u8],
    at: usize,
) -> Result<&mut [u8; N], LayoutError> {
    let capacity = output.len();
    output
        .get_mut(at..)
        .and_then(<[u8]>::first_chunk_mut)
        .ok_or(LayoutError::Overflow {
            needed: at.saturating_add(N),
            capacity,
        })
}

/// Overwrite the `N` bytes at `at`, as when filling a placeholder.
pub fn patch<const N: usize>(
    output: &mut [u8],
    at: usize,
    bytes: [u8; N],
) -> Result<(), LayoutError> {
    *record_mut(output, at)? = bytes;
    Ok(())
}

/// Fields of a fixed-size record at offsets checked when the code is built.
///
/// A field that does not fit the record fails the build, so the accessors
/// cannot fail. Get the record from a slice with `first_chunk`,
/// `split_first_chunk`, or `as_chunks`.
pub trait Fixed {
    /// The `M` bytes at `AT`, copied.
    fn array<const AT: usize, const M: usize>(&self) -> [u8; M];

    /// The `M` bytes at `AT`, borrowed.
    fn slice<const AT: usize, const M: usize>(&self) -> &[u8];

    /// Overwrite the `M` bytes at `AT`.
    fn put<const AT: usize, const M: usize>(&mut self, bytes: [u8; M]);

    /// The byte at `AT`.
    fn u8_at<const AT: usize>(&self) -> u8 {
        let [byte] = self.array::<AT, 1>();
        byte
    }

    /// The big-endian `u16` at `AT`.
    fn u16_at<const AT: usize>(&self) -> u16 {
        u16::from_be_bytes(self.array::<AT, 2>())
    }

    /// The big-endian `u32` at `AT`.
    fn u32_at<const AT: usize>(&self) -> u32 {
        u32::from_be_bytes(self.array::<AT, 4>())
    }

    /// The big-endian `u64` at `AT`.
    fn u64_at<const AT: usize>(&self) -> u64 {
        u64::from_be_bytes(self.array::<AT, 8>())
    }

    /// Write the byte at `AT`.
    fn put_u8<const AT: usize>(&mut self, value: u8) {
        self.put::<AT, 1>([value]);
    }

    /// Write a big-endian `u16` at `AT`.
    fn put_u16<const AT: usize>(&mut self, value: u16) {
        self.put::<AT, 2>(value.to_be_bytes());
    }

    /// Write a big-endian `u32` at `AT`.
    fn put_u32<const AT: usize>(&mut self, value: u32) {
        self.put::<AT, 4>(value.to_be_bytes());
    }

    /// Write a big-endian `u64` at `AT`.
    fn put_u64<const AT: usize>(&mut self, value: u64) {
        self.put::<AT, 8>(value.to_be_bytes());
    }
}

// Each body asserts its bounds at build time; the `None` arms are dead.
impl<const N: usize> Fixed for [u8; N] {
    fn array<const AT: usize, const M: usize>(&self) -> [u8; M] {
        const { assert!(AT + M <= N, "field exceeds the record") };
        self.get(AT..)
            .and_then(<[u8]>::first_chunk)
            .copied()
            .unwrap_or([0; M])
    }

    fn slice<const AT: usize, const M: usize>(&self) -> &[u8] {
        const { assert!(AT + M <= N, "field exceeds the record") };
        self.get(AT..)
            .and_then(|tail| tail.get(..M))
            .unwrap_or_default()
    }

    fn put<const AT: usize, const M: usize>(&mut self, bytes: [u8; M]) {
        const { assert!(AT + M <= N, "field exceeds the record") };
        if let Some(field) = self.get_mut(AT..).and_then(<[u8]>::first_chunk_mut) {
            *field = bytes;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_takes_fields_in_order_and_stops_at_the_end() {
        let input = [1, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 5, 6];
        let short = |needed| LayoutError::Truncated {
            needed,
            available: 17,
        };
        let mut reader = Reader::new(&input);
        assert_eq!(reader.u8(), Ok(1));
        assert_eq!(reader.u16(), Ok(2));
        assert_eq!(reader.u32(), Ok(3));
        assert_eq!(reader.u64(), Ok(4));
        assert_eq!(reader.offset(), 15);
        assert_eq!(reader.finish(), Err(LayoutError::Trailing(2)));
        assert_eq!(reader.array::<3>(), Err(short(18)));
        assert_eq!(reader.rest(), &[5, 6]);
        assert_eq!(reader.take(2), Ok(&[5, 6][..]));
        assert_eq!(reader.u8(), Err(short(18)));
        assert_eq!(reader.take(usize::MAX), Err(short(usize::MAX)));
        assert_eq!(reader.finish(), Ok(()));
    }

    #[test]
    fn writer_fills_its_buffer_and_refuses_overflow() {
        let mut output = [0; 7];
        let mut writer = Writer::new(&mut output);
        assert_eq!(writer.u8(1), Ok(()));
        assert_eq!(writer.u16(2), Ok(()));
        assert_eq!(
            writer.u64(3),
            Err(LayoutError::Overflow {
                needed: 11,
                capacity: 7
            })
        );
        assert_eq!(writer.u32(4), Ok(()));
        assert_eq!(writer.offset(), 7);
        assert!(writer.u8(5).is_err());
        assert_eq!(output, [1, 0, 2, 0, 0, 0, 4]);
        assert!(Writer::with_len(&mut output, 8).is_err());
        let mut writer = Writer::with_len(&mut output, 1).unwrap();
        assert!(writer.u16(9).is_err());
    }

    #[test]
    fn fixed_fields_round_trip_at_their_offsets() {
        let mut record = [0; 16];
        record.put_u8::<0>(1);
        record.put_u16::<1>(2);
        record.put_u32::<3>(3);
        record.put_u64::<7>(4);
        record.put::<15, 1>([9]);
        assert_eq!(record, [1, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 9]);
        assert_eq!(record.u8_at::<0>(), 1);
        assert_eq!(record.u16_at::<1>(), 2);
        assert_eq!(record.u32_at::<3>(), 3);
        assert_eq!(record.u64_at::<7>(), 4);
        assert_eq!(record.array::<14, 2>(), [4, 9]);
        assert_eq!(record.slice::<13, 3>(), &[0, 4, 9]);
    }

    #[test]
    fn records_are_bounded_by_their_input() {
        let mut bytes = [1, 2, 3, 4, 5];
        assert_eq!(record::<2>(&bytes, 3), Ok(&[4, 5]));
        assert_eq!(
            record::<2>(&bytes, 4),
            Err(LayoutError::Truncated {
                needed: 6,
                available: 5
            })
        );
        assert!(record::<1>(&bytes, usize::MAX).is_err());
        record_mut::<2>(&mut bytes, 0).unwrap().put_u16::<0>(0x0909);
        assert_eq!(bytes, [9, 9, 3, 4, 5]);
        assert!(record_mut::<6>(&mut bytes, 0).is_err());
    }

    #[test]
    fn patch_fills_a_placeholder_inside_its_output() {
        let mut output = [0; 6];
        assert_eq!(patch(&mut output, 2, [1, 2, 3, 4]), Ok(()));
        assert_eq!(output, [0, 0, 1, 2, 3, 4]);
        assert_eq!(
            patch(&mut output, 3, [5, 6, 7, 8]),
            Err(LayoutError::Overflow {
                needed: 7,
                capacity: 6
            })
        );
        assert!(patch(&mut output, usize::MAX, [9]).is_err());
        assert_eq!(output, [0, 0, 1, 2, 3, 4]);
    }
}
