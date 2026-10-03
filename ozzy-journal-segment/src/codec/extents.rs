//! Raw bodies are already packed canonical record arenas. Keep those borrowed
//! until synchronous I/O completes, instead of copying them into physical output.

use super::{
    CanonicalOperation, CodecError, ENTRY_HEADER_BYTES, PreparedEntry, PreparedGroupBodies,
    SmallVec, encoded_entry_len,
};

pub(crate) fn prepare_raw_group_extents<'a>(
    bodies: impl Iterator<Item = &'a [u8]>,
    bytes: Vec<u8>,
) -> Result<PreparedGroupBodies, CodecError> {
    prepare_raw_lengths(bodies.map(<[u8]>::len), bytes)
}

pub(super) fn prepare_raw_lengths(
    lengths: impl Iterator<Item = usize>,
    mut bytes: Vec<u8>,
) -> Result<PreparedGroupBodies, CodecError> {
    bytes.clear();
    let mut entries = SmallVec::new();
    let mut decoded_body_bytes = 0_usize;
    let mut entry_bytes = 0_usize;
    for len in lengths {
        decoded_body_bytes = decoded_body_bytes
            .checked_add(len)
            .ok_or(CodecError::LengthOverflow)?;
        let start = entry_bytes;
        entry_bytes = entry_bytes
            .checked_add(encoded_entry_len(len)?)
            .ok_or(CodecError::LengthOverflow)?;
        let header_start = bytes.len();
        let end = header_start
            .checked_add(ENTRY_HEADER_BYTES)
            .ok_or(CodecError::LengthOverflow)?;
        bytes.resize(end, 0);
        entries.push(PreparedEntry {
            start,
            header_start,
            encoded_len: len,
            decoded_len: len,
            codec: 0,
        });
    }
    if entries.is_empty() {
        return Err(CodecError::EmptyGroup);
    }
    Ok(PreparedGroupBodies {
        bytes,
        entries,
        decoded_body_bytes,
        entry_bytes,
        borrowed_raw: true,
    })
}

impl PreparedGroupBodies {
    /// Finalized raw framing only. The owner must retain the separate bodies.
    pub(crate) fn into_raw_framing(self) -> Vec<u8> {
        assert!(self.borrowed_raw, "separate raw bodies required");
        self.bytes
    }

    /// Only use after finalization validates every operation and adds the seal.
    /// No owned slice table or body owner clones are constructed.
    pub(crate) fn extents<'a>(
        &'a self,
        operations: &'a [CanonicalOperation<'a>],
    ) -> impl Iterator<Item = &'a [u8]> {
        const PADDING: [u8; 8] = [0; 8];
        let entries = self
            .entries
            .iter()
            .zip(operations)
            .flat_map(|(entry, operation)| {
                if self.borrowed_raw {
                    let header =
                        &self.bytes[entry.header_start..entry.header_start + ENTRY_HEADER_BYTES];
                    let padding = (8 - entry.encoded_len % 8) % 8;
                    [header, operation.body, &PADDING[..padding]]
                } else {
                    [&[][..]; 3]
                }
            });
        let tail = if self.borrowed_raw {
            &self.bytes[self.entries.len() * ENTRY_HEADER_BYTES..]
        } else {
            self.bytes.as_slice()
        };
        entries
            .chain(std::iter::once(tail))
            .filter(|bytes| !bytes.is_empty())
    }
}
