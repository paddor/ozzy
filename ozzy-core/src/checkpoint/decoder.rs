use super::{Error, HEADER_BYTES, Identities, Layout};
use crate::dedup::{SNAPSHOT_ROW_BYTES, SnapshotRows};

/// Private byte-stream decoder. It retains only bounded canonical metadata,
/// one partial identity row, and scalar cursor state. Row callbacks may restore
/// a separate bounded index; publish neither image before outer verification.
#[derive(Debug)]
pub struct Decoder {
    expected_bytes: u64,
    maximum_core_bytes: usize,
    header: [u8; HEADER_BYTES],
    header_bytes: usize,
    layout: Option<Layout>,
    core: Vec<u8>,
    /// Identity section and its row cursor, when the image has one.
    rows: Option<(Identities, SnapshotRows)>,
    row: [u8; SNAPSHOT_ROW_BYTES],
    row_bytes: usize,
    failed: bool,
}

impl Decoder {
    /// Declare outer image size and the independent canonical metadata ceiling.
    pub fn new(expected_bytes: u64, maximum_core_bytes: usize) -> Self {
        Self {
            expected_bytes,
            maximum_core_bytes,
            header: [0; HEADER_BYTES],
            header_bytes: 0,
            layout: None,
            core: Vec::new(),
            rows: None,
            row: [0; SNAPSHOT_ROW_BYTES],
            row_bytes: 0,
            failed: false,
        }
    }

    /// Physical parser and canonical metadata bytes; excludes callback-owned
    /// index state and the caller's charged input fragment.
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.core.capacity()
    }

    /// Consume a bounded caller fragment and deliver complete checked rows.
    /// The header callback reserves caller resources before metadata allocation.
    /// Any decoding or callback failure poisons the entire private candidate.
    pub fn push(
        &mut self,
        bytes: &[u8],
        on_header: impl FnMut(Layout) -> Result<(), Error>,
        on_row: impl FnMut(Identities, &[u8; SNAPSHOT_ROW_BYTES]) -> Result<(), crate::dedup::Error>,
    ) -> Result<(), Error> {
        if self.failed {
            return Err(Error::Length);
        }
        let result = self.consume(bytes, on_header, on_row);
        self.failed |= result.is_err();
        result
    }

    fn consume(
        &mut self,
        mut bytes: &[u8],
        mut on_header: impl FnMut(Layout) -> Result<(), Error>,
        mut on_row: impl FnMut(Identities, &[u8; SNAPSHOT_ROW_BYTES]) -> Result<(), crate::dedup::Error>,
    ) -> Result<(), Error> {
        let layout = if let Some(layout) = self.layout {
            layout
        } else {
            fill(&mut self.header, &mut self.header_bytes, &mut bytes);
            if self.header_bytes != HEADER_BYTES {
                return Ok(());
            }
            self.prepare_sections(&mut on_header)?
        };
        let core_bytes = layout.core_bytes() as usize;
        let (core, rest) = bytes.split_at(bytes.len().min(core_bytes - self.core.len()));
        self.core.extend_from_slice(core);
        bytes = rest;
        if !bytes.is_empty() {
            let Some((image, rows)) = self.rows.as_mut() else {
                return Err(Error::Length);
            };
            while !bytes.is_empty() {
                fill(&mut self.row, &mut self.row_bytes, &mut bytes);
                if self.row_bytes == SNAPSHOT_ROW_BYTES {
                    rows.push(&self.row)?;
                    on_row(*image, &self.row)?;
                    self.row_bytes = 0;
                }
            }
        }
        Ok(())
    }

    fn prepare_sections(
        &mut self,
        mut on_header: impl FnMut(Layout) -> Result<(), Error>,
    ) -> Result<Layout, Error> {
        let layout = Layout::decode(&self.header)?;
        let bytes = usize::try_from(layout.core_bytes()).map_err(|_| Error::Length)?;
        if layout.encoded_bytes()? != self.expected_bytes || bytes > self.maximum_core_bytes {
            return Err(Error::Length);
        }
        on_header(layout)?;
        self.core
            .try_reserve_exact(bytes)
            .map_err(|_| Error::Allocation)?;
        self.rows = layout
            .identities()
            .map(|image| (image, SnapshotRows::new(image.header)));
        self.layout = Some(layout);
        Ok(layout)
    }

    /// Finish complete section decoding. The caller must separately prove outer
    /// integrity, canonical metadata validity, and the image's authority.
    pub fn finish(self) -> Result<(Layout, Vec<u8>), Error> {
        let layout = self.layout.ok_or(Error::Length)?;
        if self.failed
            || self.core.len() as u64 != layout.core_bytes()
            || self.row_bytes != 0
            || self
                .rows
                .as_ref()
                .is_some_and(|(_, rows)| !rows.is_complete())
        {
            return Err(Error::Length);
        }
        Ok((layout, self.core))
    }
}

/// Move what fits of `bytes` into `buffer` after its first `filled` bytes.
fn fill(buffer: &mut [u8], filled: &mut usize, bytes: &mut &[u8]) {
    let tail = buffer.get_mut(*filled..).unwrap_or_default();
    let count = tail.len().min(bytes.len());
    let (head, rest) = bytes.split_at(count);
    tail.split_at_mut(count).0.copy_from_slice(head);
    *filled += count;
    *bytes = rest;
}
