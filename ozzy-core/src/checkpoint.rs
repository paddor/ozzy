//! Bounded canonical metadata and independent identity sections in one image.
//! Outer checkpoint checksums and authority remain the caller's responsibility.

mod decoder;
pub use decoder::Decoder;

use crate::{dedup, state::CanonicalState};
use ozzy_proto::PartitionIncarnation;

/// Fixed bytes before the canonical snapshot and optional identity rows.
pub const HEADER_BYTES: usize = 104;
const MAGIC: &[u8; 8] = b"OZYCIMG\0";
const VERSION: u16 = 1;

/// Partition binding for an independently streamed confirmed identity image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Identities {
    /// Exact partition incarnation; never a routing name or index.
    pub partition: PartitionIncarnation,
    /// Confirmed operation, record end, time, window, and live row count.
    pub header: dedup::SnapshotHeader,
}

/// One supported image layout. Identity rows follow the bounded core snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    core_bytes: u64,
    identities: Option<Identities>,
}

impl Layout {
    /// Check lengths and partition binding before capturing a build plan.
    pub fn new(core_bytes: u64, identities: Option<Identities>) -> Result<Self, Error> {
        if core_bytes < crate::state::STATE_SNAPSHOT_HEADER_BYTES as u64
            || identities.is_some_and(|image| image.partition.as_bytes() == &[0; 16])
        {
            return Err(Error::Header);
        }
        let layout = Self {
            core_bytes,
            identities,
        };
        layout.encoded_bytes()?;
        Ok(layout)
    }

    /// Exact canonical snapshot section length, independent of identity count.
    pub const fn core_bytes(&self) -> u64 {
        self.core_bytes
    }

    /// Separately streamed identity section, if enabled for this partition.
    pub const fn identities(&self) -> Option<Identities> {
        self.identities
    }

    /// Complete image length. No complete identity byte vector is needed.
    pub fn encoded_bytes(&self) -> Result<u64, Error> {
        let rows = self.identities.map_or(0, |image| {
            image.header.encoded_bytes() - dedup::SNAPSHOT_HEADER_BYTES as u64
        });
        (HEADER_BYTES as u64)
            .checked_add(self.core_bytes)
            .and_then(|bytes| bytes.checked_add(rows))
            .ok_or(Error::Length)
    }

    /// Encode the fixed header; core bytes and complete identity rows follow.
    pub fn encode(&self) -> [u8; HEADER_BYTES] {
        let mut bytes = [0; HEADER_BYTES];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..10].copy_from_slice(&VERSION.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.core_bytes.to_be_bytes());
        if let Some(image) = self.identities {
            bytes[24..40].copy_from_slice(image.partition.as_bytes());
            bytes[40..].copy_from_slice(&image.header.encode());
        }
        bytes
    }

    /// Reject legacy bare snapshots, unknown versions, and nonzero reserved bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != HEADER_BYTES
            || &bytes[..8] != MAGIC
            || bytes[8..10] != VERSION.to_be_bytes()
            || bytes[10..16] != [0; 6]
        {
            return Err(Error::Header);
        }
        let core_bytes = u64::from_be_bytes(bytes[16..24].try_into().expect("fixed length"));
        let identities = if bytes[24..] == [0; 80] {
            None
        } else {
            Some(Identities {
                partition: PartitionIncarnation::from_bytes(
                    bytes[24..40].try_into().expect("fixed incarnation"),
                ),
                header: dedup::SnapshotHeader::decode(&bytes[40..])?,
            })
        };
        Self::new(core_bytes, identities)
    }

    /// Bind independently decoded metadata and identities to the same exact
    /// confirmed state. Identity offsets may precede the payload retention floor.
    pub fn validate_state(&self, state: &CanonicalState) -> Result<(), Error> {
        let Some(image) = self.identities else {
            return Ok(());
        };
        let partition = state
            .partition(image.partition)
            .ok_or(Error::IdentityState)?;
        if state.partitions().len() != 1
            || state.revision() != image.header.operation()
            || partition.dedup_time != image.header.logical_time()
            || partition.next_offset != image.header.next_offset()
            || partition.dedup_window_millis.map(std::num::NonZeroU64::get)
                != Some(image.header.window_millis())
        {
            return Err(Error::IdentityState);
        }
        Ok(())
    }
}

/// Fail-closed section decoding, separate from outer checksums and authority.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Unknown or malformed fixed section header.
    #[error("invalid canonical checkpoint section header")]
    Header,
    /// Section lengths or byte bounds do not match the declared image.
    #[error("canonical checkpoint section length mismatch")]
    Length,
    /// Reserving the bounded canonical metadata section failed.
    #[error("canonical checkpoint metadata allocation failed")]
    Allocation,
    /// Identity coordinates do not describe the decoded canonical state.
    #[error("checkpoint identity image does not match canonical state")]
    IdentityState,
    /// An identity header, row, or restoration callback failed.
    #[error(transparent)]
    Identity(#[from] dedup::Error),
}

#[cfg(test)]
mod tests;
