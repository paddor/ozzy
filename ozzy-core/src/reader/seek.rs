//! Order-independent history selection; adapters supply validated retained metadata.

use ozzy_proto::{
    MessageId, Offset,
    reader::{IdPolicy, Start},
};

/// History selectors never guess when an application ID is missing or ambiguous.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SeekError {
    /// The ID may have expired or never existed; no retained match is available.
    #[error("record ID not found in retained history, earliest offset {earliest:?}")]
    NotFound {
        /// Earliest visible retained offset.
        earliest: Offset,
    },
    /// The default policy requires an explicit choice for repeated IDs.
    #[error("record ID is ambiguous: first {first:?}, last {last:?}")]
    Ambiguous {
        /// Oldest retained match.
        first: Offset,
        /// Newest retained match.
        last: Offset,
    },
}

/// Constant-memory resolution over any order of index/cache/storage results.
#[derive(Clone, Copy, Debug)]
pub struct Selection {
    start: Start,
    floor: Offset,
    end: Offset,
    /// First and last matching offsets.
    matched: Option<(Offset, Offset)>,
}

impl Selection {
    /// Capture confirmed retention and end boundaries before lookup.
    pub const fn new(start: Start, floor: Offset, end: Offset) -> Self {
        Self {
            start,
            floor,
            end,
            matched: None,
        }
    }

    /// Observe one validated record; unconfirmed and expired coordinates vanish.
    pub fn observe(&mut self, offset: Offset, id: MessageId, timestamp: u64) {
        let matched = match self.start {
            Start::Timestamp(target) => timestamp >= target,
            Start::RecordId { id: wanted, .. } => id == wanted,
            _ => false,
        };
        if matched {
            self.observe_match(offset);
        }
    }

    /// Merge an index match already filtered by the adapter's ID/time query.
    pub fn observe_match(&mut self, offset: Offset) {
        if offset < self.floor || offset >= self.end {
            return;
        }
        self.matched = Some(self.matched.map_or((offset, offset), |(first, last)| {
            (first.min(offset), last.max(offset))
        }));
    }

    /// Resolve once. Future deliveries advance ordinary offsets, with no filter.
    pub fn finish(self) -> Result<Offset, SeekError> {
        match self.start {
            Start::Earliest => Ok(self.floor),
            Start::Latest => Ok(self.end),
            Start::Offset(offset) => Ok(Offset::new(offset)),
            Start::Timestamp(_) => Ok(self.matched.map_or(self.end, |(first, _)| first)),
            Start::RecordId { policy, .. } => {
                let (first, last) = self.matched.ok_or(SeekError::NotFound {
                    earliest: self.floor,
                })?;
                match policy {
                    IdPolicy::RequireUnique if first != last => {
                        Err(SeekError::Ambiguous { first, last })
                    }
                    IdPolicy::RequireUnique | IdPolicy::FirstRetained => Ok(first),
                    IdPolicy::LastRetained => Ok(last),
                }
            }
        }
    }
}
