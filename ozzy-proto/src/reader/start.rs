//! Initial selection resolves once to a partition offset.

use crate::{
    MessageId,
    data::{CodecError, Cursor},
};

/// Duplicate application record IDs do not imply duplicate producer records.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum IdPolicy {
    /// Reject multiple retained matches, reporting their first/last offsets.
    #[default]
    RequireUnique = 0,
    /// Replay from the oldest retained match.
    FirstRetained = 1,
    /// Replay from the newest retained match.
    LastRetained = 2,
}

/// First position in one partition. Delivery continues by offset after resolving.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Start {
    /// First currently retained record.
    #[default]
    Earliest,
    /// Current confirmed end, followed by future records.
    Latest,
    /// Exact offset; expired offsets produce an explicit retention gap.
    Offset(u64),
    /// Lowest retained offset with broker append time >= Unix milliseconds.
    Timestamp(u64),
    /// Application record ID within this partition, with an explicit policy.
    RecordId {
        /// Original application record identity.
        id: MessageId,
        /// Selection when the same ID appears at multiple retained offsets.
        policy: IdPolicy,
    },
}

impl Start {
    pub(super) fn validate(self) -> Result<(), CodecError> {
        if matches!(self, Self::RecordId { id, .. } if id.as_bytes() == &[0; 16]) {
            return Err(CodecError::Identity);
        }
        Ok(())
    }
    pub(super) fn size(self) -> usize {
        match self {
            Self::Earliest | Self::Latest => 1,
            Self::Offset(_) | Self::Timestamp(_) => 9,
            Self::RecordId { .. } => 18,
        }
    }

    pub(super) fn encode(self, output: &mut Vec<u8>) {
        match self {
            Self::Earliest => output.push(0),
            Self::Latest => output.push(1),
            Self::Offset(offset) | Self::Timestamp(offset) => {
                output.push(if matches!(self, Self::Offset(_)) {
                    2
                } else {
                    3
                });
                output.extend_from_slice(&offset.to_be_bytes());
            }
            Self::RecordId { id, policy } => {
                output.push(4);
                output.extend_from_slice(id.as_bytes());
                output.push(policy as u8);
            }
        }
    }

    pub(super) fn decode(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        Ok(match cursor.byte()? {
            0 => Self::Earliest,
            1 => Self::Latest,
            2 => Self::Offset(cursor.u64()?),
            3 => Self::Timestamp(cursor.u64()?),
            4 => {
                let id = MessageId::from_bytes(cursor.array()?);
                let policy = match cursor.byte()? {
                    0 => IdPolicy::RequireUnique,
                    1 => IdPolicy::FirstRetained,
                    2 => IdPolicy::LastRetained,
                    _ => return Err(CodecError::Profile),
                };
                if id.as_bytes() == &[0; 16] {
                    return Err(CodecError::Identity);
                }
                Self::RecordId { id, policy }
            }
            _ => return Err(CodecError::Profile),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selectors_have_distinct_tags_and_reject_unknown_duplicate_policy() {
        for selector in [
            Start::Earliest,
            Start::Latest,
            Start::Offset(7),
            Start::Timestamp(9),
            Start::RecordId {
                id: MessageId::from_bytes([4; 16]),
                policy: IdPolicy::RequireUnique,
            },
        ] {
            let mut encoded = Vec::new();
            selector.encode(&mut encoded);
            assert_eq!(encoded.len(), selector.size());
            assert_eq!(Start::decode(&mut Cursor(&encoded)), Ok(selector));
        }
        let mut bytes = vec![4];
        bytes.extend_from_slice(&[4; 16]);
        bytes.push(3);
        assert_eq!(Start::decode(&mut Cursor(&bytes)), Err(CodecError::Profile));
    }
}
