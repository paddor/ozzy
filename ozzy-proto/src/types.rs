use std::fmt;
use std::sync::Arc;

use uuid::Uuid;

const MAX_NAME_LEN: usize = 255;

macro_rules! uuid_id {
    ($name:ident) => {
        #[doc = concat!("A 128-bit Ozzy ", stringify!($name), ".")]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Uuid);

        impl $name {
            /// Generate a time-ordered UUIDv7 identifier.
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Construct an identifier from its 16-byte representation.
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(Uuid::from_bytes(bytes))
            }

            /// Return the identifier bytes.
            pub const fn as_bytes(&self) -> &[u8; 16] {
                self.0.as_bytes()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

uuid_id!(NodeId);
uuid_id!(GroupId);
uuid_id!(TopicId);
uuid_id!(VolumeId);
uuid_id!(StoreId);
uuid_id!(CheckpointId);
uuid_id!(PartitionIncarnation);
uuid_id!(ProducerId);
uuid_id!(SubscriptionId);
uuid_id!(ConsumerGroupId);
uuid_id!(ConsumerMemberId);
uuid_id!(MessageId);
uuid_id!(RequestId);
uuid_id!(LinkSessionId);
uuid_id!(OperationId);

/// A partition-local record offset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(u64);

impl Offset {
    /// The first offset in a partition.
    pub const ZERO: Self = Self(0);

    /// Construct an offset.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw offset value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Return the next offset, if the partition has not exhausted `u64`.
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// A topic-local partition number, independent of writer identity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(u32);

impl PartitionId {
    /// The first topic partition.
    pub const ZERO: Self = Self(0);

    /// Construct a partition ID.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Return the raw partition number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Monotonic fencing epoch for one partition owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerEpoch(u64);

impl OwnerEpoch {
    /// Initial epoch for a newly created partition.
    pub const INITIAL: Self = Self(1);

    /// Construct an owner epoch.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw epoch value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic fencing epoch for one producer session on a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProducerEpoch(u64);

impl ProducerEpoch {
    /// Initial epoch for a newly created producer session.
    pub const INITIAL: Self = Self(1);

    /// Construct a producer epoch.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw epoch value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Contiguous producer-assigned sequence within one partition and epoch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProducerSequence(u64);

impl ProducerSequence {
    /// First sequence in a producer epoch.
    pub const ZERO: Self = Self(0);

    /// Construct a producer sequence.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw sequence value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Return the next sequence, if it has not exhausted `u64`.
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// A logical stream and topic selector.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Topic {
    inner: Arc<TopicInner>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TopicInner {
    stream: String,
    name: String,
}

impl Topic {
    /// Construct and validate a topic selector.
    pub fn new(stream: impl Into<String>, name: impl Into<String>) -> Result<Self, NameError> {
        let stream = stream.into();
        let name = name.into();
        validate_name("stream", &stream)?;
        validate_name("topic", &name)?;
        Ok(Self {
            inner: Arc::new(TopicInner { stream, name }),
        })
    }

    /// Return the stream name.
    pub fn stream(&self) -> &str {
        &self.inner.stream
    }

    /// Return the topic name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }
}

impl fmt::Debug for Topic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Topic")
            .field("stream", &self.inner.stream)
            .field("name", &self.inner.name)
            .finish()
    }
}

fn validate_name(field: &'static str, value: &str) -> Result<(), NameError> {
    if value.is_empty() {
        return Err(NameError::Empty(field));
    }
    if value.len() > MAX_NAME_LEN {
        return Err(NameError::TooLong {
            field,
            len: value.len(),
            max: MAX_NAME_LEN,
        });
    }
    Ok(())
}

/// Invalid stream or topic name.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    /// The name is empty.
    #[error("{0} name must not be empty")]
    Empty(&'static str),
    /// The UTF-8 representation exceeds the protocol limit.
    #[error("{field} name is {len} bytes; maximum is {max}")]
    TooLong {
        /// Name field.
        field: &'static str,
        /// Actual length.
        len: usize,
        /// Maximum length.
        max: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_names() {
        assert!(matches!(
            Topic::new("", "events"),
            Err(NameError::Empty("stream"))
        ));
        assert!(matches!(
            Topic::new("events", "x".repeat(MAX_NAME_LEN + 1)),
            Err(NameError::TooLong { field: "topic", .. })
        ));
    }

    #[test]
    fn topic_is_one_shared_pointer() {
        assert_eq!(std::mem::size_of::<Topic>(), std::mem::size_of::<usize>());
        let topic = Topic::new("orders", "created").unwrap();
        let clone = topic.clone();
        assert_eq!(clone.stream(), "orders");
        assert_eq!(clone.name(), "created");
        assert!(Arc::ptr_eq(&topic.inner, &clone.inner));
    }
}
