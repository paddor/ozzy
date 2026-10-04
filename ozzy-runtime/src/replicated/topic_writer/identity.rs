//! The application-owned token needed to attach to a producer again.

use super::TopicWriterError;
use ozzy_proto::{ProducerId, TopicId};

/// Stable topic incarnation and producer identity. Contains no pending records
/// or partition epochs. Save once; broker state supplies resume coordinates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerIdentity {
    /// Rejects attachment to a recreated topic with the same name.
    pub topic: TopicId,
    /// Logical producer, shared across the topic's partitions.
    pub producer: ProducerId,
}

impl ProducerIdentity {
    /// Fixed network-order token: topic UUID followed by producer UUID.
    pub fn to_bytes(self) -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(self.topic.as_bytes());
        bytes[16..].copy_from_slice(self.producer.as_bytes());
        bytes
    }

    /// Decode an application-saved token, rejecting empty identities.
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self, TopicWriterError> {
        let topic = bytes[..16].try_into().expect("topic UUID");
        let producer = bytes[16..].try_into().expect("producer UUID");
        if topic == [0; 16] || producer == [0; 16] {
            return Err(TopicWriterError::Configuration);
        }
        Ok(Self {
            topic: TopicId::from_bytes(topic),
            producer: ProducerId::from_bytes(producer),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saved_identity_has_independent_topic_and_producer_bytes() {
        let mut saved = [7; 32];
        saved[16..].fill(9);
        let identity = ProducerIdentity::from_bytes(saved).unwrap();
        assert_eq!(identity.topic, TopicId::from_bytes([7; 16]));
        assert_eq!(identity.producer, ProducerId::from_bytes([9; 16]));
        assert_eq!(identity.to_bytes(), saved);
        saved[..16].fill(0);
        assert!(ProducerIdentity::from_bytes(saved).is_err());
        saved.fill(0);
        saved[..16].fill(7);
        assert!(ProducerIdentity::from_bytes(saved).is_err());
    }
}
