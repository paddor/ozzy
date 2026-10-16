//! Checked lookups by configured name. Validation establishes these names, so
//! a missing one is reported as a configuration error, never a panic.

use uuid::Uuid;

use crate::{
    Broker, BrokerIdentity, ConfigError, Deployment, DeploymentIdentity, Device, PartitionIdentity,
    PartitionStore, Topic, TopicIdentity, invalid,
};

impl Deployment {
    /// One configured broker.
    pub fn broker(&self, name: &str) -> Result<&Broker, ConfigError> {
        self.brokers
            .get(name)
            .ok_or_else(|| invalid(name, "unknown broker"))
    }

    /// One configured topic.
    pub fn topic(&self, name: &str) -> Result<&Topic, ConfigError> {
        self.topics
            .get(name)
            .ok_or_else(|| invalid(name, "unknown topic"))
    }
}

impl Broker {
    /// One of this broker's storage devices.
    pub fn device(&self, name: &str) -> Result<&Device, ConfigError> {
        self.devices
            .get(name)
            .ok_or_else(|| invalid(name, "unknown device"))
    }
}

impl DeploymentIdentity {
    /// Persistent ID of one configured broker.
    pub fn broker_id(&self, name: &str) -> Result<Uuid, ConfigError> {
        self.brokers
            .get(name)
            .copied()
            .ok_or_else(|| invalid(name, "unknown broker identity"))
    }

    /// Principal binding of one configured broker.
    pub fn principal(&self, name: &str) -> Result<[u8; 32], ConfigError> {
        self.principals
            .get(name)
            .copied()
            .ok_or_else(|| invalid(name, "unknown principal"))
    }

    /// Persistent identity of one topic.
    pub fn topic(&self, name: &str) -> Result<&TopicIdentity, ConfigError> {
        self.topics
            .get(name)
            .ok_or_else(|| invalid(name, "unknown topic identity"))
    }

    /// Persistent identity of one topic partition.
    pub fn partition(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<&PartitionIdentity, ConfigError> {
        self.topic(topic)?
            .partitions
            .get(partition as usize)
            .ok_or_else(|| invalid(topic, "unknown partition identity"))
    }
}

impl BrokerIdentity {
    /// Local journal-store binding of one topic partition.
    pub fn store(&self, topic: &str, partition: u32) -> Result<&PartitionStore, ConfigError> {
        self.topics
            .get(topic)
            .and_then(|stores| stores.get(partition as usize))
            .ok_or_else(|| invalid(topic, "unknown partition store"))
    }
}
