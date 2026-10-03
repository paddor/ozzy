use super::Error;
use crate::{Digest, Scope};
use ozzy_proto::{GroupId, NodeId};

pub const CONFIGURATION_BYTES: usize = 128;
const DIGEST_START: usize = 88;

/// Persistent single-broker identity, principal binding and local durability.
/// Encoding is distinct from fixed-three membership. No implicit conversion exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Configuration {
    scope: Scope,
    broker: NodeId,
    principal: Digest,
}

impl Configuration {
    pub fn new(
        group: GroupId,
        epoch: u64,
        broker: NodeId,
        principal: Digest,
    ) -> Result<Self, Error> {
        if group.as_bytes() == &[0; 16]
            || broker.as_bytes() == &[0; 16]
            || epoch == 0
            || principal == Digest::ZERO
        {
            return Err(Error::Configuration);
        }
        let mut configuration = Self {
            scope: Scope {
                group_id: group,
                configuration_epoch: epoch,
                configuration_digest: Digest::ZERO,
                view: 0,
            },
            broker,
            principal,
        };
        configuration.scope.configuration_digest = checksum(&configuration.encode());
        Ok(configuration)
    }

    pub const fn scope(self) -> Scope {
        self.scope
    }
    pub const fn broker(self) -> NodeId {
        self.broker
    }
    pub const fn principal(self) -> Digest {
        self.principal
    }

    /// Fixed fields and integers in network byte order. Reserved bytes stay zero.
    pub fn encode(self) -> [u8; CONFIGURATION_BYTES] {
        let mut bytes = [0; CONFIGURATION_BYTES];
        bytes[..8].copy_from_slice(b"OZYLOCAL");
        bytes[8..12].copy_from_slice(&[
            ozzy_journal::integrity::PROFILE,
            ozzy_proto::VERSION,
            1,
            1,
        ]);
        bytes[16..32].copy_from_slice(self.scope.group_id.as_bytes());
        bytes[32..40].copy_from_slice(&self.scope.configuration_epoch.to_be_bytes());
        bytes[40..56].copy_from_slice(self.broker.as_bytes());
        bytes[56..88].copy_from_slice(self.principal.as_bytes());
        bytes[DIGEST_START..120].copy_from_slice(self.scope.configuration_digest.as_bytes());
        bytes
    }

    /// Reject corruption, unsupported fields, or replicated membership records.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != CONFIGURATION_BYTES {
            return Err(Error::Encoding);
        }
        let configuration = Self::new(
            GroupId::from_bytes(bytes[16..32].try_into().expect("fixed group")),
            u64::from_be_bytes(bytes[32..40].try_into().expect("fixed epoch")),
            NodeId::from_bytes(bytes[40..56].try_into().expect("fixed broker")),
            Digest::from_bytes(bytes[56..88].try_into().expect("fixed principal")),
        )?;
        if bytes != configuration.encode() {
            return Err(Error::Encoding);
        }
        Ok(configuration)
    }
}

fn checksum(bytes: &[u8; CONFIGURATION_BYTES]) -> Digest {
    let mut hasher =
        ozzy_journal::integrity::IntegrityHasher::new("ozzy single broker configuration");
    hasher.update(&bytes[..DIGEST_START]);
    hasher.update(&[0; 32]);
    hasher.update(&bytes[120..]);
    hasher.finish()
}
