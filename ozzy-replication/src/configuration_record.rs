use ozzy_proto::{GroupId, NodeId};

use crate::{Configuration, Digest, QuorumPolicy, ReplicationError};

/// Exact encoded length of a fixed-three-voter durable configuration.
pub const CONFIGURATION_RECORD_BYTES: usize = 256;
const HASH_CONTEXT: &str = "ozzy fixed durable replica configuration v1";
const MAGIC: &[u8; 8] = b"OZYVOTER";
const DIGEST_START: usize = 192;

/// One persistent voter and its independently authenticated principal binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfiguredVoter {
    /// Stable node identity. Its array position participates in primary selection.
    pub node_id: NodeId,
    /// Fingerprint of the adapter's canonical authentication-principal binding.
    /// This is not a socket identity, endpoint hash, or proof of authentication.
    /// All replicas must use the same fingerprint definition and mapping.
    pub principal: Digest,
}

/// Canonical immutable configuration for three authenticated durable voters.
///
/// The record fixes confirmation policy, the native wire version, and canonical operation
/// schema 1. Endpoint addresses and local tuning are deliberately excluded.
/// Authentication still belongs to the transport adapter: loading this record
/// does not authenticate a peer or authorize a restarted voter to enter normal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigurationRecord {
    configuration: Configuration,
    voters: [ConfiguredVoter; 3],
}

impl ConfigurationRecord {
    /// Construct a complete fixed configuration and derive its scope digest.
    /// Voter IDs and principal fingerprints must be nonzero and pairwise distinct.
    pub fn new(
        group_id: GroupId,
        configuration_epoch: u64,
        voters: [ConfiguredVoter; 3],
    ) -> Result<Self, ConfigurationRecordError> {
        Self::with_policy(group_id, configuration_epoch, voters, QuorumPolicy::Durable)
    }

    /// Bind disk or retained-memory confirmation into the immutable group identity.
    pub fn with_policy(
        group_id: GroupId,
        configuration_epoch: u64,
        voters: [ConfiguredVoter; 3],
        policy: QuorumPolicy,
    ) -> Result<Self, ConfigurationRecordError> {
        if configuration_epoch == 0
            || voters.iter().any(|voter| voter.principal == Digest::ZERO)
            || voters[0].principal == voters[1].principal
            || voters[0].principal == voters[2].principal
            || voters[1].principal == voters[2].principal
        {
            return Err(ConfigurationRecordError::InvalidConfiguration);
        }
        let bytes = unsigned_record(group_id, configuration_epoch, &voters, policy);
        let mut configuration = Configuration::new(
            group_id,
            configuration_epoch,
            record_digest(&bytes),
            voters.map(|voter| voter.node_id),
        )?;
        configuration.policy = policy;
        Ok(Self {
            configuration,
            voters,
        })
    }

    /// Verified group, ordered voter identities, and complete configuration digest.
    pub const fn configuration(self) -> Configuration {
        self.configuration
    }

    /// Ordered voter-to-principal bindings the transport adapter must enforce.
    pub const fn voters(&self) -> &[ConfiguredVoter; 3] {
        &self.voters
    }

    /// Encode without allocation. All integer fields use network byte order.
    pub fn encode(self) -> [u8; CONFIGURATION_RECORD_BYTES] {
        let scope = self.configuration.scope();
        let mut bytes = unsigned_record(
            scope.group_id,
            scope.configuration_epoch,
            &self.voters,
            self.configuration.policy(),
        );
        bytes[DIGEST_START..DIGEST_START + 32]
            .copy_from_slice(scope.configuration_digest.as_bytes());
        bytes
    }

    /// Validate exact length, supported policy/versions, canonical fields, and digest.
    /// Unknown fields and malformed membership are rejected, never normalized.
    pub fn decode(bytes: &[u8]) -> Result<Self, ConfigurationRecordError> {
        if bytes.len() != CONFIGURATION_RECORD_BYTES {
            return Err(ConfigurationRecordError::Length);
        }
        if &bytes[..8] != MAGIC
            || bytes[8..12] != [0, 2, 1, 0]
            || !matches!(bytes[12], 1 | 2)
            || bytes[13..16] != [1, ozzy_proto::VERSION, 1]
            || bytes[40] != 3
            || bytes[41..48].iter().any(|&byte| byte != 0)
            || bytes[224..].iter().any(|&byte| byte != 0)
        {
            return Err(ConfigurationRecordError::UnsupportedFields);
        }
        let stored = Digest::from_bytes(
            bytes[DIGEST_START..DIGEST_START + 32]
                .try_into()
                .expect("fixed digest"),
        );
        if record_digest(bytes) != stored {
            return Err(ConfigurationRecordError::DigestMismatch);
        }
        let group = GroupId::from_bytes(bytes[16..32].try_into().expect("fixed group"));
        let epoch = u64::from_be_bytes(bytes[32..40].try_into().expect("fixed epoch"));
        let voters = std::array::from_fn(|index| {
            let start = 48 + index * 48;
            ConfiguredVoter {
                node_id: NodeId::from_bytes(
                    bytes[start..start + 16].try_into().expect("fixed voter"),
                ),
                principal: Digest::from_bytes(
                    bytes[start + 16..start + 48]
                        .try_into()
                        .expect("fixed principal"),
                ),
            }
        });
        let policy = if bytes[12] == 1 {
            QuorumPolicy::Durable
        } else {
            QuorumPolicy::Replicated
        };
        let record = Self::with_policy(group, epoch, voters, policy)?;
        debug_assert_eq!(record.configuration.scope().configuration_digest, stored);
        Ok(record)
    }
}

fn unsigned_record(
    group: GroupId,
    epoch: u64,
    voters: &[ConfiguredVoter; 3],
    policy: QuorumPolicy,
) -> [u8; CONFIGURATION_RECORD_BYTES] {
    let mut bytes = [0; CONFIGURATION_RECORD_BYTES];
    bytes[..8].copy_from_slice(MAGIC);
    // Version 2 (XXH3-128 integrity), length 256, disk quorum, external principal binding,
    // Current native wire version, canonical operation schema 1.
    bytes[8..16].copy_from_slice(&[0, 2, 1, 0, policy as u8, 1, ozzy_proto::VERSION, 1]);
    bytes[16..32].copy_from_slice(group.as_bytes());
    bytes[32..40].copy_from_slice(&epoch.to_be_bytes());
    bytes[40] = 3;
    for (index, voter) in voters.iter().enumerate() {
        let start = 48 + index * 48;
        bytes[start..start + 16].copy_from_slice(voter.node_id.as_bytes());
        bytes[start + 16..start + 48].copy_from_slice(voter.principal.as_bytes());
    }
    bytes
}

fn record_digest(bytes: &[u8]) -> Digest {
    let mut hasher = ozzy_journal::integrity::IntegrityHasher::new(HASH_CONTEXT);
    hasher.update(&bytes[..DIGEST_START]);
    hasher.update(&[0; 32]);
    hasher.update(&bytes[DIGEST_START + 32..]);
    hasher.finish()
}

/// Rejected durable configuration. No error permits silently replacing membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConfigurationRecordError {
    /// Record must have the one exact supported length.
    #[error("invalid replica configuration record length")]
    Length,
    /// Magic, versions, policy, voter count, or reserved fields are unsupported.
    #[error("unsupported replica configuration record fields")]
    UnsupportedFields,
    /// Stored configuration bytes no longer match their domain-separated digest.
    #[error("replica configuration record digest mismatch")]
    DigestMismatch,
    /// Zero epoch/principal or repeated principal binding.
    #[error("invalid replica configuration principals or epoch")]
    InvalidConfiguration,
    /// Group or ordered voter identities violate the replication core's rules.
    #[error(transparent)]
    Replication(#[from] ReplicationError),
}
