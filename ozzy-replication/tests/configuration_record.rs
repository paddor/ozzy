use ozzy_proto::{GroupId, NodeId};
use ozzy_replication::{
    CONFIGURATION_RECORD_BYTES, ConfigurationRecord, ConfigurationRecordError, ConfiguredVoter,
    Digest,
};

fn voters() -> [ConfiguredVoter; 3] {
    std::array::from_fn(|index| ConfiguredVoter {
        node_id: NodeId::from_bytes([index as u8 + 1; 16]),
        principal: Digest::from_bytes([index as u8 + 4; 32]),
    })
}

fn record() -> ConfigurationRecord {
    ConfigurationRecord::new(
        GroupId::from_bytes([7; 16]),
        0x0102_0304_0506_0708,
        voters(),
    )
    .unwrap()
}

#[test]
fn configuration_record_has_canonical_bytes_and_roundtrips() {
    let record = record();
    let bytes = record.encode();
    assert_eq!(bytes.len(), CONFIGURATION_RECORD_BYTES);
    assert_eq!(&bytes[..16], b"OZYVOTER\0\x02\x01\0\x01\x01\x01\x01");
    assert_eq!(&bytes[16..32], &[7; 16]);
    assert_eq!(&bytes[32..40], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(&bytes[40..48], &[3, 0, 0, 0, 0, 0, 0, 0]);
    for (index, voter) in voters().iter().enumerate() {
        let start = 48 + index * 48;
        assert_eq!(&bytes[start..start + 16], voter.node_id.as_bytes());
        assert_eq!(&bytes[start + 16..start + 48], voter.principal.as_bytes());
    }
    assert_eq!(&bytes[224..], &[0; 32]);
    assert_eq!(
        &bytes[192..224],
        &[
            149, 26, 224, 160, 76, 98, 93, 108, 178, 73, 35, 17, 212, 115, 39, 107, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    );
    assert_eq!(ConfigurationRecord::decode(&bytes).unwrap(), record);
    assert_eq!(
        record.configuration().voters(),
        &voters().map(|voter| voter.node_id)
    );
    assert_eq!(record.voters(), &voters());
}

#[test]
fn corruption_truncation_and_extensions_never_load_a_configuration() {
    let bytes = record().encode();
    for length in 0..bytes.len() {
        assert_eq!(
            ConfigurationRecord::decode(&bytes[..length]),
            Err(ConfigurationRecordError::Length)
        );
    }
    let mut extended = bytes.to_vec();
    extended.push(0);
    assert_eq!(
        ConfigurationRecord::decode(&extended),
        Err(ConfigurationRecordError::Length)
    );
    for index in 0..bytes.len() {
        for bit in 0..8 {
            let mut damaged = bytes;
            damaged[index] ^= 1 << bit;
            assert!(
                ConfigurationRecord::decode(&damaged).is_err(),
                "byte {index}, bit {bit}"
            );
        }
    }
}

#[test]
fn group_epoch_voter_order_and_principal_mapping_all_change_scope_digest() {
    let original = record();
    let group = original.configuration().scope().group_id;
    let epoch = original.configuration().scope().configuration_epoch;
    let digest = original.configuration().scope().configuration_digest;
    let mut reordered = voters();
    reordered.swap(0, 1);
    let mut changed_principal = voters();
    changed_principal[1].principal = Digest::from_bytes([99; 32]);
    for record in [
        ConfigurationRecord::new(GroupId::from_bytes([8; 16]), epoch, voters()),
        ConfigurationRecord::new(group, epoch + 1, voters()),
        ConfigurationRecord::new(group, epoch, reordered),
        ConfigurationRecord::new(group, epoch, changed_principal),
    ] {
        assert_ne!(
            record.unwrap().configuration().scope().configuration_digest,
            digest
        );
    }
}

fn resign(bytes: &mut [u8; CONFIGURATION_RECORD_BYTES]) {
    bytes[192..224].fill(0);
    let mut hasher = ozzy_journal::integrity::IntegrityHasher::new(
        "ozzy fixed durable replica configuration v1",
    );
    hasher.update(bytes);
    bytes[192..224].copy_from_slice(hasher.finish().as_bytes());
}

#[test]
fn valid_checksum_does_not_authorize_unknown_policy_or_invalid_membership() {
    let original = record().encode();
    for index in [8, 10, 12, 13, 14, 15, 40, 41, 224] {
        let mut bytes = original;
        bytes[index] ^= 0x80;
        resign(&mut bytes);
        assert_eq!(
            ConfigurationRecord::decode(&bytes),
            Err(ConfigurationRecordError::UnsupportedFields)
        );
    }
    for (start, end) in [(16, 32), (32, 40), (48, 64), (64, 96)] {
        let mut bytes = original;
        bytes[start..end].fill(0);
        resign(&mut bytes);
        assert!(ConfigurationRecord::decode(&bytes).is_err());
    }
    for (source, destination, count) in [(48, 96, 16), (64, 112, 32)] {
        let mut bytes = original;
        bytes.copy_within(source..source + count, destination);
        resign(&mut bytes);
        assert!(ConfigurationRecord::decode(&bytes).is_err());
    }
}

#[test]
fn earlier_integrity_configuration_version_is_rejected_even_with_a_valid_new_checksum() {
    let mut bytes = record().encode();
    bytes[8..10].copy_from_slice(&1_u16.to_be_bytes());
    resign(&mut bytes);
    assert_eq!(
        ConfigurationRecord::decode(&bytes),
        Err(ConfigurationRecordError::UnsupportedFields)
    );
}

#[test]
fn obsolete_wire_version_is_rejected_even_with_a_valid_checksum() {
    let mut bytes = record().encode();
    bytes[14] = 2;
    resign(&mut bytes);
    assert_eq!(
        ConfigurationRecord::decode(&bytes),
        Err(ConfigurationRecordError::UnsupportedFields)
    );
}

#[test]
fn old_integrity_scope_cannot_vote_under_the_new_configuration() {
    use ozzy_proto::LinkSessionId;
    use ozzy_replication::wire::{Control, PeerBinding, WireLimits, decode, encode_control};
    use ozzy_replication::{Commit, Prefix};
    let configuration = record().configuration();
    let mut old_scope = configuration.scope();
    // Frozen obsolete-profile digest for exactly the same group and voters.
    old_scope.configuration_digest = Digest::from_bytes([
        129, 249, 44, 31, 5, 20, 87, 158, 134, 230, 251, 95, 94, 176, 38, 61, 97, 86, 1, 251, 222,
        242, 87, 210, 152, 1, 107, 59, 211, 100, 243, 96,
    ]);
    let peer = voters()[0].node_id;
    let session = LinkSessionId::from_bytes([9; 16]);
    let mut metadata = [0; 160];
    let encoded = encode_control(
        peer,
        session,
        Control::Commit(Commit {
            scope: old_scope,
            committed: Prefix::GENESIS,
        }),
        &mut metadata,
    )
    .unwrap();
    let result = decode(
        &[&encoded.header, &metadata[..encoded.metadata_bytes], &[]],
        PeerBinding::new(configuration, peer, session).unwrap(),
        WireLimits::default(),
    );
    assert!(result.is_err());
}
