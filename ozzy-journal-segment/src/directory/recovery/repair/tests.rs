use super::*;
use crate::directory::progress_tests::{FailAt, journal_mode};
use crate::directory::{NoopObserver, PersistencePhase};
use crate::{GroupIdentity, MetadataLimits, OperationKind};
use std::fs;
use std::path::PathBuf;

const CONFIG: &[u8] = b"test configuration";

struct Fixture {
    _temporary: tempfile::TempDir,
    root: PathBuf,
    identity: GroupIdentity,
    operations: Vec<CanonicalOperation<'static>>,
    healthy: Vec<(PathBuf, Vec<u8>)>,
    original: Vec<(PathBuf, Vec<u8>)>,
    accepted: LogPosition,
    sealed: Vec<SealedSegment>,
}

fn fixture() -> Fixture {
    let (temporary, mut journal) = journal_mode(true);
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    let bodies: [&[u8]; 8] = [
        &[1; 16], &[2; 16], &[3; 16], &[4; 16], &[5; 16], &[6; 16], &[7; 16], &[8; 16],
    ];
    let mut operations = Vec::new();
    let mut digest = Digest::ZERO;
    for (index, body) in bodies.into_iter().enumerate() {
        let operation = CanonicalOperation {
            group_id: identity.group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: index as u64 + 1,
            previous_digest: digest,
            kind: OperationKind::Barrier,
            body,
        };
        digest = logical_operation_digest(&operation);
        operations.push(operation);
    }
    for (index, pair) in operations.chunks(2).enumerate() {
        let position = journal.append(pair).unwrap();
        journal.sync_through(position).unwrap();
        journal.publish_durable_progress().unwrap();
        if index < 3 {
            journal = journal.roll_active(1024 * 1024).unwrap();
        }
    }
    let accepted = journal.accepted_position().unwrap();
    let sealed = journal
        .directory
        .manifest
        .segments
        .iter()
        .filter_map(|r| r.sealed)
        .collect();
    let healthy = [2, 4]
        .into_iter()
        .map(|id| {
            let path = journal.directory.segment_path(id).unwrap();
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    let original = [1, 3]
        .into_iter()
        .map(|id| {
            let path = journal.directory.segment_path(id).unwrap();
            let mut bytes = fs::read(&path).unwrap();
            bytes[if id == 1 { 0 } else { 4096 + 192 }] ^= 1;
            fs::write(&path, &bytes).unwrap();
            (path, bytes)
        })
        .collect();
    drop(journal);
    Fixture {
        _temporary: temporary,
        root,
        identity,
        operations,
        healthy,
        original,
        accepted,
        sealed,
    }
}

fn limits() -> SealedRepairLimits {
    SealedRepairLimits {
        max_segment_bytes: 1024 * 1024,
        max_chunk_operations: 2,
        max_chunk_body_bytes: 1024,
        max_staged_bytes: 2 * 1024 * 1024,
        max_orphan_probes: 8,
        body_encoding: BodyEncoding::Raw,
    }
}

impl Fixture {
    fn start(&self, generation: u128) -> SealedRepair {
        let directory = if generation == 9 {
            GroupDirectory::open_for_repair(
                &self.root,
                self.identity,
                MetadataLimits::default(),
                CONFIG,
            )
            .unwrap()
            .quarantine_for_recovery(CONFIG)
            .unwrap()
        } else {
            GroupDirectory::open_recovering(
                &self.root,
                self.identity,
                MetadataLimits::default(),
                CONFIG,
            )
            .unwrap()
        };
        directory
            .begin_sealed_repair(
                CONFIG,
                JournalGeneration(generation),
                7,
                self.accepted,
                DecodeLimits::default(),
                OperationLimits::default(),
                limits(),
            )
            .unwrap()
    }

    fn fill(&self, repair: &mut SealedRepair) -> usize {
        let mut transferred = 0;
        while let Some(range) = repair.pending().unwrap() {
            // Transfer chunks need not match physical write groups.
            repair
                .append(
                    &self.operations
                        [range.after.op_number as usize..=range.after.op_number as usize],
                )
                .unwrap();
            transferred += 1;
        }
        transferred
    }

    fn assert_unchanged(&self) {
        for (path, bytes) in self.healthy.iter().chain(&self.original) {
            assert_eq!(fs::read(path).unwrap(), *bytes);
        }
    }

    fn assert_nonvoting(&self) {
        assert_eq!(
            fs::read(self.root.join("CONFIGURATION")).unwrap(),
            crate::directory::recovery::recovery_marker(CONFIG).unwrap()
        );
        assert!(
            GroupDirectory::open_with_configuration(
                &self.root,
                self.identity,
                MetadataLimits::default(),
                CONFIG
            )
            .is_err()
        );
    }
}

#[test]
fn repairs_only_damaged_ranges_preserves_successors_and_reclaims_obsolete_incarnations() {
    let fixture = fixture();
    let mut repair = fixture.start(9);
    repair.state.limits.max_chunk_operations = 1;
    fixture.assert_nonvoting();
    assert_eq!(
        fixture.fill(&mut repair),
        1,
        "only one damaged operation fetched; header repaired locally"
    );
    let journal = repair
        .finish(CONFIG, CanonicalRecoveryLimits::default())
        .unwrap();
    assert_eq!(journal.accepted_position().unwrap(), fixture.accepted);
    assert_eq!(journal.directory.manifest.last_normal_view, 0);
    assert_eq!(journal.directory.manifest.promised_view, 7);
    let first = journal.directory.manifest.segments[0].sealed.unwrap();
    assert_eq!(first.valid_bytes, fixture.sealed[0].valid_bytes);
    assert_ne!(
        first.digest, fixture.sealed[0].digest,
        "new physical incarnation must invalidate indexes even with unchanged group sizes"
    );
    assert_eq!(
        journal
            .directory
            .manifest
            .segments
            .iter()
            .map(|r| r.file_generation)
            .collect::<Vec<_>>(),
        [1, 0, 1, 0]
    );
    fixture.assert_unchanged();
    let pin = journal.pin_segments(&[1, 3]).unwrap();
    assert!(pin.segment_path(1).unwrap().ends_with("1.1.log"));
    assert!(
        journal
            .reclaim_unreferenced_segments()
            .unwrap()
            .removed_segment_ids
            .is_empty()
    );
    drop(pin);
    assert_eq!(
        journal
            .reclaim_unreferenced_segments()
            .unwrap()
            .removed_segment_ids,
        [1, 3]
    );
    drop(journal);
    let journal = GroupDirectory::open_with_configuration(
        &fixture.root,
        fixture.identity,
        MetadataLimits::default(),
        CONFIG,
    )
    .unwrap()
    .recover(
        JournalGeneration(11),
        DecodeLimits::default(),
        OperationLimits::default(),
    )
    .unwrap();
    assert_eq!(journal.accepted_position().unwrap(), fixture.accepted);
}

#[test]
fn interrupted_chunks_keep_original_selection_and_resume_with_new_incarnations() {
    let fixture = fixture();
    let mut first = fixture.start(9);
    first.append(&fixture.operations[4..5]).unwrap();
    drop(first);
    fixture.assert_nonvoting();
    fixture.assert_unchanged();
    let mut second = fixture.start(10);
    fixture.fill(&mut second);
    let journal = second
        .finish(CONFIG, CanonicalRecoveryLimits::default())
        .unwrap();
    assert_eq!(journal.directory.manifest.segments[0].file_generation, 2);
    assert_eq!(journal.accepted_position().unwrap(), fixture.accepted);
}

#[test]
fn wrong_history_incomplete_transfer_and_resource_overflow_never_publish() {
    for case in 0..4 {
        let fixture = fixture();
        let mut repair = fixture.start(9);
        match case {
            0 => {
                assert!(
                    repair
                        .finish(CONFIG, CanonicalRecoveryLimits::default())
                        .is_err()
                );
            }
            1 => {
                let wrong = CanonicalOperation {
                    body: &[99; 16],
                    ..fixture.operations[4]
                };
                assert!(repair.append(&[wrong]).is_err());
                assert!(repair.pending().is_err());
            }
            2 => {
                repair.state.limits.max_staged_bytes = 1;
                assert!(repair.append(&fixture.operations[4..5]).is_err());
            }
            3 => {
                assert!(repair.append(&fixture.operations[..3]).is_err());
            }
            _ => unreachable!(),
        }
        fixture.assert_nonvoting();
        fixture.assert_unchanged();
    }
}

#[test]
fn every_manifest_publication_cut_keeps_repair_nonvoting() {
    for phase in [
        PersistencePhase::ManifestTemporarySynced,
        PersistencePhase::ManifestLinked,
        PersistencePhase::ManifestDirectorySynced,
        PersistencePhase::CurrentTemporarySynced,
        PersistencePhase::CurrentRenamed,
        PersistencePhase::CurrentDirectorySynced,
    ] {
        let fixture = fixture();
        let mut repair = fixture.start(9);
        fixture.fill(&mut repair);
        assert!(
            repair
                .finish_observing(
                    CONFIG,
                    CanonicalRecoveryLimits::default(),
                    &mut FailAt(phase),
                    |_| Ok(())
                )
                .is_err()
        );
        fixture.assert_nonvoting();
        fixture.assert_unchanged();
        let directory = GroupDirectory::open_recovering(
            &fixture.root,
            fixture.identity,
            MetadataLimits::default(),
            CONFIG,
        )
        .unwrap();
        let incarnations: Vec<_> = directory
            .manifest
            .segments
            .iter()
            .map(|r| r.file_generation)
            .collect();
        assert!(incarnations == [0, 0, 0, 0] || incarnations == [1, 0, 1, 0]);
    }
}

#[test]
fn configuration_publication_cuts_allow_only_marker_or_fully_repaired_history() {
    use super::super::PublicationPhase;
    for phase in [
        PublicationPhase::Validated,
        PublicationPhase::ConfigurationSynced,
        PublicationPhase::ConfigurationRenamed,
        PublicationPhase::DirectorySynced,
    ] {
        let fixture = fixture();
        let mut repair = fixture.start(9);
        fixture.fill(&mut repair);
        assert!(
            repair
                .finish_observing(
                    CONFIG,
                    CanonicalRecoveryLimits::default(),
                    &mut NoopObserver,
                    |at| if at == phase {
                        Err(io::Error::other("publication cut"))
                    } else {
                        Ok(())
                    }
                )
                .is_err()
        );
        fixture.assert_unchanged();
        if let Ok(directory) = GroupDirectory::open_with_configuration(
            &fixture.root,
            fixture.identity,
            MetadataLimits::default(),
            CONFIG,
        ) {
            let journal = directory
                .recover(
                    JournalGeneration(12),
                    DecodeLimits::default(),
                    OperationLimits::default(),
                )
                .unwrap();
            assert_eq!(journal.accepted_position().unwrap(), fixture.accepted);
            assert_eq!(journal.directory.manifest.last_normal_view, 0);
        } else {
            GroupDirectory::open_recovering(
                &fixture.root,
                fixture.identity,
                MetadataLimits::default(),
                CONFIG,
            )
            .unwrap();
        }
    }
}

#[test]
fn lost_file_truncated_entries_and_header_damage_fetch_only_unusable_operations() {
    for case in 0..4 {
        let fixture = fixture();
        let path = fixture.root.join("segments/3.log");
        let mut bytes = fs::read(&path).unwrap();
        match case {
            0 => {
                fs::remove_file(&path).unwrap();
            }
            1 => {
                bytes.truncate(4096);
                fs::write(&path, bytes).unwrap();
            }
            2 => {
                // First body already damaged; destroy the next independent header too.
                bytes[4096 + 208 + 136] ^= 1;
                fs::write(&path, bytes).unwrap();
            }
            3 => {
                // All canonical entries survive. Only the group seal is damaged.
                bytes[4096 + 192] ^= 1;
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
                fs::write(&path, bytes).unwrap();
            }
            _ => unreachable!(),
        }
        let mut repair = fixture.start(9);
        assert_eq!(fixture.fill(&mut repair), if case == 3 { 0 } else { 2 });
        let journal = repair
            .finish(CONFIG, CanonicalRecoveryLimits::default())
            .unwrap();
        assert_eq!(journal.accepted_position().unwrap(), fixture.accepted);
        for (path, bytes) in &fixture.healthy {
            assert_eq!(fs::read(path).unwrap(), *bytes);
        }
    }
}
