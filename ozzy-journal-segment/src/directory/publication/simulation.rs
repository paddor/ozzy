//! Deterministic byte faults through the production writer and publisher.

use super::*;
use crate::*;
use ozzy_journal::progress::JournalGeneration;
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};
use std::sync::{Arc, Mutex};

use crate::simulation::io::{Disk, Memory, Segment};

mod reordered;

fn publish(memory: &mut Memory, manifest: &Manifest) -> Result<(), DirectoryError> {
    let bytes = encode_manifest(manifest)?;
    let current = encode_current(CurrentReference {
        group_id: manifest.identity.group_id,
        store_id: manifest.identity.store_id,
        generation: manifest.generation,
        manifest_digest: manifest_digest(&bytes, MetadataLimits::default())?,
    })?;
    install_immutable(
        memory,
        &format!("MANIFEST.{}", manifest.generation),
        &bytes,
        &mut super::super::NoopObserver,
    )?;
    replace_current(
        memory,
        manifest.generation,
        &current,
        &mut super::super::NoopObserver,
    )
}

fn fixture(seed: u64) -> (Disk, Manifest, LogPosition, usize) {
    let mut memory = Memory::default();
    memory.0.lock().unwrap().max_write = (seed as usize % 127) + 1;
    memory.write_new("segment", &[]).unwrap();
    let group_id = GroupId::from_bytes([1; 16]);
    let header = SegmentHeader::new(group_id, 1, None, Digest::ZERO, 32768).unwrap();
    let mut writer = SegmentWriter::initialize_at(
        Segment(memory.clone(), "segment".into()),
        header,
        JournalGeneration(1),
        1,
        ChainPosition::GENESIS,
    )
    .unwrap();
    memory.sync_directory().unwrap();
    let mut manifest = Manifest {
        generation: 1,
        parent_generation: 0,
        identity: GroupIdentity {
            group_id,
            replica_node_id: NodeId::from_bytes([2; 16]),
            volume_id: VolumeId::from_bytes([3; 16]),
            store_id: StoreId::from_bytes([4; 16]),
            store_generation: 1,
        },
        configuration_epoch: 1,
        commit_mode: CommitMode::External,
        durable_evidence: false,
        promised_view: 0,
        last_normal_view: 0,
        accepted: LogPosition::GENESIS,
        committed: LogPosition::GENESIS,
        checkpoint: None,
        segments: vec![SegmentReference {
            segment_id: 1,
            file_generation: 0,
            first_group_number: 1,
            first_chain: ChainPosition::GENESIS,
            capacity: 32768,
            sealed: None,
        }],
    };
    let mut first_end = 0;
    let mut first = LogPosition::GENESIS;
    for number in 1..=2 {
        let previous = writer.written_position().next_chain().previous_digest();
        let operation = CanonicalOperation {
            group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: number,
            previous_digest: previous,
            kind: OperationKind::Barrier,
            body: &seed.wrapping_add(number).to_be_bytes().repeat(2),
        };
        let written = writer.append(&[operation]).unwrap();
        writer.sync_through(written).unwrap();
        manifest.accepted = LogPosition {
            op_number: number,
            digest: written.next_chain().previous_digest(),
        };
        if number == 1 {
            first = manifest.accepted;
            first_end = written.end_offset() as usize;
            publish(&mut memory, &manifest).unwrap();
            manifest.parent_generation = 1;
            manifest.generation = 2;
        }
    }
    drop(writer);
    let mut disk = memory.0.lock().unwrap().clone();
    disk.trace.clear();
    (disk, manifest, first, first_end)
}

fn recovered(memory: &mut Memory) -> Result<LogPosition, DirectoryError> {
    let current = decode_current(&memory.read_exact("CURRENT", CURRENT_BYTES)?)?;
    let name = format!("MANIFEST.{}", current.generation);
    let bytes = memory.read_exact(&name, MANIFEST_HEADER_BYTES + SEGMENT_REFERENCE_BYTES)?;
    if manifest_digest(&bytes, MetadataLimits::default())? != current.manifest_digest {
        return Err(DirectoryError::CurrentMismatch);
    }
    let manifest = decode_manifest(&bytes, MetadataLimits::default())?;
    let accepted = if manifest.durable_evidence {
        let bytes = memory.read_exact("DURABLE", super::super::evidence::FILE_BYTES)?;
        super::super::evidence::Copies::decode_file(&bytes, &manifest)?.protected(&manifest)?
    } else {
        manifest.accepted
    };
    let writer = SegmentWriter::recover_canonical(
        Segment(memory.clone(), "segment".into()),
        JournalGeneration(2),
        1,
        ChainPosition::GENESIS,
        DecodeLimits::default(),
        32768,
        CanonicalRecoveryRequirements {
            operation_limits: OperationLimits::default(),
            protected: [Some(accepted.following_chain()?), None],
            configuration_epoch: Some(1),
            promised_view: Some(0),
            discard_damaged_tail: true,
        },
    )?;
    Ok(LogPosition {
        op_number: writer.durable_position().next_chain().next_op_number() - 1,
        digest: writer.durable_position().next_chain().previous_digest(),
    })
}

#[test]
fn fixed_evidence_publication_crash_cuts_preserve_confirmation() {
    use super::super::evidence::{COPY_STRIDE, Copies, RECORD_BYTES, image};
    for seed in 1..=32 {
        let (baseline, mut manifest, prior, first_end) = fixture(seed);
        let latest = manifest.accepted;
        manifest.accepted = prior;
        manifest.durable_evidence = true;
        let mut initial = Memory(Arc::new(Mutex::new(baseline)));
        let bytes = image(&manifest, prior).unwrap();
        replace_evidence(&mut initial, &bytes, &mut super::super::NoopObserver).unwrap();
        publish(&mut initial, &manifest).unwrap();
        let mut baseline = initial.0.lock().unwrap().clone();
        baseline.trace.clear();
        let copies = Copies::decode_file(&bytes, &manifest).unwrap();
        let (record, first) = copies.next(&manifest, latest).unwrap();
        // Two chunked in-place writes followed by one barrier.
        let actions = 2 * RECORD_BYTES.div_ceil(baseline.max_write.max(1)) + 1;
        let whole = 0..RECORD_BYTES;
        let half = 0..RECORD_BYTES / 2;
        for cut in 1..=actions + 1 {
            // Each copy lies inside one sector, which reaches the device whole
            // or not at all. Tearing inside both sectors must fail closed.
            for (persisted, sector_atomic) in [
                ([0..0, 0..0], true),
                ([whole.clone(), 0..0], true),
                ([0..0, whole.clone()], true),
                ([whole.clone(), whole.clone()], true),
                ([half.clone(), half.clone()], false),
            ] {
                let mut memory = Memory(Arc::new(Mutex::new(baseline.clone())));
                memory.0.lock().unwrap().fail_at = Some(cut);
                let success = overwrite_evidence(
                    &mut memory,
                    first,
                    &record,
                    &mut super::super::NoopObserver,
                )
                .is_ok();
                {
                    let mut disk = memory.0.lock().unwrap();
                    for (copy, persisted) in persisted.iter().enumerate() {
                        let start = copy * COPY_STRIDE;
                        let range = start + persisted.start..start + persisted.end;
                        disk.persist_range("DURABLE", range).unwrap();
                    }
                    disk.crash();
                }
                erase_last(&memory, first_end, seed % 2 == 0);
                match recovered(&mut memory) {
                    Ok(position) => {
                        assert!(!success, "seed={seed} cut={cut} forgot confirmed history");
                        assert_eq!(position, prior);
                    }
                    Err(DirectoryError::Writer(WriterError::ProtectedPrefixMismatch(2))) => {}
                    Err(DirectoryError::Metadata(MetadataError::DigestMismatch(_)))
                        if !sector_atomic && !success => {}
                    Err(error) => panic!("seed={seed} cut={cut}: {error}"),
                }
            }
        }
    }
}

fn erase_last(memory: &Memory, first_end: usize, zero: bool) {
    let mut disk = memory.0.lock().unwrap();
    let id = disk.inode("segment").unwrap();
    let inode = &mut disk.files[id];
    if zero {
        inode.stable[first_end..].fill(0);
    } else {
        inode.stable.truncate(first_end);
    }
    inode.pending.clone_from(&inode.stable);
}

#[test]
fn seeded_publication_crashes_preserve_every_previously_confirmed_prefix() {
    for seed in 1..=32 {
        let (baseline, manifest, prior, first_end) = fixture(seed);
        let mut complete = Memory(Arc::new(Mutex::new(baseline.clone())));
        publish(&mut complete, &manifest).unwrap();
        let actions = complete.0.lock().unwrap().trace.len();
        for cut in 1..=actions + 1 {
            let mut memory = Memory(Arc::new(Mutex::new(baseline.clone())));
            memory.0.lock().unwrap().fail_at = Some(cut);
            let success = publish(&mut memory, &manifest).is_ok();
            let confirmed = if success { manifest.accepted } else { prior };
            memory.0.lock().unwrap().crash();
            erase_last(&memory, first_end, seed % 2 == 0);
            match recovered(&mut memory) {
                Ok(position) => assert_eq!(position, confirmed, "seed={seed} cut={cut}"),
                Err(error) => assert!(
                    matches!(
                        error,
                        DirectoryError::Writer(WriterError::ProtectedPrefixMismatch(2))
                    ),
                    "seed={seed} cut={cut}: {error}"
                ),
            }
        }
    }
}

#[test]
fn omitted_directory_barrier_is_detected_by_confirmation_oracle() {
    let (baseline, manifest, prior, first_end) = fixture(17);
    let mut memory = Memory(Arc::new(Mutex::new(baseline)));
    memory.0.lock().unwrap().omit_directory_sync = true;
    publish(&mut memory, &manifest).unwrap(); // Deliberately invalid success.
    memory.0.lock().unwrap().crash();
    erase_last(&memory, first_end, true);
    let actual = recovered(&mut memory).unwrap();
    assert_eq!(actual, prior);
    assert_ne!(
        actual, manifest.accepted,
        "oracle must detect forgotten confirmation"
    );
}

#[test]
fn successful_publication_survives_crash_with_intact_payload() {
    for seed in 1..=32 {
        let (baseline, manifest, _, _) = fixture(seed);
        let mut memory = Memory(Arc::new(Mutex::new(baseline)));
        publish(&mut memory, &manifest).unwrap();
        memory.0.lock().unwrap().crash();
        assert_eq!(recovered(&mut memory).unwrap(), manifest.accepted);
    }
}

#[test]
fn torn_unconfirmed_append_preserves_the_durable_confirmed_prefix() {
    for seed in 1..=16 {
        let (baseline, manifest, _, _) = fixture(seed);
        let mut memory = Memory(Arc::new(Mutex::new(baseline)));
        publish(&mut memory, &manifest).unwrap();
        let mut writer = SegmentWriter::recover_protecting(
            Segment(memory.clone(), "segment".into()),
            JournalGeneration(3),
            1,
            ChainPosition::GENESIS,
            DecodeLimits::default(),
            32768,
            manifest.accepted.following_chain().unwrap(),
        )
        .unwrap();
        let start = writer.written_position().end_offset() as usize;
        let operation = CanonicalOperation {
            group_id: manifest.identity.group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: 3,
            previous_digest: manifest.accepted.digest,
            kind: OperationKind::Barrier,
            body: &[33; 16],
        };
        writer.append(&[operation]).unwrap();
        drop(writer); // No data barrier and no confirmation for operation 3.
        let pending = memory.0.lock().unwrap().clone();
        for kept in [1, 7, 15, 127, 255, 4095] {
            let mut crash = pending.clone();
            let id = crash.inode("segment").unwrap();
            let inode = &mut crash.files[id];
            let cut = (start + kept).min(inode.pending.len() - 1);
            inode.stable = inode.pending[..cut].to_vec();
            crash.crash();
            let mut restarted = Memory(Arc::new(Mutex::new(crash)));
            assert_eq!(
                recovered(&mut restarted).unwrap(),
                manifest.accepted,
                "seed={seed} torn_bytes={kept}"
            );
        }
    }
}

#[test]
fn damaged_selected_metadata_never_falls_back_to_an_older_valid_generation() {
    for target in ["CURRENT", "MANIFEST.2"] {
        for damage in 0..3 {
            let (baseline, manifest, _, _) = fixture(19);
            let mut memory = Memory(Arc::new(Mutex::new(baseline)));
            publish(&mut memory, &manifest).unwrap();
            let mut disk = memory.0.lock().unwrap();
            let id = disk.inode(target).unwrap();
            match damage {
                0 => disk.files[id].stable[0] ^= 1,
                1 => {
                    disk.files[id].stable.pop();
                }
                2 => {
                    disk.stable_names.remove(target);
                }
                _ => unreachable!(),
            }
            disk.crash();
            drop(disk);
            assert!(recovered(&mut memory).is_err(), "{target} damage={damage}");
        }
    }
}

#[test]
fn omitted_evidence_barrier_is_detected_by_confirmation_oracle() {
    use super::super::evidence::{Copies, image};
    let (baseline, mut manifest, prior, first_end) = fixture(17);
    let latest = manifest.accepted;
    manifest.accepted = prior;
    manifest.durable_evidence = true;
    let mut memory = Memory(Arc::new(Mutex::new(baseline)));
    let bytes = image(&manifest, prior).unwrap();
    replace_evidence(&mut memory, &bytes, &mut super::super::NoopObserver).unwrap();
    publish(&mut memory, &manifest).unwrap();
    let (record, first) = Copies::decode_file(&bytes, &manifest)
        .unwrap()
        .next(&manifest, latest)
        .unwrap();
    memory.0.lock().unwrap().omit_in_place_sync = true;
    // Deliberately invalid success.
    overwrite_evidence(&mut memory, first, &record, &mut super::super::NoopObserver).unwrap();
    memory.0.lock().unwrap().crash();
    erase_last(&memory, first_end, true);
    let actual = recovered(&mut memory).unwrap();
    assert_eq!(actual, prior);
    assert_ne!(actual, latest, "oracle must detect forgotten confirmation");
}
