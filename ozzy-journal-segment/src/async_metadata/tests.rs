use super::*;
use crate::test_io::{drive, drive_with, poll};
use crate::{
    ChainPosition, CommitMode, Digest, GroupDirectory, GroupIdentity, LogPosition, SegmentHeader,
    SegmentReference,
};
use ozzy_io::simulation::{self, Controller, Effect, Image, ImageLimits, Stage};
use ozzy_io::{Class, Operation, Outcome, Quota};
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};
use std::{path::Path, task::Poll};

fn limits() -> Limits {
    Limits {
        max_file_bytes: 256 * 1024,
        chunk_bytes: 4096,
    }
}
fn metadata_limits() -> MetadataLimits {
    MetadataLimits {
        max_manifest_bytes: limits().max_file_bytes,
        max_segments: 16,
    }
}
fn config() -> simulation::Config {
    simulation::Config {
        limits: ozzy_io::Limits {
            shards: 1,
            data: Quota {
                operations: 2,
                bytes: 1024 * 1024,
            },
            progress: Quota {
                operations: 2,
                bytes: 1024 * 1024,
            },
        },
        handles: 16,
        image: ImageLimits {
            nodes: 128,
            directory_entries: 512,
            file_bytes: 1024 * 1024,
            total_bytes: 8 * 1024 * 1024,
        },
        trace_events: 10_000,
    }
}

fn setup(image: Image) -> (Controller, Local) {
    let (controller, mut clients) = Controller::new(config(), image).unwrap();
    (controller, Local::new(clients.remove(0)))
}

fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([1; 16]),
        replica_node_id: NodeId::from_bytes([2; 16]),
        volume_id: VolumeId::from_bytes([3; 16]),
        store_id: StoreId::from_bytes([4; 16]),
        store_generation: 1,
    }
}

fn manifest() -> Manifest {
    Manifest {
        generation: 1,
        parent_generation: 0,
        identity: identity(),
        configuration_epoch: 1,
        commit_mode: CommitMode::External,
        durable_evidence: true,
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
    }
}

fn current(manifest: &Manifest) -> CurrentReference {
    CurrentReference {
        group_id: manifest.identity.group_id,
        store_id: manifest.identity.store_id,
        generation: manifest.generation,
        manifest_digest: crate::manifest_digest(
            &crate::encode_manifest(manifest).unwrap(),
            metadata_limits(),
        )
        .unwrap(),
    }
}

#[tokio::test]
async fn async_metadata_is_readable_by_the_existing_segment_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("group");
    let header = SegmentHeader::new(identity().group_id, 1, None, Digest::ZERO, 32768).unwrap();
    let old = GroupDirectory::format_new_with_durable_evidence(
        &root,
        identity(),
        1,
        &header,
        b"test deployment",
    )
    .unwrap();
    let previous = old.manifest().clone();
    drop(old);
    let (pool, mut clients) = ozzy_io_pool::Pool::new(ozzy_io_pool::Config {
        threads: 1,
        max_inflight: 1,
        handles: 16,
        limits: config().limits,
    })
    .unwrap();
    let mut directory = Directory::open(root.clone(), Local::new(clients.remove(0)), limits())
        .await
        .unwrap();
    let mut next = previous.clone();
    next.generation += 1;
    next.parent_generation = previous.generation;
    let (next, digest) = directory
        .publish_manifest(&previous, next, metadata_limits())
        .await
        .unwrap();
    assert_eq!(digest, current(&next).manifest_digest);
    directory.select_current(current(&next)).await.unwrap();
    drop(directory);
    pool.shutdown().await;
    let recovered = GroupDirectory::open(&root, identity(), metadata_limits()).unwrap();
    assert_eq!(recovered.manifest(), &next);
    recovered
        .recover(
            ozzy_journal::progress::JournalGeneration(2),
            crate::DecodeLimits::default(),
            crate::OperationLimits::default(),
        )
        .unwrap();
}

#[test]
fn exact_immutable_reuse_conflicts_short_transfers_and_name_bounds() {
    let (mut controller, io) = setup(Image::default());
    let mut directory = drive(&mut controller, Directory::open("/".into(), io, limits())).unwrap();
    let bytes = vec![7; 8192];
    drive_with(
        &mut controller,
        directory.install_immutable("MANIFEST.1", &bytes),
        |operation| {
            if matches!(operation, Operation::Write { .. } | Operation::Read { .. }) {
                Effect::Short(19)
            } else {
                Effect::Normal
            }
        },
    )
    .unwrap();
    drive_with(
        &mut controller,
        directory.install_immutable("MANIFEST.1", &bytes),
        |operation| {
            if matches!(operation, Operation::Read { .. }) {
                Effect::Short(13)
            } else {
                Effect::Normal
            }
        },
    )
    .unwrap();
    for name in ["../bad", "group.lock", "a/b", "a\\b", ".", ".."] {
        assert!(drive(&mut controller, directory.install_immutable(name, b"bad")).is_err());
        assert!(!directory.is_faulted());
    }
    assert!(matches!(
        drive(
            &mut controller,
            directory.install_immutable("MANIFEST.1", &[8; 8192])
        ),
        Err(DirectoryError::ImmutableConflict)
    ));
    assert!(directory.is_faulted());
    assert_eq!(
        controller
            .image()
            .bytes(Path::new("/MANIFEST.1"), true)
            .unwrap(),
        bytes
    );
}

fn established() -> Image {
    let (mut controller, io) = setup(Image::default());
    let mut directory = drive(&mut controller, Directory::open("/".into(), io, limits())).unwrap();
    let manifest = manifest();
    drive(
        &mut controller,
        directory.install_immutable(
            "identity",
            &crate::encode_group_identity(identity()).unwrap(),
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        directory.install_immutable("CONFIGURATION", b"test deployment"),
    )
    .unwrap();
    drive(
        &mut controller,
        directory.install_immutable("MANIFEST.1", &crate::encode_manifest(&manifest).unwrap()),
    )
    .unwrap();
    drive(
        &mut controller,
        directory.select_current(current(&manifest)),
    )
    .unwrap();
    let evidence = crate::directory::evidence::image(&manifest, LogPosition::GENESIS).unwrap();
    drive(
        &mut controller,
        directory.replace("DURABLE", ".DURABLE.tmp", &evidence),
    )
    .unwrap();
    drop(directory);
    controller.crash(true).unwrap().0
}

#[test]
fn every_metadata_execution_and_observation_cut_selects_complete_old_or_new_manifest() {
    let baseline = established();
    // Stop before and after every physical operation, independently of whether
    // its successful completion has been delivered to the publication future.
    for deliver in [false, true] {
        let mut reached_completion = false;
        for cut in 0..100 {
            let (mut controller, io) = setup(baseline.clone());
            let mut directory =
                drive(&mut controller, Directory::open("/".into(), io, limits())).unwrap();
            let previous = manifest();
            let mut next = previous.clone();
            next.generation = 2;
            next.parent_generation = 1;
            let complete = {
                let mut future = std::pin::pin!(async {
                    let (next, _) = directory
                        .publish_manifest(&previous, next, metadata_limits())
                        .await?;
                    directory.select_current(current(&next)).await
                });
                let mut complete = false;
                for _ in 0..cut {
                    if let Poll::Ready(result) = poll(future.as_mut()) {
                        result.unwrap();
                        complete = true;
                        break;
                    }
                    let (id, stage) = controller.jobs()[0];
                    match stage {
                        Stage::Queued => {
                            controller.execute(id, Effect::Normal).unwrap();
                            if deliver {
                                controller.deliver(id).unwrap();
                            }
                        }
                        Stage::Executed => controller.deliver(id).unwrap(),
                    }
                }
                complete
            };
            drop(directory);
            let image = controller.crash(true).unwrap().0;
            let selected =
                crate::decode_current(image.bytes(Path::new("/CURRENT"), false).unwrap()).unwrap();
            assert!([1, 2].contains(&selected.generation));
            let name = format!("/MANIFEST.{}", selected.generation);
            let bytes = image.bytes(Path::new(&name), false).unwrap();
            assert_eq!(
                crate::manifest_digest(bytes, metadata_limits()).unwrap(),
                selected.manifest_digest
            );
            let decoded = crate::decode_manifest(bytes, metadata_limits()).unwrap();
            assert_eq!(decoded.generation, selected.generation);
            if complete {
                assert_eq!(selected.generation, 2);
                reached_completion = true;
                break;
            }
        }
        assert!(reached_completion);
    }
}

#[test]
fn publication_stopped_by_process_death_at_any_step_is_published_again() {
    let baseline = established();
    let mut reached_completion = false;
    for cut in 0..100 {
        let (mut controller, io) = setup(baseline.clone());
        let mut directory =
            drive(&mut controller, Directory::open("/".into(), io, limits())).unwrap();
        let previous = manifest();
        let mut next = previous.clone();
        next.generation = 2;
        next.parent_generation = 1;
        let complete = {
            let mut future = std::pin::pin!(async {
                let (next, _) = directory
                    .publish_manifest(&previous, next.clone(), metadata_limits())
                    .await?;
                directory.select_current(current(&next)).await
            });
            let mut complete = false;
            for _ in 0..cut {
                if let Poll::Ready(result) = poll(future.as_mut()) {
                    result.unwrap();
                    complete = true;
                    break;
                }
                let (id, _) = controller.jobs()[0];
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
            complete
        };
        drop(directory);
        // Process death keeps every created name and written byte, also of
        // temporaries that were created but not yet written.
        let image = controller.crash(false).unwrap().0;
        let selected =
            crate::decode_current(image.bytes(Path::new("/CURRENT"), false).unwrap()).unwrap();
        let (mut controller, io) = setup(image);
        let mut directory =
            drive(&mut controller, Directory::open("/".into(), io, limits())).unwrap();
        let expected = if selected.generation == 1 {
            // The restarted owner still selects the old manifest and
            // publishes the same successor again. An unfinished manifest
            // temporary makes it skip that generation.
            let published = drive(&mut controller, async {
                let (next, _) = directory
                    .publish_manifest(&previous, next.clone(), metadata_limits())
                    .await?;
                directory.select_current(current(&next)).await?;
                Ok::<_, DirectoryError>(next)
            });
            let published = published.unwrap_or_else(|error| panic!("cut={cut}: {error:?}"));
            assert!([2, 3].contains(&published.generation), "cut={cut}");
            current(&published)
        } else {
            current(&next)
        };
        drop(directory);
        let image = controller.crash(false).unwrap().0;
        let selected =
            crate::decode_current(image.bytes(Path::new("/CURRENT"), false).unwrap()).unwrap();
        assert_eq!(selected, expected, "cut={cut}");
        if complete {
            reached_completion = true;
            break;
        }
    }
    assert!(reached_completion);
}

#[test]
fn unfinished_temporaries_with_other_bytes_are_replaced() {
    for unfinished in [&b"partial"[..], &[0xa5; crate::CURRENT_BYTES][..]] {
        let (mut controller, io) = setup(established());
        let mut directory =
            drive(&mut controller, Directory::open("/".into(), io, limits())).unwrap();
        for name in [".CURRENT.2.tmp", ".X.tmp"] {
            drive(
                &mut controller,
                directory.install_immutable(name, unfinished),
            )
            .unwrap();
        }
        let mut next = manifest();
        next.generation = 2;
        next.parent_generation = 1;
        let (next, _) = drive(
            &mut controller,
            directory.publish_manifest(&manifest(), next, metadata_limits()),
        )
        .unwrap();
        drive(&mut controller, directory.select_current(current(&next))).unwrap();
        drive(
            &mut controller,
            directory.install_immutable("X", b"installed"),
        )
        .unwrap();
        let image = controller.image();
        assert_eq!(
            crate::decode_current(image.bytes(Path::new("/CURRENT"), false).unwrap()).unwrap(),
            current(&next)
        );
        assert_eq!(image.bytes(Path::new("/X"), false).unwrap(), b"installed");
        assert!(!image.exists(Path::new("/.X.tmp"), false));
    }
}

#[test]
fn canceled_publication_fences_owner_and_keeps_group_locked_until_physical_work_settles() {
    let (mut controller, io) = setup(established());
    let mut directory = drive(
        &mut controller,
        Directory::open("/".into(), io.clone(), limits()),
    )
    .unwrap();
    let pending = {
        let mut future =
            std::pin::pin!(directory.replace("MEMORY_VOTING", ".MEMORY_VOTING.tmp", b"state"));
        assert!(poll(future.as_mut()).is_pending());
        controller.jobs()[0].0
    };
    assert!(directory.is_faulted());
    assert!(matches!(
        drive(&mut controller, directory.install_immutable("x", b"x")),
        Err(DirectoryError::Writer(WriterError::Faulted))
    ));
    drop(directory);
    // Only the outstanding job now holds the original group-lock handle.
    let opening = io
        .submit(
            Class::Progress,
            Operation::Open {
                path: "/group.lock".into(),
                mode: ozzy_io::OpenMode::ReadWrite,
                direct: false,
                data_sync: false,
            },
        )
        .unwrap();
    let id = controller.jobs().last().unwrap().0;
    controller.execute(id, Effect::Normal).unwrap();
    controller.deliver(id).unwrap();
    let opened = drive(&mut controller, opening).unwrap();
    let Outcome::Opened(lock) = &*opened else {
        panic!("lock")
    };
    let lock = lock.clone();
    drop(opened);
    // Do not execute the canceled pending job while inspecting the contender.
    let completion = io
        .submit(
            Class::Progress,
            Operation::LockExclusive {
                handle: lock.clone(),
            },
        )
        .unwrap();
    let id = controller.jobs().last().unwrap().0;
    controller.execute(id, Effect::Normal).unwrap();
    controller.deliver(id).unwrap();
    assert_eq!(
        drive(&mut controller, completion).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    controller.execute(pending, Effect::Normal).unwrap();
    controller.deliver(pending).unwrap();
    drive(
        &mut controller,
        io.execute(Class::Progress, Operation::LockExclusive { handle: lock }),
    )
    .unwrap();
}

#[test]
fn partial_evidence_write_stops_before_touching_the_second_copy() {
    use crate::directory::evidence::{COPY_STRIDE, Copies};
    let (mut controller, io) = setup(established());
    let mut directory = drive(&mut controller, Directory::open("/".into(), io, limits())).unwrap();
    let manifest = manifest();
    let old = controller
        .image()
        .bytes(Path::new("/DURABLE"), false)
        .unwrap();
    let copies = Copies::decode_file(old, &manifest).unwrap();
    let accepted = LogPosition {
        op_number: 1,
        digest: Digest::from_bytes([9; 32]),
    };
    let (record, first) = copies.next(&manifest, accepted).unwrap();
    let mut writes = Vec::new();
    let result = drive_with(
        &mut controller,
        directory.overwrite_evidence(first, &record),
        |operation| {
            if let Operation::Write { offset, .. } = operation {
                writes.push(*offset);
                Effect::Short(80)
            } else {
                Effect::Normal
            }
        },
    );
    assert!(
        matches!(result, Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::WriteZero)
    );
    assert_eq!(writes, [(first * COPY_STRIDE) as u64]);
    assert!(directory.is_faulted());
    controller
        .persist_range(
            Path::new("/DURABLE"),
            first * COPY_STRIDE..first * COPY_STRIDE + 80,
        )
        .unwrap();
    drop(directory);
    let image = controller.crash(true).unwrap().0;
    let copies = Copies::decode_file(
        image.bytes(Path::new("/DURABLE"), false).unwrap(),
        &manifest,
    )
    .unwrap();
    assert!(!copies.mirrored());
    assert_eq!(copies.protected(&manifest).unwrap(), LogPosition::GENESIS);
}

#[test]
fn failed_barrier_never_reports_evidence_success_even_if_bytes_reached_media() {
    use crate::directory::evidence::Copies;
    for after in [false, true] {
        let (mut controller, io) = setup(established());
        let mut directory =
            drive(&mut controller, Directory::open("/".into(), io, limits())).unwrap();
        let manifest = manifest();
        let copies = Copies::decode_file(
            controller
                .image()
                .bytes(Path::new("/DURABLE"), false)
                .unwrap(),
            &manifest,
        )
        .unwrap();
        let accepted = LogPosition {
            op_number: 1,
            digest: Digest::from_bytes([9; 32]),
        };
        let (record, first) = copies.next(&manifest, accepted).unwrap();
        let mut syncs = 0;
        let result = drive_with(
            &mut controller,
            directory.overwrite_evidence(first, &record),
            |operation| {
                if matches!(
                    operation,
                    Operation::Sync {
                        mode: ozzy_io::SyncMode::Data,
                        ..
                    }
                ) {
                    syncs += 1;
                    if after {
                        Effect::FailAfter(io::ErrorKind::Other)
                    } else {
                        Effect::FailBefore(io::ErrorKind::Other)
                    }
                } else {
                    Effect::Normal
                }
            },
        );
        assert!(result.is_err());
        assert!(directory.is_faulted());
        assert_eq!(syncs, 1);
        drop(directory);
        let image = controller.crash(true).unwrap().0;
        let recovered = Copies::decode_file(
            image.bytes(Path::new("/DURABLE"), false).unwrap(),
            &manifest,
        )
        .unwrap()
        .protected(&manifest)
        .unwrap();
        assert_eq!(
            recovered,
            if after {
                accepted
            } else {
                LogPosition::GENESIS
            }
        );
    }
}

#[test]
fn existing_store_open_never_creates_a_missing_lock() {
    let (mut controller, io) = setup(Image::default());
    assert!(
        matches!(drive(&mut controller, Directory::open_existing("/".into(), io, limits())), Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::NotFound)
    );
    assert!(!controller.image().exists(Path::new("/group.lock"), false));
}

#[test]
fn selected_metadata_checks_identity_configuration_evidence_and_exact_generation() {
    let (mut controller, io) = setup(established());
    let mut directory = drive(
        &mut controller,
        Directory::open_existing("/".into(), io, limits()),
    )
    .unwrap();
    let loaded = drive(
        &mut controller,
        directory.read_selected(identity(), metadata_limits(), Some(b"test deployment")),
    )
    .unwrap();
    assert_eq!(loaded.current, current(&manifest()));
    assert_eq!(loaded.manifest, manifest());
    assert_eq!(loaded.protected, LogPosition::GENESIS);
    assert_eq!(
        loaded.configuration.as_deref(),
        Some(b"test deployment".as_slice())
    );
    let mut wrong = identity();
    wrong.store_generation += 1;
    assert!(matches!(
        drive(
            &mut controller,
            directory.read_selected(wrong, metadata_limits(), None)
        ),
        Err(DirectoryError::IdentityMismatch)
    ));
    assert!(matches!(
        drive(
            &mut controller,
            directory.read_selected(identity(), metadata_limits(), Some(b"wrong"))
        ),
        Err(DirectoryError::ConfigurationMismatch)
    ));
    // A valid older generation exists, but CURRENT explicitly selects another.
    let missing = CurrentReference {
        generation: 99,
        ..loaded.current
    };
    drive(&mut controller, directory.select_current(missing)).unwrap();
    assert!(
        matches!(drive(&mut controller, directory.read_selected(identity(), metadata_limits(), None)), Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::NotFound)
    );
}

#[test]
fn evidence_descriptor_is_reused_but_invalidated_when_its_inode_is_replaced() {
    use crate::directory::evidence::{Copies, NAME};
    let (mut controller, io) = setup(established());
    let mut directory = drive(
        &mut controller,
        Directory::open_existing("/".into(), io, limits()),
    )
    .unwrap();
    let metadata = manifest();
    let copies = Copies::decode_file(
        controller
            .image()
            .bytes(Path::new("/DURABLE"), false)
            .unwrap(),
        &metadata,
    )
    .unwrap();
    let (record, first) = copies.next(&metadata, LogPosition::GENESIS).unwrap();
    let mut opens = 0;
    for _ in 0..2 {
        drive_with(&mut controller, directory.overwrite_evidence(first, &record), |operation| {
            if matches!(operation, Operation::Open { path, .. } if path.file_name().unwrap() == NAME) { opens += 1; }
            Effect::Normal
        }).unwrap();
    }
    assert_eq!(opens, 1);
    let replacement = crate::directory::evidence::image(&metadata, LogPosition::GENESIS).unwrap();
    drive(
        &mut controller,
        directory.replace(NAME, ".DURABLE.tmp", &replacement),
    )
    .unwrap();
    let newer = LogPosition {
        op_number: 1,
        digest: Digest::from_bytes([7; 32]),
    };
    let copies = Copies::decode_file(&replacement, &metadata).unwrap();
    let (record, first) = copies.next(&metadata, newer).unwrap();
    drive_with(&mut controller, directory.overwrite_evidence(first, &record), |operation| {
        if matches!(operation, Operation::Open { path, .. } if path.file_name().unwrap() == NAME) { opens += 1; }
        Effect::Normal
    }).unwrap();
    assert_eq!(opens, 2);
    let actual = Copies::decode_file(
        controller
            .image()
            .bytes(Path::new("/DURABLE"), true)
            .unwrap(),
        &metadata,
    )
    .unwrap();
    assert_eq!(actual.protected(&metadata).unwrap(), newer);
}
