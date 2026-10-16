use super::*;
use crate::CheckpointError;

fn source() -> (Controller, Journal) {
    let (mut controller, mut journal) = empty_journal();
    drive(&mut controller, write(&mut journal, &barrier(1)));
    drive(&mut controller, confirm(&mut journal));
    (controller, journal)
}

fn schema() -> Digest {
    Digest::from_bytes([0x71; 32])
}

#[test]
fn detached_checkpoint_fragments_cross_file_boundaries_without_blocking_journal_progress() {
    let (mut controller, mut journal) = source();
    let captured = journal.committed_position().unwrap();
    let generation = journal.manifest.generation;
    let id = CheckpointId::from_bytes([0x72; 16]);
    let state: Vec<_> = (0..10_007).map(|position| (position % 251) as u8).collect();
    let plan = journal
        .prepare_checkpoint(id, schema(), 4096, state.len() as u64)
        .unwrap();
    assert_eq!(plan.spec().position, captured);
    assert_eq!(plan.spec().source_manifest_generation, generation);
    assert!(plan.retained_bytes() < 16 * 1024);
    let mut writer = drive(&mut controller, plan.start()).unwrap();
    assert_eq!(writer.fragment_bytes(), 4096);
    assert!(writer.retained_bytes() < 16 * 1024);
    for (position, fragment) in state.chunks(1379).enumerate() {
        let before = writer.remaining_bytes();
        drive(&mut controller, writer.push(fragment)).unwrap();
        assert_eq!(writer.remaining_bytes(), before - fragment.len() as u64);
        if position % 2 == 0 {
            drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        }
        drive(
            &mut controller,
            write(&mut journal, &barrier(2 + (before % 200) as u8)),
        );
        drive(&mut controller, confirm(&mut journal));
        assert!(!journal.is_faulted());
    }
    let cleaned = drive(&mut controller, journal.reclaim_unreferenced_metadata(32)).unwrap();
    assert_ne!(cleaned.removed_manifest_generations, [] as [u64; 0]);
    assert!(!cleaned.removed_manifest_generations.contains(&generation));
    let files = drive(&mut controller, writer.finish()).unwrap();
    assert_eq!(files.reference().position, captured);
    assert_eq!(files.manifest().chunks.len(), 3);
    assert_eq!(drive(&mut controller, files.read_state()).unwrap(), state);
    let cleaned = drive(&mut controller, journal.reclaim_unreferenced_checkpoints(8)).unwrap();
    assert_eq!(cleaned.pinned_checkpoint_ids, [id]);
    let now = journal.committed_position().unwrap();
    assert!(now.op_number > captured.op_number);
    drive(&mut controller, journal.install_checkpoint(id)).unwrap();
    assert_eq!(journal.committed_position().unwrap(), now);
    assert_eq!(journal.manifest.checkpoint.unwrap(), files.reference());
    assert_eq!(controller.jobs().len(), 0);
}

#[test]
fn streamed_and_materialized_checkpoint_manifests_and_digests_are_identical() {
    let (mut controller, mut journal) = source();
    let id = CheckpointId::from_bytes([0x73; 16]);
    let state: Vec<_> = (0..5003).map(|position| position as u8).collect();
    let expected = crate::CheckpointManifest {
        group_id: journal.manifest.identity.group_id,
        store_id: journal.manifest.identity.store_id,
        checkpoint_id: id,
        position: journal.committed_position().unwrap(),
        configuration_epoch: journal.manifest.configuration_epoch,
        source_manifest_generation: journal.manifest.generation,
        source_manifest_digest: journal.current.manifest_digest,
        state_schema_digest: schema(),
        state_bytes: state.len() as u64,
        chunk_bytes: 1024,
        state_digest: ozzy_journal::integrity::hash("ozzy journal checkpoint state v1", &state),
        chunks: state
            .chunks(1024)
            .enumerate()
            .map(|(ordinal, bytes)| crate::CheckpointChunk {
                ordinal: ordinal as u32,
                logical_offset: ordinal as u64 * 1024,
                bytes: bytes.len() as u64,
                digest: ozzy_journal::integrity::hash("ozzy journal checkpoint chunk v1", bytes),
            })
            .collect(),
    };
    let expected_digest = crate::checkpoint_manifest_digest(
        &crate::encode_checkpoint_manifest(&expected, limits().checkpoint).unwrap(),
        limits().checkpoint,
    )
    .unwrap();
    let mut writer = drive(
        &mut controller,
        journal
            .prepare_checkpoint(id, schema(), 1024, state.len() as u64)
            .unwrap()
            .start(),
    )
    .unwrap();
    for bytes in state.chunks(233) {
        drive(&mut controller, writer.push(bytes)).unwrap();
    }
    let files = drive(&mut controller, writer.finish()).unwrap();
    assert_eq!(files.manifest(), &expected);
    assert_eq!(files.reference().manifest_digest, expected_digest);
    let repeated = drive(
        &mut controller,
        journal.build_checkpoint(id, schema(), 1024, &state),
    )
    .unwrap();
    assert_eq!(repeated.reference(), files.reference());
    assert_eq!(repeated.manifest(), files.manifest());
}

#[test]
fn changed_state_cannot_replace_an_existing_streamed_checkpoint() {
    let (mut controller, mut journal) = source();
    let id = CheckpointId::from_bytes([0x74; 16]);
    let files = drive(
        &mut controller,
        journal.build_checkpoint(id, schema(), 4, b"original"),
    )
    .unwrap();
    let mut writer = drive(
        &mut controller,
        journal
            .prepare_checkpoint(id, schema(), 4, 8)
            .unwrap()
            .start(),
    )
    .unwrap();
    assert!(matches!(
        drive(&mut controller, writer.push(b"altered!")),
        Err(CheckpointError::ImmutableConflict)
    ));
    assert_eq!(
        drive(&mut controller, files.read_state()).unwrap(),
        b"original"
    );
    assert!(!journal.is_faulted());
    drive(&mut controller, write(&mut journal, &barrier(2)));
}

#[test]
fn invalid_fragments_do_not_advance_and_incomplete_streams_cannot_publish() {
    let (mut controller, journal) = source();
    let id = CheckpointId::from_bytes([0x75; 16]);
    let mut writer = drive(
        &mut controller,
        journal
            .prepare_checkpoint(id, schema(), 4096, 5000)
            .unwrap()
            .start(),
    )
    .unwrap();
    for bytes in [&[][..], &[0; 4097][..]] {
        assert!(matches!(
            drive(&mut controller, writer.push(bytes)),
            Err(CheckpointError::LengthMismatch)
        ));
        assert_eq!(writer.remaining_bytes(), 5000);
    }
    drive(&mut controller, writer.push(b"prefix")).unwrap();
    assert!(matches!(
        drive(&mut controller, writer.finish()),
        Err(CheckpointError::LengthMismatch)
    ));
    assert_eq!(journal.manifest.checkpoint, None);
    assert!(!journal.is_faulted());
}

#[test]
fn canceled_stream_write_keeps_physical_work_owned_and_fences_only_its_builder() {
    let (mut controller, mut journal) = source();
    let id = CheckpointId::from_bytes([0x76; 16]);
    let mut writer = drive(
        &mut controller,
        journal
            .prepare_checkpoint(id, schema(), 4096, 10)
            .unwrap()
            .start(),
    )
    .unwrap();
    drive(&mut controller, writer.push(b"a")).unwrap();
    let before = journal.current;
    let mut future = Box::pin(writer.push(b"bcdefghij"));
    assert!(poll(future.as_mut()).is_pending());
    let jobs = controller.jobs();
    assert_eq!(jobs.len(), 1);
    let (job, stage) = jobs[0];
    assert_eq!(stage, Stage::Queued);
    assert!(matches!(
        controller.operation(job).unwrap().unprotected(),
        Operation::Write { .. }
    ));
    drop(future);
    assert_eq!(journal.current, before);
    assert!(!journal.is_faulted());
    assert!(
        matches!(drive(&mut controller, writer.push(b"bcdefghij")), Err(CheckpointError::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe)
    );
    controller.execute(job, Effect::Normal).unwrap();
    controller.deliver(job).unwrap();
    assert!(
        matches!(drive(&mut controller, writer.finish()), Err(CheckpointError::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe)
    );
    drive(&mut controller, write(&mut journal, &barrier(3)));
    drive(&mut controller, confirm(&mut journal));
    assert_eq!(journal.manifest.checkpoint, None);
    assert_eq!(controller.jobs().len(), 0);
}

#[test]
fn failed_chunk_sync_preserves_journal_authority_and_refuses_checkpoint_completion() {
    let (mut controller, mut journal) = source();
    let id = CheckpointId::from_bytes([0x77; 16]);
    let mut writer = drive(
        &mut controller,
        journal
            .prepare_checkpoint(id, schema(), 8, 8)
            .unwrap()
            .start(),
    )
    .unwrap();
    let result = drive_with(&mut controller, writer.push(b"complete"), |operation| {
        if matches!(operation, Operation::Sync { .. }) {
            Effect::FailBefore(io::ErrorKind::Other)
        } else {
            Effect::Normal
        }
    });
    assert!(result.is_err());
    assert!(drive(&mut controller, writer.finish()).is_err());
    assert!(!journal.is_faulted());
    assert_eq!(journal.manifest.checkpoint, None);
    drive(&mut controller, write(&mut journal, &barrier(4)));
}

#[test]
fn checkpoint_stream_bounds_each_read_and_verifies_only_the_complete_image() {
    let (mut controller, mut journal) = source();
    let id = CheckpointId::from_bytes([0x78; 16]);
    let state: Vec<_> = (0..9001).map(|position| (position % 251) as u8).collect();
    let files = drive(
        &mut controller,
        journal.build_checkpoint(id, schema(), 4096, &state),
    )
    .unwrap();
    let mut stream = files.stream();
    assert!(!stream.is_verified());
    assert!(
        matches!(drive(&mut controller, stream.next(0)), Err(CheckpointError::Io(error)) if error.kind() == io::ErrorKind::InvalidInput)
    );
    let mut restored = Vec::new();
    while let Some(bytes) = drive_with(&mut controller, stream.next(137), |operation| {
        if let Operation::Read { length, .. } = operation {
            assert!(*length <= 137);
        }
        Effect::Normal
    })
    .unwrap()
    {
        assert!(bytes.len() <= 137);
        restored.extend_from_slice(&bytes);
        assert_eq!(stream.is_verified(), restored.len() == state.len());
    }
    assert!(stream.is_verified());
    assert_eq!(restored, state);
    assert!(drive(&mut controller, stream.next(1)).unwrap().is_none());
    assert_eq!(controller.jobs().len(), 0);
}

#[test]
fn changed_chunk_bytes_poison_the_stream_before_it_can_expose_complete_state() {
    let (mut controller, mut journal) = source();
    let id = CheckpointId::from_bytes([0x79; 16]);
    let files = drive(
        &mut controller,
        journal.build_checkpoint(id, schema(), 8, b"checkpoint bytes"),
    )
    .unwrap();
    let path = journal
        .root()
        .join("checkpoints")
        .join(crate::checkpoint_name(id))
        .join("CHUNK.00000000");
    let handle = drive(
        &mut controller,
        journal
            .access
            .open(path, ozzy_io::OpenMode::ReadWrite, false, false),
    )
    .unwrap();
    drive(&mut controller, journal.access.write_all(&handle, 0, b"X")).unwrap();
    drive(
        &mut controller,
        journal.access.done(Operation::Close { handle }),
    )
    .unwrap();
    let mut stream = files.stream();
    assert_eq!(
        drive(&mut controller, stream.next(4))
            .unwrap()
            .unwrap()
            .len(),
        4
    );
    assert!(!stream.is_verified());
    assert!(matches!(
        drive(&mut controller, stream.next(4)),
        Err(CheckpointError::DigestMismatch("checkpoint chunk"))
    ));
    assert!(!stream.is_verified());
    assert!(
        matches!(drive(&mut controller, stream.next(4)), Err(CheckpointError::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe)
    );
    assert!(!journal.is_faulted());
}

#[test]
fn canceled_stream_read_cannot_resume_from_a_partially_observed_chunk() {
    let (mut controller, mut journal) = source();
    let id = CheckpointId::from_bytes([0x7a; 16]);
    let files = drive(
        &mut controller,
        journal.build_checkpoint(id, schema(), 4096, b"checkpoint bytes"),
    )
    .unwrap();
    let mut stream = files.stream();
    drive(&mut controller, stream.next(1)).unwrap().unwrap();
    let mut future = Box::pin(stream.next(4));
    assert!(poll(future.as_mut()).is_pending());
    let jobs = controller.jobs();
    assert_eq!(jobs.len(), 1);
    let (job, _) = jobs[0];
    assert!(matches!(
        controller.operation(job).unwrap().unprotected(),
        Operation::Read { .. }
    ));
    drop(future);
    controller.execute(job, Effect::Normal).unwrap();
    controller.deliver(job).unwrap();
    assert!(
        matches!(drive(&mut controller, stream.next(4)), Err(CheckpointError::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe)
    );
    assert!(!stream.is_verified());
    let mut fresh = files.stream();
    let mut bytes = Vec::new();
    while let Some(next) = drive(&mut controller, fresh.next(4)).unwrap() {
        bytes.extend_from_slice(&next);
    }
    assert_eq!(bytes, b"checkpoint bytes");
    assert!(fresh.is_verified());
}

#[test]
fn matching_chunk_digests_cannot_bypass_the_complete_state_digest() {
    let (mut controller, mut journal) = source();
    let id = CheckpointId::from_bytes([0x7b; 16]);
    drive(
        &mut controller,
        journal.build_checkpoint(id, schema(), 8, b"checkpoint bytes"),
    )
    .unwrap();
    drive(&mut controller, journal.install_checkpoint(id)).unwrap();
    let checkpoint = journal.checkpoint.as_mut().unwrap();
    checkpoint.manifest.state_digest = Digest::from_bytes([0xff; 32]);
    let mut reader = crate::checkpoint::asynchronous::Reader::new(checkpoint, limits().checkpoint);
    assert_eq!(
        drive(&mut controller, reader.next(8)).unwrap().unwrap(),
        b"checkpoi"
    );
    assert!(matches!(
        drive(&mut controller, reader.next(8)),
        Err(CheckpointError::DigestMismatch("checkpoint state"))
    ));
    assert!(!reader.verified());
}
