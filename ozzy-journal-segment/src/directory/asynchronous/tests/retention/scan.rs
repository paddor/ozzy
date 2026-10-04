use super::super::*;
use ozzy_journal::operation::{ChainPosition, logical_operation_digest};

fn sealed(controller: &mut Controller, mut journal: Journal, encoding: BodyEncoding) -> Journal {
    journal.limits.decode.max_entries = 32;
    journal.limits.decode.max_group_decoded_body_bytes = 4096;
    journal.limits.decode.max_decoded_body_bytes = 4096;
    for _ in 0..3 {
        let mut chain = journal.writer.written_position().next_chain();
        let mut operations = Vec::new();
        for _ in 0..20 {
            let operation = CanonicalOperation {
                op_number: chain.next_op_number(),
                previous_digest: chain.previous_digest(),
                ..operation(&journal)
            };
            chain = ChainPosition::new(
                operation.op_number + 1,
                logical_operation_digest(&operation),
            );
            operations.push(operation);
        }
        let written = drive(controller, journal.append(&operations, encoding)).unwrap();
        drive(controller, journal.sync_through(written)).unwrap();
    }
    drive(controller, journal.roll_active(32768, 4)).unwrap();
    journal
}

#[test]
fn retention_scratch_depends_on_group_limits_instead_of_segment_capacity() {
    let (mut controller, journal) = empty_journal();
    let mut journal = sealed(&mut controller, journal, BodyEncoding::Raw);
    let small = journal.retention_scratch_bytes().unwrap();
    journal.limits.io.max_segment_bytes = 1024 * 1024 * 1024;
    journal.limits.decode.max_groups = 262_144;
    journal.limits.decode.max_segment_decoded_body_bytes = 1024 * 1024 * 1024;
    assert_eq!(journal.retention_scratch_bytes().unwrap(), small);
}

#[test]
fn retention_checks_all_groups_and_zero_tail_after_canceled_reads() {
    let (mut controller, journal) = empty_journal();
    let journal = sealed(&mut controller, journal, BodyEncoding::Raw);
    let before = journal.current;
    // A canceled observer never releases the backend's queued read or handle.
    let mut future = Box::pin(journal.retention_segment(1, indexes::partition()));
    loop {
        assert!(poll(future.as_mut()).is_pending());
        let (id, _) = controller.jobs()[0];
        if matches!(
            controller.operation(id).unwrap().unprotected(),
            Operation::Read { .. }
        ) {
            drop(future);
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
            break;
        }
        controller.execute(id, Effect::Normal).unwrap();
        controller.deliver(id).unwrap();
    }
    let summary = drive_with(
        &mut controller,
        journal.retention_segment(1, indexes::partition()),
        |operation| {
            if let Operation::Read { length, .. } = operation.unprotected() {
                assert!(*length <= journal.limits.io.chunk_bytes);
            }
            Effect::Normal
        },
    )
    .unwrap();
    assert_eq!(summary.last_operation, 60);
    assert_eq!(summary.record_end, ozzy_proto::Offset::ZERO);
    assert_eq!(journal.current, before);
    assert!(!journal.is_faulted());
    let handle = drive(
        &mut controller,
        journal.access.open(
            journal.root().join("segments/1.log"),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(&handle, 32767, &[1]),
    )
    .unwrap();
    assert!(
        drive(
            &mut controller,
            journal.retention_segment(1, indexes::partition())
        )
        .is_err()
    );
    assert_eq!(journal.current, before);
}

#[test]
#[cfg(feature = "lz4")]
fn retention_shared_compressed_groups_match_independent_format_evidence() {
    let (mut controller, journal) = empty_journal();
    let journal = sealed(
        &mut controller,
        journal,
        BodyEncoding::Lz4 {
            min_savings_bytes: 0,
        },
    );
    let reference = journal.manifest.segments[0];
    let bytes = controller
        .image()
        .bytes(Path::new("/group/segments/1.log"), false)
        .unwrap()
        .to_vec();
    let evidence = crate::scan_segment(
        &bytes,
        reference.first_group_number,
        reference.first_chain,
        journal.limits.decode,
    )
    .unwrap();
    assert_eq!(evidence.groups.len(), 3);
    assert!(
        evidence.groups[0]
            .operations
            .iter()
            .all(|op| matches!(op.body, std::borrow::Cow::Owned(_)))
    );
    let summary = drive(
        &mut controller,
        journal.retention_segment(1, indexes::partition()),
    )
    .unwrap();
    assert_eq!(
        summary.last_operation,
        evidence.next_chain.next_op_number() - 1
    );
    assert_eq!(evidence.digest, reference.sealed.unwrap().digest);
}

#[tokio::test]
async fn sparse_gib_segment_scans_under_a_group_sized_reservation() {
    let root = tempfile::tempdir().unwrap();
    let (pool, mut clients) = ozzy_io_pool::Pool::new(ozzy_io_pool::Config {
        threads: 1,
        max_inflight: 1,
        handles: 32,
        limits: io_limits(),
    })
    .unwrap();
    let mut config = limits();
    config.io.max_segment_bytes = 1024 * 1024 * 1024;
    config.io.chunk_bytes = 64 * 1024;
    config.decode.max_entries = 32;
    config.decode.max_decoded_body_bytes = 4096;
    config.decode.max_group_decoded_body_bytes = 4096;
    config.operations.max_payload_bytes = 4096;
    config.operations.max_append_batches = 4;
    let mut format = spec(CommitMode::External);
    format.first_segment = SegmentHeader::new(
        identity().group_id,
        1,
        None,
        Digest::ZERO,
        config.io.max_segment_bytes,
    )
    .unwrap();
    let mut journal = Journal::format(
        root.path().join("group"),
        Local::new(clients.remove(0)),
        format,
        JournalGeneration(1),
        config,
    )
    .await
    .unwrap();
    append_confirmed(&mut journal).await;
    journal
        .roll_active(config.io.max_segment_bytes, 4)
        .await
        .unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(journal.root().join("segments/1.log"))
        .unwrap();
    file.set_len(config.io.max_segment_bytes).unwrap();
    drop(file);
    assert!(journal.retention_scratch_bytes().unwrap() < 256 * 1024);
    let summary = journal
        .retention_segment(1, indexes::partition())
        .await
        .unwrap();
    assert_eq!(summary.capacity, 1024 * 1024 * 1024);
    assert_eq!(summary.last_operation, 1);
    drop(journal);
    pool.shutdown().await;
}
