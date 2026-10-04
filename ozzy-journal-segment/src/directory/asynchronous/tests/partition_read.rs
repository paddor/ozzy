use super::*;
use crate::{
    AsyncJournalPartitionIndex as Index, AsyncPartitionReadLimits as ReadConfig, JournalIndexError,
    PreparedOperationRecords, SharedJournalOperation,
};
use ozzy_journal::{ReadLimits, operation::canonical_body_digest};
use ozzy_proto::Offset;

fn read_config(resident: usize) -> ReadConfig {
    ReadConfig {
        index: crate::IndexBuildLimits {
            max_resident_bytes: resident,
            ..indexes::build_limits()
        },
        max_resident_operations: 128,
        cached_index_bytes: 1024,
        cached_indexes: 2,
        concurrent_reads: 1,
    }
}

fn append(
    controller: &mut Controller,
    journal: Journal,
    index: &mut Index,
    number: u8,
    backing: usize,
) -> Journal {
    let bytes = indexes::append_body(number);
    let backing = backing.max(bytes.len());
    let operation = CanonicalOperation {
        kind: OperationKind::Append,
        body: &bytes,
        ..operation(&journal)
    };
    let records = PreparedOperationRecords::new(
        operation.header(),
        bytes.clone().into(),
        limits().operations,
    )
    .unwrap()
    .with_shared_backing_bytes(backing.max(bytes.len()));
    let mut pipeline = journal.begin_write_pipeline(1).unwrap();
    let group = pipeline
        .begin_group_encoding()
        .unwrap()
        .encode_shared_raw(vec![SharedJournalOperation {
            header: operation.header(),
            body_digest: canonical_body_digest(&bytes),
            body: bytes.into(),
        }])
        .unwrap();
    let work = pipeline.prepare(group, backing).unwrap();
    let completed = drive(controller, work.write());
    let locations = pipeline.complete(completed).unwrap();
    let journal = pipeline.finish().unwrap();
    index.appended(&journal, &locations, &[records]).unwrap();
    journal
}

fn capture(
    index: &Index,
    journal: &Journal,
    first: u64,
    end: u64,
    through: u64,
) -> crate::AsyncPartitionRead {
    index
        .prepare_read(
            journal,
            indexes::partition(),
            Offset::new(first),
            Offset::new(end),
            through,
            ReadLimits {
                max_records: 16,
                max_bytes: 8192,
            },
        )
        .unwrap()
}

fn read(controller: &mut Controller, captured: crate::AsyncPartitionRead) -> (Vec<u64>, usize) {
    let mut offsets = Vec::new();
    let mut jobs = 0;
    drive_with(
        controller,
        captured.visit(|span| {
            for record in span.records() {
                assert_eq!(record.parts().next().unwrap(), b"payload");
                offsets.push(record.offset().get());
            }
            span.len()
        }),
        |_| {
            jobs += 1;
            Effect::Normal
        },
    )
    .unwrap();
    (offsets, jobs)
}

#[test]
fn async_incremental_reads_capture_exact_prefix_without_file_work() {
    let (mut controller, journal) = empty_journal();
    let mut index = drive(&mut controller, Index::open(&journal, read_config(8192))).unwrap();
    let journal = append(&mut controller, journal, &mut index, 0, 0);
    let captured = capture(&index, &journal, 0, 1, 1);
    assert!(journal.pins.is_pinned(1).unwrap());
    assert!(matches!(
        index.prepare_read(
            &journal,
            indexes::partition(),
            Offset::ZERO,
            Offset::new(1),
            1,
            ReadLimits {
                max_records: 1,
                max_bytes: 8192
            }
        ),
        Err(JournalIndexError::ReadIndexCapacity)
    ));
    let journal = append(&mut controller, journal, &mut index, 1, 0);
    assert_eq!(read(&mut controller, captured), (vec![0], 0));
    assert!(!journal.pins.is_pinned(1).unwrap());
    let captured = capture(&index, &journal, 0, 2, 1);
    assert_eq!(read(&mut controller, captured), (vec![0], 0));
    let captured = capture(&index, &journal, 0, 2, 2);
    assert_eq!(read(&mut controller, captured), (vec![0, 1], 0));
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn async_incremental_retry_read_selects_exact_offsets_and_positions() {
    let (mut controller, journal) = empty_journal();
    let mut index = drive(&mut controller, Index::open(&journal, read_config(0))).unwrap();
    let mut journal = append(&mut controller, journal, &mut index, 0, 0);
    journal = append(&mut controller, journal, &mut index, 1, 0);
    journal = append(&mut controller, journal, &mut index, 2, 0);
    let limits = ReadLimits {
        max_records: 3,
        max_bytes: 8192,
    };
    let selected = drive(
        &mut controller,
        index.read_offsets_with_positions(
            &journal,
            indexes::partition(),
            &[Offset::new(0), Offset::new(2)],
            limits,
        ),
    )
    .unwrap()
    .expect("active offsets");
    assert_eq!(
        selected
            .iter()
            .map(|(record, position)| (record.offset.get(), position.op_number))
            .collect::<Vec<_>>(),
        [(0, 1), (2, 3)]
    );
    assert!(
        drive(
            &mut controller,
            index.read_offsets_with_positions(
                &journal,
                indexes::partition(),
                &[Offset::new(0), Offset::new(3)],
                limits,
            ),
        )
        .unwrap()
        .is_none()
    );
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn async_retry_reads_reuse_predecessor_and_cached_sealed_selectors() {
    let (mut controller, journal) = empty_journal();
    let mut config = read_config(0);
    config.cached_index_bytes = 8192;
    let mut index = drive(&mut controller, Index::open(&journal, config)).unwrap();
    let mut journal = append(&mut controller, journal, &mut index, 0, 0);
    for number in 1..=2 {
        drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        index.rolled(&journal).unwrap();
        journal = append(&mut controller, journal, &mut index, number, 0);
        for offset in 0..number {
            let mut jobs = 0;
            let records = drive_with(
                &mut controller,
                index.read_offsets_with_positions(
                    &journal,
                    indexes::partition(),
                    &[Offset::new(u64::from(offset))],
                    ReadLimits {
                        max_records: 1,
                        max_bytes: 8192,
                    },
                ),
                |_| {
                    jobs += 1;
                    Effect::Normal
                },
            )
            .unwrap()
            .expect("retained sealed selector");
            assert_eq!(records.len(), 1);
            let (record, position) = &records[0];
            assert_eq!(record.offset.get(), u64::from(offset));
            assert_eq!(
                record.parts.as_slice(),
                &[bytes::Bytes::from_static(b"payload")]
            );
            assert_eq!(position.op_number, u64::from(offset) + 1);
            assert!(jobs <= 4, "retry must not rescan or publish indexes");
        }
    }
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn async_resident_delivery_releases_file_protection_and_keeps_bounded_selection() {
    let (mut controller, journal) = empty_journal();
    let mut index = drive(&mut controller, Index::open(&journal, read_config(0))).unwrap();
    let journal = append(&mut controller, journal, &mut index, 0, 0);
    let resident = capture(&index, &journal, 0, 1, 1).into_resident().unwrap();
    assert!(!journal.pins.is_pinned(1).unwrap());
    let mut journal = append(&mut controller, journal, &mut index, 1, 0);
    let cold = capture(&index, &journal, 0, 1, 1)
        .into_resident()
        .unwrap_err();
    assert_eq!(read(&mut controller, cold).0, [0]);
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    index.rolled(&journal).unwrap();
    let mut offsets = Vec::new();
    resident
        .visit_spans(|span| {
            offsets.extend(span.records().map(|record| record.offset().get()));
            span.len()
        })
        .unwrap();
    assert_eq!(offsets, [0]);
    assert_eq!(controller.jobs().len(), 0);
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn async_read_cache_charges_whole_backing_and_evicted_reads_still_work() {
    for (resident, backing) in [(0, 0), (1024, 4096)] {
        let (mut controller, journal) = empty_journal();
        let mut index = drive(
            &mut controller,
            Index::open(&journal, read_config(resident)),
        )
        .unwrap();
        let journal = append(&mut controller, journal, &mut index, 0, backing);
        // The existing cache permits one newest oversized operation. Evict it
        // with a successor to exercise the cold path and full-backing charge.
        let journal = append(&mut controller, journal, &mut index, 1, backing);
        let captured = capture(&index, &journal, 0, 1, 1);
        let (offsets, jobs) = read(&mut controller, captured);
        assert_eq!(offsets, [0]);
        assert!(jobs > 0);
        drive(&mut controller, journal.close()).unwrap();
    }
}

#[test]
fn two_complete_backings_serve_prior_operations_without_file_jobs() {
    let (mut controller, journal) = empty_journal();
    let mut index = drive(
        &mut controller,
        Index::open(&journal, read_config(9 * 1024)),
    )
    .unwrap();
    let journal = append(&mut controller, journal, &mut index, 0, 4096);
    let journal = append(&mut controller, journal, &mut index, 1, 4096);
    let held = capture(&index, &journal, 0, 1, 2).into_resident().unwrap();
    assert_eq!(
        read(&mut controller, capture(&index, &journal, 0, 2, 2)),
        (vec![0, 1], 0)
    );

    let journal = append(&mut controller, journal, &mut index, 2, 4096);
    assert_eq!(
        read(&mut controller, capture(&index, &journal, 1, 3, 3)),
        (vec![1, 2], 0)
    );
    let mut offsets = Vec::new();
    held.visit_spans(|span| {
        offsets.extend(span.records().map(|record| record.offset().get()));
        span.len()
    })
    .unwrap();
    assert_eq!(offsets, [0]);
    let (offsets, jobs) = read(&mut controller, capture(&index, &journal, 0, 1, 3));
    assert_eq!(offsets, [0]);
    assert!(
        jobs > 0,
        "oldest backing should be evicted, while its held alias survives"
    );
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn async_reader_predecessors_survive_rolls_and_cold_cache_misses() {
    let (mut controller, mut journal) = empty_journal();
    let mut config = read_config(0);
    config.cached_index_bytes = 0;
    let mut index = drive(&mut controller, Index::open(&journal, config)).unwrap();
    journal = append(&mut controller, journal, &mut index, 0, 0);
    let captured = capture(&index, &journal, 0, 1, 1);
    for number in 1..=3 {
        drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        index.rolled(&journal).unwrap();
        journal = append(&mut controller, journal, &mut index, number, 0);
    }
    assert_eq!(read(&mut controller, captured).0, [0]);
    for number in 0..4 {
        let captured = capture(&index, &journal, number, number + 1, 4);
        assert_eq!(read(&mut controller, captured).0, [number]);
    }
    // Opening builds active state once, not again for each read.
    let reopened = drive(&mut controller, Index::open(&journal, config)).unwrap();
    let captured = capture(&reopened, &journal, 3, 4, 4);
    assert_eq!(read(&mut controller, captured).0, [3]);
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn async_cold_repair_reuses_selected_index_after_cache_eviction() {
    let (mut controller, mut journal) = empty_journal();
    let mut config = read_config(0);
    config.cached_index_bytes = 8192;
    let mut index = drive(&mut controller, Index::open(&journal, config)).unwrap();
    journal = append(&mut controller, journal, &mut index, 0, 0);
    for number in 1..=6 {
        drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        index.rolled(&journal).unwrap();
        journal = append(&mut controller, journal, &mut index, number, 0);
    }
    let (previous, payload_jobs) = read(&mut controller, capture(&index, &journal, 5, 6, 7));
    assert_eq!(previous, [5]);
    let (oldest, cold_jobs) = read(&mut controller, capture(&index, &journal, 0, 1, 7));
    assert_eq!(oldest, [0]);
    assert!(
        cold_jobs > payload_jobs,
        "first cold repair must build its index"
    );
    for _ in 0..4 {
        let (oldest, jobs) = read(&mut controller, capture(&index, &journal, 0, 1, 7));
        assert_eq!(oldest, [0]);
        assert!(
            jobs > 0 && jobs <= payload_jobs,
            "repair must read only selected payload"
        );
    }
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn async_read_cancellation_releases_admission_without_invalidating_owner() {
    let (mut controller, journal) = empty_journal();
    let mut index = drive(&mut controller, Index::open(&journal, read_config(0))).unwrap();
    let journal = append(&mut controller, journal, &mut index, 0, 0);
    let journal = append(&mut controller, journal, &mut index, 1, 0);
    let captured = capture(&index, &journal, 0, 1, 1);
    drop(captured);
    assert!(!journal.pins.is_pinned(1).unwrap());
    let captured = capture(&index, &journal, 0, 1, 1);
    #[expect(
        clippy::redundant_closure_for_method_calls,
        reason = "closure is higher-ranked over record lifetime"
    )]
    let mut future = Box::pin(captured.visit(|span| span.len()));
    assert!(poll(future.as_mut()).is_pending());
    let jobs = controller.jobs();
    assert_eq!(jobs.len(), 1);
    drop(future);
    for (id, _) in jobs {
        controller.execute(id, Effect::Normal).unwrap();
        controller.deliver(id).unwrap();
    }
    let captured = capture(&index, &journal, 0, 1, 1);
    assert_eq!(read(&mut controller, captured).0, [0]);
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn async_read_obeys_byte_limits_and_rejects_uninstalled_index_growth() {
    let (mut controller, journal) = empty_journal();
    let mut index = drive(&mut controller, Index::open(&journal, read_config(8192))).unwrap();
    let mut journal = append(&mut controller, journal, &mut index, 0, 0);
    journal = append(&mut controller, journal, &mut index, 1, 0);
    for bytes in [1, 7, 13] {
        let captured = index
            .prepare_read(
                &journal,
                indexes::partition(),
                Offset::ZERO,
                Offset::new(2),
                2,
                ReadLimits {
                    max_records: 2,
                    max_bytes: bytes,
                },
            )
            .unwrap();
        assert_eq!(read(&mut controller, captured).0, [0]);
    }
    let body = indexes::append_body(2);
    let operation = CanonicalOperation {
        kind: OperationKind::Append,
        body: &body,
        ..operation(&journal)
    };
    drive(
        &mut controller,
        journal.append(&[operation], BodyEncoding::Raw),
    )
    .unwrap();
    assert!(matches!(
        index.prepare_read(
            &journal,
            indexes::partition(),
            Offset::ZERO,
            Offset::new(3),
            3,
            ReadLimits {
                max_records: 3,
                max_bytes: 8192
            }
        ),
        Err(JournalIndexError::StaleCatalog)
    ));
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn retirement_prunes_live_reader_sources_and_preserves_already_captured_reads() {
    let (mut controller, journal) = empty_journal();
    let mut index = drive(&mut controller, Index::open(&journal, read_config(8192))).unwrap();
    let mut journal = append(&mut controller, journal, &mut index, 0, 1024);
    let held = capture(&index, &journal, 0, 1, 1);
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    index.rolled(&journal).unwrap();
    journal = append(&mut controller, journal, &mut index, 1, 1024);
    drive(
        &mut controller,
        journal.sync_through(journal.writer().written_position()),
    )
    .unwrap();
    let mut next = journal.next_manifest().unwrap();
    next.accepted = journal.written_position().unwrap();
    next.committed = next.accepted;
    drive(&mut controller, journal.install_metadata(next)).unwrap();
    let id = ozzy_proto::CheckpointId::from_bytes([0x71; 16]);
    drive(
        &mut controller,
        journal.build_checkpoint(id, Digest::from_bytes([0x72; 32]), 8, b"retained state"),
    )
    .unwrap();
    drive(&mut controller, journal.install_checkpoint(id)).unwrap();
    let retired = drive(
        &mut controller,
        journal.retire_sealed_prefix(
            &crate::RetentionFloors::new(vec![(indexes::partition(), Offset::new(1))]).unwrap(),
            crate::AsyncRetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [1]);
    index
        .retired(&journal, &retired.unreferenced_segment_ids)
        .unwrap();
    let cleanup = drive(&mut controller, journal.reclaim_unreferenced_segments(1)).unwrap();
    assert_eq!(cleanup.pinned_segment_ids, [1]);
    assert_eq!(read(&mut controller, held).0, [0]);
    let current = capture(&index, &journal, 1, 2, 2);
    assert_eq!(read(&mut controller, current), (vec![1], 0));
    let cleanup = drive(&mut controller, journal.reclaim_unreferenced_segments(1)).unwrap();
    assert_eq!(cleanup.removed_segment_ids, [1]);
    drive(&mut controller, journal.close()).unwrap();
}
