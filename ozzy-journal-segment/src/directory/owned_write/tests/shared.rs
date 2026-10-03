use super::*;

#[test]
fn pipeline_roll_reuses_encoding_and_empty_segment_oversize_fails() {
    for encoding in [
        BodyEncoding::Raw,
        BodyEncoding::Lz4 {
            min_savings_bytes: 64,
        },
    ]
    .into_iter()
    .filter(|encoding| encoding.is_supported())
    {
        let (_temporary, mut journal) = journal_mode(true);
        let bytes = body(1);
        journal.decode_limits.max_decoded_body_bytes = bytes.len();
        journal.decode_limits.max_group_decoded_body_bytes = bytes.len();
        journal.decode_limits.max_segment_decoded_body_bytes = bytes.len();
        let first = operation(&journal, 1, Digest::ZERO, &bytes);
        let digest = logical_operation_digest(&first);
        let mut pending = journal.begin_write_pipeline(4).unwrap();
        let group = pending.preencode(&[first], encoding).unwrap();
        let work = pending.prepare(group).unwrap();
        let second = operation(pending.journal(), 2, digest, &bytes);
        let group = pending.preencode(&[second], encoding).unwrap();
        assert!(matches!(
            pending.check(&group),
            Err(DirectoryError::Codec(
                crate::CodecError::SegmentDecodedBodyLimit { .. }
            ))
        ));
        assert_eq!(pending.journal().accepted_position().unwrap().op_number, 0);
        pending.complete(work.write()).unwrap();
        assert_eq!(pending.journal().accepted_position().unwrap().op_number, 1);
        let journal = pending.finish().unwrap().roll_active(1024 * 1024).unwrap();
        let mut pending = journal.begin_write_pipeline(4).unwrap();
        let work = pending.prepare(group).unwrap();
        pending.complete(work.write()).unwrap();
        let journal = pending.finish().unwrap();
        assert_eq!(journal.accepted_position().unwrap().op_number, 2);
        assert_eq!(journal.writer.header().segment_id(), 2);
    }
    let (_temporary, mut journal) = journal_mode(true);
    let bytes = body(1);
    journal.decode_limits.max_segment_decoded_body_bytes = bytes.len() - 1;
    let op = operation(&journal, 1, Digest::ZERO, &bytes);
    let mut pending = journal.begin_write_pipeline(4).unwrap();
    let group = pending.preencode(&[op], BodyEncoding::Raw).unwrap();
    assert!(pending.check(&group).is_err());
}

#[test]
fn abandoned_pipeline_keeps_store_locked_until_detached_io_finishes() {
    let (_temporary, journal) = journal_mode(true);
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    let bytes = body(1);
    let op = operation(&journal, 1, Digest::ZERO, &bytes);
    let mut pending = journal.begin_write_pipeline(4).unwrap();
    let group = pending.preencode(&[op], BodyEncoding::Raw).unwrap();
    let work = pending.prepare(group).unwrap();
    assert!(
        pending.finish().is_err(),
        "unfinished reservations cannot restore mutation"
    );
    assert!(
        crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default()).is_err()
    );
    drop(work.write());
    assert!(crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default()).is_ok());
}

#[test]
fn cached_digests_do_not_bypass_schema_authority_or_storage_bounds() {
    for case in 0..5 {
        let (_temporary, mut journal) = journal_mode(true);
        let bytes = body(1);
        if case == 4 {
            journal.decode_limits.max_decoded_body_bytes = bytes.len() - 1;
        }
        if case == 3 {
            journal.decode_limits.max_group_decoded_body_bytes = bytes.len() - 1;
        }
        let mut op = operation(&journal, 1, Digest::ZERO, &bytes);
        match case {
            0 => op.body = &bytes[..bytes.len() - 1],
            1 => op.configuration_epoch += 1,
            2 => op.original_view += 1,
            _ => {}
        }
        let hashed = [(op, canonical_body_digest(op.body))];
        let mut pipeline = journal.begin_write_pipeline(4).unwrap();
        assert!(
            pipeline
                .preencode_verified(&hashed, BodyEncoding::Raw)
                .is_err()
        );
        assert!(
            pipeline
                .begin_group_encoding()
                .unwrap()
                .encode_verified(
                    &mut JournalGroupEncoder::default(),
                    &hashed,
                    BodyEncoding::Raw
                )
                .is_err()
        );
        assert_eq!(pipeline.pending(), 0);
        assert_eq!(pipeline.journal().accepted_position().unwrap().op_number, 0);
    }
}
