use super::*;
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Tracked {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn shared_raw_matches_file_bytes_and_retains_bodies_until_completion() {
    for abandon in [false, true] {
        let (_temporary, mut journal) = journal_mode(true);
        journal
            .set_write_mode(crate::SegmentWriteMode::Buffered)
            .unwrap();
        let root = journal.directory.root.clone();
        let identity = journal.directory.identity;
        let drops = Arc::new(AtomicUsize::new(0));
        let bodies: Vec<_> = [0, 1, 7, 8, 9, 128, 4096, 65537]
            .into_iter()
            .enumerate()
            .map(|(index, len)| {
                Bytes::from_owner(Tracked {
                    bytes: body_with_payload(index as u64 + 1, &vec![index as u8; len]),
                    drops: drops.clone(),
                })
            })
            .collect();
        let mut previous = Digest::ZERO;
        let ops: Vec<_> = bodies
            .iter()
            .enumerate()
            .map(|(index, body)| {
                let op = operation(&journal, index as u64 + 1, previous, body);
                previous = logical_operation_digest(&op);
                op
            })
            .collect();
        let expected = crate::encode_group(
            journal.writer.header(),
            1,
            crate::SEGMENT_HEADER_BYTES as u64,
            crate::ChainPosition::GENESIS,
            &ops,
        )
        .unwrap();
        let shared = ops
            .iter()
            .zip(&bodies)
            .map(|(op, body)| SharedJournalOperation {
                header: op.header(),
                body: body.clone(),
                body_digest: canonical_body_digest(body),
            })
            .collect();
        let addresses: Vec<_> = bodies.iter().map(|body| body.as_ptr()).collect();
        drop(ops);
        drop(bodies);
        let mut pipeline = journal.begin_write_pipeline(4).unwrap();
        let group = pipeline.preencode_shared_raw(shared).unwrap();
        let write = pipeline.prepare(group).unwrap();
        let WriteBytes::SharedRaw { framing, bodies } = &write.bytes else {
            panic!("copied body")
        };
        assert!(framing.len() < 8192);
        assert_eq!(
            bodies.iter().map(|body| body.as_ptr()).collect::<Vec<_>>(),
            addresses
        );
        assert_eq!(
            write.bytes.slices().collect::<Vec<_>>().concat(),
            expected.as_bytes()
        );
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        if abandon {
            drop(pipeline);
            assert!(
                crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default())
                    .is_err()
            );
            assert_eq!(drops.load(Ordering::Relaxed), 0);
            drop(write);
        } else {
            pipeline.complete(write.write()).unwrap();
            let mut journal = pipeline.finish().unwrap();
            assert_eq!(journal.accepted_position().unwrap().op_number, 0);
            journal.sync_through(journal.begin_sync()).unwrap();
            let end = journal.accepted_position().unwrap();
            assert_eq!(end.digest, previous);
            drop(journal);
            let recovered =
                crate::GroupDirectory::open(&root, identity, crate::MetadataLimits::default())
                    .unwrap()
                    .recover(
                        ozzy_journal::progress::JournalGeneration(2),
                        crate::DecodeLimits::default(),
                        OperationLimits::default(),
                    )
                    .unwrap();
            assert_eq!(recovered.accepted_position().unwrap(), end);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 8);
    }
}

#[test]
fn shared_raw_syncs_in_data_sync_mode_and_reuses_body_after_segment_roll() {
    let (_temporary, mut journal) = journal_mode(true);
    let bytes = Bytes::from(body(1));
    journal.decode_limits.max_segment_decoded_body_bytes = bytes.len();
    let first = operation(&journal, 1, Digest::ZERO, &bytes);
    let digest = logical_operation_digest(&first);
    let body_digest = canonical_body_digest(&bytes);
    let mut second = first.header();
    second.op_number = 2;
    second.previous_digest = digest;
    let shared = |header| {
        vec![SharedJournalOperation {
            header,
            body: bytes.clone(),
            body_digest,
        }]
    };
    let mut pipeline = journal.begin_write_pipeline(4).unwrap();
    let group = pipeline
        .preencode_shared_raw(shared(first.header()))
        .unwrap();
    let write = pipeline.prepare(group).unwrap();
    let next = pipeline.preencode_shared_raw(shared(second)).unwrap();
    assert!(pipeline.check(&next).is_err());
    pipeline.complete(write.write()).unwrap();
    // O_DSYNC completion is durable without a separate barrier.
    assert_eq!(pipeline.journal().accepted_position().unwrap().op_number, 1);
    let journal = pipeline.finish().unwrap();
    let journal = journal.roll_active(1024 * 1024).unwrap();
    let mut pipeline = journal.begin_write_pipeline(4).unwrap();
    let write = pipeline.prepare(next).unwrap();
    pipeline.complete(write.write()).unwrap();
    assert_eq!(pipeline.journal().written_position().unwrap().op_number, 2);
    assert_eq!(pipeline.journal().accepted_position().unwrap().op_number, 2);
    assert_eq!(pipeline.journal().writer.header().segment_id(), 2);
}

#[test]
fn validated_bodies_skip_only_the_body_walk_and_only_with_the_journal_limits() {
    let (_temporary, mut journal) = journal_mode(true);
    // The 16 KiB payload exceeds the journal's own limit.
    journal.operation_limits.max_payload_bytes = 1024;
    let bytes = Bytes::from(body(1));
    let first = operation(&journal, 1, Digest::ZERO, &bytes).header();
    let shared = |configuration_epoch| {
        let mut header = first;
        header.configuration_epoch = configuration_epoch;
        vec![SharedJournalOperation {
            header,
            body: bytes.clone(),
            body_digest: canonical_body_digest(&bytes),
        }]
    };
    let limits = journal.operation_limits;
    let encode = |journal: &mut OpenGroupJournal, validated: Option<OperationLimits>, epoch| {
        let encoding = journal.begin_group_encoding().unwrap();
        let encoding = match validated {
            Some(limits) => encoding.with_validated_bodies(limits),
            None => encoding,
        };
        encoding.encode_shared_raw(shared(epoch)).map(|_| ())
    };
    assert!(encode(&mut journal, None, 1).is_err());
    // A decode with other limits does not count.
    assert!(encode(&mut journal, Some(OperationLimits::default()), 1).is_err());
    assert!(encode(&mut journal, Some(limits), 1).is_ok());
    // The envelope stays checked.
    assert!(encode(&mut journal, Some(limits), 2).is_err());
    // Trusting producer LZ4 blocks still walks descriptors against the limits.
    let payloads = |journal: &mut OpenGroupJournal| {
        let encoding = journal.begin_group_encoding().unwrap();
        encoding
            .with_validated_payloads()
            .encode_shared_raw(shared(1))
            .map(|_| ())
    };
    assert!(payloads(&mut journal).is_err());
    journal.operation_limits = OperationLimits::default();
    payloads(&mut journal).unwrap();
}
