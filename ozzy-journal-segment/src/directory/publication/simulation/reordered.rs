//! Recovery must not assume unsynchronized writes persist in append order.

use super::*;
use crate::directory::{NoopObserver, evidence};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, encode_operation_body,
};
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};
use std::ops::Range;

fn appended(encoding: BodyEncoding, protect: bool) -> (Disk, Manifest, LogPosition, Range<usize>) {
    let (baseline, mut manifest, _, _) = fixture(17);
    let mut memory = Memory(Arc::new(Mutex::new(baseline)));
    manifest.durable_evidence = true;
    replace_evidence(
        &mut memory,
        &evidence::image(&manifest, manifest.accepted).unwrap(),
        &mut NoopObserver,
    )
    .unwrap();
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
    let mut payload = [42; 6000];
    let mut random = 17_u64;
    for byte in &mut payload[..2048] {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        *byte = random as u8;
    }
    let body = encode_operation_body(
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: PartitionIncarnation::from_bytes([1; 16]),
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([2; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(0),
                first_offset: Offset::ZERO,
                append_timestamp_millis: 0,
                records: vec![AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([3; 16]),
                    parts: vec![payload.as_slice()].into(),
                }]
                .into(),
            }],
        }),
        OperationLimits::default(),
    )
    .unwrap();
    let written = writer
        .append_with_body_encoding(
            &[CanonicalOperation {
                group_id: manifest.identity.group_id,
                configuration_epoch: 1,
                original_view: 0,
                op_number: 3,
                previous_digest: manifest.accepted.digest,
                kind: OperationKind::Append,
                body: &body,
            }],
            encoding,
        )
        .unwrap();
    let range = start..written.end_offset() as usize;
    assert_eq!(
        range.len(),
        if encoding == BodyEncoding::Raw {
            8192
        } else {
            4096
        }
    );
    let latest = LogPosition {
        op_number: 3,
        digest: written.next_chain().previous_digest(),
    };
    if protect {
        writer.sync_through(written).unwrap();
        let bytes = memory
            .read_exact(evidence::NAME, evidence::FILE_BYTES)
            .unwrap();
        let (record, first) = evidence::Copies::decode_file(&bytes, &manifest)
            .unwrap()
            .next(&manifest, latest)
            .unwrap();
        overwrite_evidence(&mut memory, first, &record, &mut NoopObserver).unwrap();
    }
    // Capture bytes with or without completed data and evidence barriers.
    let disk = memory.0.lock().unwrap().clone();
    (disk, manifest, latest, range)
}

fn check_images(encoding: BodyEncoding) {
    for protected in [false, true] {
        let (pending, manifest, latest, range) = appended(encoding, protected);
        let split = range.start + range.len() / 2;
        // Four sector-aligned regions: header, two body regions, and final seal.
        let regions = [
            range.start..range.start + 512,
            range.start + 512..split,
            split..range.end - 512,
            range.end - 512..range.end,
        ];
        for length_first in [false, true] {
            for mask in 0..16 {
                let mut disk = pending.clone();
                let id = disk.inode("segment").unwrap();
                // For a protected operation, this injects post-sync damage.
                // Otherwise it selects which unsynchronized regions survive.
                disk.files[id].stable.truncate(range.start);
                if length_first {
                    disk.files[id].stable.resize(range.end, 0);
                }
                // Persist in reverse order, including seal-before-header cases.
                for (bit, region) in regions.iter().enumerate().rev() {
                    if mask & (1 << bit) != 0 {
                        disk.persist_range("segment", region.clone()).unwrap();
                    }
                }
                let mut memory = Memory(Arc::new(Mutex::new(disk)));
                memory.0.lock().unwrap().crash();
                let before = memory.inspect("segment").unwrap();
                match recovered(&mut memory) {
                    Ok(position) => {
                        assert!(
                            position == latest || (!protected && position == manifest.accepted)
                        );
                        if position == latest {
                            // Never accept a partially decoded or changed record.
                            assert_eq!(before, pending.files[id].pending);
                        }
                        assert!(mask != 0 || !protected, "forgot protected operation");
                        assert!(mask != 15 || position == latest, "lost complete operation");
                    }
                    Err(error) => {
                        assert_ne!(mask, 15, "intact image rejected: {error}");
                        assert!(protected || mask != 0, "empty crash tail rejected: {error}");
                        assert!(matches!(error, DirectoryError::Writer(_)), "{error}");
                        assert_eq!(memory.inspect("segment").unwrap(), before);
                    }
                }
            }
        }
    }
}

#[test]
fn reordered_raw_writes_preserve_protected_history_or_refuse_without_mutation() {
    check_images(BodyEncoding::Raw);
}

#[cfg(feature = "lz4")]
#[test]
fn reordered_lz4_writes_preserve_protected_history_or_refuse_without_mutation() {
    check_images(BodyEncoding::Lz4 {
        min_savings_bytes: 1,
    });
}

#[test]
fn partial_writeback_does_not_publish_names_or_count_as_a_complete_file_barrier() {
    let mut memory = Memory::default();
    memory.write_new("unpublished", b"abcdefgh").unwrap();
    let mut disk = memory.0.lock().unwrap();
    disk.persist_range("unpublished", 4..8).unwrap();
    disk.persist_range("unpublished", 0..2).unwrap();
    assert_eq!(disk.files[0].stable, b"ab\0\0efgh");
    assert!(disk.persist_range("unpublished", 0..9).is_err());
    assert_eq!(disk.files[0].stable, b"ab\0\0efgh");
    disk.crash();
    assert!(disk.inode("unpublished").is_err());
}
