//! Count real source scans rather than asserting noisy wall-clock performance.

use super::*;
use crate::{BodyEncoding, Digest, GroupDirectory, OperationKind, SegmentHeader};
use ozzy_journal::operation::{
    Barrier, OperationBody, encode_operation_body, logical_operation_digest,
};
use ozzy_proto::{GroupId, NodeId, OperationId, StoreId, VolumeId};

const CAPACITY: u64 = 64 * 1024;

#[test]
fn sequential_chunks_and_position_queries_validate_one_loaded_segment_once() {
    assert_indexed_history(64);
}

#[test]
fn segment_changes_replace_the_index_without_growing_its_reservation() {
    assert_indexed_history(8);
}

#[test]
fn history_cache_uses_the_captured_prefix_not_file_capacity() {
    let (_temporary, mut journal) = empty();
    let identity = journal.directory().identity();
    let body = encode_operation_body(
        &OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes([7; 16]),
        }),
        OperationLimits::default(),
    )
    .unwrap();
    let operation = CanonicalOperation {
        group_id: identity.group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Barrier,
        body: &body,
    };
    let written = journal
        .append_with_body_encoding(&[operation], BodyEncoding::Raw)
        .unwrap();
    journal.sync_through(written).unwrap();
    let history = journal.freeze_history(CAPACITY as usize).unwrap();
    assert!(history.state.bytes.capacity() < CAPACITY as usize);
    assert!(history.state.entries.capacity() < CAPACITY as usize / ENTRY_HEADER_BYTES);
}

fn empty() -> (tempfile::TempDir, OpenGroupJournal) {
    let temporary = tempfile::Builder::new()
        .prefix(".history-cache-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let identity = GroupIdentity {
        group_id: GroupId::from_bytes([1; 16]),
        replica_node_id: NodeId::from_bytes([2; 16]),
        volume_id: VolumeId::from_bytes([3; 16]),
        store_id: StoreId::from_bytes([4; 16]),
        store_generation: 1,
    };
    let header = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, CAPACITY).unwrap();
    let journal = GroupDirectory::format_new(temporary.path().join("group"), identity, 1, &header)
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    (temporary, journal)
}

#[expect(
    clippy::too_many_lines,
    reason = "linear cache and reservation lifetime check"
)]
fn assert_indexed_history(operations_per_segment: usize) {
    let (_temporary, mut journal) = empty();
    let identity = journal.directory().identity();
    let bodies: Vec<_> = (1..=64u8)
        .map(|index| {
            encode_operation_body(
                &OperationBody::Barrier(Barrier {
                    operation_id: OperationId::from_bytes([index; 16]),
                }),
                OperationLimits::default(),
            )
            .unwrap()
        })
        .collect();
    let mut previous = LogPosition::GENESIS;
    let operations: Vec<_> = bodies
        .iter()
        .map(|body| {
            let operation = CanonicalOperation {
                group_id: identity.group_id,
                configuration_epoch: 1,
                original_view: 0,
                op_number: previous.op_number + 1,
                previous_digest: previous.digest,
                kind: OperationKind::Barrier,
                body,
            };
            previous = LogPosition {
                op_number: operation.op_number,
                digest: logical_operation_digest(&operation),
            };
            operation
        })
        .collect();
    for (index, group) in operations.chunks(operations_per_segment).enumerate() {
        if index != 0 {
            journal = journal.roll_active(CAPACITY).unwrap();
        }
        let written = journal
            .append_with_body_encoding(group, BodyEncoding::Raw)
            .unwrap();
        journal.sync_through(written).unwrap();
    }
    let mut history = journal.freeze_history(CAPACITY as usize).unwrap();
    let reserved = (
        history.state.bytes.capacity(),
        history.state.entries.capacity(),
    );
    assert!(history.state.entry_limit >= operations_per_segment);
    assert!(history.state.entry_limit < CAPACITY as usize / ENTRY_HEADER_BYTES);
    let mut predecessor = LogPosition::GENESIS;
    for expected in &operations {
        let chunk = history
            .read_after(predecessor, 1, expected.body.len())
            .unwrap();
        assert_eq!(chunk.operations().next(), Some(*expected));
        assert_eq!(
            chunk.verified_operations().next(),
            Some((
                *expected,
                ozzy_journal::operation::canonical_body_digest(expected.body)
            ))
        );
        predecessor = chunk.end();
        assert_eq!(
            history.position(expected.op_number).unwrap(),
            Some(predecessor)
        );
    }
    assert_eq!(predecessor, history.through());
    assert_eq!(
        history.state.full_scans.get(),
        operations.len() / operations_per_segment,
        "each chunk rescanned the entire source"
    );
    // Reverse lookups reload each earlier segment once, replacing only the index
    // contents. No encoded arena or index growth is needed across boundaries.
    for expected in operations.iter().rev() {
        assert_eq!(
            history.position(expected.op_number).unwrap(),
            Some(LogPosition {
                op_number: expected.op_number,
                digest: logical_operation_digest(expected),
            })
        );
    }
    assert_eq!(
        history.state.full_scans.get(),
        2 * (operations.len() / operations_per_segment) - 1
    );
    assert_eq!(
        reserved,
        (
            history.state.bytes.capacity(),
            history.state.entries.capacity()
        )
    );
    assert!(history.state.entries.len() <= history.state.entry_limit);

    // Cached prefixes never authorize returning unchecked payload bytes. Inject
    // a bit flip into the private encoded snapshot after its successful scan.
    let offset = history.state.entries[0].offset + ENTRY_HEADER_BYTES;
    history.state.bytes[offset] ^= 1;
    assert!(matches!(
        history.read_after(LogPosition::GENESIS, 1, operations[0].body.len()),
        Err(HistoryError::Codec(_))
    ));
}
