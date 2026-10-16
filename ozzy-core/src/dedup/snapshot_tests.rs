use super::{tests::*, *};

fn row(identity: u8, offset: u64, time: u64) -> [u8; SNAPSHOT_ROW_BYTES] {
    let mut bytes = [identity; SNAPSHOT_ROW_BYTES];
    bytes[16..24].copy_from_slice(&offset.to_be_bytes());
    bytes[24..32].copy_from_slice(&time.to_be_bytes());
    bytes
}

fn captured() -> (SnapshotHeader, Vec<u8>) {
    let mut index = index();
    index.admit(1, 4, Offset::ZERO, &[id(1), id(2)]).unwrap();
    index.commit_through(1).unwrap();
    let mut snapshot = index.begin_snapshot().unwrap();
    let header = *snapshot.header();
    let mut bytes = vec![0; 2 * SNAPSHOT_ROW_BYTES];
    assert_eq!(
        index.write_snapshot_rows(&mut snapshot, &mut bytes, 2),
        Ok(bytes.len())
    );
    index.finish_snapshot(&snapshot).unwrap();
    (header, bytes)
}

#[test]
fn snapshot_has_independent_fixed_header_and_row_bytes() {
    let (header, bytes) = captured();
    let expected = [
        b'O', b'Z', b'Y', b'D', b'I', b'D', b'X', 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2,
    ];
    assert_eq!(header.encode(), expected);
    assert_eq!(SnapshotHeader::decode(&expected).unwrap(), header);
    assert_eq!(header.encoded_bytes(), 128);
    assert_eq!(bytes, [row(1, 0, 4), row(2, 1, 4)].concat());
}

#[test]
fn capture_stays_at_its_confirmed_boundary_across_commits_reacceptance_and_branch_cuts() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1), id(2)]).unwrap();
    index.commit_through(1).unwrap();
    index.admit(2, 1, Offset::new(2), &[id(3)]).unwrap();
    let mut snapshot = index.begin_snapshot().unwrap();
    let header = *snapshot.header();
    assert_eq!(
        (header.operation(), header.logical_time(), header.rows()),
        (1, 0, 2)
    );
    let mut first = [0; SNAPSHOT_ROW_BYTES];
    assert_eq!(
        index.write_snapshot_rows(&mut snapshot, &mut first, 1),
        Ok(32)
    );
    assert_eq!(first, row(1, 0, 0));
    index.commit_through(2).unwrap();
    index.advance_time(3, 10).unwrap();
    index.commit_through(3).unwrap();
    assert_eq!(index.expire(16, 4096), 0);
    let admitted = index.admit(4, 10, Offset::new(3), &[id(1)]).unwrap();
    assert_eq!(admitted.outcomes[0].disposition, AppendDisposition::Stored);
    index.commit_through(4).unwrap();
    index.admit(5, 11, Offset::new(4), &[id(4)]).unwrap();
    index.discard_unconfirmed();
    let mut second = [0; SNAPSHOT_ROW_BYTES];
    assert_eq!(
        index.write_snapshot_rows(&mut snapshot, &mut second, 8),
        Ok(32)
    );
    assert_eq!(second, row(2, 1, 0));
    assert!(snapshot.is_complete());
    index.finish_snapshot(&snapshot).unwrap();
    assert_eq!(index.expire(16, 4096), 2);
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::new(3))));
    let mut restore = RestoringIndex::new(limits(), header).unwrap();
    restore.push_rows(&first).unwrap();
    restore.push_rows(&second).unwrap();
    let mut restored = restore.finish().unwrap();
    assert_eq!(restored.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    assert_eq!(restored.lookup(id(3)), None);
    let duplicate = restored.admit(2, 0, Offset::new(2), &[id(1)]).unwrap();
    assert_eq!(
        duplicate.outcomes[0].disposition,
        AppendDisposition::Duplicate
    );
    restored.commit_through(2).unwrap();
    let expired = restored.admit(3, 10, Offset::new(2), &[id(1)]).unwrap();
    assert_eq!(expired.outcomes[0].disposition, AppendDisposition::Stored);
    assert_eq!(expired.next_offset, Offset::new(3));
}

#[test]
fn snapshot_delta_is_bounded_without_blocking_duplicates_or_clock_progress() {
    let mut bounds = limits();
    bounds.pending_records = 2;
    bounds.batch_records = 2;
    let mut index = Index::new(bounds, Offset::ZERO, 0).unwrap();
    index.admit(1, 0, Offset::ZERO, &[id(1), id(2)]).unwrap();
    index.commit_through(1).unwrap();
    let snapshot = index.begin_snapshot().unwrap();
    index.admit(2, 0, Offset::new(2), &[id(3)]).unwrap();
    index.commit_through(2).unwrap();
    index.admit(3, 0, Offset::new(3), &[id(4)]).unwrap();
    index.commit_through(3).unwrap();
    assert_eq!(
        index.admit(4, 0, Offset::new(4), &[id(5)]).unwrap_err(),
        Error::SnapshotCapacity
    );
    index.admit(4, 0, Offset::new(4), &[id(1)]).unwrap();
    index.commit_through(4).unwrap();
    index.advance_time(5, 10).unwrap();
    index.commit_through(5).unwrap();
    assert_eq!(index.expire(16, 4096), 0);
    drop(snapshot);
    assert_eq!(index.expire(16, 4096), 4);
    index.admit(6, 10, Offset::new(4), &[id(5)]).unwrap();
}

#[test]
fn existing_pending_rows_share_the_snapshot_delta_reservation() {
    let mut bounds = limits();
    bounds.pending_records = 2;
    bounds.batch_records = 2;
    let mut index = Index::new(bounds, Offset::ZERO, 0).unwrap();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    index.commit_through(1).unwrap();
    index.admit(2, 0, Offset::new(1), &[id(2)]).unwrap();
    let snapshot = index.begin_snapshot().unwrap();
    index.admit(3, 0, Offset::new(2), &[id(3)]).unwrap();
    index.commit_through(3).unwrap();
    assert_eq!(
        index.admit(4, 0, Offset::new(3), &[id(4)]).unwrap_err(),
        Error::SnapshotCapacity
    );
    index.abort_snapshot(&snapshot).unwrap();
    index.admit(4, 0, Offset::new(3), &[id(4)]).unwrap();
}

#[test]
fn foreign_stale_incomplete_and_zero_budget_cursors_cannot_advance_or_release_capture() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    index.commit_through(1).unwrap();
    let mut snapshot = index.begin_snapshot().unwrap();
    assert_eq!(index.begin_snapshot().unwrap_err(), Error::SnapshotBusy);
    assert_eq!(index.finish_snapshot(&snapshot), Err(Error::Snapshot));
    let mut output = [0xaa; SNAPSHOT_ROW_BYTES];
    let mut other = Index::new(limits(), Offset::ZERO, 0).unwrap();
    assert_eq!(
        other.write_snapshot_rows(&mut snapshot, &mut output, 1),
        Err(Error::Snapshot)
    );
    assert_eq!(other.abort_snapshot(&snapshot), Err(Error::Snapshot));
    assert_eq!(
        index.write_snapshot_rows(&mut snapshot, &mut output, 0),
        Err(Error::Snapshot)
    );
    assert_eq!(
        index.write_snapshot_rows(&mut snapshot, &mut output[..31], 1),
        Err(Error::Snapshot)
    );
    assert_eq!(snapshot.written_rows(), 0);
    assert_eq!(output, [0xaa; SNAPSHOT_ROW_BYTES]);
    index.abort_snapshot(&snapshot).unwrap();
    let replacement = index.begin_snapshot().unwrap();
    assert_eq!(
        index.write_snapshot_rows(&mut snapshot, &mut output, 1),
        Err(Error::Snapshot)
    );
    assert_eq!(index.abort_snapshot(&snapshot), Err(Error::Snapshot));
    drop(replacement);
    assert!(index.begin_snapshot().is_ok());
}

#[test]
fn capture_excludes_expired_and_superseded_rows_without_requiring_a_full_cleanup() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1), id(2)]).unwrap();
    index.commit_through(1).unwrap();
    index.admit(2, 10, Offset::new(2), &[id(1)]).unwrap();
    index.commit_through(2).unwrap();
    assert_eq!(index.retained_entries(), 3);
    let mut snapshot = index.begin_snapshot().unwrap();
    assert_eq!(snapshot.header().first_offset(), Offset::new(2));
    assert_eq!(snapshot.header().rows(), 1);
    let mut bytes = [0; SNAPSHOT_ROW_BYTES];
    assert_eq!(
        index.write_snapshot_rows(&mut snapshot, &mut bytes, 1),
        Ok(32)
    );
    assert_eq!(bytes, row(1, 2, 10));
}

#[test]
fn empty_capture_preserves_the_committed_end_clock_and_operation() {
    let mut index = Index::new(limits(), Offset::new(17), 8).unwrap();
    index.advance_time(9, 20).unwrap();
    index.commit_through(9).unwrap();
    let mut snapshot = index.begin_snapshot().unwrap();
    assert!(snapshot.is_complete());
    assert_eq!(snapshot.header().first_offset(), Offset::new(17));
    assert_eq!(index.write_snapshot_rows(&mut snapshot, &mut [], 0), Ok(0));
    let mut restored = RestoringIndex::new(limits(), *snapshot.header())
        .unwrap()
        .finish()
        .unwrap();
    let accepted = restored.admit(10, 20, Offset::new(17), &[id(1)]).unwrap();
    assert_eq!(accepted.outcomes[0].offset, Offset::new(17));
    assert_eq!(
        restored
            .admit(11, 19, Offset::new(18), &[id(2)])
            .unwrap_err(),
        Error::TimeRegression
    );
}

#[test]
fn header_rejects_reserved_unknown_inconsistent_and_overflowing_images() {
    let (header, _) = captured();
    for (offset, value) in [(0, 0), (9, 2), (10, 1), (23, 0), (39, 0), (47, 1), (63, 3)] {
        let mut bytes = header.encode();
        bytes[offset] = value;
        assert_eq!(
            SnapshotHeader::decode(&bytes),
            Err(Error::Snapshot),
            "byte {offset}"
        );
    }
    assert_eq!(
        SnapshotHeader::decode(&header.encode()[..63]),
        Err(Error::Snapshot)
    );
    let mut overflow = header.encode();
    overflow[48..56].copy_from_slice(&u64::MAX.to_be_bytes());
    overflow[56..64].copy_from_slice(&u64::MAX.to_be_bytes());
    assert_eq!(SnapshotHeader::decode(&overflow), Err(Error::Snapshot));
}

#[test]
fn restore_rejects_policy_or_capacity_mismatch_before_allocating_an_image() {
    let (header, _) = captured();
    let mut bounds = limits();
    bounds.window_millis += 1;
    assert_eq!(
        RestoringIndex::new(bounds, header).unwrap_err(),
        Error::Snapshot
    );
    bounds = limits();
    bounds.entries = 1;
    assert_eq!(
        RestoringIndex::new(bounds, header).unwrap_err(),
        Error::Snapshot
    );
}

#[test]
fn malformed_rows_poison_the_whole_restore_even_after_a_valid_prefix() {
    let (header, bytes) = captured();
    let bad_rows = [
        row(0, 1, 4),
        row(2, 9, 4),
        row(2, 1, 5),
        row(2, 1, 3),
        row(1, 1, 4),
    ];
    for bad in bad_rows {
        let mut restore = RestoringIndex::new(limits(), header).unwrap();
        restore.push_rows(&bytes[..32]).unwrap();
        assert_eq!(restore.push_rows(&bad), Err(Error::Snapshot));
        assert_eq!(restore.push_rows(&bytes[32..]), Err(Error::Snapshot));
        assert_eq!(restore.finish().unwrap_err(), Error::Snapshot);
    }
    let mut restore = RestoringIndex::new(limits(), header).unwrap();
    assert_eq!(restore.push_rows(&bytes[..31]), Err(Error::Snapshot));
    assert_eq!(restore.finish().unwrap_err(), Error::Snapshot);
    let mut restore = RestoringIndex::new(limits(), header).unwrap();
    restore.push_rows(&bytes[..32]).unwrap();
    assert_eq!(restore.finish().unwrap_err(), Error::Snapshot);
    let mut restore = RestoringIndex::new(limits(), header).unwrap();
    restore.push_rows(&bytes).unwrap();
    assert_eq!(restore.push_rows(&row(3, 2, 4)), Err(Error::Snapshot));
    assert_eq!(restore.finish().unwrap_err(), Error::Snapshot);
}

#[test]
fn restore_rejects_expired_rows_and_bounds_each_chunk() {
    let (header, bytes) = captured();
    let mut encoded = header.encode();
    encoded[24..32].copy_from_slice(&14u64.to_be_bytes());
    let expired = SnapshotHeader::decode(&encoded).unwrap();
    let mut restore = RestoringIndex::new(limits(), expired).unwrap();
    assert_eq!(restore.push_rows(&bytes), Err(Error::Snapshot));
    let mut bounds = limits();
    bounds.batch_records = 1;
    let mut restore = RestoringIndex::new(bounds, header).unwrap();
    assert_eq!(restore.push_rows(&bytes), Err(Error::Snapshot));
    assert_eq!(restore.finish().unwrap_err(), Error::Snapshot);
    let mut restore = RestoringIndex::new(bounds, header).unwrap();
    restore.push_rows(&bytes[..32]).unwrap();
    restore.push_rows(&bytes[32..]).unwrap();
    assert_eq!(restore.finish().unwrap().retained_entries(), 2);
}
