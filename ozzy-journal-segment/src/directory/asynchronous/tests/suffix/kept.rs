use super::*;

/// A journal with four synchronized operations, the first `committed` of
/// them confirmed.
fn settled(committed: usize) -> (Controller, Journal, Vec<CanonicalOperation<'static>>) {
    let (mut controller, mut journal) = empty_journal();
    let old = operations(LogPosition::GENESIS, &BODIES[..4], 0);
    for chunk in old.chunks(2) {
        let written = drive(&mut controller, journal.append(chunk, BodyEncoding::Raw)).unwrap();
        drive(&mut controller, journal.sync_through(written)).unwrap();
    }
    let mut next = journal.next_manifest().unwrap();
    next.accepted = position(old[3]);
    next.committed = position(old[committed - 1]);
    drive(&mut controller, journal.install_metadata(next)).unwrap();
    (controller, journal, old)
}

fn keeping(journal: &Journal) -> crate::SuffixRequest {
    let end = journal.accepted_position().unwrap();
    let replacement = SuffixReplacement {
        segment_capacity: journal.manifest.segments[0].capacity,
        ..request(journal, end)
    };
    stream_request(replacement, end)
}

#[test]
fn a_selection_that_keeps_the_whole_log_installs_without_a_new_segment() {
    let (mut controller, journal, old) = settled(4);
    let io = journal.access.io.clone();
    let segments = journal.manifest.segments.clone();
    let request = keeping(&journal);
    assert!(journal.keeps_suffix(&request).unwrap());
    let mut journal = drive(&mut controller, journal.keep_suffix(request)).unwrap();
    assert_eq!(journal.manifest.segments, segments);
    assert_eq!(journal.manifest.promised_view, 2);
    assert_eq!(journal.manifest.last_normal_view, 2);
    assert_eq!(journal.accepted_position().unwrap(), position(old[3]));
    assert_eq!(journal.committed_position().unwrap(), position(old[3]));
    assert_eq!(
        journal.writer().durable_position().generation(),
        JournalGeneration(9)
    );
    // The new owner appends to the kept segment.
    let new = operations(position(old[3]), &BODIES[4..6], 2);
    let written = drive(&mut controller, journal.append(&new, BodyEncoding::Raw)).unwrap();
    assert_eq!(written.generation(), JournalGeneration(9));
    drive(&mut controller, journal.sync_through(written)).unwrap();
    let expected = old
        .iter()
        .chain(&new)
        .map(|op| (op.op_number, op.body.to_vec()))
        .collect::<Vec<_>>();
    assert_eq!(drive(&mut controller, replay(&journal)), expected);
    drop(journal);
    let journal = drive(&mut controller, open(io, 12)).unwrap();
    assert_eq!(journal.manifest.segments, segments);
    assert_eq!(journal.manifest.last_normal_view, 2);
    assert_eq!(drive(&mut controller, replay(&journal)), expected);
}

#[test]
fn a_selection_that_replaces_or_resizes_is_not_kept() {
    // An unconfirmed tail may be replaced.
    let (mut controller, journal, old) = settled(2);
    let replacement = SuffixReplacement {
        segment_capacity: journal.manifest.segments[0].capacity,
        ..request(&journal, position(old[1]))
    };
    let shorter = stream_request(replacement, position(old[1]));
    assert!(!journal.keeps_suffix(&shorter).unwrap());
    assert!(matches!(
        drive(&mut controller, journal.keep_suffix(shorter)),
        Err(Error::IncompleteSuffix)
    ));
    // Another segment size needs a new segment.
    let (_controller, journal, _) = settled(4);
    let mut resized = keeping(&journal);
    resized.replacement.segment_capacity *= 2;
    assert!(!journal.keeps_suffix(&resized).unwrap());
}

#[test]
fn a_selection_that_ends_at_the_own_log_keeps_an_unconfirmed_tail() {
    let (mut controller, journal, old) = settled(2);
    let io = journal.access.io.clone();
    let segments = journal.manifest.segments.clone();
    let end = journal.accepted_position().unwrap();
    let replacement = SuffixReplacement {
        segment_capacity: journal.manifest.segments[0].capacity,
        ..request(&journal, position(old[1]))
    };
    assert_eq!(replacement.retained, position(old[1]));
    let request = stream_request(replacement, end);
    assert!(journal.keeps_suffix(&request).unwrap());
    let journal = drive(&mut controller, journal.keep_suffix(request)).unwrap();
    assert_eq!(journal.manifest.segments, segments);
    assert_eq!(journal.accepted_position().unwrap(), position(old[3]));
    assert_eq!(journal.committed_position().unwrap(), position(old[1]));
    let expected = old
        .iter()
        .map(|op| (op.op_number, op.body.to_vec()))
        .collect::<Vec<_>>();
    assert_eq!(drive(&mut controller, replay(&journal)), expected);
    drop(journal);
    let journal = drive(&mut controller, open(io, 12)).unwrap();
    assert_eq!(journal.manifest.segments, segments);
    assert_eq!(journal.committed_position().unwrap(), position(old[1]));
    assert_eq!(drive(&mut controller, replay(&journal)), expected);
}
