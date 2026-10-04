use ozzy_core::reader::seek::Selection;
use ozzy_proto::{
    MessageId, Offset,
    reader::{IdPolicy, Start},
};

#[test]
fn timestamp_selection_uses_lowest_offset_even_when_clock_moves_backwards() {
    let mut selection = Selection::new(Start::Timestamp(50), Offset::new(2), Offset::new(5));
    for (offset, timestamp) in [(4, 90), (3, 100), (2, 60)] {
        selection.observe(
            Offset::new(offset),
            MessageId::from_bytes([1; 16]),
            timestamp,
        );
    }
    assert_eq!(selection.finish(), Ok(Offset::new(2)));
}

#[test]
fn duplicate_ids_require_an_explicit_retained_match_policy() {
    let id = MessageId::from_bytes([1; 16]);
    for (policy, expected) in [
        (IdPolicy::RequireUnique, None),
        (IdPolicy::FirstRetained, Some(2)),
        (IdPolicy::LastRetained, Some(4)),
    ] {
        let mut selection = Selection::new(
            Start::RecordId { id, policy },
            Offset::new(2),
            Offset::new(5),
        );
        for offset in [4, 1, 2, 5] {
            selection.observe(Offset::new(offset), id, 0);
        }
        if let Some(expected) = expected {
            assert_eq!(selection.finish(), Ok(Offset::new(expected)));
        } else {
            assert!(
                matches!(selection.finish(), Err(ozzy_core::reader::seek::SeekError::Ambiguous { first, last })
            if first == Offset::new(2) && last == Offset::new(4))
            );
        }
    }
}
