//! Lookup cache tests.

use super::*;

fn prefix(op: u64) -> Prefix {
    Prefix {
        op: OpNumber(op),
        digest: Digest::from_bytes([op as u8; 32]),
    }
}

fn scan(lookup: &mut Lookup, wanted: &[u64], ops: std::ops::Range<u64>) {
    let mut slots = [None; 8];
    for (slot, op) in slots.iter_mut().zip(wanted) {
        *slot = Some(OpNumber(*op));
    }
    lookup.scan = Passed::new(slots);
    for op in ops {
        lookup.scan.observe(prefix(op));
    }
}

#[test]
fn scans_keep_the_wanted_prefixes_they_passed_for_their_source() {
    let source = LogSource {
        voter: NodeId::from_bytes([1; 16]),
        generation: ozzy_replication::JournalGeneration(1),
        accepted: prefix(100),
    };
    let mut lookup = Lookup::default();
    scan(&mut lookup, &[10, 20, 95], 10..90);
    lookup.remember(source);
    assert_eq!(lookup.get(source, OpNumber(20)), Some(prefix(20).digest));
    assert_eq!(lookup.get(source, OpNumber(15)), None);
    assert_eq!(lookup.get(source, OpNumber(95)), None);
    // A later, shorter scan of the same source adds to them.
    scan(&mut lookup, &[20, 95], 90..100);
    lookup.remember(source);
    assert_eq!(lookup.get(source, OpNumber(10)), Some(prefix(10).digest));
    assert_eq!(lookup.get(source, OpNumber(95)), Some(prefix(95).digest));
    // A scan that starts over keeps its wants, not what it saw.
    let mut restarted = Lookup::default();
    scan(&mut restarted, &[10], 10..11);
    restarted.scan.restart();
    assert_eq!(restarted.scan.found, [None; 8]);
    assert_eq!(restarted.scan.wanted[0], Some(OpNumber(10)));
    // Another source's scan replaces them.
    let other = LogSource {
        voter: NodeId::from_bytes([2; 16]),
        ..source
    };
    scan(&mut lookup, &[50], 40..60);
    lookup.remember(other);
    assert_eq!(lookup.get(source, OpNumber(10)), None);
    assert_eq!(lookup.get(other, OpNumber(50)), Some(prefix(50).digest));
}
