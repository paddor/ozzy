//! Cache accounting only. Actor tests populate it through real quorum application.

use super::*;
use ozzy_proto::GroupId;
use ozzy_replication::{Digest, OpNumber};

fn prefix(op: u8) -> Prefix {
    Prefix {
        op: OpNumber(u64::from(op)),
        digest: Digest::from_bytes([op; 32]),
    }
}

fn entry(first: u8, count: u8, bytes: usize) -> Entry {
    Entry {
        scope: Scope {
            group_id: GroupId::from_bytes([1; 16]),
            configuration_epoch: 1,
            configuration_digest: Digest::from_bytes([2; 32]),
            view: 0,
        },
        generation: JournalGeneration(1),
        predecessor: prefix(first - 1),
        end: prefix(first + count - 1),
        operations: usize::from(count),
        retained_bytes: bytes,
        packets: [None, None, None],
    }
}

#[test]
fn operation_bound_evicts_without_growing_or_waiting_for_peers() {
    let mut cache = Recent::new(PipelineLimits {
        max_operations: 4,
        max_body_bytes: 1024,
    });
    let capacity = cache.entries.capacity();
    for first in (1..=15).step_by(2) {
        cache.retain(entry(first, 2, 100));
        assert!(cache.operations <= 4);
        assert!(cache.entries.len() <= 2);
        assert_eq!(cache.entries.capacity(), capacity);
    }
    let last = cache.entries.back().unwrap();
    assert_eq!(
        cache
            .after(last.scope, last.generation, prefix(12))
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        cache
            .after(last.scope, last.generation, prefix(11))
            .unwrap(),
        None
    );
    // Interior operation has no retained packet boundary: exact disk lookup required.
    assert_eq!(
        cache
            .after(last.scope, last.generation, prefix(13))
            .unwrap(),
        None
    );
    let wrong = Prefix {
        digest: Digest::ZERO,
        ..prefix(14)
    };
    assert!(matches!(
        cache.after(last.scope, last.generation, wrong),
        Err(ActorError::History(ref failure)) if failure.reason() == crate::replica_actor::HistoryReason::Lookup
            && failure.site().file().ends_with("normal/recent.rs")
    ));
    cache.clear();
    assert_eq!((cache.operations, cache.retained_bytes), (0, 0));
    assert_eq!(cache.entries.capacity(), capacity);
}

#[test]
fn body_bound_evicts_even_when_operation_slots_remain() {
    let mut cache = Recent::new(PipelineLimits {
        max_operations: 4,
        max_body_bytes: 100,
    });
    cache.retain(entry(1, 1, 40));
    cache.retain(entry(2, 1, 50));
    cache.retain(entry(3, 1, 60));
    assert_eq!(
        (cache.entries.len(), cache.operations, cache.retained_bytes),
        (1, 1, 60)
    );
    assert_eq!(cache.entries.front().unwrap().predecessor, prefix(2));
}

#[test]
fn view_generation_and_discontinuity_never_join_cached_lineages() {
    let mut cache = Recent::new(PipelineLimits {
        max_operations: 4,
        max_body_bytes: 1024,
    });
    cache.retain(entry(1, 1, 100));
    let old = &cache.entries[0];
    let (scope, generation) = (old.scope, old.generation);
    assert_eq!(
        cache
            .after(Scope { view: 1, ..scope }, generation, prefix(0))
            .unwrap(),
        None
    );
    assert_eq!(
        cache.after(scope, JournalGeneration(2), prefix(0)).unwrap(),
        None
    );
    let mut next = entry(2, 1, 100);
    next.scope.view = 1;
    cache.retain(next);
    assert_eq!(cache.entries.len(), 1);
    let mut next = entry(3, 1, 100);
    next.scope.view = 1;
    next.generation = JournalGeneration(2);
    cache.retain(next);
    assert_eq!(cache.entries.len(), 1);
    let mut next = entry(5, 1, 100); // Same scope/generation, missing op 4.
    next.scope.view = 1;
    next.generation = JournalGeneration(2);
    cache.retain(next);
    assert_eq!(cache.entries.len(), 1);
    assert_eq!(cache.entries[0].predecessor, prefix(4));
}

#[test]
fn oversized_backing_bypasses_cache_and_releases_old_packets() {
    let mut cache = Recent::new(PipelineLimits {
        max_operations: 4,
        max_body_bytes: 100,
    });
    cache.retain(entry(1, 1, 80));
    cache.retain(entry(2, 1, 101));
    assert!(cache.entries.is_empty());
    assert_eq!((cache.operations, cache.retained_bytes), (0, 0));
    cache.retain(entry(3, 1, 40));
    let last = cache.entries.back().unwrap();
    assert_eq!(
        cache.after(last.scope, last.generation, prefix(2)).unwrap(),
        Some(0)
    );
}
