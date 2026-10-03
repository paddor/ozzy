use super::*;
use crate::memory::{Arena, Quota};

fn quota(bytes: usize, buffers: usize) -> Quota {
    Quota { bytes, buffers }
}

#[test]
fn concurrent_foreign_admission_cannot_spend_a_reservation_twice() {
    let domain = Domain::new(None, 100).unwrap();
    let owner = domain.owner(limits(100, 100)).unwrap();
    let allowance = Arc::new(owner.external(quota(100, 100)).unwrap());
    let charges = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let allowance = allowance.clone();
                scope.spawn(move || {
                    (0..50)
                        .filter_map(|_| allowance.admit(quota(1, 1)).ok())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(charges.len(), 100);
    assert_eq!(owner.reserved_capacity(), quota(0, 0));
    assert_eq!(owner.claimed_capacity(), quota(100, 100));
    drop(charges);
    assert_eq!(owner.claimed_capacity(), quota(0, 0));
}

#[test]
fn unused_grants_and_physical_aliases_share_the_same_allocation_bound() {
    let domain = Domain::new(None, 128).unwrap();
    let owner = domain.owner(limits(128, 2)).unwrap();
    let first = owner.capacity();
    let second = owner.capacity();
    owner.reserve(&first, quota(64, 1)).unwrap();
    owner.reserve(&second, quota(64, 1)).unwrap();
    assert_eq!(owner.reserved_capacity(), quota(128, 2));
    assert_eq!(owner.allocated_bytes(), 0);
    assert_eq!(
        owner.try_lease(1).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let mut arena = first.allocator().try_arena(64).unwrap();
    arena.bytes_mut().extend_from_slice(&[7; 16]);
    let bytes = Arena::share(&Arc::new(arena));
    let alias = bytes.slice(0..1);
    assert_eq!(first.remaining(), quota(0, 0));
    assert_eq!(owner.reserved_capacity(), quota(64, 1));
    assert_eq!(owner.allocated_bytes(), 64);
    assert_eq!(
        owner.reserve(&first, quota(1, 0)).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop((first, bytes));
    let other = second.allocator().try_arena(64).unwrap();
    assert_eq!(owner.allocated_bytes(), 128);
    assert_eq!(owner.reserved_capacity(), quota(0, 0));
    assert_eq!(second.remaining(), quota(0, 0));
    std::thread::spawn(move || drop(alias)).join().unwrap();
    owner.reserve(&second, quota(64, 1)).unwrap();
    assert_eq!(owner.allocated_bytes(), 64);
    assert_eq!(owner.reserved_capacity(), quota(64, 1));
    drop(other);
    owner.trim_cache();
    // Actual buffer release never advertises another allocation automatically.
    assert_eq!(second.remaining(), quota(64, 1));
    drop(second);
    assert_eq!(owner.reserved_capacity(), quota(0, 0));
}

#[test]
fn reserved_allocator_cannot_inflate_a_grant_by_reusing_an_oversized_cache_entry() {
    let domain = Domain::new(None, 256).unwrap();
    let owner = domain.owner(limits(256, 3)).unwrap();
    drop(owner.try_lease(128).unwrap());
    let capacity = owner.capacity();
    owner.reserve(&capacity, quota(32, 1)).unwrap();
    let arena = capacity.allocator().try_arena(32).unwrap();
    assert_eq!(arena.capacity(), 32);
    assert_eq!(owner.allocated_bytes(), 160);
    assert_eq!(capacity.remaining(), quota(0, 0));
    assert_eq!(
        capacity.allocator().try_arena(1).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(owner.try_lease(128).unwrap().capacity(), 128);
}

#[test]
fn cached_backing_cannot_consume_a_reserved_second_body() {
    let domain = Domain::new(None, 256).unwrap();
    let owner = domain.owner(limits(256, 4)).unwrap();
    drop(owner.try_lease(96).unwrap());
    let capacity = owner.capacity();
    owner.reserve(&capacity, quota(128, 2)).unwrap();
    let original = capacity.allocator().try_arena(64).unwrap();
    let replacement = capacity.allocator().try_arena(64).unwrap();
    assert_eq!(original.capacity() + replacement.capacity(), 128);
    assert_eq!(capacity.remaining(), quota(0, 0));
}

#[test]
fn separate_byte_and_buffer_grants_refuse_foreign_owners_and_keep_failed_charges() {
    let domain = Domain::new(None, 256).unwrap();
    let owner = domain.owner(limits(128, 2)).unwrap();
    let foreign = domain.owner(limits(128, 2)).unwrap();
    let capacity = owner.capacity();
    assert_eq!(
        foreign.reserve(&capacity, quota(32, 1)).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    owner.reserve(&capacity, quota(32, 0)).unwrap();
    assert_eq!(
        capacity.allocator().try_arena(32).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(capacity.remaining(), quota(32, 0));
    owner.reserve(&capacity, quota(0, 1)).unwrap();
    assert_eq!(
        capacity.allocator().try_arena(64).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(capacity.remaining(), quota(32, 1));
    assert_eq!(
        capacity.release(quota(33, 0)).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    capacity.release(quota(16, 0)).unwrap();
    assert_eq!(capacity.remaining(), quota(16, 1));
    let arena = capacity.allocator().try_arena(16).unwrap();
    assert_eq!(owner.reserved_capacity(), quota(0, 0));
    assert_eq!(owner.allocated_bytes(), 16);
    drop(arena);
}

#[test]
fn dropped_capacity_fences_allocators_but_keeps_live_payload_charged() {
    let domain = Domain::new(None, 128).unwrap();
    let owner = domain.owner(limits(128, 2)).unwrap();
    let capacity = owner.capacity();
    owner.reserve(&capacity, quota(128, 2)).unwrap();
    let allocator = capacity.allocator();
    let mut arena = allocator.try_arena(64).unwrap();
    arena.bytes_mut().extend_from_slice(&[7; 16]);
    let bytes = Arena::share(&Arc::new(arena));
    drop(capacity);
    assert_eq!(
        allocator.try_arena(1).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(owner.reserved_capacity(), quota(0, 0));
    assert_eq!(owner.allocated_bytes(), 64);
    drop(owner);
    assert_eq!(domain.reserved_bytes(), 128);
    assert_eq!(bytes.as_ref(), &[7; 16]);
    std::thread::spawn(move || drop(bytes)).join().unwrap();
    assert_eq!(domain.reserved_bytes(), 0);
}
