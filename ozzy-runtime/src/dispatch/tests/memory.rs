use super::*;
use crate::memory::{Domain, Limits as MemoryLimits, Owner as MemoryOwner, Quota as MemoryQuota};
use std::io;

fn memory(bytes: usize) -> (Domain, MemoryOwner) {
    let domain = Domain::new(None, bytes).unwrap();
    let owner = domain
        .owner(MemoryLimits {
            bytes,
            buffers: 32,
            cache_bytes: bytes,
        })
        .unwrap();
    (domain, owner)
}

#[test]
fn canonical_buffers_unused_credit_and_foreign_aliases_share_one_bound() {
    let domain = Domain::new(None, 128).unwrap();
    let data = domain
        .owner(MemoryLimits {
            bytes: 128,
            buffers: 9,
            cache_bytes: 128,
        })
        .unwrap();
    let (_, control) = memory(32);
    let mut owner = Owner::new(limits()).unwrap();
    owner.bind_memory(&data, &control).unwrap();
    let first = owner.client(session(1), limits().capacity).unwrap();
    let second = owner.client(session(2), limits().capacity).unwrap();
    let canonical = data.try_lease(64).unwrap();
    let mut grant = owner.grant(&first, Class::Data, quota(2, 64)).unwrap();
    let key = grant.key();
    assert_eq!(
        data.claimed_capacity(),
        MemoryQuota {
            bytes: 128,
            buffers: 9
        }
    );
    // The dispatch-only budget has room, but canonical backing owns that room.
    assert_eq!(
        owner.grant(&second, Class::Data, quota(1, 1)).unwrap_err(),
        Error::Full
    );
    assert_eq!(
        data.try_lease(1).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let control_grant = owner.grant(&second, Class::Control, quota(1, 20)).unwrap();
    assert_eq!(
        control.claimed_capacity(),
        MemoryQuota {
            bytes: 20,
            buffers: 4
        }
    );
    assert_eq!(grant.admit(session(9), 16).unwrap_err(), Error::Session);
    let alias = grant
        .admit(session(1), 16)
        .unwrap()
        .dequeued()
        .attach(Bytes::from_static(b"retained payload"))
        .slice(0..1);
    assert_eq!(
        data.claimed_capacity(),
        MemoryQuota {
            bytes: 128,
            buffers: 9
        }
    );
    assert_eq!(
        data.reserved_capacity(),
        MemoryQuota {
            bytes: 48,
            buffers: 4
        }
    );
    assert_eq!(data.allocated_bytes(), 80);
    // Return queue capacity independently. Its four foreign allocation slots
    // and full backing bytes stay held until the payload alias disappears.
    assert_eq!(owner.extend(&key, quota(1, 0)).unwrap_err(), Error::Full);
    owner.revoke_key(&key).unwrap();
    assert_eq!(
        data.claimed_capacity(),
        MemoryQuota {
            bytes: 80,
            buffers: 5
        }
    );
    assert_eq!(data.reserved_capacity(), MemoryQuota::default());
    assert_eq!(first.usage(Class::Data).bytes, 16);
    assert_eq!(
        data.try_lease(49).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(
        data.claimed_capacity(),
        MemoryQuota {
            bytes: 64,
            buffers: 1
        }
    );
    let replacement = owner.grant(&second, Class::Data, quota(1, 64)).unwrap();
    assert_eq!(
        data.claimed_capacity(),
        MemoryQuota {
            bytes: 128,
            buffers: 5
        }
    );
    drop((grant, replacement, control_grant, canonical));
    data.trim_cache();
    assert_eq!(data.claimed_capacity(), MemoryQuota::default());
    assert_eq!(control.claimed_capacity(), MemoryQuota::default());
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_observation_and_session_fences_keep_foreign_backing_charged() {
    let (domain, data) = memory(128);
    let (_, control) = memory(32);
    let mut owner = Owner::new(limits()).unwrap();
    owner.bind_memory(&data, &control).unwrap();
    let mut client = owner.client(session(1), limits().capacity).unwrap();
    let mut grant = owner.grant(&client, Class::Data, quota(2, 100)).unwrap();
    let generation = data.generation();
    // Canceling an observer does not release any submitted reservation.
    drop(data.changed_after(generation));
    let retained = grant.admit(session(1), 70).unwrap().dequeued();
    let alias = retained.clone().attach(Bytes::from_static(b"opaque"));
    drop(retained);
    client.replace_session(session(2)).unwrap();
    assert_eq!(
        data.claimed_capacity(),
        MemoryQuota {
            bytes: 70,
            buffers: 4
        }
    );
    assert_eq!(data.reserved_capacity(), MemoryQuota::default());
    assert_eq!(client.usage(Class::Data).bytes, 70);
    drop((grant, client, owner));
    let returned = data.changed_after(data.generation());
    drop(data);
    assert_eq!(domain.reserved_bytes(), 128);
    std::thread::spawn(move || drop(alias)).join().unwrap();
    returned.await;
    // The observer itself retains only signal state, not the owner reservation.
    assert_eq!(domain.reserved_bytes(), 0);
}

#[test]
fn credit_extension_reserves_independent_byte_and_physical_count_returns() {
    let (_, data) = memory(128);
    let (_, control) = memory(32);
    let mut owner = Owner::new(limits()).unwrap();
    assert_eq!(owner.bind_memory(&data, &data).unwrap_err(), Error::Invalid);
    owner.bind_memory(&data, &control).unwrap();
    let client = owner.client(session(1), limits().capacity).unwrap();
    let mut grant = owner.grant(&client, Class::Data, quota(1, 64)).unwrap();
    let key = grant.key();
    let retained = grant.admit(session(1), 64).unwrap().dequeued();
    owner.extend(&key, quota(1, 0)).unwrap();
    assert_eq!(
        data.reserved_capacity(),
        MemoryQuota {
            bytes: 0,
            buffers: 4
        }
    );
    assert_eq!(grant.admit(session(1), 1).unwrap_err(), Error::Full);
    drop(retained);
    owner.extend(&key, quota(0, 64)).unwrap();
    assert_eq!(
        data.claimed_capacity(),
        MemoryQuota {
            bytes: 64,
            buffers: 4
        }
    );
    let retained = grant.admit(session(1), 64).unwrap().dequeued();
    drop((grant, retained));
    assert_eq!(data.claimed_capacity(), MemoryQuota::default());
}
