use super::*;

#[test]
fn shard_refills_existing_dispatcher_token_without_reclaiming_unused_bytes() {
    let mut limits = limits();
    limits.capacity.data.bytes = 10;
    let mut owner = Owner::new(limits).unwrap();
    let client = owner.client(session(1), limits.capacity).unwrap();
    let mut grant = owner.grant(&client, Class::Data, quota(1, 10)).unwrap();
    let key = grant.key();
    let first = grant.admit(session(1), 3).unwrap().dequeued();
    assert_eq!(grant.remaining(), quota(0, 7));
    assert_eq!(owner.usage(Class::Data), budget(0, 1, 10));
    assert_eq!(owner.extend(&key, quota(1, 1)), Err(Error::Full));
    assert_eq!(grant.remaining(), quota(0, 7));
    owner.extend(&key, quota(1, 0)).unwrap();
    let second = grant.admit(session(1), 7).unwrap();
    assert_eq!(owner.usage(Class::Data), budget(1, 2, 10));
    drop(second);
    owner.extend(&key, quota(0, 7)).unwrap();
    assert_eq!(owner.usage(Class::Data), budget(0, 1, 10));
    drop(first);
    owner.extend(&key, quota(1, 3)).unwrap();
    assert_eq!(grant.remaining(), quota(1, 10));
    assert_eq!(owner.usage(Class::Data), budget(1, 1, 10));
    drop(grant);
    assert_eq!(owner.usage(Class::Data), Budget::default());
}

#[test]
fn obsolete_refill_key_cannot_target_reused_slot_or_reconnected_session() {
    let mut limits = limits();
    limits.grants = 1;
    let mut owner = Owner::new(limits).unwrap();
    let mut client = owner.client(session(1), limits.capacity).unwrap();
    let old = owner.grant(&client, Class::Data, quota(1, 10)).unwrap();
    let key = old.key();
    client.replace_session(session(2)).unwrap();
    assert_eq!(owner.extend(&key, quota(1, 10)), Err(Error::Revoked));
    drop(old);
    let fresh = owner.grant(&client, Class::Data, quota(1, 10)).unwrap();
    assert_eq!(owner.extend(&key, quota(1, 10)), Err(Error::Revoked));
    assert_eq!(fresh.remaining(), quota(1, 10));
    let mut other = Owner::new(limits).unwrap();
    assert_eq!(other.extend(&key, quota(1, 10)), Err(Error::Destination));
    assert_eq!(owner.extend(&fresh.key(), quota(0, 0)), Err(Error::Invalid));
}

#[test]
fn live_token_can_stay_on_another_thread_during_shard_refill() {
    let limits = limits();
    let mut owner = Owner::new(limits).unwrap();
    let client = owner.client(session(1), limits.capacity).unwrap();
    let mut grant = owner.grant(&client, Class::Data, quota(1, 10)).unwrap();
    let key = grant.key();
    let barrier = Arc::new(Barrier::new(2));
    let remote = barrier.clone();
    let thread = std::thread::spawn(move || {
        for _ in 0..100 {
            drop(grant.admit(session(1), 10).unwrap());
            remote.wait();
            remote.wait();
        }
        grant
    });
    for _ in 0..100 {
        barrier.wait();
        assert_eq!(owner.usage(Class::Data), Budget::default());
        owner.extend(&key, quota(1, 10)).unwrap();
        barrier.wait();
    }
    let grant = thread.join().unwrap();
    assert_eq!(grant.remaining(), quota(1, 10));
    assert_eq!(owner.usage(Class::Data), budget(1, 1, 10));
}

#[test]
fn shard_can_revoke_remote_token_without_releasing_admitted_work() {
    let limits = limits();
    let mut owner = Owner::new(limits).unwrap();
    let client = owner.client(session(1), limits.capacity).unwrap();
    let mut grant = owner.grant(&client, Class::Data, quota(2, 10)).unwrap();
    let key = grant.key();
    let retained = grant.admit(session(1), 3).unwrap().dequeued();
    let barrier = Arc::new(Barrier::new(2));
    let remote = barrier.clone();
    let thread = std::thread::spawn(move || {
        remote.wait();
        assert_eq!(grant.admit(session(1), 3).unwrap_err(), Error::Revoked);
        grant
    });
    assert_eq!(owner.revoke_unused(&key).unwrap(), quota(1, 7));
    assert_eq!(owner.usage(Class::Data), budget(0, 1, 3));
    barrier.wait();
    let grant = thread.join().unwrap();
    drop(grant);
    assert_eq!(owner.usage(Class::Data), budget(0, 1, 3));
    drop(retained);
    assert_eq!(owner.usage(Class::Data), Budget::default());
    let fresh = owner.grant(&client, Class::Data, quota(2, 10)).unwrap();
    assert_eq!(owner.revoke_key(&key), Err(Error::Revoked));
    assert_eq!(fresh.remaining(), quota(2, 10));
    let mut other = Owner::new(limits).unwrap();
    assert_eq!(other.revoke_key(&fresh.key()), Err(Error::Destination));
    assert_eq!(owner.revoke_unused(&key), Err(Error::Revoked));
    assert_eq!(fresh.remaining(), quota(2, 10));
}

#[test]
fn dropped_dispatcher_token_reports_unused_quota_after_slot_reuse() {
    let mut limits = limits();
    limits.grants = 1;
    let mut owner = Owner::new(limits).unwrap();
    let client = owner.client(session(1), limits.capacity).unwrap();
    let mut grant = owner.grant(&client, Class::Data, quota(2, 10)).unwrap();
    let key = grant.key();
    let alias = key.clone();
    let retained = grant.admit(session(1), 3).unwrap().dequeued();
    std::thread::spawn(move || drop(grant)).join().unwrap();
    let fresh = owner.grant(&client, Class::Data, quota(1, 1)).unwrap();
    let mut other = Owner::new(limits).unwrap();
    assert_eq!(other.revoke_unused(&key), Err(Error::Destination));
    assert_eq!(owner.revoke_unused(&key).unwrap(), quota(1, 7));
    assert_eq!(owner.revoke_unused(&alias), Err(Error::Revoked));
    assert_eq!(fresh.remaining(), quota(1, 1));
    assert_eq!(owner.usage(Class::Data), budget(1, 2, 4));
    drop(fresh);
    assert_eq!(owner.usage(Class::Data), budget(0, 1, 3));
    drop(retained);
    assert_eq!(owner.usage(Class::Data), Budget::default());
}

#[test]
fn revocation_and_dispatch_admission_settle_exact_unused_capacity() {
    for _ in 0..32 {
        let limits = limits();
        let mut owner = Owner::new(limits).unwrap();
        let client = owner.client(session(1), limits.capacity).unwrap();
        let mut grant = owner.grant(&client, Class::Data, quota(1, 10)).unwrap();
        let key = grant.key();
        let barrier = Arc::new(Barrier::new(2));
        let remote = barrier.clone();
        let thread = std::thread::spawn(move || {
            remote.wait();
            let received = grant.admit(session(1), 3).map(Admission::dequeued);
            (received, grant)
        });
        barrier.wait();
        let unused = owner.revoke_unused(&key).unwrap();
        let (received, grant) = thread.join().unwrap();
        match &received {
            Ok(_) => {
                assert_eq!(unused, quota(0, 7));
                assert_eq!(owner.usage(Class::Data), budget(0, 1, 3));
            }
            Err(error) => {
                assert_eq!(*error, Error::Revoked);
                assert_eq!(unused, quota(1, 10));
                assert_eq!(owner.usage(Class::Data), Budget::default());
            }
        }
        drop(grant);
        drop(received);
        assert_eq!(owner.usage(Class::Data), Budget::default());
    }
}
