use std::{
    sync::{Arc, Barrier},
    task::{Context, Poll, Waker},
};

use bytes::Bytes;
use ozzy_proto::LinkSessionId;

use super::*;

mod lanes;
mod memory;
mod refill;

fn budget(queue_slots: usize, retained_messages: usize, bytes: usize) -> Budget {
    Budget {
        queue_slots,
        retained_messages,
        bytes,
    }
}

fn session(value: u8) -> LinkSessionId {
    LinkSessionId::from_bytes([value; 16])
}

fn limits() -> Limits {
    Limits {
        capacity: Budgets {
            data: budget(4, 8, 100),
            control: budget(2, 4, 20),
        },
        clients: 4,
        grants: 8,
    }
}

fn quota(messages: usize, bytes: usize) -> Quota {
    Quota { messages, bytes }
}

#[test]
fn unused_grants_share_destination_and_client_capacity() {
    let limits = limits();
    let mut owner = Owner::new(limits).unwrap();
    let client_limits = Budgets {
        data: budget(3, 6, 60),
        ..limits.capacity
    };
    let first = owner.client(session(1), client_limits).unwrap();
    let second = owner.client(session(2), client_limits).unwrap();
    let grant = owner.grant(&first, Class::Data, quota(2, 60)).unwrap();
    assert_eq!(owner.usage(Class::Data), budget(2, 2, 60));
    assert_eq!(
        owner.grant(&first, Class::Data, quota(1, 1)).unwrap_err(),
        Error::Full
    );
    assert_eq!(
        owner.grant(&second, Class::Data, quota(2, 41)).unwrap_err(),
        Error::Full
    );
    let other = owner.grant(&second, Class::Data, quota(2, 40)).unwrap();
    assert_eq!(owner.usage(Class::Data), budget(4, 4, 100));
    assert_eq!(
        owner.grant(&second, Class::Data, quota(1, 1)).unwrap_err(),
        Error::Full
    );
    drop(grant);
    assert_eq!(first.usage(Class::Data), Budget::default());
    assert_eq!(owner.usage(Class::Data), budget(2, 2, 40));
    drop(other);
    assert_eq!(owner.usage(Class::Data), Budget::default());
}

#[test]
fn control_and_other_destinations_progress_while_data_is_full() {
    let limits = limits();
    let mut first = Owner::new(limits).unwrap();
    let mut second = Owner::new(limits).unwrap();
    let client = first.client(session(1), limits.capacity).unwrap();
    let other = second.client(session(1), limits.capacity).unwrap();
    let _full = first.grant(&client, Class::Data, quota(4, 100)).unwrap();
    let mut control = first.grant(&client, Class::Control, quota(1, 10)).unwrap();
    drop(control.admit(session(1), 10).unwrap());
    let mut healthy = second.grant(&other, Class::Data, quota(1, 10)).unwrap();
    drop(healthy.admit(session(1), 10).unwrap());
    assert_eq!(first.usage(Class::Data), budget(4, 4, 100));
    assert_eq!(first.usage(Class::Control), Budget::default());
    assert_eq!(second.usage(Class::Data), Budget::default());
    assert_eq!(
        first.grant(&other, Class::Data, quota(1, 1)).unwrap_err(),
        Error::Destination
    );
    assert_eq!(first.revoke(&healthy), Err(Error::Destination));
}

#[test]
fn dequeue_and_final_alias_release_return_different_capacity() {
    let limits = limits();
    let mut owner = Owner::new(limits).unwrap();
    let client = owner.client(session(1), limits.capacity).unwrap();
    let mut grant = owner.grant(&client, Class::Data, quota(1, 100)).unwrap();
    let admission = grant.admit(session(1), 100).unwrap();
    assert_eq!(owner.usage(Class::Data), budget(1, 1, 100));
    let retained = admission.dequeued();
    assert_eq!(retained.bytes(), 100);
    assert_eq!(owner.usage(Class::Data), budget(0, 1, 100));
    let payload = retained.attach(Bytes::from(vec![7; 100]));
    let tiny_slice = payload.slice(0..1);
    drop(payload);
    assert_eq!(
        owner.grant(&client, Class::Data, quota(1, 1)).unwrap_err(),
        Error::Full
    );
    std::thread::spawn(move || drop(tiny_slice)).join().unwrap();
    assert_eq!(owner.usage(Class::Data), Budget::default());
    assert!(owner.grant(&client, Class::Data, quota(1, 100)).is_ok());
}

#[test]
fn retained_message_count_bounds_tiny_requests_after_queue_release() {
    let mut limits = limits();
    limits.capacity.data.retained_messages = 2;
    let mut owner = Owner::new(limits).unwrap();
    let client = owner.client(session(1), limits.capacity).unwrap();
    let mut grant = owner.grant(&client, Class::Data, quota(2, 2)).unwrap();
    let first = grant.admit(session(1), 1).unwrap().dequeued();
    let second = grant.admit(session(1), 1).unwrap().dequeued();
    assert_eq!(owner.usage(Class::Data), budget(0, 2, 2));
    assert_eq!(
        owner.grant(&client, Class::Data, quota(1, 1)).unwrap_err(),
        Error::Full
    );
    drop(first);
    let _new = owner.grant(&client, Class::Data, quota(1, 1)).unwrap();
    drop(second);
    assert_eq!(owner.usage(Class::Data), budget(1, 1, 1));
}

#[test]
fn reconnect_reclaims_unused_credit_but_keeps_admitted_work_charged() {
    let limits = limits();
    let mut owner = Owner::new(limits).unwrap();
    let mut client = owner.client(session(1), limits.capacity).unwrap();
    let mut old = owner.grant(&client, Class::Data, quota(4, 100)).unwrap();
    let admitted = old.admit(session(1), 70).unwrap();
    client.replace_session(session(2)).unwrap();
    assert_eq!(old.remaining(), Quota::default());
    assert_eq!(old.admit(session(1), 1).unwrap_err(), Error::Revoked);
    assert_eq!(owner.usage(Class::Data), budget(1, 1, 70));
    assert_eq!(
        owner.grant(&client, Class::Data, quota(1, 31)).unwrap_err(),
        Error::Full
    );
    let mut fresh = owner.grant(&client, Class::Data, quota(3, 30)).unwrap();
    assert_eq!(fresh.admit(session(1), 1).unwrap_err(), Error::Session);
    assert_eq!(fresh.remaining(), quota(3, 30));
    let current = fresh.admit(session(2), 10).unwrap();
    assert_eq!(owner.usage(Class::Data), budget(4, 4, 100));
    drop((old, fresh, current));
    assert_eq!(owner.usage(Class::Data), budget(1, 1, 70));
    drop(admitted);
    assert_eq!(owner.usage(Class::Data), Budget::default());
}

#[test]
fn revoked_tokens_and_disconnected_payloads_still_bound_metadata() {
    let mut limits = limits();
    limits.clients = 1;
    limits.grants = 1;
    let mut owner = Owner::new(limits).unwrap();
    let mut client = owner.client(session(1), limits.capacity).unwrap();
    let mut grant = owner.grant(&client, Class::Data, quota(1, 1)).unwrap();
    let payload = grant.admit(session(1), 1).unwrap().dequeued();
    client.replace_session(session(2)).unwrap();
    assert_eq!(
        owner.grant(&client, Class::Data, quota(1, 1)).unwrap_err(),
        Error::Full
    );
    // Data token exhaustion never consumes the reserved control token table.
    drop(owner.grant(&client, Class::Control, quota(1, 1)).unwrap());
    drop(client);
    assert_eq!(
        owner.client(session(3), limits.capacity).unwrap_err(),
        Error::Full
    );
    drop(grant);
    assert_eq!(
        owner.client(session(3), limits.capacity).unwrap_err(),
        Error::Full
    );
    drop(payload);
    let fresh = owner.client(session(3), limits.capacity).unwrap();
    assert!(owner.grant(&fresh, Class::Data, quota(1, 1)).is_ok());
}

#[test]
fn cancellation_and_owner_shutdown_preserve_resource_lifetimes() {
    let limits = limits();
    let mut owner = Owner::new(limits).unwrap();
    let client = owner.client(session(1), limits.capacity).unwrap();
    let mut grant = owner.grant(&client, Class::Data, quota(3, 90)).unwrap();
    let queued = grant.admit(session(1), 20).unwrap();
    let retained = grant.admit(session(1), 20).unwrap().dequeued();
    drop(queued);
    assert_eq!(owner.usage(Class::Data), budget(1, 2, 70));
    drop(owner);
    assert_eq!(grant.admit(session(1), 1).unwrap_err(), Error::Closed);
    assert_eq!(client.usage(Class::Data), budget(0, 1, 20));
    drop(retained);
    assert_eq!(client.usage(Class::Data), Budget::default());
}

#[test]
fn invalid_admissions_do_not_spend_credit() {
    let limits = limits();
    let mut owner = Owner::new(limits).unwrap();
    assert_eq!(
        owner.client(session(0), limits.capacity).unwrap_err(),
        Error::Invalid
    );
    let mut client = owner.client(session(1), limits.capacity).unwrap();
    assert_eq!(client.replace_session(session(1)), Err(Error::Invalid));
    for invalid in [quota(0, 1), quota(2, 1)] {
        assert_eq!(
            owner.grant(&client, Class::Data, invalid).unwrap_err(),
            Error::Invalid
        );
    }
    assert_eq!(
        owner
            .grant(&client, Class::Data, quota(usize::MAX, usize::MAX))
            .unwrap_err(),
        Error::Full
    );
    let mut grant = owner.grant(&client, Class::Data, quota(1, 10)).unwrap();
    assert_eq!(grant.admit(session(1), 0).unwrap_err(), Error::Invalid);
    assert_eq!(grant.admit(session(2), 1).unwrap_err(), Error::Session);
    assert_eq!(grant.admit(session(1), 11).unwrap_err(), Error::Full);
    assert_eq!(grant.remaining(), quota(1, 10));
    owner.revoke(&grant).unwrap();
    owner.revoke(&grant).unwrap();
    assert_eq!(grant.admit(session(1), 1).unwrap_err(), Error::Revoked);
    assert_eq!(owner.usage(Class::Data), Budget::default());
}

#[tokio::test(flavor = "current_thread")]
async fn capacity_changes_survive_canceled_and_not_yet_polled_waits() {
    let limits = limits();
    let mut owner = Owner::new(limits).unwrap();
    let client = owner.client(session(1), limits.capacity).unwrap();
    let grant = owner.grant(&client, Class::Data, quota(1, 1)).unwrap();
    let seen = owner.generation();
    let mut wait = Box::pin(owner.changed_after(seen));
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(wait.as_mut().poll(&mut cx), Poll::Pending);
    drop(wait);
    drop(grant);
    let mut wait = Box::pin(owner.changed_after(seen));
    assert_eq!(wait.as_mut().poll(&mut cx), Poll::Ready(()));
    drop(wait);
    owner.changed_after(seen).await;
}

#[test]
fn reconnect_and_cross_thread_admission_cannot_double_spend() {
    let limits = limits();
    let mut owner = Owner::new(limits).unwrap();
    let mut client = owner.client(session(1), limits.capacity).unwrap();
    for round in 1..=100_u8 {
        let prior = session(round);
        let next = session(round + 1);
        let mut grant = owner.grant(&client, Class::Data, quota(4, 100)).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let send = barrier.clone();
        let thread = std::thread::spawn(move || {
            send.wait();
            let admission = grant.admit(prior, 100);
            (grant, admission)
        });
        barrier.wait();
        client.replace_session(next).unwrap();
        let (grant, result) = thread.join().unwrap();
        match result {
            Ok(admission) => {
                assert_eq!(owner.usage(Class::Data), budget(1, 1, 100));
                assert_eq!(
                    owner.grant(&client, Class::Data, quota(1, 1)).unwrap_err(),
                    Error::Full
                );
                drop(admission);
            }
            Err(error) => {
                assert_eq!(error, Error::Revoked);
                assert_eq!(owner.usage(Class::Data), Budget::default());
            }
        }
        drop(grant);
        assert_eq!(owner.usage(Class::Data), Budget::default());
    }
}
