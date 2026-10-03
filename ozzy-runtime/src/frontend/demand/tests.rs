use super::*;
use crate::{
    dispatch::{Budgets, Quota},
    frontend::{Kind, ReceiveError, Rejection, service::tests::*, test_support::*},
};
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{LinkSessionId, ProducerId, handshake};
use std::sync::atomic::{AtomicUsize, Ordering};

fn request(peer: u8, writer: u8, class: Class) -> GrantRequest {
    GrantRequest {
        binding: Binding {
            peer: NodeId::from_bytes([peer; 16]),
            session: LinkSessionId::from_bytes([2; 16]),
            kind: Kind::Client,
        },
        route: Routed {
            placement: placement(0, 0),
            class,
            writer: (class == Class::Data).then_some(ProducerId::from_bytes([writer; 16])),
        },
    }
}

#[tokio::test(flavor = "current_thread")]
async fn requests_coalesce_bound_peers_and_keep_control_capacity() {
    let (mut table, mut requests) = Requests::new(2, 1).unwrap();
    let generation = requests.generation();
    let wait = requests.changed_after(generation);
    table.request(request(1, 4, Class::Data), 1024);
    wait.await;
    let previous = requests.generation();
    table.request(request(1, 4, Class::Data), 2048);
    assert_eq!(
        requests.retained_bytes(request(1, 4, Class::Data)),
        Some(2048)
    );
    assert_ne!(requests.generation(), previous);
    let unchanged = requests.generation();
    table.request(request(1, 4, Class::Data), 1024);
    table.request(request(1, 5, Class::Data), 1024);
    assert_eq!(requests.generation(), unchanged);
    table.request(request(2, 5, Class::Data), 1024);
    table.request(request(3, 6, Class::Data), 1024); // Global table is full.
    table.request(request(1, 0, Class::Control), 1024);
    assert_eq!(requests.next_request(), Some(request(1, 4, Class::Data)));
    assert_eq!(requests.next_request(), Some(request(1, 0, Class::Control)));
    assert_eq!(requests.next_request(), Some(request(2, 5, Class::Data)));
    requests.dismiss(request(1, 4, Class::Data));
    table.request(request(1, 5, Class::Data), 1024);
    // An obsolete observer cannot dismiss a replacement scope.
    requests.dismiss(request(1, 4, Class::Data));
    table.fence(NodeId::from_bytes([2; 16]));
    assert_eq!(requests.next_request(), Some(request(1, 0, Class::Control)));
    assert_eq!(requests.next_request(), Some(request(1, 5, Class::Data)));
    table.installed(
        NodeId::from_bytes([1; 16]),
        GrantTarget::Partition(Subject {
            group: placement(0, 0).group,
            writer: None,
        }),
        Class::Control,
    );
    assert_eq!(requests.next_request(), Some(request(1, 5, Class::Data)));
    let generation = requests.generation();
    let closed = requests.changed_after(generation);
    drop(table);
    closed.await;
    assert!(requests.is_closed());
    assert!(requests.next_request().is_none());
}

struct Payload {
    bytes: Vec<u8>,
    released: Arc<AtomicUsize>,
}

#[test]
fn observed_demand_keeps_its_backing_charge_after_concurrent_installation() {
    let (mut table, mut requests) = Requests::new(4, 4).unwrap();
    let first = request(1, 0, Class::Control);
    table.request(first, 1024);
    let (observed, bytes) = requests.next_request_with_bytes().unwrap();
    table.installed(first.binding.peer, GrantTarget::Control(0), Class::Control);
    assert_eq!(requests.retained_bytes(first), None);
    assert_eq!((observed, bytes), (first, 1024));
    // A later refusal updates the table independently of this captured request.
    table.request(first, 2048);
    assert_eq!(requests.next_request_with_bytes(), Some((first, 2048)));
    assert_eq!(bytes, 1024);
}

#[test]
fn dismissed_demand_can_be_requested_again_without_a_stale_observation() {
    let (mut table, mut requests) = Requests::new(1, 1).unwrap();
    let scope = request(1, 0, Class::Control);
    table.request(scope, 1024);
    assert_eq!(requests.next_request_with_bytes(), Some((scope, 1024)));
    requests.dismiss(scope);
    assert_eq!(requests.next_request(), None);
    table.request(scope, 2048);
    assert_eq!(requests.next_request_with_bytes(), Some((scope, 2048)));
}

#[test]
fn control_demand_preserves_partition_scopes_until_shared_or_narrow_installation() {
    let (mut table, mut requests) = Requests::new(4, 4).unwrap();
    let first = request(1, 0, Class::Control);
    let mut second = first;
    second.route.placement = placement(1, 0);
    table.request(first, 1024);
    table.request(second, 2048);
    assert_eq!(requests.next_request(), Some(first));
    assert_eq!(requests.next_request(), Some(second));
    table.installed(
        first.binding.peer,
        GrantTarget::Partition(Subject {
            group: first.route.placement.group,
            writer: None,
        }),
        Class::Control,
    );
    assert_eq!(requests.next_request(), Some(second));
    table.installed(first.binding.peer, GrantTarget::Control(0), Class::Control);
    assert!(requests.next_request().is_none());
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.released.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn refused_payloads_release_and_reconnect_requires_fresh_destination_credit() {
    let (mut service, mut lanes, placements) = setup();
    let mut requests = service.grant_requests(0, 1).unwrap();
    let mut healthy = service.grant_requests(17, 1).unwrap();
    let remote = remote(1, handshake::PRODUCER | handshake::CONSUMER);
    let (old, _) = begin(&mut service, &remote, NodeId::from_bytes([1; 16]));
    let original = append(placements[0], old);
    let released = Arc::new(AtomicUsize::new(0));
    let message = Message::multipart([
        Bytes::copy_from_slice(original.part_slice(0).unwrap()),
        Bytes::copy_from_slice(original.part_slice(1).unwrap()),
        Bytes::copy_from_slice(original.part_slice(2).unwrap()),
        Bytes::from_owner(Payload {
            bytes: original.part_slice(3).unwrap().to_vec(),
            released: released.clone(),
        }),
    ]);
    drop(original);
    assert!(matches!(
        service.receive(message, 4096),
        Err(ReceiveError::Dispatch(Rejection::NoGrant))
    ));
    assert_eq!(released.load(Ordering::Relaxed), 1);
    assert!(lanes[0].try_recv().unwrap().is_none());
    let demand = requests.next_request().unwrap();
    assert_eq!(demand.binding, old);
    assert_eq!(demand.route.placement, placements[0]);
    // Full demand metadata on one shard cannot consume another shard's slots.
    assert!(matches!(
        service.receive(append(placements[1], old), 4096),
        Err(ReceiveError::Dispatch(Rejection::NoGrant))
    ));
    assert_eq!(
        healthy.next_request().unwrap().route.placement,
        placements[1]
    );
    let capacity = Budgets {
        data: crate::dispatch::Budget {
            queue_slots: 2,
            retained_messages: 4,
            bytes: 8192,
        },
        control: crate::dispatch::Budget {
            queue_slots: 1,
            retained_messages: 2,
            bytes: 4096,
        },
    };
    let mut client = lanes[0].credits().client(old.session, capacity).unwrap();
    let quota = Quota {
        messages: 2,
        bytes: 8192,
    };
    let grant = lanes[0]
        .credits()
        .grant(&client, Class::Data, quota)
        .unwrap();
    service.install(old.peer, demand.target(), grant).unwrap();
    assert!(requests.next_request().is_none());
    assert!(
        service
            .receive(append(placements[0], old), 4096)
            .unwrap()
            .is_some()
    );
    let held = lanes[0].try_recv().unwrap().unwrap();
    assert!(service.disconnect(old));
    assert!(healthy.next_request().is_none());
    remote.disconnect(service.local());
    let (current, _) = begin(&mut service, &remote, old.peer);
    assert_ne!(current.session, old.session);
    client.replace_session(current.session).unwrap();
    assert!(matches!(
        service.receive(append(placements[0], current), 4096),
        Err(ReceiveError::Dispatch(Rejection::NoGrant))
    ));
    let replacement = requests.next_request().unwrap();
    requests.dismiss(demand);
    assert_eq!(requests.next_request(), Some(replacement));
    // Old retained bytes stay charged after the session's unused credit is gone.
    assert_eq!(lanes[0].credits().usage(Class::Data).bytes, 4096);
    assert!(
        lanes[0]
            .credits()
            .grant(&client, Class::Data, quota)
            .is_err()
    );
    drop(held);
    let grant = lanes[0]
        .credits()
        .grant(&client, Class::Data, quota)
        .unwrap();
    service
        .install(current.peer, replacement.target(), grant)
        .unwrap();
    assert!(requests.next_request().is_none());
    assert!(
        service
            .receive(append(placements[0], current), 4096)
            .unwrap()
            .is_some()
    );
    let input = lanes[0].try_recv().unwrap().unwrap();
    assert_eq!(input.session, current.session);
    drop(input);
}
