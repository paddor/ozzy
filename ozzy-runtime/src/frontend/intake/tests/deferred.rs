use super::*;

#[test]
fn deferred_input_retains_backing_while_another_partition_progresses() {
    let mut f = fixture(32768, 256);
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    let original = f.message();
    f.service.receive(original.clone(), 1024).unwrap();
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    let slot = received.reservation;
    f.intake.defer(received).unwrap();
    assert!(!f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(f.client.usage(Class::Data).retained_messages, 1);
    assert!(
        f.intake
            .revoke_idle(Class::Data, 0, 8, |_| Ok(()))
            .unwrap()
            .is_none()
    );
    let placement = test_support::placement(1, 0);
    let healthy = f
        .intake
        .destination(
            placement.group,
            Kind::Client,
            Class::Data,
            memory::Quota {
                bytes: 256,
                buffers: 2,
            },
        )
        .unwrap();
    let request = GrantRequest {
        route: crate::frontend::Routed {
            placement,
            ..f.request.route
        },
        ..f.request
    };
    f.intake
        .install(&mut f.port, &f.links, request, None, &f.client, 1024)
        .unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    f.service
        .receive(test_support::append(placement, request.binding), 1024)
        .unwrap();
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    assert_eq!(received.request, request);
    assert!(received.current);
    drop(received);
    assert!(f.intake.settle(&healthy, |_| Ok(())).unwrap());
    assert_eq!(f.client.usage(Class::Data).retained_messages, 1);
    let retry = f
        .intake
        .retry_deferred(slot, &f.links, &f.routes)
        .unwrap()
        .unwrap();
    assert!(retry.current);
    assert_eq!(retry.message.part_slice(3), original.part_slice(3));
    assert!(
        f.intake
            .retry_deferred(slot, &f.links, &f.routes)
            .unwrap()
            .is_none()
    );
    let arena = f.destination.capacity().allocator().try_arena(64).unwrap();
    drop(retry);
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(f.client.usage(Class::Data), Budget::default());
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
    drop(arena);
}

#[test]
fn deferred_input_rechecks_session_and_preserves_new_session_allowance() {
    let mut f = fixture(32768, 256);
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    f.observe_install();
    f.service.receive(f.message(), 1024).unwrap();
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    let slot = received.reservation;
    f.intake.defer(received).unwrap();
    assert!(f.service.disconnect(f.request.binding));
    f.intake.reconcile(&f.links, 0, 8, |_| Ok(())).unwrap();
    assert!(!f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    f.sessions.disconnect(f.service.local());
    let (binding, _) = begin(&mut f.service, &f.sessions, f.request.binding.peer);
    f.request.binding = binding;
    f.client.replace_session(binding.session).unwrap();
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    let stale = f
        .intake
        .retry_deferred(slot, &f.links, &f.routes)
        .unwrap()
        .unwrap();
    assert!(
        !stale.current,
        "dequeue-time validation cannot survive reconnect"
    );
    let alias = stale.message.part_bytes(3).unwrap();
    drop(stale);
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(
        f.destination.capacity().remaining(),
        memory::Quota {
            bytes: 256,
            buffers: 2,
        }
    );
    f.service.receive(f.message(), 1024).unwrap();
    let fresh = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    assert!(fresh.current);
    drop(fresh);
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(f.client.usage(Class::Data).retained_messages, 1);
    drop(alias);
    assert_eq!(f.client.usage(Class::Data), Budget::default());
}
