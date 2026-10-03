use super::*;
use crate::replicated::TopicRoutes;
use ozzy_proto::directory::RouteState;

async fn observed(routes: &TopicRoutes, view: u64) -> RouteState {
    loop {
        let generation = routes.generation();
        if let Some(route) = routes.route(0).unwrap()
            && route.view >= view
        {
            return route;
        }
        routes.changed_after(generation).await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lost_routing_snapshot_retries_on_injected_clock_without_blocking_shared_control() {
    tokio::time::timeout(Duration::from_secs(10), retry_scenario())
        .await
        .unwrap();
}

async fn retry_scenario() {
    let (mut harness, links, clock, _) = setup_shared().await;
    let topic = harness.drive(links.topic("orders"), false).await.unwrap();
    let routes = links.routes(topic).unwrap();
    harness.drop_snapshots = true;
    routes.interest(0).unwrap();
    for _ in 0..10000 {
        harness.pump(false);
        if harness.snapshots != 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(harness.snapshots, 1);
    let watch = harness.watch.unwrap();
    assert_eq!(routes.route(0).unwrap(), None);
    assert_eq!(
        harness
            .drive(links.topic("orders"), false)
            .await
            .unwrap()
            .partition_count(),
        1
    );
    let generation = routes.generation();
    clock.advance(Duration::from_secs(5)).unwrap();
    harness
        .drive(routes.changed_after(generation), false)
        .await
        .unwrap();
    assert_eq!(harness.snapshots, 1);
    assert_eq!(routes.route(0).unwrap(), None);
    harness.drop_snapshots = false;
    clock.advance(Duration::from_millis(5100)).unwrap();
    assert!(
        harness
            .drive(observed(&routes, 0), false)
            .await
            .leader
            .is_some()
    );
    assert_eq!(harness.snapshots, 2);
    assert_eq!(harness.watch, Some(watch));
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn shared_topic_routes_coalesce_hints_refresh_and_ignore_obsolete_link_notifications() {
    tokio::time::timeout(Duration::from_secs(10), scenario())
        .await
        .unwrap();
}

async fn scenario() {
    let (mut harness, links, _, authority) = setup_shared().await;
    let topic = harness.drive(links.topic("orders"), false).await.unwrap();
    let routes = links.routes(topic).unwrap();
    assert_eq!(routes.route(0).unwrap(), None);
    let reused = links.routes(routes.metadata().clone()).unwrap();
    assert!(std::ptr::eq(routes.metadata(), reused.metadata()));
    assert!(routes.interest(1).is_err());
    routes.interest(0).unwrap();
    routes.interest(0).unwrap();
    let initial = harness.drive(observed(&routes, 0), false).await;
    assert_eq!(initial.leader, Some(authority.primary));
    let old_session = links.session(authority.primary).unwrap();
    let old_watch = harness.watch.unwrap();
    let mut latest = initial.clone();
    for index in 1..=3 {
        latest.view = initial.view + index;
        latest.leader = if index == 2 {
            None
        } else {
            Some(authority.primary)
        };
        harness.service.publish_route(&latest).unwrap();
    }
    assert_eq!(
        harness.drive(observed(&routes, latest.view), false).await,
        latest
    );
    let mut generation = routes.generation();
    routes.refresh(0).unwrap();
    let refreshed = harness
        .drive(
            async {
                loop {
                    routes.changed_after(generation).await.unwrap();
                    generation = routes.generation();
                    if routes.route(0).unwrap().is_some() {
                        return true;
                    }
                }
            },
            false,
        )
        .await;
    assert!(refreshed);
    assert_eq!(
        harness.watch,
        Some(old_watch),
        "same-session refresh grew broker registrations"
    );
    harness.client = None;
    harness.service.start(link(70, 80).binding.peer).unwrap();
    harness
        .drive(
            async {
                loop {
                    let generation = routes.generation();
                    if links
                        .session(authority.primary)
                        .is_some_and(|session| session != old_session)
                    {
                        return;
                    }
                    routes.changed_after(generation).await.unwrap();
                }
            },
            false,
        )
        .await;
    assert_eq!(
        harness.drive(observed(&routes, latest.view), false).await,
        latest
    );
    assert_ne!(harness.watch, Some(old_watch));
    stale_notification(
        &mut harness,
        authority.primary,
        old_session,
        old_watch,
        latest.clone(),
    )
    .await;
    for _ in 0..100 {
        harness.pump(false);
        tokio::task::yield_now().await;
    }
    assert_eq!(routes.route(0).unwrap(), Some(latest));
    assert_eq!(links.socket_count(), 3);
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}

async fn stale_notification(
    harness: &mut Harness,
    sender: NodeId,
    old_session: LinkSessionId,
    old_watch: RequestId,
    latest: RouteState,
) {
    let mut stale = latest;
    stale.view = u64::MAX;
    stale.config_epoch += 1;
    let mut metadata = Vec::with_capacity(256);
    let header = directory::encode_update(
        Envelope {
            opcode: Opcode::StateUpdate,
            response: false,
            request_id: None,
            sender,
            session: Some(old_session),
        },
        &directory::Update {
            watch: old_watch,
            route: stale,
        },
        &mut metadata,
        limits().envelope,
        directory::Limits::default(),
    )
    .unwrap();
    harness
        .server
        .send(crate::native_frames::message(
            link(70, 80).binding.peer.as_bytes(),
            header,
            &metadata,
            Bytes::new(),
        ))
        .await
        .unwrap();
}
