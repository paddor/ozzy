//! Routing hints fence unanswered subscriptions without waiting for their timeout.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn buffered_publications_are_fenced_after_route_changes_and_sdk_shutdown() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for shutdown in [false, true] {
            buffered_source_change(shutdown).await;
        }
    })
    .await
    .unwrap();
}

async fn buffered_source_change(shutdown: bool) {
    let (mut harness, links, clock, authority) =
        setup_shared_profile(&[], 1, Some((8, 8192, 4))).await;
    harness.publish = false;
    let metadata = harness.drive(links.topic("orders"), true).await.unwrap();
    let routes = links.routes(metadata).unwrap();
    let mut reader = harness
        .drive(
            TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
            true,
        )
        .await
        .unwrap();
    wake::wait_for_request(&mut harness, &mut reader, Opcode::Subscribe).await;
    let source = reader::Source::Group {
        authority: Authority {
            group_id: harness.groups[0],
            ..authority.authority
        },
        partition: multiple::incarnation(0),
        owner_epoch: 1,
    };
    links.inject_reader_publication(
        authority.primary,
        &publication(
            authority.primary,
            source,
            0,
            &[(50, 0), (51, 1), (52, 2), (53, 3)],
            harness.wire,
        ),
    );
    for offset in 0..3 {
        let record = harness.drive(reader.next(), true).await.unwrap();
        assert_eq!(record.offset, Offset::new(offset));
        drop(record);
    }
    if shutdown {
        harness.drive(links.shutdown(), true).await.unwrap();
        assert!(reader.next().await.is_err());
    } else {
        let mut route = routes.route(0).unwrap().unwrap();
        route.view += 1;
        route.leader = None;
        harness.service.publish_route(&route).unwrap();
        harness
            .drive(
                async {
                    loop {
                        let seen = routes.generation();
                        if routes
                            .route(0)
                            .unwrap()
                            .is_some_and(|r| r.view == route.view)
                        {
                            break;
                        }
                        routes.changed_after(seen).await.unwrap();
                    }
                },
                true,
            )
            .await;
        assert!(futures::poll!(pin!(reader.next())).is_pending());
        harness.drive(reader.close(), true).await.unwrap();
        harness.drive(links.shutdown(), true).await.unwrap();
    }
    assert_eq!(clock.now(), Duration::ZERO);
    assert_eq!(reader.checkpoint().positions, vec![(0, Offset::new(3))]);
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn changed_sources_replace_unanswered_opens_with_exhausted_control_slots() {
    tokio::time::timeout(Duration::from_secs(10), changed_sources())
        .await
        .unwrap();
}

async fn unanswered() -> (
    Harness,
    BrokerLinks,
    SdkClock,
    Vec<TopicReader>,
    Vec<ozzy_proto::directory::RouteState>,
) {
    let (mut harness, links, clock, _) = setup_shared_profile(&[], 2, Some((8, 8192, 4))).await;
    let metadata = harness.drive(links.topic("orders"), true).await.unwrap();
    let routes = links.routes(metadata).unwrap();
    for number in 0..2 {
        routes.interest(number).unwrap();
    }
    let initial = harness
        .drive(
            async {
                loop {
                    let seen = routes.generation();
                    let known = (0..2)
                        .filter_map(|number| routes.route(number).unwrap())
                        .collect::<Vec<_>>();
                    if known.len() == 2 {
                        return known;
                    }
                    routes.changed_after(seen).await.unwrap();
                }
            },
            true,
        )
        .await;
    harness.readers.drop_subscribed = true;
    let mut readers = Vec::new();
    for wanted in [2, 4] {
        let mut reader = harness
            .drive(
                TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
                true,
            )
            .await
            .unwrap();
        {
            let mut next = pin!(reader.next());
            for _ in 0..10000 {
                harness.pump(true);
                assert!(futures::poll!(next.as_mut()).is_pending());
                if harness.readers.requests.len() == wanted {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        assert_eq!(harness.readers.requests.len(), wanted);
        readers.push(reader);
    }
    (harness, links, clock, readers, initial)
}

async fn changed_sources() {
    let (mut harness, links, clock, mut readers, initial) = unanswered().await;
    let original = harness.readers.requests.clone();
    // These hints do not grant replication authority. The SDK must abandon its
    // old opening and try the advertised source even if the broker rejects it.
    for mut route in initial {
        route.view += 1;
        harness.service.publish_route(&route).unwrap();
    }
    for _ in 0..10000 {
        harness.pump(true);
        for reader in &mut readers {
            let mut next = pin!(reader.next());
            assert!(futures::poll!(next.as_mut()).is_pending());
        }
        if harness.readers.requests.len() >= 8 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(clock.now(), Duration::ZERO);
    assert_eq!(
        harness.readers.requests.len(),
        8,
        "all four control slots must be reclaimed before timeout"
    );
    for (_, old) in original {
        assert!(harness.readers.requests[4..].iter().any(|&(opcode, new)| {
            opcode == Opcode::Subscribe && new.id == old.id && new.generation != old.generation
        }));
    }
    for reader in &readers {
        assert!(
            reader
                .checkpoint()
                .positions
                .iter()
                .all(|&(_, next)| next == Offset::ZERO)
        );
    }
    harness.drive(links.shutdown(), true).await.unwrap();
    for reader in &mut readers {
        let _ = harness.drive(reader.close(), true).await;
    }
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn close_reclaims_canceled_opens_with_exhausted_control_slots() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut harness, links, clock, mut readers, _) = unanswered().await;
        let original = harness.readers.requests.clone();
        for reader in &mut readers {
            drop(reader.close());
        }
        let results = harness
            .drive(
                futures::future::join_all(readers.iter_mut().map(TopicReader::close)),
                true,
            )
            .await;
        for result in results {
            result.unwrap();
        }
        assert_eq!(clock.now(), Duration::ZERO);
        assert_eq!(harness.readers.requests.len(), 8);
        for (_, subscription) in original {
            assert!(harness.readers.requests[4..].contains(&(Opcode::Unsubscribe, subscription)));
        }
        harness.drive(links.shutdown(), true).await.unwrap();
        harness.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn reader_subscribes_when_a_route_names_the_new_leader() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut harness, links, clock, _) = setup_shared_profile(&[], 1, Some((8, 8192, 4))).await;
        let metadata = harness.drive(links.topic("orders"), true).await.unwrap();
        let routes = links.routes(metadata).unwrap();
        routes.interest(0).unwrap();
        let initial = harness
            .drive(
                async {
                    loop {
                        let seen = routes.generation();
                        if let Some(route) = routes.route(0).unwrap() {
                            return route;
                        }
                        routes.changed_after(seen).await.unwrap();
                    }
                },
                true,
            )
            .await;
        harness.readers.drop_subscribed = true;
        let mut reader = harness
            .drive(
                TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
                true,
            )
            .await
            .unwrap();
        let subscribes = |harness: &Harness| {
            harness
                .readers
                .requests
                .iter()
                .filter(|(opcode, _)| *opcode == Opcode::Subscribe)
                .count()
        };
        let observe = async |harness: &mut Harness, reader: &mut TopicReader, wanted| {
            for _ in 0..10000 {
                harness.pump(true);
                assert!(futures::poll!(pin!(reader.next())).is_pending());
                if wanted == Some(subscribes(harness)) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        observe(&mut harness, &mut reader, Some(1)).await;
        assert_eq!(subscribes(&harness), 1);
        // The group changes its leader. No broker leads the next view yet.
        let leader = initial.leader;
        assert!(leader.is_some());
        let mut route = initial;
        route.view += 1;
        route.leader = None;
        harness.service.publish_route(&route).unwrap();
        observe(&mut harness, &mut reader, None).await;
        assert_eq!(subscribes(&harness), 1);
        // The new leader is ready. The reader must not wait for its refresh.
        route.leader = leader;
        harness.service.publish_route(&route).unwrap();
        observe(&mut harness, &mut reader, Some(2)).await;
        assert_eq!(clock.now(), Duration::ZERO);
        assert_eq!(
            subscribes(&harness),
            2,
            "the reader waited for its refresh interval"
        );
        harness.drive(links.shutdown(), true).await.unwrap();
        let _ = harness.drive(reader.close(), true).await;
        harness.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn refused_subscription_is_sent_again_at_the_request_retry_interval() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut harness, links, clock, _) = setup_shared_profile(&[], 1, Some((8, 8192, 4))).await;
        harness.readers.refuse_subscribes = 1;
        harness.readers.drop_subscribed = true;
        // The refresh interval is ten times the 100 ms request retry interval.
        let mut reader = harness
            .drive(
                TopicReader::open(
                    links.clone(),
                    "orders",
                    TopicReaderConfig {
                        refresh: Duration::from_secs(1),
                        ..TopicReaderConfig::default()
                    },
                ),
                true,
            )
            .await
            .unwrap();
        let mut refused_at = None;
        for _ in 0..3000 {
            harness.pump(true);
            assert!(futures::poll!(pin!(reader.next())).is_pending());
            if refused_at.is_none() && harness.readers.refuse_subscribes == 0 {
                refused_at = Some(clock.now());
            }
            if harness.readers.requests.len() == 2 {
                break;
            }
            clock
                .advance(clock.now().saturating_add(Duration::from_millis(1)))
                .unwrap();
            tokio::task::yield_now().await;
        }
        assert_eq!(harness.readers.requests.len(), 2, "no second subscription");
        let waited = clock.now().saturating_sub(refused_at.unwrap());
        assert!(
            waited <= Duration::from_millis(150),
            "the refused subscription waited {waited:?}"
        );
        harness.drive(links.shutdown(), true).await.unwrap();
        let _ = harness.drive(reader.close(), true).await;
        harness.shutdown().await;
    })
    .await
    .unwrap();
}
