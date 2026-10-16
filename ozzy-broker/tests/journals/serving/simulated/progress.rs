use super::*;
use ozzy_proto::{ConsumerGroupId, ConsumerMemberId, progress};
use ozzy_runtime::replicated::{BrokerLinkError, ProgressError};

#[tokio::test(flavor = "current_thread")]
async fn production_progress_negotiates_and_fences_unassigned_consumers_in_all_modes() {
    for policy in [
        Confirmation::LocalDurable,
        Confirmation::DiskQuorum,
        Confirmation::ReplicatedPersisting,
    ] {
        let runtime = WriterRuntime::new().unwrap();
        let root = std::path::PathBuf::from(format!("/ozzy-progress-{}", Uuid::now_v7()));
        let deployment = deployment(
            &root,
            if policy == Confirmation::LocalDurable {
                DeploymentMode::Single
            } else {
                DeploymentMode::Three
            },
            policy,
            1,
        );
        let consumer = role_links_with_config(
            &runtime,
            &deployment[0].0,
            handshake::CONSUMER,
            limits(),
            |config| {
                config.parameters.capabilities |= handshake::OWNER_PROGRESS;
                config.parameters.required_capabilities |= handshake::OWNER_PROGRESS;
                config.reader = None;
            },
        )
        .await;
        let (brokers, _) = Box::pin(start_controlled(&runtime, deployment, true)).await;
        let topic = live_many(
            &brokers,
            "progress source metadata",
            consumer.topic("orders"),
        )
        .await
        .unwrap();
        let source = topic.partition(0).unwrap();
        let routes = consumer.routes(topic.clone()).unwrap();
        routes.interest(0).unwrap();
        let route = live_many(&brokers, "progress source authority", async {
            loop {
                if let Some(route) = routes.route(0).unwrap()
                    && route.leader.is_some()
                {
                    break route;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let request = progress::Request {
            authority: ozzy_proto::append::Authority {
                group_id: source.group,
                config_epoch: source.config_epoch,
                view: route.view,
            },
            assignment: progress::Assignment {
                group: ConsumerGroupId::from_bytes([61; 16]),
                partition: source.incarnation,
                member: ConsumerMemberId::from_bytes([62; 16]),
                epoch: 1,
            },
            policy: topic.policy(),
            cursor: None,
        };
        let result = live_many(&brokers, "reject unassigned processing", async {
            loop {
                let result = consumer
                    .group_progress(route.leader.unwrap(), request)
                    .await;
                if !matches!(
                    result,
                    Err(BrokerLinkError::Rejected {
                        code: 10,
                        retry: ozzy_proto::nack::RetryClass::AfterBackoff,
                        ..
                    })
                ) {
                    break result;
                }
                // Routing can become visible before source initialization
                // finishes. That admission refusal confirms no progress.
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            matches!(
                result,
                Err(BrokerLinkError::Progress(ProgressError::Fenced))
            ),
            "{policy:?}: {result:?}"
        );
        assert_eq!(consumer.socket_count(), 2);
        consumer.shutdown().await.unwrap();
        for broker in brokers {
            broker.shutdown().await.unwrap();
        }
    }
}
