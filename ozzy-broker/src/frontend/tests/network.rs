use super::*;
use omq_tokio::{ByteRateLimit, NetworkRateLimits};

fn limited_plan(ingress: bool, egress: bool) -> BrokerPlan {
    let mut plan = plan();
    plan.omq_ingress = ingress.then_some(ozzy_config::ByteRateLimit {
        bytes_per_second: 1,
        burst_bytes: 2,
    });
    plan.omq_egress = egress.then_some(ozzy_config::ByteRateLimit {
        bytes_per_second: 3,
        burst_bytes: 4,
    });
    plan
}

#[tokio::test]
async fn owned_frontend_installs_each_network_direction_before_binding() {
    for (ingress, egress) in [(true, false), (false, true), (true, true)] {
        let plan = limited_plan(ingress, egress);
        let expected = NetworkRateLimits {
            ingress: ingress.then_some(ByteRateLimit::new(1, 2)),
            egress: egress.then_some(ByteRateLimit::new(3, 4)),
        };
        let endpoints = endpoints(true);
        let frontend = Frontend::start(frontend_plan(&plan, &endpoints), move |context| {
            assert_eq!(context.omq_context().network_rate_limits(), expected);
            serve(context)
        })
        .await
        .unwrap();
        frontend.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn injected_context_keeps_matching_limits_after_other_sockets_exist() {
    let context = Context::new();
    let expected = NetworkRateLimits {
        ingress: Some(ByteRateLimit::new(1, 2)),
        egress: Some(ByteRateLimit::new(3, 4)),
    };
    context.set_network_rate_limits(expected).unwrap();
    let socket = context.socket(SocketType::Pub, omq_tokio::Options::default());
    let plan = limited_plan(true, true);
    let endpoints = endpoints(true);
    let frontend =
        Frontend::start_with_context(frontend_plan(&plan, &endpoints), context.clone(), serve)
            .await
            .unwrap();
    assert_eq!(context.network_rate_limits(), expected);
    frontend.shutdown().await.unwrap();
    socket.close().await.unwrap();
}

#[tokio::test]
async fn injected_active_context_rejects_conflicting_network_limits_without_binding() {
    for limited in [false, true] {
        let context = Context::new();
        let expected = NetworkRateLimits {
            ingress: limited.then_some(ByteRateLimit::new(9, 10)),
            egress: None,
        };
        context.set_network_rate_limits(expected).unwrap();
        let socket = context.socket(SocketType::Pub, omq_tokio::Options::default());
        let plan = limited_plan(!limited, false);
        let endpoints = endpoints(true);
        let result = Frontend::start_with_context(
            frontend_plan(&plan, &endpoints),
            context.clone(),
            |_| async { panic!("factory ran with conflicting network limits") },
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("before creating sockets")
        );
        assert_eq!(context.network_rate_limits(), expected);
        rebound(&context, &endpoints).await;
        socket.close().await.unwrap();
    }
}
