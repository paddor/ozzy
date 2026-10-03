use super::*;
use ozzy_config::{Affinity, Deployment, HostResources, IoBackend, QueueBudget, Shard};
use std::{
    rc::Rc,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::sync::Semaphore;

mod dispatch;
mod sessions;

fn plan() -> BrokerPlan {
    let mut deployment = Deployment::parse(include_str!(
        "../../../ozzy-config/tests/fixtures/single.toml"
    ))
    .unwrap();
    let broker = deployment.brokers.get_mut("laptop").unwrap();
    broker.devices.get_mut("ssd").unwrap().workers.backend = IoBackend::Pool;
    broker.topology.shards = [0, 7]
        .into_iter()
        .map(|id| Shard {
            id,
            device: "ssd".into(),
            affinity: Affinity::default(),
            budget: QueueBudget::default(),
        })
        .collect();
    deployment
        .validate()
        .unwrap()
        .broker_plan(
            "laptop",
            &HostResources {
                cpus: [(0, Some(0))].into(),
                memory_nodes: [0].into(),
                linux_aio: true,
            },
        )
        .unwrap()
}

fn endpoints(follower: bool) -> Endpoints {
    let name = ozzy_proto::RequestId::new();
    Endpoints {
        peer: format!("inproc://{name}-peer"),
        data_peer: format!("inproc://{name}-data"),
        reader_pub: format!("inproc://{name}-reader"),
        follower_pub: follower.then(|| format!("inproc://{name}-follower")),
    }
}

fn limits() -> TransportLimits {
    TransportLimits {
        send_messages: 8,
        receive_messages: 8,
        message_bytes: 4096,
        close_linger: Duration::from_millis(5),
    }
}

fn local() -> NodeId {
    NodeId::from_bytes([9; 16])
}

async fn serve(mut context: FrontendContext) -> Result<(), StartupError> {
    context.ready()?;
    context.shutdown.requested().await;
    Ok(())
}

async fn rebound(context: &Context, endpoints: &Endpoints) {
    let sockets = Sockets::bind(
        context,
        local(),
        Bindings::parse(endpoints).unwrap(),
        limits(),
    )
    .await
    .expect("frontend left a bound endpoint behind");
    sockets.close().await.unwrap();
}

#[tokio::test]
async fn owned_dispatcher_runs_non_send_state_and_closes_all_shared_sockets() {
    let main = std::thread::current().id();
    let context = Context::new();
    let endpoints = endpoints(true);
    let (tx, rx) = oneshot::channel();
    let frontend = Frontend::start_with_context(
        &plan(),
        local(),
        &endpoints,
        limits(),
        context.clone(),
        move |mut context| {
            assert_ne!(std::thread::current().id(), main);
            assert_eq!(std::thread::current().name(), Some("ozzy_dispatch"));
            assert_eq!(
                tokio::runtime::Handle::current().runtime_flavor(),
                tokio::runtime::RuntimeFlavor::CurrentThread
            );
            let state = Rc::new(std::cell::Cell::new(0));
            async move {
                tx.send((
                    context.peer.clone(),
                    context.reader_pub.clone(),
                    context.follower_pub.clone().unwrap(),
                ))
                .unwrap();
                let task = tokio::task::spawn_local({
                    let state = state.clone();
                    async move {
                        state.set(1);
                    }
                });
                task.await.unwrap();
                context.ready()?;
                context.shutdown.requested().await;
                assert_eq!(state.get(), 1);
                Ok(())
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(frontend.dispatcher_threads(), 1);
    assert_eq!(frontend.io_threads(), 1);
    let (peer, reader, follower) = rx.await.unwrap();
    frontend.shutdown().await.unwrap();
    assert!(peer.recv().await.is_err());
    for socket in [reader, follower] {
        assert!(matches!(
            socket.bind("inproc://closed-clone".parse().unwrap()).await,
            Err(omq_tokio::Error::Closed)
        ));
    }
    rebound(&context, &endpoints).await;
}

#[tokio::test]
async fn failed_bind_closes_previously_bound_endpoints_before_returning() {
    for occupied in [false, true] {
        let context = Context::new();
        let endpoints = endpoints(true);
        let blocker = context.socket(SocketType::Pub, omq_tokio::Options::default());
        let endpoint = if occupied {
            endpoints.follower_pub.as_ref().unwrap()
        } else {
            &endpoints.reader_pub
        };
        blocker.bind(endpoint.parse().unwrap()).await.unwrap();
        let result = Frontend::start_with_context(
            &plan(),
            local(),
            &endpoints,
            limits(),
            context.clone(),
            |_| async {
                panic!("factory must not run after a failed bind");
            },
        )
        .await;
        assert!(result.is_err());
        blocker.close().await.unwrap();
        rebound(&context, &endpoints).await;
    }
}

#[tokio::test]
async fn startup_error_and_synchronous_factory_panic_close_bound_sockets() {
    for panic in [false, true] {
        let context = Context::new();
        let endpoints = endpoints(true);
        let result = Frontend::start_with_context(
            &plan(),
            local(),
            &endpoints,
            limits(),
            context.clone(),
            move |_| {
                assert!(!panic, "injected factory panic");
                async { Err(error("injected startup failure")) }
            },
        )
        .await
        .unwrap_err();
        let reason = result.to_string();
        assert!(
            reason.contains(if panic {
                "panicked"
            } else {
                "injected startup failure"
            }),
            "{reason}"
        );
        rebound(&context, &endpoints).await;
    }
}

#[tokio::test]
async fn canceled_shutdown_observation_preserves_drain_and_error() {
    let finish = Arc::new(Semaphore::new(0));
    let frontend = Frontend::start(&plan(), local(), &endpoints(false), limits(), {
        let finish = finish.clone();
        move |mut context| async move {
            context.ready()?;
            context.shutdown.requested().await;
            finish.acquire().await.unwrap().forget();
            Err(error("injected drain failure"))
        }
    })
    .await
    .unwrap();
    let mut shutdown = Box::pin(frontend.shutdown());
    assert!(futures::poll!(&mut shutdown).is_pending());
    drop(shutdown);
    finish.add_permits(1);
    assert!(
        frontend
            .closed()
            .await
            .unwrap_err()
            .to_string()
            .contains("injected drain failure")
    );
}

#[tokio::test]
async fn canceled_startup_and_drop_request_shutdown_without_waiting() {
    for ready in [false, true] {
        let plan = plan();
        let endpoints = endpoints(false);
        let context = Context::new();
        let (entered, observed) = oneshot::channel();
        let finish = Arc::new(Semaphore::new(0));
        let completed = Arc::new(AtomicBool::new(false));
        let mut startup = Box::pin(Frontend::start_with_context(
            &plan,
            local(),
            &endpoints,
            limits(),
            context.clone(),
            {
                let finish = finish.clone();
                let completed = completed.clone();
                move |mut context| async move {
                    entered.send(()).unwrap();
                    if ready {
                        context.ready()?;
                    }
                    context.shutdown.requested().await;
                    finish.acquire().await.unwrap().forget();
                    completed.store(true, Ordering::Release);
                    Ok(())
                }
            },
        ));
        assert!(futures::poll!(&mut startup).is_pending());
        observed.await.unwrap();
        let frontend = if ready {
            Some(startup.as_mut().await.unwrap())
        } else {
            None
        };
        let state = frontend.as_ref().map(|frontend| frontend.state.clone());
        // Neither drop may wait for the blocked drain on its worker.
        drop(startup);
        drop(frontend);
        assert!(!completed.load(Ordering::Acquire));
        finish.add_permits(1);
        if let Some(state) = state {
            state.finished.requested().await;
            state.result().unwrap();
        } else {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !completed.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    }
}

#[tokio::test]
async fn validation_rejects_borrowed_runtime_and_bad_placement_before_factory() {
    let plan = plan();
    let endpoints = endpoints(false);
    assert!(
        Frontend::start_with_context(
            &plan,
            local(),
            &endpoints,
            limits(),
            Context::current(),
            serve
        )
        .await
        .is_err()
    );
    let mut plan = plan;
    plan.dispatcher = Affinity {
        cpu: Some(u32::MAX),
        numa_node: None,
    };
    assert!(
        Frontend::start(&plan, local(), &endpoints, limits(), serve)
            .await
            .is_err()
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn dispatcher_affinity_precedes_factory_and_leaves_coordinator_unchanged() {
    use rustix::thread::{CpuSet, sched_getaffinity};
    let inherited = sched_getaffinity(None).unwrap();
    let cpu = (0..CpuSet::MAX_CPU)
        .find(|&cpu| inherited.is_set(cpu))
        .unwrap();
    let mut plan = plan();
    plan.dispatcher.cpu = Some(cpu as u32);
    let frontend = Frontend::start(
        &plan,
        local(),
        &endpoints(false),
        limits(),
        move |context| {
            let actual = sched_getaffinity(None).unwrap();
            assert_eq!(
                (0..CpuSet::MAX_CPU)
                    .filter(|&cpu| actual.is_set(cpu))
                    .collect::<Vec<_>>(),
                [cpu]
            );
            serve(context)
        },
    )
    .await
    .unwrap();
    assert_eq!(sched_getaffinity(None).unwrap(), inherited);
    frontend.shutdown().await.unwrap();
}
