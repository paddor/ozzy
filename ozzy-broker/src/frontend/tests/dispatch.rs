use super::*;
use crate::{ApplicationShards, DevicePools};
use omq_tokio::Message;
use ozzy_proto::{
    Envelope, EnvelopeLimits, GroupId, LinkSessionId, MessageId, Opcode, PartitionIncarnation,
    ProducerId, RequestId, SubscriptionId, append, data::Authority, reader,
};
use ozzy_runtime::{
    dispatch::{self, Budget, Budgets, Class, Quota},
    frontend::{
        Binding, Dispatcher, DispatcherLimits, Kind, Placement, Rejection, ReplyLimits,
        RoutingTable, Subject,
    },
    replica_transport::QueueLimits,
};

fn budgets() -> dispatch::Limits {
    dispatch::Limits {
        capacity: Budgets {
            data: Budget {
                queue_slots: 2,
                retained_messages: 2,
                bytes: 8192,
            },
            control: Budget {
                queue_slots: 1,
                retained_messages: 1,
                bytes: 4096,
            },
        },
        clients: 1,
        grants: 2,
    }
}

fn target(shard: u32) -> Placement {
    Placement {
        shard,
        group: GroupId::from_bytes([shard as u8 + 10; 16]),
        partition: PartitionIncarnation::from_bytes([shard as u8 + 20; 16]),
    }
}

fn message(binding: Binding, target: Placement, control: bool) -> Message {
    let authority = Authority {
        group_id: target.group,
        config_epoch: 1,
        view: 2,
    };
    let envelope = Envelope {
        opcode: if control {
            Opcode::Subscribe
        } else {
            Opcode::Append
        },
        response: false,
        request_id: Some(RequestId::from_bytes([3; 16])),
        sender: binding.peer,
        session: Some(binding.session),
    };
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let header = if control {
        reader::encode_subscribe(
            envelope,
            &reader::Subscribe {
                subscription: reader::Subscription {
                    id: SubscriptionId::from_bytes([5; 16]),
                    generation: 1,
                },
                target: reader::Target::Group {
                    authority,
                    partition: target.partition,
                    owner_epoch: 1,
                },
                start: 0,
            },
            &mut metadata,
            EnvelopeLimits::default(),
        )
        .unwrap()
    } else {
        append::encode_append(
            envelope,
            append::Append {
                authority,
                partition: target.partition,
                owner_epoch: 1,
                key: append::AppendKey {
                    producer_id: ProducerId::from_bytes([4; 16]),
                    producer_epoch: 1,
                    first_sequence: 0,
                },
                policy: append::Policy::LocalDurable,
                records: &[append::Record {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([5; 16]),
                    parts: &[b"opaque payload"],
                }],
            },
            &mut metadata,
            &mut payload,
            append::DataLimits::default(),
        )
        .unwrap()
    };
    Message::multipart([
        Bytes::copy_from_slice(local().as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::from(payload),
    ])
}

type LaneSetup = (
    u32,
    dispatch::Sender<Message>,
    dispatch::Grant,
    dispatch::Grant,
    std::thread::ThreadId,
);

async fn run_shard(
    mut context: crate::ShardContext,
    binding: Binding,
    setup: tokio::sync::mpsc::Sender<LaneSetup>,
    observed: tokio::sync::mpsc::Sender<(u32, Vec<Class>)>,
    release_stalled: Arc<Semaphore>,
) -> Result<(), StartupError> {
    let id = context.plan.id;
    let (sender, mut receiver) = dispatch::channel::<Message>(budgets()).unwrap();
    let client = receiver
        .credits()
        .client(binding.session, budgets().capacity)
        .unwrap();
    let data = receiver
        .credits()
        .grant(
            &client,
            Class::Data,
            Quota {
                messages: 2,
                bytes: 8192,
            },
        )
        .unwrap();
    let control = receiver
        .credits()
        .grant(
            &client,
            Class::Control,
            Quota {
                messages: 1,
                bytes: 4096,
            },
        )
        .unwrap();
    setup
        .send((id, sender, data, control, std::thread::current().id()))
        .await
        .unwrap();
    context.ready()?;
    if id == 0 {
        release_stalled.acquire().await.unwrap().forget();
    }
    let count = if id == 0 { 3 } else { 1 };
    let mut classes = Vec::new();
    for _ in 0..count {
        receiver.ready().await;
        let work = receiver.try_recv().unwrap().unwrap();
        assert_eq!(work.session, binding.session);
        classes.push(work.class);
        let message = work.into_retained_message();
        if classes.last() == Some(&Class::Data) {
            assert_eq!(message.part_slice(3), Some(b"opaque payload".as_slice()));
        }
    }
    observed.send((id, classes)).await.unwrap();
    context.shutdown.requested().await;
    drop(client);
    Ok(())
}

async fn run_frontend(
    mut context: FrontendContext,
    binding: Binding,
    shard_threads: Vec<std::thread::ThreadId>,
    senders: Vec<(u32, dispatch::Sender<Message>)>,
    grants: Vec<(u32, dispatch::Grant, dispatch::Grant)>,
    delivered: oneshot::Sender<usize>,
) -> Result<(), StartupError> {
    assert!(!shard_threads.contains(&std::thread::current().id()));
    let routes = RoutingTable::new(
        &[0, 7],
        &[target(0), target(7)],
        2,
        EnvelopeLimits::default(),
    )
    .unwrap();
    let queue = QueueLimits {
        messages: 2,
        bytes: 8192,
        message_bytes: 4096,
    };
    let mut dispatcher = Dispatcher::new(
        context.local,
        routes,
        senders,
        DispatcherLimits {
            peers: 1,
            grants_per_class: 2,
            replies: ReplyLimits {
                control: queue,
                data: queue,
            },
        },
    )
    .unwrap();
    dispatcher.bind(binding).unwrap();
    for (id, data, control) in grants {
        dispatcher
            .install(
                binding.peer,
                Subject {
                    group: target(id).group,
                    writer: Some(ProducerId::from_bytes([4; 16])),
                },
                data,
            )
            .unwrap();
        dispatcher
            .install(
                binding.peer,
                Subject {
                    group: target(id).group,
                    writer: None,
                },
                control,
            )
            .unwrap();
    }
    context.ready()?;
    let mut rejected = 0;
    for _ in 0..5 {
        let message = tokio::select! {
            result = context.peer.recv() => result.unwrap(),
            () = context.shutdown.requested() => return Ok(()),
        };
        // Trusted inproc fixture owns bounded fresh 1 KiB frame buffers.
        // This is not a general charge for network receive allocations.
        if let Err(failure) = dispatcher.dispatch(binding.peer, message, 4096) {
            assert!(matches!(
                failure.reason,
                Rejection::Admission(dispatch::SendFailure::Admission(dispatch::Error::Full))
            ));
            rejected += 1;
        }
    }
    delivered.send(rejected).unwrap();
    context.shutdown.requested().await;
    Ok(())
}

#[tokio::test]
async fn ordinary_peer_dispatch_preserves_healthy_shard_and_reserved_control_progress() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let plan = plan();
        let (devices, lanes) = DevicePools::start(&plan).unwrap();
        let binding = Binding {
            peer: NodeId::from_bytes([1; 16]),
            session: LinkSessionId::from_bytes([2; 16]),
            kind: Kind::Client,
        };
        let (setup, mut setups) = tokio::sync::mpsc::channel(2);
        let (observed, mut observations) = tokio::sync::mpsc::channel(2);
        let release_stalled = Arc::new(Semaphore::new(0));
        let shards = ApplicationShards::start(&plan, lanes, {
            let release_stalled = release_stalled.clone();
            move |context| {
                run_shard(
                    context,
                    binding,
                    setup.clone(),
                    observed.clone(),
                    release_stalled.clone(),
                )
            }
        })
        .await
        .unwrap();
        let mut senders = Vec::new();
        let mut grants = Vec::new();
        let mut shard_threads = Vec::new();
        for _ in 0..2 {
            let (id, sender, data, control, thread) = setups.recv().await.unwrap();
            senders.push((id, sender));
            grants.push((id, data, control));
            shard_threads.push(thread);
        }
        let context = Context::new();
        let endpoints = endpoints(false);
        let (delivered, received) = oneshot::channel();
        let frontend = Frontend::start_with_context(
            &plan,
            local(),
            &endpoints,
            limits(),
            context.clone(),
            move |context| {
                run_frontend(context, binding, shard_threads, senders, grants, delivered)
            },
        )
        .await
        .unwrap();
        let sdk = context.socket(
            SocketType::Peer,
            omq_tokio::Options::default()
                .identity(Bytes::copy_from_slice(binding.peer.as_bytes()))
                .router_mandatory(true),
        );
        sdk.connect(endpoints.peer.parse().unwrap()).await.unwrap();
        sdk.wait_connected(1, Duration::from_secs(5)).await.unwrap();
        for (shard, control) in [(0, false), (0, false), (0, false), (7, false), (0, true)] {
            sdk.send(message(binding, target(shard), control))
                .await
                .unwrap();
        }
        assert_eq!(received.await.unwrap(), 1);
        assert_eq!(observations.recv().await.unwrap(), (7, vec![Class::Data]));
        release_stalled.add_permits(1);
        assert_eq!(
            observations.recv().await.unwrap(),
            (0, vec![Class::Data, Class::Data, Class::Control])
        );
        sdk.close().await.unwrap();
        frontend.shutdown().await.unwrap();
        shards.shutdown().await.unwrap();
        devices.shutdown().await;
    })
    .await
    .expect("shared broker frontend stalled");
}
