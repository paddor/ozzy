use super::*;
use crate::frontend::{Placement, data_channel, test_support::*};
use ozzy_proto::EnvelopeLimits;

pub(in crate::frontend) fn fixture() -> (
    Dispatcher,
    [crate::frontend::DataReceiver; 2],
    [Placement; 2],
    Binding,
) {
    let placements = [placement(0, 0), placement(1, 17)];
    let table = RoutingTable::new(&[0, 17], &placements, 2, EnvelopeLimits::default()).unwrap();
    let (first, a) = data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        Class::Control,
        1,
        4096,
        8192,
    )
    .unwrap();
    let (second, b) = data_channel(
        &omq_tokio::Context::new(),
        17,
        Kind::Client,
        Class::Control,
        1,
        4096,
        8192,
    )
    .unwrap();
    let mut dispatcher = Dispatcher::new(
        NodeId::from_bytes([9; 16]),
        table,
        vec![(0, first), (17, second)],
        DispatcherLimits {
            peers: 2,

            replies: ReplyLimits {
                control: crate::replica_transport::QueueLimits {
                    messages: 2,
                    bytes: 8192,
                    message_bytes: 4096,
                },
                data: crate::replica_transport::QueueLimits {
                    messages: 2,
                    bytes: 8192,
                    message_bytes: 4096,
                },
            },
        },
    )
    .unwrap();
    let binding = binding(Kind::Client);
    dispatcher.bind(binding).unwrap();
    (dispatcher, [a, b], placements, binding)
}

#[test]
fn direct_producer_queue_returns_full_frame_without_a_grant() {
    let (mut dispatcher, _old_lanes, placements, binding) = fixture();
    let (first, mut first_rx) = data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        crate::dispatch::Class::Data,
        1,
        4096,
        8192,
    )
    .unwrap();
    let (second, mut second_rx) = data_channel(
        &omq_tokio::Context::new(),
        17,
        Kind::Client,
        crate::dispatch::Class::Data,
        1,
        4096,
        8192,
    )
    .unwrap();
    dispatcher.install_data_lane(0, first).unwrap();
    dispatcher.install_data_lane(17, second).unwrap();
    dispatcher
        .dispatch_data(binding.peer, append(placements[0], binding), 4096)
        .unwrap();
    let original = append(placements[0], binding);
    let rejected = dispatcher
        .dispatch_data(binding.peer, original.clone(), 4096)
        .unwrap_err();
    assert!(matches!(
        rejected.reason,
        Rejection::Data(DataPressure::Full { shard: 0, .. })
    ));
    assert_eq!(rejected.message.part_slice(3), original.part_slice(3));
    dispatcher
        .dispatch_data(binding.peer, append(placements[1], binding), 4096)
        .unwrap();
    assert!(second_rx.try_recv().unwrap().is_some());
    assert!(first_rx.try_recv().unwrap().is_some());
    dispatcher
        .dispatch_data(binding.peer, rejected.message, 4096)
        .unwrap();
    assert!(first_rx.try_recv().unwrap().is_some());
}
