//! Real OMQ saturation tests. No replica ACK or durability is inferred from sends.

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::{Context, IdentitySocket, Message, Options, SocketType};
use ozzy_proto::{GroupId, NodeId};
use ozzy_replication::{Configuration, Digest};
use ozzy_runtime::replica_transport::{
    EnqueueError, OutboxError, QueueLimits, ReplicaOutbox, SendAttempt, SendClass,
};

const WAIT: Duration = Duration::from_secs(10);

fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}

fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [node(0), node(1), node(2)],
    )
    .unwrap()
}

fn limits() -> QueueLimits {
    QueueLimits {
        messages: 2,
        bytes: 128,
        message_bytes: 64,
    }
}

fn outbox() -> ReplicaOutbox {
    ReplicaOutbox::new(configuration(), node(0), limits(), limits()).unwrap()
}

fn message(value: u8) -> Message {
    Message::multipart([Bytes::from(vec![value]), Bytes::new(), Bytes::new()])
}

fn enqueue(outbox: &mut ReplicaOutbox, peer: u8, class: SendClass, value: u8) {
    outbox
        .try_enqueue(node(peer), class, message(value))
        .unwrap();
}

#[test]
fn admission_reserves_counts_and_bytes_independently_without_consuming_rejections() {
    let mut outbox = outbox();
    for class in [SendClass::Control, SendClass::Data] {
        for peer in [1, 2] {
            enqueue(&mut outbox, peer, class, 42);
            enqueue(&mut outbox, peer, class, 43);
            let (error, message) = outbox
                .try_enqueue(node(peer), class, message(44))
                .unwrap_err();
            assert_eq!(error, EnqueueError::Full);
            assert_eq!(message.len(), 3);
            assert_eq!(message.part_bytes(0).unwrap().as_ref(), &[44]);
            assert_eq!(outbox.queued(node(peer), class), Some((2, 34)));
        }
    }
    let byte_bound = QueueLimits {
        messages: 8,
        bytes: 34,
        message_bytes: 34,
    };
    let mut outbox = ReplicaOutbox::new(configuration(), node(0), byte_bound, byte_bound).unwrap();
    enqueue(&mut outbox, 1, SendClass::Data, 1);
    enqueue(&mut outbox, 1, SendClass::Data, 2);
    assert_eq!(
        outbox
            .try_enqueue(node(1), SendClass::Data, message(3))
            .unwrap_err()
            .0,
        EnqueueError::Full
    );
    assert_eq!(outbox.queued(node(1), SendClass::Data), Some((2, 34)));
    enqueue(&mut outbox, 1, SendClass::Control, 4);
    enqueue(&mut outbox, 2, SendClass::Data, 5);
}

#[test]
fn flow_slots_coalesce_without_consuming_durable_control_or_data_capacity() {
    let mut outbox = outbox();
    for class in [SendClass::Control, SendClass::Data] {
        enqueue(&mut outbox, 1, class, 1);
        enqueue(&mut outbox, 1, class, 2);
    }
    for class in [SendClass::Receipt, SendClass::Exchange] {
        for value in 0..100 {
            enqueue(&mut outbox, 1, class, value);
            assert_eq!(outbox.queued(node(1), class), Some((1, 17)));
        }
        let oversized = Message::multipart([Bytes::from(vec![0; 49]), Bytes::new(), Bytes::new()]);
        assert_eq!(
            outbox.try_enqueue(node(1), class, oversized).unwrap_err().0,
            EnqueueError::Size
        );
        assert_eq!(outbox.queued(node(1), class), Some((1, 17)));
    }
    assert_eq!(outbox.queued(node(1), SendClass::Control), Some((2, 34)));
    assert_eq!(outbox.queued(node(1), SendClass::Data), Some((2, 34)));
    assert_eq!(outbox.discard(node(1), SendClass::Data), Some((2, 34)));
    assert_eq!(outbox.queued(node(1), SendClass::Control), Some((2, 34)));
    assert_eq!(outbox.queued(node(1), SendClass::Receipt), Some((1, 17)));
    assert_eq!(outbox.discard(node(9), SendClass::Data), None);
}

#[test]
fn invalid_configuration_destination_frames_and_size_leave_queues_unchanged() {
    for bad in [
        QueueLimits {
            messages: 0,
            ..limits()
        },
        QueueLimits {
            bytes: 1,
            ..limits()
        },
        QueueLimits {
            message_bytes: 15,
            ..limits()
        },
        QueueLimits {
            bytes: usize::MAX,
            message_bytes: usize::MAX,
            ..limits()
        },
    ] {
        assert!(matches!(
            ReplicaOutbox::new(configuration(), node(0), bad, limits()),
            Err(OutboxError::Limits)
        ));
    }
    assert!(matches!(
        ReplicaOutbox::new(configuration(), node(9), limits(), limits()),
        Err(OutboxError::LocalVoter)
    ));
    let mut outbox = outbox();
    for peer in [0, 9] {
        assert_eq!(
            outbox
                .try_enqueue(node(peer), SendClass::Data, message(1))
                .unwrap_err()
                .0,
            EnqueueError::Peer
        );
    }
    assert_eq!(
        outbox
            .try_enqueue(node(1), SendClass::Data, Message::from_slice(b"x"))
            .unwrap_err()
            .0,
        EnqueueError::Frames
    );
    let oversized = Message::multipart([Bytes::from(vec![0; 49]), Bytes::new(), Bytes::new()]);
    let (error, returned) = outbox
        .try_enqueue(node(1), SendClass::Data, oversized)
        .unwrap_err();
    assert_eq!(error, EnqueueError::Size);
    assert_eq!(returned.part_bytes(0).unwrap().len(), 49);
    assert!(!outbox.has_pending());
}

fn socket(context: &Context, index: u8) -> IdentitySocket {
    context
        .socket(
            SocketType::Peer,
            Options::default()
                .identity(Bytes::copy_from_slice(node(index).as_bytes()))
                .router_mandatory(true)
                .send_hwm(2)
                .recv_hwm(2)
                .linger(Duration::ZERO),
        )
        .identity_routing()
        .unwrap()
}

async fn connected(context: &Context) -> [IdentitySocket; 3] {
    let sockets = [socket(context, 0), socket(context, 1), socket(context, 2)];
    for other in &sockets[1..] {
        let endpoint = other
            .bind(
                format!("inproc://outbox-{}", NodeId::new())
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();
        sockets[0].connect(endpoint).await.unwrap();
    }
    sockets[0].wait_connected(2, WAIT).await.unwrap();
    for other in &sockets[1..] {
        other.wait_connected(1, WAIT).await.unwrap();
    }
    sockets
}

async fn receive(socket: &IdentitySocket) -> u8 {
    let mut message = socket.recv().await.unwrap();
    assert_eq!(message.pop_front().unwrap().as_ref(), node(0).as_bytes());
    assert_eq!(message.len(), 3);
    message.part_bytes(0).unwrap()[0]
}

#[tokio::test(flavor = "current_thread")]
async fn flush_has_a_fixed_budget_and_control_precedes_queued_data() {
    tokio::time::timeout(WAIT, async {
        let context = Context::current();
        let sockets = connected(&context).await;
        let mut outbox = outbox();
        for peer in [1, 2] {
            enqueue(&mut outbox, peer, SendClass::Data, 1);
            enqueue(&mut outbox, peer, SendClass::Data, 2);
            enqueue(&mut outbox, peer, SendClass::Control, 3);
        }
        let progress = outbox.flush(&sockets[0]).unwrap();
        for peer in progress.0 {
            assert_eq!(peer.control, SendAttempt::Submitted);
            // OMQ may fill before data; never more than one data submission.
            assert!(matches!(
                peer.data,
                SendAttempt::Submitted | SendAttempt::Blocked
            ));
            assert!(outbox.queued(peer.peer, SendClass::Data).unwrap().0 >= 1);
        }
        for other in &sockets[1..] {
            assert_eq!(receive(other).await, 3);
        }
        let ((), a, b) = tokio::join!(
            async {
                while outbox.has_pending() {
                    outbox.flush_ready(&sockets[0]).await.unwrap();
                }
            },
            async { [receive(&sockets[1]).await, receive(&sockets[1]).await] },
            async { [receive(&sockets[2]).await, receive(&sockets[2]).await] }
        );
        assert_eq!(a, [1, 2]);
        assert_eq!(b, [1, 2]);
    })
    .await
    .unwrap();
}

async fn saturate(outbox: &mut ReplicaOutbox, sender: &IdentitySocket) {
    // Bounded setup. Never read voter 1, eventually filling its receive/send pipes.
    let mut submitted = 0;
    // PEER inproc also has a fixed 1024-message transport queue plus actor buffers.
    for _ in 0..4096 {
        if outbox.queued(node(1), SendClass::Data).unwrap().0 == 0 {
            enqueue(outbox, 1, SendClass::Data, 10);
        }
        if outbox.flush(sender).unwrap().0[0].data == SendAttempt::Submitted {
            submitted += 1;
        }
        tokio::task::yield_now().await;
        if outbox.flush(sender).unwrap().0[0].data == SendAttempt::Blocked {
            // Wait for all already admitted pipe work to reach stable backpressure.
            tokio::time::sleep(Duration::from_millis(10)).await;
            if outbox.flush(sender).unwrap().0[0].data == SendAttempt::Blocked {
                return;
            }
        }
    }
    panic!("fixture failed to establish OMQ HWM backpressure: {submitted} submitted");
}

#[tokio::test(flavor = "current_thread")]
async fn saturated_voter_does_not_block_other_voter_timers_or_completions() {
    tokio::time::timeout(WAIT, async {
        let context = Context::current();
        let sockets = connected(&context).await;
        let mut outbox = outbox();
        saturate(&mut outbox, &sockets[0]).await;
        enqueue(&mut outbox, 1, SendClass::Control, 11);
        enqueue(&mut outbox, 1, SendClass::Data, 12);
        for value in 20..40 {
            enqueue(&mut outbox, 2, SendClass::Control, value);
            let progress = outbox.flush(&sockets[0]).unwrap();
            assert_eq!(progress.0[0].data, SendAttempt::Blocked);
            assert_eq!(progress.0[1].control, SendAttempt::Submitted);
            assert_eq!(receive(&sockets[2]).await, value);
        }
        let (done, completed) = tokio::sync::oneshot::channel();
        let completion = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            done.send(()).unwrap();
        });
        let ((), ()) = tokio::join!(async {
            tokio::select! {
                result = outbox.flush_ready(&sockets[0]) => panic!("saturated send completed: {result:?}"),
                () = async { completed.await.unwrap(); } => {}
            }
        }, async { tokio::time::sleep(Duration::from_millis(5)).await; });
        completion.await.unwrap();
        assert_eq!(outbox.queued(node(1), SendClass::Control), Some((1, 17)));
        assert_eq!(outbox.queued(node(1), SendClass::Data), Some((2, 34)));
        // Cancellation retained messages. Resume draining the formerly slow peer.
        let ((), ()) = tokio::join!(async {
            while outbox.has_pending() { outbox.flush_ready(&sockets[0]).await.unwrap(); }
        }, async {
            let mut control_received = false;
            for _ in 0..4096 {
                match receive(&sockets[1]).await {
                    10 => {}
                    11 => { assert!(!control_received); control_received = true; }
                    12 => { assert!(control_received); return; }
                    value => panic!("unexpected retained message {value}"),
                }
            }
            panic!("retained control or data was lost");
        });
        assert!(!outbox.has_pending());
    }).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn absent_voter_reports_unroutable_without_discarding_healthy_peer_work() {
    tokio::time::timeout(WAIT, async {
        let context = Context::current();
        let sender = socket(&context, 0);
        let healthy = socket(&context, 2);
        let endpoint = healthy
            .bind(
                format!("inproc://outbox-{}", NodeId::new())
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();
        sender.connect(endpoint).await.unwrap();
        sender.wait_connected(1, WAIT).await.unwrap();
        let mut outbox = outbox();
        enqueue(&mut outbox, 1, SendClass::Control, 1);
        enqueue(&mut outbox, 2, SendClass::Control, 2);
        let progress = outbox.flush_ready(&sender).await.unwrap();
        assert_eq!(progress.0[0].control, SendAttempt::Unroutable);
        assert_eq!(progress.0[1].control, SendAttempt::Submitted);
        assert_eq!(receive(&healthy).await, 2);
        assert!(!outbox.has_pending());
    })
    .await
    .unwrap();
}
