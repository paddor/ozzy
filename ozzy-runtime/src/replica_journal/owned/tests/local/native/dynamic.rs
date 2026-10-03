use super::*;
use ozzy_proto::nack::{self, RetryClass};

fn request(
    link: Link,
    authority: Authority,
    producer: u8,
    id: u8,
    partition: PartitionIncarnation,
) -> Message {
    request_epoch(link, authority, producer, id, partition, None)
}

fn request_epoch(
    link: Link,
    authority: Authority,
    producer: u8,
    id: u8,
    partition: PartitionIncarnation,
    expected_epoch: Option<u64>,
) -> Message {
    let mut metadata = Vec::with_capacity(256);
    let header = producer::encode_open(
        envelope(Opcode::OpenProducer, link, id),
        Open {
            authority,
            partition,
            producer: ProducerId::from_bytes([producer; 16]),
            mode: Mode::Resume,
            expected_epoch,
            operation: OperationId::from_bytes([id; 16]),
        },
        &mut metadata,
        limits().envelope,
    )
    .unwrap();
    crate::native_frames::message(
        link.binding.peer.as_bytes(),
        header,
        &metadata,
        Bytes::new(),
    )
}

fn rejected(output: &mut Vec<Message>, code: u16, retry: RetryClass) {
    assert_eq!(output.len(), 1);
    let message = output.pop().unwrap();
    let decoded = nack::decode(packet(&message), limits().envelope).unwrap();
    assert_eq!(decoded.code, code);
    assert_eq!(decoded.retry, retry);
}

#[test]
#[allow(clippy::too_many_lines)]
fn dynamic_writers_keep_trust_capacity_and_old_io_ownership_separate() {
    scenario(
        NativeAccess::Clients {
            peers: vec![link(70, 80).binding.peer],
            writers: 1,
        },
        false,
    );
}

#[test]
fn trusted_clients_keep_writer_capacity_and_old_io_ownership_separate() {
    scenario(
        NativeAccess::TrustedClients {
            clients: 1,
            writers: 1,
        },
        true,
    );
}

#[test]
fn idle_writer_arenas_reuse_live_sessions_without_losing_retry_history() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut intake = intake_access(
        &mut actor,
        NativeAccess::TrustedClients {
            clients: 1,
            writers: 1,
        },
        partition(),
    );
    let link = link(70, 80);
    let hint = actor.authority_hint();
    let mut output = Vec::new();
    let invalid = open(hint.authority, link, Mode::Fence, Some(99), 42);
    intake.receive(&invalid, link, hint).unwrap();
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    rejected(&mut output, 6, RetryClass::Permanent);
    assert_eq!(actor.snapshot().applied.op.0, 3);
    for (writer, id, sequence) in [(50, 51, 0), (60, 61, 0), (50, 52, 1)] {
        let opening = request_epoch(
            link,
            hint.authority,
            writer,
            id,
            partition(),
            (sequence != 0).then_some(1),
        );
        assert_eq!(
            intake.receive(&opening, link, hint).unwrap(),
            NativeReceive::Accepted
        );
        settle(&mut controller, &mut actor, &mut intake, &mut output);
        assert_eq!(output.len(), 1);
        let message = output.pop().unwrap();
        let decoded = packet(&message);
        assert_eq!(
            decoded.envelope.opcode,
            Opcode::ProducerOpened,
            "writer {writer}: {:?}",
            nack::decode(decoded, limits().envelope),
        );
        let opened = producer::decode_opened(decoded, limits().envelope).unwrap();
        assert_eq!(opened.producer, ProducerId::from_bytes([writer; 16]));
        assert_eq!(opened.epoch, 1);
        assert_eq!(opened.next_sequence, sequence);
        let retry = append(hint.authority, link, writer, 0, 1);
        assert_eq!(
            intake.receive(&retry, link, hint).unwrap(),
            NativeReceive::Accepted
        );
        settle(&mut controller, &mut actor, &mut intake, &mut output);
        assert_eq!(output.len(), 1);
        let message = output.pop().unwrap();
        let confirmed =
            append::stream::decode_confirmed(packet(&message), limits().envelope).unwrap();
        assert_eq!(confirmed.key.first_sequence, 0);
        assert_eq!(confirmed.first_offset, u64::from(writer == 60));
    }
    drop(intake);
    close(&mut controller, actor);
}

#[allow(clippy::too_many_lines)]
fn scenario(access: NativeAccess, trusted: bool) {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut intake = intake_access(&mut actor, access, partition());
    let hint = actor.authority_hint();
    let old = link(70, 80);
    let current = link(70, 90);
    let mut output = Vec::new();
    let untrusted = request(link(71, 81), hint.authority, 50, 51, partition());
    let mut unauthorized = link(71, 81);
    if trusted {
        unauthorized.binding.kind = Kind::Broker;
    }
    assert_eq!(
        intake.receive(&untrusted, unauthorized, hint).unwrap(),
        NativeReceive::Ignored
    );
    let mut consumer = old;
    consumer.remote.roles = handshake::CONSUMER;
    let denied = request(consumer, hint.authority, 50, 51, partition());
    assert_eq!(
        intake.receive(&denied, consumer, hint).unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..10 {
        progress(&mut actor, &mut intake, None, &mut output, false);
    }
    rejected(&mut output, 3, RetryClass::Permanent);
    let wrong = request(
        old,
        hint.authority,
        50,
        51,
        PartitionIncarnation::from_bytes([99; 16]),
    );
    assert_eq!(
        intake.receive(&wrong, old, hint).unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..10 {
        progress(&mut actor, &mut intake, None, &mut output, false);
    }
    rejected(&mut output, 3, RetryClass::Permanent);
    assert_eq!(
        actor.snapshot().accepted.op.0,
        3,
        "invalid opening mutated history"
    );
    let first = request(old, hint.authority, 40, 42, partition());
    assert_eq!(
        intake.receive(&first, old, hint).unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..100 {
        progress(&mut actor, &mut intake, None, &mut output, false);
    }
    assert!(output.is_empty());
    assert!(!controller.jobs().is_empty());
    // Capacity failure has its own control slot while the first writer owns I/O.
    let second = request(old, hint.authority, 50, 52, partition());
    assert_eq!(
        intake.receive(&second, old, hint).unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..10 {
        progress(&mut actor, &mut intake, None, &mut output, false);
    }
    rejected(&mut output, 10, RetryClass::AfterCredit);
    let second = request(current, hint.authority, 50, 53, partition());
    assert_eq!(
        intake.receive(&second, current, hint).unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..10 {
        progress(&mut actor, &mut intake, Some(current), &mut output, false);
    }
    rejected(&mut output, 10, RetryClass::AfterCredit);
    // A new session cannot reclaim the old writer's executing arena early.
    for _ in 0..10000 {
        progress(&mut actor, &mut intake, Some(current), &mut output, false);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        if !intake.has_work() {
            break;
        }
    }
    assert!(!intake.has_work());
    assert!(
        output.is_empty(),
        "old session received a completed opening"
    );
    assert_eq!(actor.snapshot().applied.op.0, 4);
    assert_eq!(
        intake
            .receive(&first, current, actor.authority_hint())
            .unwrap(),
        NativeReceive::Ignored
    );
    assert_eq!(
        intake
            .receive(&second, current, actor.authority_hint())
            .unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..10000 {
        progress(&mut actor, &mut intake, Some(current), &mut output, false);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        if !intake.has_work() {
            break;
        }
    }
    assert!(!intake.has_work());
    assert_eq!(output.len(), 1);
    let message = output.pop().unwrap();
    assert_eq!(
        packet(&message).envelope.session,
        Some(current.binding.session)
    );
    let opened = producer::decode_opened(packet(&message), limits().envelope).unwrap();
    assert_eq!(opened.producer, ProducerId::from_bytes([50; 16]));
    assert_eq!(opened.epoch, 1);
    assert_eq!(opened.next_sequence, 0);
    assert_eq!(opened.policy, Policy::LocalDurable);
    assert_eq!(actor.snapshot().applied.op.0, 5);
    drop(intake);
    close(&mut controller, actor);
}

struct Wakes(std::sync::atomic::AtomicUsize);

impl std::task::Wake for Wakes {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[test]
fn intake_without_work_leaves_its_scheduler_asleep() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    // Six slots, scanned with the two-slot turn bound of this fixture.
    let mut intake = intake_access(
        &mut actor,
        NativeAccess::TrustedClients {
            clients: 2,
            writers: 2,
        },
        partition(),
    );
    let counted = std::sync::Arc::new(Wakes(std::sync::atomic::AtomicUsize::new(0)));
    let waker = Waker::from(counted.clone());
    let hint = actor.authority_hint();
    let poll = |intake: &mut NativeIntake, output: &mut Vec<Message>| {
        intake
            .poll_progress(
                &mut Context::from_waker(&waker),
                hint,
                |_| Some(link(70, 80)),
                |_, message| {
                    output.push(message);
                    Ok(())
                },
            )
            .unwrap()
    };
    let mut output = Vec::new();
    for _ in 0..100 {
        assert!(!poll(&mut intake, &mut output));
    }
    assert_eq!(
        counted.0.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "an intake without requests asked for another turn"
    );
    assert!(output.is_empty());
    // A request in the last writer slot is found by the next call.
    for writer in [50, 60] {
        let opening = request(
            link(70, 80),
            hint.authority,
            writer,
            writer + 1,
            partition(),
        );
        assert_eq!(
            intake.receive(&opening, link(70, 80), hint).unwrap(),
            NativeReceive::Accepted
        );
    }
    for _ in 0..100 {
        pump(&mut actor);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    }
    assert!(poll(&mut intake, &mut output));
    assert_eq!(output.len(), 2, "one call reaches both completed requests");
}
