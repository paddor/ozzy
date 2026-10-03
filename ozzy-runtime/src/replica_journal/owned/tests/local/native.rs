use super::{
    actor::{actor, actor_with_partition_memory, close, pump},
    *,
};
use crate::{
    frontend::{Binding, Kind, Link},
    replica_actor::{NativeAccess, NativeIntake, NativeIntakeConfig, NativeReceive},
    replicated::ClientAccess,
};
use bytes::Bytes;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{
    Envelope, EnvelopeLimits, LinkSessionId, NodeId, Opcode, Packet, RequestId,
    append::{self, Authority, Policy},
    data::{DataLimits, Encoding},
    decode_packet, handshake,
    producer::{self, Mode, Open},
};

mod dynamic;
mod frontend;
mod pipeline;

fn limits() -> DataLimits {
    DataLimits {
        envelope: EnvelopeLimits {
            max_metadata_bytes: 1024,
            max_payload_bytes: 1024,
        },
        max_records: 4,
        max_parts: 4,
        max_record_bytes: 1024,
    }
}

fn link(peer: u8, session: u8) -> Link {
    let parameters =
        handshake::Parameters::streaming(limits(), handshake::PRODUCER, 4, 4096).unwrap();
    Link {
        binding: Binding {
            peer: NodeId::from_bytes([peer; 16]),
            session: LinkSessionId::from_bytes([session; 16]),
            kind: Kind::Client,
        },
        send: limits(),
        remote: parameters,
    }
}

fn intake(actor: &mut crate::replica_actor::LocalActor) -> NativeIntake {
    intake_with_peers(actor, [70, 71])
}

fn intake_with_peers(actor: &mut crate::replica_actor::LocalActor, peers: [u8; 2]) -> NativeIntake {
    intake_partition(actor, peers, partition())
}

fn intake_partition(
    actor: &mut crate::replica_actor::LocalActor,
    peers: [u8; 2],
    incarnation: PartitionIncarnation,
) -> NativeIntake {
    intake_access(
        actor,
        NativeAccess::Writers(vec![
            ClientAccess {
                node: link(peers[0], 80).binding.peer,
                producer: ProducerId::from_bytes([40; 16]),
            },
            ClientAccess {
                node: link(peers[1], 81).binding.peer,
                producer: ProducerId::from_bytes([30; 16]),
            },
        ]),
        incarnation,
    )
}

fn intake_access(
    actor: &mut crate::replica_actor::LocalActor,
    access: NativeAccess,
    incarnation: PartitionIncarnation,
) -> NativeIntake {
    intake_access_reserved(actor, access, incarnation, None)
}

fn intake_access_reserved(
    actor: &mut crate::replica_actor::LocalActor,
    access: NativeAccess,
    incarnation: PartitionIncarnation,
    capacities: Option<(&crate::memory::Capacity, &crate::memory::Capacity)>,
) -> NativeIntake {
    intake_access_profile(actor, access, incarnation, capacities, limits())
}

fn intake_access_profile(
    actor: &mut crate::replica_actor::LocalActor,
    access: NativeAccess,
    incarnation: PartitionIncarnation,
    capacities: Option<(&crate::memory::Capacity, &crate::memory::Capacity)>,
    wire: DataLimits,
) -> NativeIntake {
    let config = NativeIntakeConfig {
        local: actor.authority_hint().primary,
        group: actor.group(),
        partition: incarnation,
        policy: Policy::LocalDurable,
        access,
        limits: wire,
        requests_per_writer: 1,
        turn_slots: 2,
    };
    let buffers = (0..config.access.required_buffers(1).unwrap())
        .map(|index| {
            let mut buffer = actor
                .lease_proposal_buffer_with_limits(config.buffer_limits(index).unwrap())
                .unwrap();
            if let Some((control, data)) = capacities {
                let stride = config.requests_per_writer + 1;
                buffer
                    .bind_capacity(if index.is_multiple_of(stride) {
                        control
                    } else {
                        data
                    })
                    .unwrap();
            }
            buffer
        })
        .collect();
    NativeIntake::new(config, actor.take_submitter().unwrap(), buffers).unwrap()
}

fn envelope(opcode: Opcode, link: Link, id: u8) -> Envelope {
    Envelope {
        opcode,
        response: false,
        request_id: Some(RequestId::from_bytes([id; 16])),
        sender: link.binding.peer,
        session: Some(link.binding.session),
    }
}

fn open(authority: Authority, link: Link, mode: Mode, old: Option<u64>, id: u8) -> Message {
    let mut metadata = Vec::with_capacity(256);
    let header = producer::encode_open(
        envelope(Opcode::OpenProducer, link, id),
        Open {
            authority,
            partition: partition(),
            producer: ProducerId::from_bytes([40; 16]),
            mode,
            expected_epoch: old,
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

fn append(authority: Authority, link: Link, writer: u8, first: u64, count: u8) -> Message {
    let records: Vec<_> = (0..count)
        .map(|index| append::Record {
            encoding: Encoding::Raw,
            message_id: ozzy_proto::MessageId::from_bytes([writer + first as u8 + index; 16]),
            parts: &[b"opaque"],
        })
        .collect();
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let header = append::encode_append(
        envelope(Opcode::Append, link, writer + first as u8),
        append::Append {
            authority,
            partition: partition(),
            owner_epoch: 1,
            key: append::AppendKey {
                producer_id: ProducerId::from_bytes([writer; 16]),
                producer_epoch: 1,
                first_sequence: first,
            },
            policy: Policy::LocalDurable,
            records: &records,
        },
        &mut metadata,
        &mut payload,
        limits(),
    )
    .unwrap();
    crate::native_frames::message(
        link.binding.peer.as_bytes(),
        header,
        &metadata,
        Bytes::from(payload),
    )
}

fn packet(message: &Message) -> Packet<'_> {
    let frames = std::array::from_fn::<_, 3, _>(|index| message.part_slice(index + 1).unwrap());
    decode_packet(&frames, limits().envelope).unwrap()
}

fn progress(
    actor: &mut crate::replica_actor::LocalActor,
    intake: &mut NativeIntake,
    replacement: Option<Link>,
    output: &mut Vec<Message>,
    blocked: bool,
) {
    pump(actor);
    intake
        .poll_progress(
            &mut Context::from_waker(Waker::noop()),
            actor.authority_hint(),
            |peer| {
                Some(if peer == link(70, 80).binding.peer {
                    replacement.unwrap_or(link(70, 80))
                } else {
                    link(71, 81)
                })
            },
            |_, message| {
                if blocked
                    && message.part_slice(0)
                        == Some(link(70, 80).binding.peer.as_bytes().as_slice())
                {
                    Err(TrySendError::Full(message))
                } else {
                    output.push(message);
                    Ok(())
                }
            },
        )
        .unwrap();
}

fn settle(
    controller: &mut Controller,
    actor: &mut crate::replica_actor::LocalActor,
    intake: &mut NativeIntake,
    output: &mut Vec<Message>,
) {
    for _ in 0..10000 {
        progress(actor, intake, None, output, false);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        if !intake.has_work() {
            return;
        }
    }
    panic!("native partition intake did not settle");
}

#[test]
fn native_producer_open_requires_observed_local_confirmation_and_fences_reconnect_reply() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut intake = intake(&mut actor);
    let hint = actor.authority_hint();
    let request = open(hint.authority, link(70, 80), Mode::Resume, None, 42);
    assert_eq!(
        intake.receive(&request, link(70, 80), hint).unwrap(),
        NativeReceive::Accepted
    );
    let mut output = Vec::new();
    for _ in 0..100 {
        progress(&mut actor, &mut intake, None, &mut output, false);
    }
    assert!(output.is_empty(), "accepted open is not confirmed");
    assert_eq!(actor.snapshot().accepted.op.0, 4);
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    assert_eq!(output.len(), 1);
    let opened =
        producer::decode_opened(packet(&output.pop().unwrap()), limits().envelope).unwrap();
    assert_eq!(opened.epoch, 1);
    assert_eq!(opened.next_sequence, 0);
    let old = actor.snapshot().accepted;
    assert_eq!(
        intake
            .receive(&request, link(70, 80), actor.authority_hint())
            .unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..100 {
        progress(
            &mut actor,
            &mut intake,
            Some(link(70, 90)),
            &mut output,
            false,
        );
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    }
    assert!(output.is_empty(), "old session received completed retry");
    assert!(!intake.has_work());
    assert_eq!(actor.snapshot().accepted, old);
    assert_eq!(
        intake
            .receive(&request, link(70, 90), actor.authority_hint())
            .unwrap(),
        NativeReceive::Ignored
    );
    let fresh = open(
        actor.authority_hint().authority,
        link(70, 90),
        Mode::Resume,
        None,
        42,
    );
    assert_eq!(
        intake
            .receive(&fresh, link(70, 90), actor.authority_hint())
            .unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..100 {
        progress(
            &mut actor,
            &mut intake,
            Some(link(70, 90)),
            &mut output,
            false,
        );
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    }
    assert_eq!(output.len(), 1);
    assert_eq!(
        packet(&output[0]).envelope.session,
        Some(link(70, 90).binding.session)
    );
    close(&mut controller, actor);
}

#[test]
fn deferred_writer_open_returns_retry_without_proposing_or_advancing_epoch() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut intake = intake(&mut actor);
    let hint = actor.authority_hint();
    let request = open(hint.authority, link(70, 80), Mode::Resume, None, 42);
    let accepted = actor.snapshot().accepted;
    assert_eq!(
        intake.defer(&request, link(70, 80), hint).unwrap(),
        NativeReceive::Accepted
    );
    assert_eq!(
        intake.defer(&request, link(70, 80), hint).unwrap(),
        NativeReceive::Busy
    );
    let mut output = Vec::new();
    for _ in 0..10 {
        progress(&mut actor, &mut intake, None, &mut output, false);
    }
    assert_eq!(actor.snapshot().accepted, accepted);
    assert_eq!(output.len(), 1);
    let reply = output.pop().unwrap();
    let retry = ozzy_proto::nack::decode(packet(&reply), limits().envelope).unwrap();
    assert_eq!(retry.retry, ozzy_proto::nack::RetryClass::AfterCredit);
    assert_eq!(
        intake.receive(&request, link(70, 80), hint).unwrap(),
        NativeReceive::Accepted
    );
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    let opened =
        producer::decode_opened(packet(&output.pop().unwrap()), limits().envelope).unwrap();
    assert_eq!(opened.epoch, 1);
    drop(intake);
    close(&mut controller, actor);
}

#[test]
fn native_shared_partition_retry_returns_interleaved_offsets_and_slow_peer_stays_isolated() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut intake = intake(&mut actor);
    let hint = actor.authority_hint();
    let mut output = Vec::new();
    intake
        .receive(
            &open(hint.authority, link(70, 80), Mode::Resume, None, 42),
            link(70, 80),
            hint,
        )
        .unwrap();
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    output.clear();
    for (writer, sequence, peer) in [
        (40, 0, link(70, 80)),
        (30, 0, link(71, 81)),
        (40, 1, link(70, 80)),
    ] {
        let hint = actor.authority_hint();
        assert_eq!(
            intake
                .receive(
                    &append(hint.authority, peer, writer, sequence, 1),
                    peer,
                    hint
                )
                .unwrap(),
            NativeReceive::Accepted
        );
        settle(&mut controller, &mut actor, &mut intake, &mut output);
    }
    output.clear();
    let before = actor.snapshot().accepted;
    let hint = actor.authority_hint();
    intake
        .receive(
            &append(hint.authority, link(70, 80), 40, 0, 2),
            link(70, 80),
            hint,
        )
        .unwrap();
    for _ in 0..100 {
        progress(&mut actor, &mut intake, None, &mut output, true);
    }
    assert!(output.is_empty());
    assert_eq!(
        intake
            .receive(
                &open(hint.authority, link(70, 80), Mode::Resume, Some(1), 50),
                link(70, 80),
                hint,
            )
            .unwrap(),
        NativeReceive::Accepted,
        "full data reply slots consumed producer-open capacity",
    );
    intake
        .receive(
            &append(hint.authority, link(71, 81), 30, 1, 1),
            link(71, 81),
            hint,
        )
        .unwrap();
    for _ in 0..1000 {
        progress(&mut actor, &mut intake, None, &mut output, true);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        if !output.is_empty() {
            break;
        }
    }
    assert_eq!(
        output.len(),
        1,
        "slow peer stalled independent confirmation"
    );
    assert_eq!(actor.snapshot().accepted.op.0, before.op.0 + 1);
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    let confirmations = confirmations(&output, hint.primary, link(70, 80).binding.peer);
    assert_eq!(
        confirmations
            .iter()
            .map(|reply| (
                reply.key.first_sequence,
                reply.first_offset,
                reply.end_sequence
            ))
            .collect::<Vec<_>>(),
        [(0, 0, 1), (1, 2, 2)]
    );
    close(&mut controller, actor);
}

fn confirmations(
    output: &[Message],
    local: NodeId,
    peer: NodeId,
) -> Vec<append::stream::Confirmed> {
    output
        .iter()
        .filter(|message| {
            packet(message).envelope.sender == local
                && packet(message).envelope.opcode == Opcode::Appended
                && message.part_slice(0) == Some(peer.as_bytes().as_slice())
        })
        .map(|message| {
            append::stream::decode_confirmed(packet(message), limits().envelope).unwrap()
        })
        .collect()
}

#[test]
fn native_zero_record_identity_is_permanently_rejected_before_actor_admission() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut intake = intake(&mut actor);
    let before = actor.snapshot().accepted;
    let hint = actor.authority_hint();
    let request = append(hint.authority, link(71, 81), 30, 0, 1);
    let mut metadata = request.part_slice(2).unwrap().to_vec();
    let start = metadata
        .windows(16)
        .rposition(|bytes| bytes == [30; 16])
        .unwrap();
    metadata[start..start + 16].fill(0);
    let request = Message::multipart([
        request.part_bytes(0).unwrap(),
        request.part_bytes(1).unwrap(),
        Bytes::from(metadata),
        request.part_bytes(3).unwrap(),
    ]);
    assert_eq!(
        intake.receive(&request, link(71, 81), hint).unwrap(),
        NativeReceive::Accepted
    );
    let mut output = Vec::new();
    for _ in 0..10 {
        progress(&mut actor, &mut intake, None, &mut output, false);
    }
    assert_eq!(output.len(), 1);
    let rejection = ozzy_proto::nack::decode(packet(&output[0]), limits().envelope).unwrap();
    assert_eq!(
        (rejection.code, rejection.retry),
        (1, ozzy_proto::nack::RetryClass::Permanent)
    );
    assert_eq!(actor.snapshot().accepted, before);
    close(&mut controller, actor);
}

#[test]
fn native_one_link_multiplexes_independent_writers_and_fences_only_one() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut intake = intake_with_peers(&mut actor, [70, 70]);
    let mut output = Vec::new();
    let hint = actor.authority_hint();
    intake
        .receive(
            &open(hint.authority, link(70, 80), Mode::Resume, None, 42),
            link(70, 80),
            hint,
        )
        .unwrap();
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    output.clear();
    for writer in [40, 30] {
        assert_eq!(
            intake
                .receive(
                    &append(hint.authority, link(70, 80), writer, 0, 1),
                    link(70, 80),
                    hint
                )
                .unwrap(),
            NativeReceive::Accepted
        );
    }
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    let confirmed = confirmations(&output, hint.primary, link(70, 80).binding.peer);
    assert_eq!(confirmed.len(), 2);
    assert_eq!(
        confirmed
            .iter()
            .map(|reply| (reply.key.producer_id.as_bytes()[0], reply.first_offset))
            .collect::<Vec<_>>(),
        [(40, 0), (30, 1)]
    );
    output.clear();
    intake
        .receive(
            &open(hint.authority, link(70, 80), Mode::Fence, Some(1), 44),
            link(70, 80),
            hint,
        )
        .unwrap();
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    let opened =
        producer::decode_opened(packet(&output.pop().unwrap()), limits().envelope).unwrap();
    assert_eq!(opened.epoch, 2);
    for writer in [40, 30] {
        assert_eq!(
            intake
                .receive(
                    &append(hint.authority, link(70, 80), writer, 1, 1),
                    link(70, 80),
                    hint
                )
                .unwrap(),
            NativeReceive::Accepted
        );
    }
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    assert_eq!(output.len(), 2);
    let rejected = output
        .iter()
        .find(|message| packet(message).envelope.opcode == Opcode::Nack)
        .unwrap();
    let rejected = ozzy_proto::nack::decode(packet(rejected), limits().envelope).unwrap();
    assert_eq!(rejected.code, 6);
    let confirmed = confirmations(&output, hint.primary, link(70, 80).binding.peer);
    assert_eq!(confirmed.len(), 1);
    assert_eq!(
        confirmed[0].key.producer_id,
        ProducerId::from_bytes([30; 16])
    );
    assert_eq!(
        (confirmed[0].key.first_sequence, confirmed[0].first_offset),
        (1, 2)
    );
    close(&mut controller, actor);
}
