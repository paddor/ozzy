use super::*;
use crate::{
    frontend::{Binding, Kind, Link},
    replica_actor::{NativeIntake, NativeIntakeConfig, NativeReceive},
    replicated::ClientAccess,
};
use bytes::Bytes;
use ozzy_journal::operation::{
    CreatePartition, OperationBody, RetentionPolicy, encode_operation_body,
};
use ozzy_proto::{
    Envelope, EnvelopeLimits, Opcode, OperationId, OwnerEpoch, PartitionId, PartitionIncarnation,
    ProducerId, RequestId, append, decode_packet, handshake, producer,
};

fn limits() -> append::DataLimits {
    append::DataLimits {
        envelope: EnvelopeLimits {
            max_metadata_bytes: 1024,
            max_payload_bytes: 1024,
        },
        max_records: 4,
        max_parts: 4,
        max_record_bytes: 1024,
    }
}

fn link() -> Link {
    Link {
        binding: Binding {
            peer: NodeId::from_bytes([70; 16]),
            session: LinkSessionId::from_bytes([80; 16]),
            kind: Kind::Client,
        },
        send: limits(),
        remote: handshake::Parameters::streaming(limits(), handshake::PRODUCER).unwrap(),
    }
}

fn envelope(opcode: Opcode) -> Envelope {
    Envelope {
        opcode,
        response: false,
        request_id: Some(RequestId::from_bytes([42; 16])),
        sender: link().binding.peer,
        session: Some(link().binding.session),
    }
}

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([11; 16])
}

fn create(actor: &ReplicaActor) -> crate::replica_journal::ProposalBuffer {
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    let body = OperationBody::CreatePartition(CreatePartition {
        partition: partition(),
        stream: "test",
        topic: "orders",
        partition_id: PartitionId::ZERO,
        owner_epoch: OwnerEpoch::INITIAL,
        retention: RetentionPolicy::default(),
    });
    buffer
        .push(
            body.kind(),
            &encode_operation_body(&body, ozzy_journal::operation::OperationLimits::default())
                .unwrap(),
        )
        .unwrap();
    buffer
}

fn cluster(
    controller: &mut Controller,
    io: &Local,
    policy: QuorumPolicy,
) -> (Vec<Scheduled>, NativeIntake, PendingProposal) {
    cluster_with_window(controller, io, policy, 1)
}

fn cluster_with_window(
    controller: &mut Controller,
    io: &Local,
    policy: QuorumPolicy,
    window: usize,
) -> (Vec<Scheduled>, NativeIntake, PendingProposal) {
    let mut actors = Vec::new();
    let mut intake = None;
    let mut creation = None;
    for broker in 0..3 {
        let mut actor = unstarted_actor(controller, io, 0, policy, broker, 64);
        if broker == 0 {
            let mut submitter = actor.take_submitter().unwrap();
            creation = Some(submitter.try_submit(create(&actor)).unwrap());
            let buffers = (0..=window)
                .map(|_| actor.lease_proposal_buffer().unwrap())
                .collect();
            intake = Some(
                NativeIntake::new(
                    NativeIntakeConfig {
                        local: NodeId::from_bytes([1; 16]),
                        group: actor.subscribe().borrow().scope.group_id,
                        partition: partition(),
                        policy: match policy {
                            QuorumPolicy::Durable => append::Policy::QuorumDurable,
                            QuorumPolicy::Replicated => append::Policy::QuorumReplicatedPersisting,
                        },
                        access: crate::replica_actor::NativeAccess::Writers(vec![ClientAccess {
                            node: link().binding.peer,
                            producer: ProducerId::from_bytes([40; 16]),
                        }]),
                        limits: limits(),
                        requests_per_writer: window,
                        turn_slots: window,
                    },
                    submitter,
                    buffers,
                )
                .unwrap(),
            );
        }
        actors.push(Scheduled::new(actor).unwrap());
    }
    (actors, intake.unwrap(), creation.unwrap())
}

fn progress(
    actors: &mut [Scheduled],
    intake: &mut NativeIntake,
    output: &mut Vec<Message>,
    held: Option<&mut Vec<(usize, Message)>>,
) {
    let mut held = held;
    for (to, message) in collect(actors, Duration::ZERO, false) {
        if opcode(&message) == Opcode::PrepareFlow
            && let Some(held) = &mut held
        {
            held.push((to, message));
        } else {
            actors[to].receive(&message, Duration::ZERO).unwrap();
        }
    }
    intake
        .poll_progress(
            &mut Context::from_waker(Waker::noop()),
            actors[0].authority_hint(),
            |_| Some(link()),
            |_, message| {
                output.push(message);
                Ok(())
            },
        )
        .unwrap();
}

fn producer_open(actor: &Scheduled) -> Message {
    let mut metadata = Vec::with_capacity(256);
    let header = producer::encode_open(
        envelope(Opcode::OpenProducer),
        producer::Open {
            authority: actor.authority_hint().authority,
            partition: partition(),
            producer: ProducerId::from_bytes([40; 16]),
            mode: producer::Mode::Resume,
            expected_epoch: None,
            operation: OperationId::from_bytes([42; 16]),
        },
        &mut metadata,
        limits().envelope,
    )
    .unwrap();
    crate::native_frames::message(
        link().binding.peer.as_bytes(),
        header,
        &metadata,
        Bytes::new(),
    )
}

#[test]
fn native_producer_open_and_append_require_group_confirmation_under_both_policies() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, mut intake, creation) = cluster(&mut controller, &io, policy);
        let mut creation = Box::pin(creation);
        let mut output = Vec::new();
        let mut created = false;
        for _ in 0..10000 {
            progress(&mut actors, &mut intake, &mut output, None);
            settle(&mut controller, &[]);
            if let Poll::Ready(reply) = poll(creation.as_mut()) {
                assert!(matches!(
                    reply.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                created = true;
                break;
            }
        }
        assert!(created);
        assert_eq!(
            intake
                .receive(
                    &producer_open(&actors[0]),
                    link(),
                    actors[0].authority_hint()
                )
                .unwrap(),
            NativeReceive::Accepted
        );
        let mut held = Vec::new();
        for _ in 0..100 {
            progress(&mut actors, &mut intake, &mut output, Some(&mut held));
            settle(&mut controller, &[]);
        }
        assert_ne!(held.len(), 0);
        assert!(
            output.is_empty(),
            "leader storage alone confirmed producer open"
        );
        assert!(intake.has_work());
        for (to, message) in held {
            actors[to].receive(&message, Duration::ZERO).unwrap();
        }
        drain(&mut controller, &mut actors, &mut intake, &mut output);
        let message = output.pop().unwrap();
        let frames = std::array::from_fn::<_, 3, _>(|index| message.part_slice(index + 1).unwrap());
        let opened = producer::decode_opened(
            decode_packet(&frames, limits().envelope).unwrap(),
            limits().envelope,
        )
        .unwrap();
        assert_eq!(opened.epoch, 1);
        assert_eq!(
            opened.policy,
            match policy {
                QuorumPolicy::Durable => append::Policy::QuorumDurable,
                QuorumPolicy::Replicated => append::Policy::QuorumReplicatedPersisting,
            }
        );
        let message = append_request(&actors[0], opened.policy);
        assert_eq!(
            intake
                .receive(&message, link(), actors[0].authority_hint())
                .unwrap(),
            NativeReceive::Accepted
        );
        drain(&mut controller, &mut actors, &mut intake, &mut output);
        let message = output.pop().unwrap();
        let frames = std::array::from_fn::<_, 3, _>(|index| message.part_slice(index + 1).unwrap());
        let confirmed = append::stream::decode_confirmed(
            decode_packet(&frames, limits().envelope).unwrap(),
            limits().envelope,
        )
        .unwrap();
        assert_eq!(
            (
                confirmed.key.first_sequence,
                confirmed.end_sequence,
                confirmed.first_offset
            ),
            (0, 1, 0)
        );
        assert_eq!(confirmed.policy, opened.policy);
        for actor in actors {
            drive(&mut controller, actor.shutdown()).unwrap();
        }
    }
}

#[test]
fn three_pipelined_appends_settle_in_sequence() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, mut intake, creation) =
            cluster_with_window(&mut controller, &io, policy, 3);
        let mut creation = Box::pin(creation);
        let mut output = Vec::new();
        let mut created = false;
        for _ in 0..10000 {
            progress(&mut actors, &mut intake, &mut output, None);
            settle(&mut controller, &[]);
            if let Poll::Ready(reply) = poll(creation.as_mut()) {
                assert!(matches!(
                    reply.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                created = true;
                break;
            }
        }
        assert!(created);
        assert_eq!(
            intake
                .receive(
                    &producer_open(&actors[0]),
                    link(),
                    actors[0].authority_hint()
                )
                .unwrap(),
            NativeReceive::Accepted
        );
        drain(&mut controller, &mut actors, &mut intake, &mut output);
        output.clear();
        let append_policy = match policy {
            QuorumPolicy::Durable => append::Policy::QuorumDurable,
            QuorumPolicy::Replicated => append::Policy::QuorumReplicatedPersisting,
        };
        for sequence in 0..3 {
            assert_eq!(
                intake
                    .receive(
                        &append_request_at(&actors[0], append_policy, sequence),
                        link(),
                        actors[0].authority_hint(),
                    )
                    .unwrap(),
                NativeReceive::Accepted
            );
        }
        drain(&mut controller, &mut actors, &mut intake, &mut output);
        let mut confirmed = output
            .iter()
            .map(|message| {
                let frames =
                    std::array::from_fn::<_, 3, _>(|index| message.part_slice(index + 1).unwrap());
                append::stream::decode_confirmed(
                    decode_packet(&frames, limits().envelope).unwrap(),
                    limits().envelope,
                )
                .unwrap()
                .key
                .first_sequence
            })
            .collect::<Vec<_>>();
        confirmed.sort_unstable();
        assert_eq!(confirmed, [0, 1, 2]);
        for actor in actors {
            drive(&mut controller, actor.shutdown()).unwrap();
        }
    }
}

fn drain(
    controller: &mut Controller,
    actors: &mut [Scheduled],
    intake: &mut NativeIntake,
    output: &mut Vec<Message>,
) {
    for _ in 0..10000 {
        progress(actors, intake, output, None);
        settle(controller, &[]);
        if !intake.has_work() {
            return;
        }
    }
    panic!("native group intake did not settle");
}

fn append_request(actor: &Scheduled, policy: append::Policy) -> Message {
    append_request_at(actor, policy, 0)
}

fn append_request_at(actor: &Scheduled, policy: append::Policy, sequence: u8) -> Message {
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let header = append::encode_append(
        Envelope {
            request_id: Some(RequestId::from_bytes([sequence + 1; 16])),
            ..envelope(Opcode::Append)
        },
        append::Append {
            authority: actor.authority_hint().authority,
            partition: partition(),
            owner_epoch: 1,
            key: append::AppendKey {
                producer_id: ProducerId::from_bytes([40; 16]),
                producer_epoch: 1,
                first_sequence: u64::from(sequence),
            },
            policy,
            records: &[append::Record {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: ozzy_proto::MessageId::from_bytes([45 + sequence; 16]),
                parts: &[b"opaque"],
            }],
        },
        &mut metadata,
        &mut payload,
        limits(),
    )
    .unwrap();
    crate::native_frames::message(
        link().binding.peer.as_bytes(),
        header,
        &metadata,
        Bytes::from(payload),
    )
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one controlled PUB and held-write regression across both quorum policies"
)]
fn validated_follower_batch_waits_for_physical_write_capacity() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, mut intake, creation) =
            cluster_with_window(&mut controller, &io, policy, 3);
        let mut creation = Box::pin(creation);
        let mut output = Vec::new();
        let mut created = false;
        for _ in 0..10000 {
            progress(&mut actors, &mut intake, &mut output, None);
            settle(&mut controller, &[]);
            if let Poll::Ready(reply) = poll(creation.as_mut()) {
                assert!(matches!(
                    reply.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                created = true;
                break;
            }
        }
        assert!(created);
        assert_eq!(
            intake
                .receive(
                    &producer_open(&actors[0]),
                    link(),
                    actors[0].authority_hint()
                )
                .unwrap(),
            NativeReceive::Accepted
        );
        drain(&mut controller, &mut actors, &mut intake, &mut output);
        output.clear();
        let memory = payload_owner(32768);
        // Small received bodies reuse large backing blocks, as under real load.
        let cached = [
            memory.try_lease(8192).unwrap(),
            memory.try_lease(8192).unwrap(),
        ];
        drop(cached);
        actors[1].bind_receive_owner(&memory).unwrap();
        for actor in &mut actors {
            actor.enable_publication();
        }
        let (mut publications, mut repairs) = (0, 0);
        let mut network =
            |actors: &mut [Scheduled], intake: &mut NativeIntake, output: &mut Vec<Message>| {
                super::publications::step(
                    actors,
                    Duration::ZERO,
                    false,
                    &[],
                    &mut publications,
                    &mut repairs,
                );
                intake
                    .poll_progress(
                        &mut Context::from_waker(Waker::noop()),
                        actors[0].authority_hint(),
                        |_| Some(link()),
                        |_, message| {
                            output.push(message);
                            Ok(())
                        },
                    )
                    .unwrap();
            };
        let append_policy = match policy {
            QuorumPolicy::Durable => append::Policy::QuorumDurable,
            QuorumPolicy::Replicated => append::Policy::QuorumReplicatedPersisting,
        };
        assert_eq!(
            intake
                .receive(
                    &append_request_at(&actors[0], append_policy, 0),
                    link(),
                    actors[0].authority_hint()
                )
                .unwrap(),
            NativeReceive::Accepted
        );
        let mut writes = Vec::new();
        for _ in 0..10000 {
            network(&mut actors, &mut intake, &mut output);
            for (id, _) in controller.jobs() {
                if matches!(controller.operation(id).unwrap().unprotected(), Operation::Write { offset, .. } if *offset >= 4096)
                {
                    if !writes.contains(&id) {
                        writes.push(id);
                    }
                } else {
                    controller.execute(id, Effect::Normal).unwrap();
                    controller.deliver(id).unwrap();
                }
            }
            if writes.len() == 3 {
                break;
            }
        }
        assert_eq!(writes.len(), 3);
        assert_eq!(
            intake
                .receive(
                    &append_request_at(&actors[0], append_policy, 1),
                    link(),
                    actors[0].authority_hint()
                )
                .unwrap(),
            NativeReceive::Accepted
        );
        for _ in 0..100 {
            network(&mut actors, &mut intake, &mut output);
            settle(&mut controller, &writes);
        }
        assert!(writes.iter().all(|id| controller.operation(*id).is_some()));
        let mut confirmed = false;
        for _ in 0..10000 {
            network(&mut actors, &mut intake, &mut output);
            settle(&mut controller, &[]);
            if output.len() == 2
                && actors.iter().all(|actor| {
                    actor.status().normal.is_some_and(|state| {
                        state.accepted.op.0 == 4 && state.applied == state.accepted
                    })
                })
            {
                confirmed = true;
                break;
            }
        }
        assert!(confirmed);
        let mut offsets = output
            .iter()
            .map(|message| {
                let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
                let confirmed = append::stream::decode_confirmed(
                    decode_packet(&frames, limits().envelope).unwrap(),
                    limits().envelope,
                )
                .unwrap();
                assert_eq!(confirmed.key.first_sequence, confirmed.first_offset);
                confirmed.first_offset
            })
            .collect::<Vec<_>>();
        offsets.sort_unstable();
        assert_eq!(offsets, [0, 1]);
        for actor in actors {
            drive(&mut controller, actor.shutdown()).unwrap();
        }
    }
}
