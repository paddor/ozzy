use super::*;
use crate::replica_actor::{RecoveryActor, RecoveryTiming, ScheduledRecovery};
use omq_tokio::Message;
use ozzy_proto::LinkSessionId;
use ozzy_replication::wire::{
    self, CheckpointMessage, PeerBinding, RecoveryMessage, ReplicaMessage, WireLimits,
};

fn turn(actor: &mut ScheduledRecovery) -> Vec<Message> {
    let mut output = Vec::new();
    actor
        .poll_progress(
            &mut Context::from_waker(Waker::noop()),
            Duration::ZERO,
            |_, message| {
                output.push(message);
                Ok(())
            },
        )
        .unwrap();
    output
}

fn packet(
    sender: NodeId,
    encoded: wire::ControlEncoding,
    metadata: &[u8],
    bytes: bytes::Bytes,
) -> Message {
    Message::multipart([
        bytes::Bytes::copy_from_slice(sender.as_bytes()),
        bytes::Bytes::copy_from_slice(&encoded.header),
        bytes::Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
        bytes,
    ])
}

fn chunk(
    request: wire::CheckpointRequest,
    bytes: bytes::Bytes,
    session: LinkSessionId,
    limits: WireLimits,
) -> Message {
    let mut metadata = [0; 1024];
    let encoded = wire::encode_checkpoint(
        request.source.voter,
        session,
        CheckpointMessage::Chunk {
            request,
            bytes: &bytes,
        },
        &mut metadata,
        limits,
    )
    .unwrap();
    packet(request.source.voter, encoded, &metadata, bytes)
}

#[test]
fn checkpoint_chunks_ignore_reordered_duplicates_and_old_link_sessions() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        exercise(policy);
    }
}

fn exercise(policy: QuorumPolicy) {
    let (mut controller, io) = setup();
    let (mut primary, mut backup) = donors(&mut controller, &io, policy);
    let (mut receiver, startup) = drive(
        &mut controller,
        RecoveringJournal::start(
            receiver_config(policy),
            io,
            generations(820),
            RecoveryOpen::FormatNew {
                segment_capacity: 32768,
            },
        ),
    )
    .unwrap();
    let memory = payload_owner(32768);
    receiver.bind_append_memory(&memory).unwrap();
    let local = startup.local();
    let configuration = startup.configuration();
    let mut config = crate::replica_journal::owned::tests::shard::actors::actor_config();
    config.transfer.max_body_bytes = 64;
    let session = config.sessions[0];
    let limits = WireLimits::for_transfer(config.transfer.max_operations, 64).unwrap();
    let journal =
        ShardRecoveringJournal::from_owned(receiver, ShardJournalConfig::default(), || 123)
            .unwrap();
    let mut actor = ScheduledRecovery::new(
        RecoveryActor::new(journal, startup, config, RecoveryTiming::default()).unwrap(),
    );
    actor.bind_receive_owner(&memory).unwrap();
    let mut pin = None;
    for message in turn(&mut actor) {
        let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
        let binding = PeerBinding::new(configuration, local, session).unwrap();
        let ReplicaMessage::Recovery(RecoveryMessage::Request(request)) =
            wire::decode(&frames, binding, limits).unwrap()
        else {
            panic!("recovery request")
        };
        let donor = if message.part_slice(0).unwrap()
            == primary.config.identity.replica_node_id.as_bytes()
        {
            &mut primary
        } else {
            &mut backup
        };
        let mut response = donor
            .driver
            .normal()
            .unwrap()
            .recovery_response(request.nonce)
            .unwrap();
        if donor.config.identity.replica_node_id == primary_node() {
            let captured =
                drive(&mut controller, donor.journal.pin_recovery(local, response)).unwrap();
            response = captured.response();
            pin = Some(captured);
        }
        let mut metadata = [0; 1024];
        let encoded = wire::encode_recovery_state(
            donor.config.identity.replica_node_id,
            session,
            wire::RecoveryState {
                request_id: request.request_id,
                response,
            },
            &mut metadata,
            limits,
        )
        .unwrap();
        actor
            .receive(
                &packet(
                    donor.config.identity.replica_node_id,
                    encoded,
                    &metadata,
                    bytes::Bytes::new(),
                ),
                Duration::ZERO,
            )
            .unwrap();
    }
    transfer(
        &mut controller,
        &mut primary,
        &mut actor,
        &pin.unwrap(),
        &memory,
        session,
        limits,
    );
    drive(&mut controller, actor.shutdown()).unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
    primary.journal.release_recovery(pin.unwrap()).unwrap();
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}

fn primary_node() -> NodeId {
    NodeId::from_bytes([1; 16])
}

fn transfer(
    controller: &mut Controller,
    primary: &mut Replica,
    actor: &mut ScheduledRecovery,
    pin: &PinnedRecovery,
    memory: &crate::memory::Owner,
    session: LinkSessionId,
    limits: WireLimits,
) {
    let mut previous = None;
    let mut received = 0;
    let mut current_session = session;
    for _ in 0..10000 {
        turn(actor);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        let Some(request) = actor.checkpoint_receive_demand() else {
            if received > 1 && !actor.receive_has_work() {
                return;
            }
            continue;
        };
        if let Some(old) = &previous {
            actor.receive(old, Duration::ZERO).unwrap();
        }
        if received == 1 {
            current_session = LinkSessionId::from_bytes([91; 16]);
            actor
                .replace_session(
                    request.source.voter,
                    session,
                    current_session,
                    Duration::ZERO,
                )
                .unwrap();
        }
        let work = primary
            .journal
            .prepare_checkpoint_read(*pin, request)
            .unwrap();
        let done = drive(controller, work.read());
        let bytes = primary
            .journal
            .complete_checkpoint_read(done)
            .unwrap()
            .bytes;
        let original = chunk(request, bytes.clone(), current_session, limits);
        let future = chunk(
            wire::CheckpointRequest {
                offset: request.offset + 1,
                ..request
            },
            bytes.clone(),
            current_session,
            limits,
        );
        actor.receive(&future, Duration::ZERO).unwrap();
        assert_eq!(actor.checkpoint_receive_demand(), Some(request));
        let wrong = chunk(
            request,
            bytes.clone(),
            if received == 0 {
                LinkSessionId::from_bytes([92; 16])
            } else {
                session
            },
            limits,
        );
        actor.receive(&wrong, Duration::ZERO).unwrap();
        assert_eq!(actor.checkpoint_receive_demand(), Some(request));
        if received == 0 {
            let held = memory.try_lease(32768 - memory.allocated_bytes()).unwrap();
            actor.receive(&original, Duration::ZERO).unwrap();
            assert_eq!(actor.checkpoint_receive_demand(), Some(request));
            drop(held);
            memory.trim_cache();
        }
        actor.receive(&original, Duration::ZERO).unwrap();
        let charged = memory.allocated_bytes();
        actor.receive(&original, Duration::ZERO).unwrap();
        assert_eq!(memory.allocated_bytes(), charged);
        previous = Some(original);
        received += 1;
    }
    panic!("checkpoint transfer did not finish");
}
