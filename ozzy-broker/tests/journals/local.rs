use super::*;
use ozzy_broker::DevicePools;
use ozzy_journal::operation::{
    AppendRecord, CreatePartition, OpenProducer, OperationBody, OperationLimits, RetentionPolicy,
    encode_operation_body,
};
use ozzy_proto::{
    MessageId, OperationId, OwnerEpoch, PartitionId, ProducerEpoch, ProducerId, ProducerSequence,
};
use ozzy_runtime::{
    replica_actor::{LocalActor, LocalActorConfig, ProposalOutcome},
    replica_journal::{ProducerAppend, ProposalBuffer},
};

async fn submit(actor: &mut LocalActor, buffer: ProposalBuffer) -> ProposalBuffer {
    let mut submitter = actor
        .take_submitter()
        .expect("test config has enough lanes");
    let reply = submitter.try_submit(buffer).unwrap();
    tokio::pin!(reply);
    tokio::select! {
        result = &mut reply => {
            let reply = result.unwrap();
            assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }), "{:?}", reply.outcome);
            reply.buffer
        }
        result = std::future::poll_fn(|cx| actor.poll_progress(cx)) => panic!("actor stopped: {result:?}"),
    }
}

#[tokio::test]
async fn configured_local_actor_persists_full_sdk_batches_and_near_limit_payloads() {
    Box::pin(configured_actor(IoBackend::Pool)).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn configured_local_actor_uses_shared_aio_with_the_same_durability_contract() {
    Box::pin(configured_actor(IoBackend::Aio)).await;
}

async fn configured_actor(backend: IoBackend) {
    let temporary = tempfile::tempdir().unwrap();
    let (checked, local, journals) = fixture_backend(
        temporary.path(),
        DeploymentMode::Single,
        Confirmation::LocalDurable,
        1,
        backend,
    )
    .remove(0);
    storage(&checked, &local);
    let plan = journals.partitions[0].clone();
    let incarnation = plan.incarnation;
    let (devices, mut lanes) = DevicePools::start(&checked.plan).unwrap();
    let io = ozzy_io::Local::new(lanes.remove(0).client);
    assert!(
        Box::pin(plan.clone().open(io.clone(), JournalGeneration(1)))
            .await
            .is_err()
    );
    assert!(!plan.placement.directory.exists());
    let mut opened = Box::pin(plan.clone().format(io.clone(), JournalGeneration(2)))
        .await
        .unwrap();
    let budget = &checked.plan.shards[0].budget;
    let bytes = usize::try_from(budget.resident_bytes).unwrap();
    let memory = ozzy_runtime::memory::Domain::new(None, bytes)
        .unwrap()
        .owner(ozzy_runtime::memory::Limits {
            bytes,
            buffers: budget.append_slots,
            cache_bytes: bytes,
        })
        .unwrap();
    opened.journal.bind_append_memory(&memory).unwrap();
    let PartitionAuthority::Local(driver) = opened.authority else {
        panic!("wrong local mode")
    };
    let mut actor = LocalActor::new(
        opened.journal,
        driver,
        LocalActorConfig {
            proposal_lanes: 3,
            ..Default::default()
        },
        || 777,
    )
    .unwrap();
    append_records(&mut actor, incarnation).await;
    assert!(memory.allocated_bytes() > 0);
    assert!(memory.allocated_bytes() <= bytes);
    actor.shutdown().await.unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
    assert!(
        Box::pin(plan.clone().format(io.clone(), JournalGeneration(3)))
            .await
            .is_err()
    );
    let mut changed = principals(&checked.identity);
    changed.insert(local.broker, Digest::from_bytes([98; 32]));
    let altered = JournalPlan::new(&checked, &local, &changed)
        .unwrap()
        .partitions
        .remove(0);
    assert!(
        Box::pin(altered.open(io.clone(), JournalGeneration(4)))
            .await
            .is_err()
    );
    let mut wrong_store = local.clone();
    wrong_store.topics.get_mut("orders").unwrap()[0].store = Uuid::now_v7();
    let altered = JournalPlan::new(&checked, &wrong_store, &principals(&checked.identity))
        .unwrap()
        .partitions
        .remove(0);
    assert!(
        Box::pin(altered.open(io.clone(), JournalGeneration(4)))
            .await
            .is_err()
    );
    let opened = Box::pin(plan.open(io, JournalGeneration(4))).await.unwrap();
    let PartitionAuthority::Local(driver) = opened.authority else {
        panic!("wrong local mode")
    };
    assert_eq!(driver.snapshot().applied.op.0, 4);
    opened.journal.shutdown().await.unwrap();
    devices.shutdown().await;
}

async fn append_records(actor: &mut LocalActor, incarnation: ozzy_proto::PartitionIncarnation) {
    let producer = ProducerId::from_bytes([19; 16]);
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    for body in [
        OperationBody::CreatePartition(CreatePartition {
            partition: incarnation,
            stream: "test",
            topic: "orders",
            partition_id: PartitionId::ZERO,
            owner_epoch: OwnerEpoch::INITIAL,
            retention: RetentionPolicy::default(),
        }),
        OperationBody::OpenProducer(OpenProducer {
            partition: incarnation,
            producer_id: producer,
            expected_epoch: None,
            new_epoch: ProducerEpoch::INITIAL,
            operation_id: OperationId::from_bytes([20; 16]),
        }),
    ] {
        buffer
            .push(
                body.kind(),
                &encode_operation_body(&body, OperationLimits::default()).unwrap(),
            )
            .unwrap();
    }
    buffer = submit(actor, buffer).await;
    for (first, count, length) in [(0, 2048, 1), (2048, 1, 64 * 1024 - 512)] {
        buffer.clear();
        let mut random = 0x8123_4567_u32;
        let payload: Vec<_> = (0..length)
            .map(|_| {
                random ^= random << 13;
                random ^= random >> 17;
                random ^= random << 5;
                random as u8
            })
            .collect();
        buffer
            .prepare_append(ProducerAppend {
                partition: incarnation,
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: producer,
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(first),
                records: (0..count)
                    .map(|index| AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes(
                            (first + index + 1)
                                .to_be_bytes()
                                .repeat(2)
                                .try_into()
                                .unwrap(),
                        ),
                        parts: vec![payload.as_slice()].into(),
                    })
                    .collect(),
            })
            .unwrap();
        buffer = submit(actor, buffer).await;
    }
    assert_eq!(actor.snapshot().applied.op.0, 4);
    drop(buffer);
}
