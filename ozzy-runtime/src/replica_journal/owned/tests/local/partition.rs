use super::*;
use crate::replica_actor::{LocalActor, LocalActorConfig, ProposalOutcome};

fn definition() -> CreatePartition<'static> {
    CreatePartition {
        partition: partition(),
        stream: "stream",
        topic: "orders",
        partition_id: PartitionId::ZERO,
        owner_epoch: OwnerEpoch::INITIAL,
        retention: RetentionPolicy::default(),
    }
}

fn request(journal: &OwnedJournal) -> ProposalBuffer {
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer.prepare_partition(definition()).unwrap();
    buffer
}

fn resolved(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    buffer: ProposalBuffer,
) -> Prefix {
    let result = drive(
        controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    );
    let ProposalValidation::Resolved { through, .. } = result else {
        panic!("matching creation must resolve without another operation: {result:?}");
    };
    through
}

#[test]
fn owned_partition_creation_resolves_accepted_applied_and_reopened_state() {
    let (mut controller, io) = setup();
    let config = local_config();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    journal
        .bind_append_memory(&payload_owner(1024 * 1024))
        .unwrap();
    let buffer = request(&journal);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    let accepted = driver.snapshot().accepted;
    assert_eq!(accepted.op.0, 1);
    assert_eq!(driver.snapshot().applied, Prefix::GENESIS);
    let buffer = request(&journal);
    assert_eq!(
        resolved(&mut controller, &mut journal, &driver, buffer),
        accepted
    );
    assert_eq!(driver.snapshot().applied, Prefix::GENESIS);
    assert_eq!(journal.images().unwrap().speculative().revision(), 1);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let buffer = request(&journal);
    assert_eq!(
        resolved(&mut controller, &mut journal, &driver, buffer),
        accepted
    );
    let stale = request(&journal);
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    let buffer = request(&journal);
    assert_eq!(
        resolved(&mut controller, &mut journal, &driver, buffer),
        accepted
    );
    assert!(matches!(
        drive(
            &mut controller,
            journal.propose_append(driver.begin_validation().unwrap(), stale, 777)
        ),
        ProposalValidation::Rejected {
            reason: JournalError::AppendMismatch,
            ..
        }
    ));
    assert_eq!(driver.snapshot().accepted, accepted);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owned_partition_creation_rejects_changed_identity_without_changing_policy() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(local_config(), io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let before = driver.snapshot();
    for case in 0..5 {
        let mut changed = definition();
        match case {
            0 => changed.stream = "different",
            1 => changed.topic = "different",
            2 => changed.partition_id = PartitionId::new(1),
            3 => changed.owner_epoch = OwnerEpoch::new(2),
            _ => changed.partition = PartitionIncarnation::from_bytes([99; 16]),
        }
        let mut buffer = journal.lease_proposal_buffer().unwrap();
        buffer.prepare_partition(changed).unwrap();
        let result = drive(
            &mut controller,
            journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
        );
        assert!(
            matches!(result, ProposalValidation::Rejected { .. }),
            "{result:?}"
        );
        assert_eq!(driver.snapshot(), before);
    }
    let mut existing = definition();
    existing.retention.max_bytes = std::num::NonZeroU64::new(1234);
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer.prepare_partition(existing).unwrap();
    assert!(
        buffer
            .push(ozzy_journal::operation::OperationKind::Barrier, &[77; 16])
            .is_err()
    );
    assert_eq!(
        resolved(&mut controller, &mut journal, &driver, buffer),
        before.applied
    );
    assert_eq!(
        journal
            .images()
            .unwrap()
            .committed()
            .partition(partition())
            .unwrap()
            .retention,
        RetentionPolicy::default()
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn local_actor_partition_creation_retry_waits_for_durability() {
    let (mut controller, io) = setup();
    let (journal, driver) = drive(
        &mut controller,
        OwnedJournal::format_local(local_config(), io, JournalGeneration(1), 32768),
    )
    .unwrap();
    let mut actor = LocalActor::new(journal, driver, LocalActorConfig::default(), || 777).unwrap();
    let mut submitter = actor.take_submitter().unwrap();
    let mut pending: Vec<_> = (0..2)
        .map(|_| {
            let mut buffer = actor.lease_proposal_buffer().unwrap();
            buffer.prepare_partition(definition()).unwrap();
            Box::pin(submitter.try_submit(buffer).unwrap())
        })
        .collect();
    for _ in 0..100 {
        assert!(
            actor
                .poll_progress(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(actor.snapshot().accepted.op.0, 1);
    assert_eq!(actor.snapshot().applied, Prefix::GENESIS);
    assert!(
        pending
            .iter_mut()
            .all(|reply| poll(reply.as_mut()).is_pending())
    );
    for _ in 0..10000 {
        assert!(
            actor
                .poll_progress(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        pending.retain_mut(|reply| match poll(reply.as_mut()) {
            Poll::Pending => true,
            Poll::Ready(reply) => {
                assert!(matches!(reply.unwrap().outcome, ProposalOutcome::Committed { through, .. } if through.op.0 == 1));
                false
            }
        });
        if pending.is_empty() {
            break;
        }
    }
    assert!(pending.is_empty());
    assert_eq!(actor.snapshot().applied.op.0, 1);
    drive(&mut controller, actor.shutdown()).unwrap();
}
