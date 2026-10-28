use super::*;
use crate::replica_journal::{AppendAdmissionError, DelayLimits};
use ozzy_core::state::ProducerResultSource;
use ozzy_journal::operation::{OperationKind, OperationLimits, decode_operation_body};
use ozzy_proto::{ProducerSequence, append::AppendDisposition};

mod release;

const NOW: u64 = 10_000;

fn delayed(max_entries: u64) -> (Controller, OwnedJournal, Driver) {
    delayed_with(DelayLimits::new(max_entries))
}

fn delayed_with(limits: DelayLimits) -> (Controller, OwnedJournal, Driver) {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.delay = Some(limits);
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    (controller, journal, driver)
}

/// Propose one streaming request, scheduled when `due` is present.
fn produce(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    request: crate::replica_journal::ProducerAppend<'static>,
    due: Option<&[u64]>,
    timestamp: u64,
) -> ProposalValidation {
    let ticket = driver.begin_validation().unwrap();
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_stream_append(
            ozzy_proto::append::Authority {
                group_id: ticket.scope().group_id,
                config_epoch: ticket.scope().configuration_epoch,
                view: ticket.scope().view,
            },
            request,
        )
        .unwrap();
    if let Some(due) = due {
        buffer.schedule(due).unwrap();
    }
    drive(
        controller,
        journal.propose_append(ticket, buffer, timestamp),
    )
}

fn accept(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    result: ProposalValidation,
) {
    let ProposalValidation::Ready(mut validated) = result else {
        panic!("ready: {result:?}")
    };
    assert!(journal.can_admit(&mut validated));
    let ticket = driver
        .prepare_validated(validated.validation(), validated.prepared())
        .unwrap();
    let receipt = journal
        .admit_append(ticket, validated)
        .unwrap()
        .into_parts()
        .2;
    write(controller, journal, driver, receipt);
    sync_apply(controller, journal, driver);
}

/// The proposed `Schedule`: first schedule sequence, first offset,
/// timestamp, first producer sequence, and due times.
struct Proposed {
    first_schedule: u64,
    first_offset: Offset,
    timestamp: u64,
    first_sequence: ProducerSequence,
    due_millis: Vec<u64>,
}

fn proposed(result: &ProposalValidation) -> Proposed {
    let ProposalValidation::Ready(validated) = result else {
        panic!("ready: {result:?}")
    };
    let mut operations = validated.buffer().operations();
    let operation = operations.next().unwrap();
    assert!(operations.next().is_none());
    assert_eq!(operation.kind, OperationKind::Schedule);
    let OperationBody::Schedule(body) =
        decode_operation_body(operation.kind, operation.body, OperationLimits::default()).unwrap()
    else {
        unreachable!()
    };
    Proposed {
        first_schedule: body.first_schedule,
        first_offset: body.batch.first_offset,
        timestamp: body.batch.append_timestamp_millis,
        first_sequence: body.batch.first_sequence,
        due_millis: body.due_millis,
    }
}

fn rejection(result: ProposalValidation) -> JournalError {
    let ProposalValidation::Rejected { reason, buffer } = result else {
        panic!("rejected: {result:?}")
    };
    // A refused schedule returns to its original APPEND entry.
    assert!(
        buffer
            .bodies()
            .all(|(kind, _)| kind == OperationKind::Append)
    );
    reason
}

fn held(journal: &OwnedJournal) -> (u64, u64, Offset) {
    let state = journal
        .images()
        .unwrap()
        .committed()
        .partition(partition())
        .unwrap();
    let delay = state.delay.as_ref().unwrap();
    (delay.held_records(), delay.next_schedule, state.next_offset)
}

#[test]
fn scheduled_batch_takes_schedule_sequences_and_the_acceptance_time() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let due = [NOW + 5_000, NOW + 1_000, NOW - 1];
    let result = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 3),
        Some(&due),
        NOW,
    );
    let body = proposed(&result);
    assert_eq!(body.first_schedule, 0);
    assert_eq!(body.first_offset, Offset::ZERO);
    assert_eq!(body.timestamp, NOW);
    assert_eq!(body.due_millis, due);
    let ProposalValidation::Ready(validated) = &result else {
        unreachable!()
    };
    let reply = validated.buffer().scheduled_result().unwrap();
    assert_eq!(reply.disposition(), AppendDisposition::Scheduled);
    assert_eq!((reply.first_offset, reply.records), (Offset::ZERO, 3));
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (3, 3, Offset::ZERO));

    // A leader clock behind the latest schedule keeps that time.
    let result = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 3, 2),
        Some(&[NOW, NOW]),
        NOW - 500,
    );
    let body = proposed(&result);
    assert_eq!((body.first_schedule, body.timestamp), (3, NOW));
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (5, 5, Offset::ZERO));
}

#[test]
fn retry_gets_the_original_schedule_sequences_and_a_fresh_suffix() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let due = [NOW + 1, NOW + 2, NOW + 3, NOW + 4, NOW + 5];
    let first = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 3),
        Some(&due[..3]),
        NOW,
    );
    accept(&mut controller, &mut journal, &mut driver, first);

    let retry = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 3),
        Some(&due[..3]),
        NOW + 9,
    );
    let ProposalValidation::Resolved { buffer, .. } = retry else {
        panic!("resolved: {retry:?}")
    };
    let [span] = buffer.producer_retry_results() else {
        panic!("one retry span")
    };
    assert_eq!(span.disposition(), AppendDisposition::Scheduled);
    assert!(matches!(
        span.source,
        ProducerResultSource::Scheduled { operation } if operation > 0
    ));
    assert_eq!((span.first_offset, span.records), (Offset::ZERO, 3));

    // A resend that overlaps held records keeps them and schedules the rest.
    let wider = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 5),
        Some(&due),
        NOW + 9,
    );
    let body = proposed(&wider);
    assert_eq!(body.first_schedule, 3);
    assert_eq!(body.first_sequence, ProducerSequence::new(3));
    assert_eq!(body.due_millis, due[3..]);
    let ProposalValidation::Ready(validated) = &wider else {
        unreachable!()
    };
    let spans = &validated.buffer().retry_results.as_ref().unwrap().ranges;
    assert_eq!(spans.len(), 2);
    assert!(
        spans
            .iter()
            .all(|span| span.disposition() == AppendDisposition::Scheduled)
    );
    assert_eq!(
        (spans[1].first_offset, spans[1].records),
        (Offset::new(3), 2)
    );
    accept(&mut controller, &mut journal, &mut driver, wider);
    assert_eq!(held(&journal), (5, 5, Offset::ZERO));
}

#[test]
fn stale_validated_schedule_proposes_again_as_its_append() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let due = [NOW + 2, NOW + 1];
    let result = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 2),
        Some(&due),
        NOW,
    );
    let ProposalValidation::Ready(validated) = result else {
        panic!("ready: {result:?}")
    };
    let mut buffer = ProposalBuffer::from(validated.into_buffer());
    buffer.revert_schedule().unwrap();
    assert!(
        buffer
            .bodies()
            .all(|(kind, _)| kind == OperationKind::Append)
    );
    let result = drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, NOW + 1),
    );
    let body = proposed(&result);
    assert_eq!(
        (body.first_schedule, body.timestamp, body.due_millis),
        (0, NOW + 1, due.to_vec())
    );
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (2, 2, Offset::ZERO));
}

#[test]
fn resend_with_other_bytes_due_time_or_kind_conflicts() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let due = [NOW + 1, NOW + 2];
    let first = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 2),
        Some(&due),
        NOW,
    );
    accept(&mut controller, &mut journal, &mut driver, first);
    let immediate = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(30, 0, 1),
        None,
        NOW,
    );
    accept(&mut controller, &mut journal, &mut driver, immediate);

    let mut changed = request(12, 0, 2);
    changed.records[1].parts = vec![b"other".as_slice()].into();
    let resends = [
        (request(12, 0, 2), Some([NOW + 1, NOW + 3].as_slice())),
        (changed, Some(due.as_slice())),
        (request(12, 0, 2), None),
        (request(30, 0, 1), Some([NOW].as_slice())),
    ];
    for (resend, due) in resends {
        let result = produce(&mut controller, &mut journal, &driver, resend, due, NOW);
        assert!(matches!(
            rejection(result),
            JournalError::ProducerAppend(AppendAdmissionError::RetryConflict)
        ));
    }
}

#[test]
fn leader_refuses_late_due_times_oversized_batches_full_capacity_and_plain_partitions() {
    let (mut controller, mut journal, mut driver) = delayed(2);
    let late = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 1),
        Some(&[NOW + DELAY_MAX_MILLIS + 1]),
        NOW,
    );
    assert!(matches!(
        rejection(late),
        JournalError::ProducerAppend(AppendAdmissionError::DueOutOfRange)
    ));
    let oversized = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 3),
        Some(&[NOW; 3]),
        NOW,
    );
    assert!(matches!(
        rejection(oversized),
        JournalError::ProducerAppend(AppendAdmissionError::ScheduleTooLarge)
    ));
    let edge = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 2),
        Some(&[NOW + DELAY_MAX_MILLIS, NOW]),
        NOW,
    );
    accept(&mut controller, &mut journal, &mut driver, edge);
    let full = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 2, 1),
        Some(&[NOW]),
        NOW,
    );
    assert!(matches!(
        rejection(full),
        JournalError::ProducerAppend(AppendAdmissionError::ScheduleCapacity)
    ));

    let (mut controller, io) = setup();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(local_config(), io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let plain = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 1),
        Some(&[NOW]),
        NOW,
    );
    assert!(matches!(
        rejection(plain),
        JournalError::ProducerAppend(AppendAdmissionError::DelayDisabled)
    ));
}

/// Pump the actor and run its storage jobs until `done`.
fn run_until(
    controller: &mut Controller,
    actor: &mut crate::replica_actor::LocalActor,
    mut done: impl FnMut(&mut crate::replica_actor::LocalActor) -> bool,
) {
    for _ in 0..10000 {
        super::actor::pump(actor);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        if done(actor) {
            return;
        }
    }
    panic!("actor did not progress");
}

#[test]
fn local_actor_releases_held_records_at_their_due_times() {
    use std::time::Duration;
    let clock = std::rc::Rc::new(std::cell::Cell::new(NOW));
    let (mut controller, io) = setup();
    let mut actor = super::actor::actor_with_options(
        &mut controller,
        io,
        1,
        &[12, 30],
        partition(),
        PartitionId::ZERO,
        super::actor::ActorOptions {
            delay: Some(DelayLimits::new(16)),
            clock: Some(clock.clone()),
            ..Default::default()
        },
    );
    let mut lane = actor.take_submitter().unwrap();
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    buffer.prepare_append(request(12, 0, 2)).unwrap();
    buffer.schedule(&[NOW + 50, NOW + 10]).unwrap();
    let mut pending = Box::pin(lane.try_submit(buffer).unwrap());
    run_until(&mut controller, &mut actor, |_| {
        let Poll::Ready(reply) = poll(pending.as_mut()) else {
            return false;
        };
        assert!(matches!(
            reply.unwrap().outcome,
            crate::replica_actor::ProposalOutcome::Committed { .. }
        ));
        true
    });
    let at = |actor: &crate::replica_actor::LocalActor| {
        actor
            .facts(partition())
            .map(|facts| (facts.next_offset, actor.next_deadline(Duration::ZERO)))
    };
    // The actor sleeps until the first due time, on its own clock.
    let first = Some((Offset::ZERO, Some(Duration::from_millis(10))));
    run_until(&mut controller, &mut actor, |actor| at(actor) == first);
    clock.set(NOW + 10);
    let second = Some((Offset::new(1), Some(Duration::from_millis(40))));
    run_until(&mut controller, &mut actor, |actor| at(actor) == second);
    clock.set(NOW + 50);
    run_until(&mut controller, &mut actor, |actor| {
        at(actor) == Some((Offset::new(2), None))
    });
    super::actor::close(&mut controller, actor);
}

/// Pump the actor and run its storage jobs, except segment writes, until
/// `done`. Accepted operations then stay pending in the pipeline.
fn run_holding_writes(
    controller: &mut Controller,
    actor: &mut crate::replica_actor::LocalActor,
    mut done: impl FnMut(&mut crate::replica_actor::LocalActor) -> bool,
) {
    for _ in 0..10000 {
        super::actor::pump(actor);
        for (id, _) in controller.jobs() {
            let operation = controller.operation(id).unwrap().unprotected();
            if !matches!(operation, Operation::Write { .. }) {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
        if done(actor) {
            return;
        }
    }
    panic!("actor did not progress");
}

#[test]
fn local_actor_holds_a_due_release_until_the_pipeline_has_room() {
    let clock = std::rc::Rc::new(std::cell::Cell::new(NOW));
    let (mut controller, io) = setup();
    let mut actor = super::actor::actor_with_options(
        &mut controller,
        io,
        1,
        &[12, 30],
        partition(),
        PartitionId::ZERO,
        super::actor::ActorOptions {
            delay: Some(DelayLimits::new(16)),
            clock: Some(clock.clone()),
            proposals: Some(16),
            ..Default::default()
        },
    );
    let mut lane = actor.take_submitter().unwrap();
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    buffer.prepare_append(request(12, 0, 1)).unwrap();
    buffer.schedule(&[NOW + 10]).unwrap();
    let mut held = Box::pin(lane.try_submit(buffer).unwrap());
    run_until(&mut controller, &mut actor, |_| {
        poll(held.as_mut()).is_ready()
    });
    // Stored records fill the pipeline while their writes wait.
    let room = actor.snapshot().pending_operations + pipeline().max_operations;
    let mut stored = Vec::new();
    run_holding_writes(&mut controller, &mut actor, |actor| {
        let full = actor.snapshot().pending_operations == room;
        if !full && let Ok(mut buffer) = actor.lease_proposal_buffer() {
            buffer
                .prepare_append(request(30, stored.len() as u64, 1))
                .unwrap();
            if let Ok(pending) = lane.try_submit(buffer) {
                stored.push(Box::pin(pending));
            }
        }
        full
    });
    // The release comes due on a full pipeline. It waits instead of
    // failing the actor.
    clock.set(NOW + 10);
    let offset = |actor: &crate::replica_actor::LocalActor| {
        actor.facts(partition()).map(|facts| facts.next_offset)
    };
    let before = offset(&actor);
    let mut rounds = 0;
    run_holding_writes(&mut controller, &mut actor, |_| {
        rounds += 1;
        rounds == 100
    });
    assert_eq!(offset(&actor), before);
    let total = stored.len() as u64 + 1;
    run_until(&mut controller, &mut actor, |actor| {
        offset(actor) == Some(Offset::new(total))
    });
    for mut pending in stored {
        let Poll::Ready(reply) = poll(pending.as_mut()) else {
            panic!("a stored record stayed unconfirmed");
        };
        assert!(matches!(
            reply.unwrap().outcome,
            crate::replica_actor::ProposalOutcome::Committed { .. }
        ));
    }
    super::actor::close(&mut controller, actor);
}

#[test]
fn local_actor_charges_the_schedule_trailer_before_admission() {
    let clock = std::rc::Rc::new(std::cell::Cell::new(NOW));
    let (mut controller, io) = setup();
    let mut actor = super::actor::actor_with_options(
        &mut controller,
        io,
        1,
        &[12, 30],
        partition(),
        PartitionId::ZERO,
        super::actor::ActorOptions {
            delay: Some(DelayLimits::new(64)),
            clock: Some(clock.clone()),
            ..Default::default()
        },
    );
    let mut lane = actor.take_submitter().unwrap();
    // Validation adds 8 bytes per record and one more field to the body.
    let due = [NOW + 10; 40];
    let mut held = actor.lease_proposal_buffer().unwrap();
    held.prepare_append(request(12, 0, due.len())).unwrap();
    held.schedule(&due).unwrap();
    let trailer = 8 * (due.len() + 1);
    let room = pipeline().max_body_bytes - held.body_bytes();
    // A stored append leaves room for the scheduled body but not its
    // trailer.
    let stored_bytes = |records| {
        let mut buffer = actor.lease_proposal_buffer().unwrap();
        buffer.prepare_append(request(30, 0, records)).unwrap();
        buffer.body_bytes()
    };
    let records = (1..=1000)
        .take_while(|&n| stored_bytes(n) <= room)
        .last()
        .unwrap();
    let stored_body = stored_bytes(records);
    assert!(stored_body + trailer > room);
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    buffer.prepare_append(request(30, 0, records)).unwrap();
    let mut stored = Box::pin(lane.try_submit(buffer).unwrap());
    run_holding_writes(&mut controller, &mut actor, |actor| {
        actor.snapshot().pending_body_bytes == stored_body
    });
    // The scheduled batch waits for the stored write instead of failing
    // the actor.
    let mut held = Box::pin(lane.try_submit(held).unwrap());
    let mut rounds = 0;
    run_holding_writes(&mut controller, &mut actor, |_| {
        rounds += 1;
        rounds == 100
    });
    assert_eq!(actor.snapshot().pending_body_bytes, stored_body);
    for pending in [&mut stored, &mut held] {
        run_until(&mut controller, &mut actor, |_| {
            let Poll::Ready(reply) = poll(pending.as_mut()) else {
                return false;
            };
            assert!(matches!(
                reply.unwrap().outcome,
                crate::replica_actor::ProposalOutcome::Committed { .. }
            ));
            true
        });
    }
    super::actor::close(&mut controller, actor);
}
