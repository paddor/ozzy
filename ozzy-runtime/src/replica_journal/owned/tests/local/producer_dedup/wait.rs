use super::*;
use crate::replica_actor::{
    LocalActor, LocalActorConfig, PendingProposal, ProposalOutcome, ProposalReply,
};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::Wake;

fn actor(controller: &mut Controller, io: Local) -> LocalActor {
    let mut config = local_config();
    config.deduplication = Some(ozzy_core::dedup::Limits {
        window_millis: 300_000,
        ..limits()
    });
    config.append_buffers = 12;
    let (mut journal, mut driver) = drive(
        controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(controller, &mut journal, &mut driver);
    LocalActor::new(
        journal,
        driver,
        LocalActorConfig {
            proposal_capacity: 4,
            turn_steps: 4,
            ..Default::default()
        },
        || u64::MAX,
    )
    .unwrap()
}

#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn hold_sync(controller: &mut Controller, actor: &mut LocalActor) {
    for _ in 0..1000 {
        super::super::actor::pump(actor);
        for (id, _) in controller.jobs() {
            if !matches!(
                controller.operation(id).unwrap().unprotected(),
                Operation::Sync { .. }
            ) {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
        std::thread::yield_now();
    }
}

#[test]
fn redundant_producer_waits_for_original_sync_without_spinning_or_confirming_early() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io);
    let mut lane = actor.take_submitter().unwrap();
    let mut pending = Vec::new();
    for writer in [12, 30] {
        let mut buffer = actor.lease_proposal_buffer().unwrap();
        buffer.prepare_append(request(writer, 0, 1)).unwrap();
        pending.push(Box::pin(lane.try_submit(buffer).unwrap()));
    }
    for _ in 0..100 {
        super::super::actor::pump(&mut actor);
    }
    assert!(actor.snapshot().accepted.op.0 <= 4);
    assert_eq!(actor.snapshot().applied.op.0, 3);
    assert!(
        pending
            .iter_mut()
            .all(|work| poll(work.as_mut()).is_pending())
    );

    // Deliver writes and metadata jobs, but never a sync. Transport receipt and
    // write completion still do not establish the original's confirmation.
    hold_sync(&mut controller, &mut actor);
    assert_eq!(actor.snapshot().accepted.op.0, 4);
    assert_eq!(actor.snapshot().applied.op.0, 3);
    assert!(
        pending
            .iter_mut()
            .all(|work| poll(work.as_mut()).is_pending())
    );
    let counter = Arc::new(Wakes::default());
    let waker = Waker::from(counter.clone());
    assert!(
        actor
            .poll_progress(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(
        counter.0.load(Ordering::Relaxed),
        0,
        "wait must sleep on original progress"
    );

    let mut replies = Vec::new();
    for mut work in pending {
        let mut reply = None;
        for _ in 0..10000 {
            super::super::actor::pump(&mut actor);
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
            if let Poll::Ready(result) = poll(work.as_mut()) {
                reply = Some(result.unwrap());
                break;
            }
        }
        replies.push(reply.expect("deferred redundant producer must resume"));
    }
    for reply in &replies {
        assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }));
    }
    let first = replies[0].buffer.0.operations().next().unwrap();
    let second = replies[1].buffer.0.operations().next().unwrap();
    assert_eq!(
        decode_resolved_append(
            first.body,
            ozzy_journal::operation::OperationLimits::default()
        )
        .unwrap()
        .results
        .stored(),
        1
    );
    let second = decode_resolved_append(
        second.body,
        ozzy_journal::operation::OperationLimits::default(),
    )
    .unwrap();
    assert_eq!(second.results.stored(), 0);
    assert_eq!(
        second
            .results
            .attempts_with_fingerprints()
            .next()
            .unwrap()
            .0,
        duplicate(0)
    );
    assert_eq!(actor.snapshot().applied.op.0, 5);
    super::super::actor::close(&mut controller, actor);
}

#[test]
fn fencing_a_deferred_producer_never_admits_its_duplicate_results() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io);
    let mut lane = actor.take_submitter().unwrap();
    let mut first = actor.lease_proposal_buffer().unwrap();
    first.prepare_append(request(12, 0, 1)).unwrap();
    let first = lane.try_submit(first).unwrap();
    let mut second = actor.lease_proposal_buffer().unwrap();
    second.prepare_append(request(30, 0, 1)).unwrap();
    let mut second = Box::pin(lane.try_submit(second).unwrap());
    for _ in 0..100 {
        super::super::actor::pump(&mut actor);
    }
    assert!(poll(second.as_mut()).is_pending());
    hold_sync(&mut controller, &mut actor);
    second.fence_unadmitted();
    for _ in 0..100 {
        super::super::actor::pump(&mut actor);
    }
    let Poll::Ready(reply) = poll(second.as_mut()) else {
        panic!("fenced wait must resolve")
    };
    assert!(matches!(
        reply.unwrap().outcome,
        ProposalOutcome::NotAdmitted
    ));
    assert_eq!(actor.snapshot().accepted.op.0, 4);
    drop(first);
    super::super::actor::close(&mut controller, actor);
}

#[test]
fn other_producers_proceed_while_a_duplicate_waits_for_its_original() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io);
    let mut lane = actor.take_submitter().unwrap();
    // Writer 30 repeats writer 12's records; its second APPEND arrives while
    // its first still waits and must stay behind it.
    let mut pending = Vec::new();
    for (writer, first) in [(12, 0), (30, 0), (12, 1), (30, 1)] {
        let mut buffer = actor.lease_proposal_buffer().unwrap();
        buffer.prepare_append(request(writer, first, 1)).unwrap();
        pending.push(Box::pin(lane.try_submit(buffer).unwrap()));
    }
    hold_sync(&mut controller, &mut actor);
    assert_eq!(
        actor.snapshot().accepted.op.0,
        5,
        "writer 12 keeps admitting while writer 30 waits"
    );
    assert_eq!(actor.snapshot().applied.op.0, 3);
    for (mut work, stored) in pending.into_iter().zip([1, 0, 1, 0]) {
        let mut reply = None;
        for _ in 0..10000 {
            super::super::actor::pump(&mut actor);
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
            if let Poll::Ready(result) = poll(work.as_mut()) {
                reply = Some(result.unwrap());
                break;
            }
        }
        let reply = reply.expect("every proposal must finish");
        assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }));
        let operation = reply.buffer.0.operations().next().unwrap();
        let resolved = decode_resolved_append(
            operation.body,
            ozzy_journal::operation::OperationLimits::default(),
        )
        .unwrap();
        assert_eq!(resolved.results.stored(), stored);
    }
    super::super::actor::close(&mut controller, actor);
}

/// Deliver every job until `work` completes.
fn complete(
    controller: &mut Controller,
    actor: &mut LocalActor,
    work: &mut Pin<Box<PendingProposal>>,
) -> ProposalReply {
    for _ in 0..10000 {
        super::super::actor::pump(actor);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        if let Poll::Ready(result) = poll(work.as_mut()) {
            return result.unwrap();
        }
    }
    panic!("proposal did not complete");
}

#[test]
fn fencing_a_parked_duplicate_releases_the_producers_queued_appends() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io);
    let mut lane = actor.take_submitter().unwrap();
    // Writer 12's original stays accepted-only under held syncs. Writer 30's
    // duplicate parks behind it and its fresh record queues behind that.
    let mut pending = Vec::new();
    for (writer, first) in [(12, 0), (30, 0), (30, 1)] {
        let mut buffer = actor.lease_proposal_buffer().unwrap();
        buffer.prepare_append(request(writer, first, 1)).unwrap();
        pending.push(Box::pin(lane.try_submit(buffer).unwrap()));
    }
    hold_sync(&mut controller, &mut actor);
    assert_eq!(actor.snapshot().accepted.op.0, 4);
    assert_eq!(actor.snapshot().applied.op.0, 3);
    assert!(
        pending
            .iter_mut()
            .all(|work| poll(work.as_mut()).is_pending())
    );
    pending[1].fence_unadmitted();
    hold_sync(&mut controller, &mut actor);
    let Poll::Ready(fenced) = poll(pending[1].as_mut()) else {
        panic!("fenced parked proposal must finish");
    };
    assert!(matches!(
        fenced.unwrap().outcome,
        ProposalOutcome::NotAdmitted
    ));
    assert_eq!(actor.snapshot().accepted.op.0, 4);
    let mut original = pending.remove(0);
    let mut follow = pending.remove(1);
    assert!(matches!(
        complete(&mut controller, &mut actor, &mut original).outcome,
        ProposalOutcome::Committed { .. }
    ));
    // The queued request validates once the journal settles and fails its
    // sequence check, so the writer replays from its confirmed prefix.
    assert!(matches!(
        complete(&mut controller, &mut actor, &mut follow).outcome,
        ProposalOutcome::Invalid(crate::replica_journal::JournalError::ProducerAppend(
            AppendAdmissionError::SequenceGap
        ))
    ));
    assert_eq!(actor.snapshot().applied.op.0, 4);
    super::super::actor::close(&mut controller, actor);
}
