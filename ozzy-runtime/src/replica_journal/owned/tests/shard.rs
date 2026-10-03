use super::*;
use crate::replica_journal::{
    JournalCompletion, ProposalValidation, ReplicaJournal, ShardJournal, ShardJournalConfig,
};
use ozzy_replication::{Admission, driver::ReplicaDriver};

#[cfg(feature = "simulation")]
mod actors;

type Adapter = ReplicaJournal<ShardJournal>;

fn adapter(
    controller: &mut Controller,
    io: Local,
    root: &str,
    id: u8,
    policy: QuorumPolicy,
    depth: usize,
) -> (Adapter, ReplicaDriver) {
    let mut config = config(root, id, policy);
    config.write_group_bytes = 1;
    let (mut owner, startup) = drive(
        controller,
        OwnedJournal::format_new(config, io, JournalGeneration(u128::from(id)), 32768),
    )
    .unwrap();
    owner
        .bind_append_memory(&payload_owner(1024 * 1024))
        .unwrap();
    let adapter = owner
        .into_shard_journal(
            ShardJournalConfig {
                write_depth: depth,
                turn_steps: 4,
                ..Default::default()
            },
            || 123,
        )
        .unwrap();
    (
        adapter,
        startup
            .into_driver(Duration::ZERO, timing(), pipeline())
            .unwrap(),
    )
}

fn pump(adapter: &mut Adapter) {
    let mut future = std::pin::pin!(adapter.stopped());
    assert!(poll(future.as_mut()).is_pending(), "adapter terminated");
}

fn complete<T>(
    controller: &mut Controller,
    adapter: &mut Adapter,
    completion: JournalCompletion<T>,
) -> Result<T, JournalError> {
    let mut completion = std::pin::pin!(completion);
    for step in 0..10000 {
        pump(adapter);
        if let Poll::Ready(result) = poll(completion.as_mut()) {
            return result;
        }
        if step % 16 == 15 {
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
    }
    panic!("adapter did not complete");
}

fn shutdown(controller: &mut Controller, adapter: &mut Adapter) {
    let mut close = std::pin::pin!(adapter.shutdown());
    for _ in 0..10000 {
        if let Poll::Ready(result) = poll(close.as_mut()) {
            result.unwrap();
            return;
        }
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    }
    panic!("shutdown did not complete");
}

fn admit(
    controller: &mut Controller,
    adapter: &mut Adapter,
    driver: &mut ReplicaDriver,
    value: u8,
) -> JournalCompletion<ozzy_replication::WriteTicket> {
    let mut buffer = adapter.lease_proposal_buffer().unwrap();
    buffer
        .push(
            ozzy_journal::operation::OperationKind::Barrier,
            &[value; 16],
        )
        .unwrap();
    let ticket = driver.begin_validation().unwrap();
    let request = adapter.propose_append(ticket, buffer).unwrap();
    let ProposalValidation::Ready(validated) = complete(controller, adapter, request).unwrap()
    else {
        panic!("fresh proposal");
    };
    let Admission::Write { ticket: write, .. } = driver
        .prepare_validated(
            NodeId::from_bytes([1; 16]),
            ticket,
            validated.prepared(),
            Duration::ZERO,
        )
        .unwrap()
    else {
        panic!("write");
    };
    let request = adapter.admit_append(write, validated).unwrap();
    complete(controller, adapter, request)
        .unwrap()
        .into_parts()
        .2
}

#[test]
fn shard_adapter_installs_reordered_writes_in_order_and_captures_exact_sync() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut journal, mut driver) = adapter(&mut controller, io, "/journal", 5, policy, 2);
        let first = admit(&mut controller, &mut journal, &mut driver, 1);
        let second = admit(&mut controller, &mut journal, &mut driver, 2);
        for _ in 0..16 {
            pump(&mut journal);
        }
        // Real backend jobs may finish in reverse order. Owner installation
        // stays ordered and only complete logical commands yield write tickets.
        let jobs = controller.jobs();
        assert_eq!(jobs.len(), 2, "two independently executing writes");
        for (id, _) in jobs.into_iter().rev() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        let first = complete(&mut controller, &mut journal, first).unwrap();
        driver.complete_write(first).unwrap();
        let second = complete(&mut controller, &mut journal, second).unwrap();
        driver.complete_write(second).unwrap();
        let sync = driver.begin_sync().unwrap();
        let request = journal.begin_pipelined_sync(sync).unwrap();
        let ready = complete(&mut controller, &mut journal, request).unwrap();
        let request = journal.finish_pipelined_sync(ready).unwrap();
        assert_eq!(
            complete(&mut controller, &mut journal, request).unwrap(),
            sync
        );
        driver.complete_sync(sync, Duration::ZERO).unwrap();
        assert_eq!(driver.normal().unwrap().snapshot().journal.durable.0, 2);
        shutdown(&mut controller, &mut journal);
    }
}

#[test]
fn shard_adapter_stalled_partition_does_not_stop_another_on_same_thread() {
    let (mut controller, io) = setup();
    let (mut blocked, mut first_driver) = adapter(
        &mut controller,
        io.clone(),
        "/blocked",
        5,
        QuorumPolicy::Durable,
        1,
    );
    let (mut healthy, mut second_driver) =
        adapter(&mut controller, io, "/healthy", 6, QuorumPolicy::Durable, 1);
    let abandoned = admit(&mut controller, &mut blocked, &mut first_driver, 1);
    drop(abandoned);
    for _ in 0..16 {
        pump(&mut blocked);
    }
    let held: Vec<_> = controller.jobs().into_iter().map(|(id, _)| id).collect();
    assert!(!held.is_empty());
    // Validate/admit on the other owner without releasing held file work.
    let mut buffer = healthy.lease_proposal_buffer().unwrap();
    buffer
        .push(ozzy_journal::operation::OperationKind::Barrier, &[2; 16])
        .unwrap();
    let ticket = second_driver.begin_validation().unwrap();
    let mut proposal = healthy.propose_append(ticket, buffer).unwrap();
    let validated = loop {
        pump(&mut healthy);
        if let Poll::Ready(result) = poll(Pin::new(&mut proposal)) {
            let ProposalValidation::Ready(validated) = result.unwrap() else {
                panic!("fresh proposal");
            };
            break validated;
        }
        for (id, _) in controller.jobs() {
            if !held.contains(&id) {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
    };
    let Admission::Write { ticket: write, .. } = second_driver
        .prepare_validated(
            NodeId::from_bytes([1; 16]),
            ticket,
            validated.prepared(),
            Duration::ZERO,
        )
        .unwrap()
    else {
        panic!("write");
    };
    let mut request = healthy.admit_append(write, validated).unwrap();
    let receipt = loop {
        pump(&mut healthy);
        if let Poll::Ready(result) = poll(Pin::new(&mut request)) {
            break result.unwrap().into_parts().2;
        }
    };
    let mut receipt = std::pin::pin!(receipt);
    let mut settled = false;
    for _ in 0..1000 {
        pump(&mut healthy);
        if let Poll::Ready(result) = poll(receipt.as_mut()) {
            second_driver.complete_write(result.unwrap()).unwrap();
            settled = true;
            break;
        }
        for (id, _) in controller.jobs() {
            if !held.contains(&id) {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
    }
    assert!(
        settled,
        "healthy partition stalled behind another partition"
    );
    assert!(held.iter().all(|id| controller.operation(*id).is_some()));
    // Shutdown owns/drains abandoned writes and itself remains cancel-safe.
    {
        let mut canceled = std::pin::pin!(blocked.shutdown());
        assert!(poll(canceled.as_mut()).is_pending());
    }
    assert!(matches!(
        blocked.lease_proposal_buffer(),
        Err(SubmitError::Stopped)
    ));
    for id in held {
        controller.execute(id, Effect::Normal).unwrap();
        controller.deliver(id).unwrap();
    }
    shutdown(&mut controller, &mut blocked);
    shutdown(&mut controller, &mut healthy);
}

#[test]
fn shard_adapter_write_failure_closes_admission_without_advancing_driver() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) =
        adapter(&mut controller, io, "/failed", 5, QuorumPolicy::Durable, 1);
    let mut receipt = admit(&mut controller, &mut journal, &mut driver, 1);
    for _ in 0..16 {
        pump(&mut journal);
    }
    let jobs = controller.jobs();
    assert_eq!(jobs.len(), 1);
    let id = jobs[0].0;
    controller
        .execute(id, Effect::FailAfter(std::io::ErrorKind::Other))
        .unwrap();
    controller.deliver(id).unwrap();
    let mut stopped = false;
    for _ in 0..100 {
        let mut wait = std::pin::pin!(journal.stopped());
        if let Poll::Ready(error) = poll(wait.as_mut()) {
            assert!(matches!(error, JournalError::Faulted));
            stopped = true;
            break;
        }
    }
    assert!(stopped);
    assert!(matches!(
        journal.lease_proposal_buffer(),
        Err(SubmitError::Stopped)
    ));
    assert!(matches!(poll(Pin::new(&mut receipt)), Poll::Ready(Err(_))));
    assert_eq!(driver.normal().unwrap().snapshot().journal.written.0, 0);
}
