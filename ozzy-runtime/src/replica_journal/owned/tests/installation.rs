use super::*;
use crate::replica_journal::InstallationConfig;
use ozzy_journal::operation::{CanonicalOperation, OperationKind, logical_operation_digest};
use ozzy_journal_segment::{BodyEncoding, LogPosition};
use ozzy_proto::RequestId;
use ozzy_replication::{
    InstallTicket, LogSource, OpNumber,
    driver::{Action, ReplicaDriver},
    wire::{Control, FetchOps},
};

struct Voter {
    config: OwnedConfig,
    journal: OwnedJournal,
    driver: ReplicaDriver,
    source: LogSource,
}

fn voter(
    controller: &mut Controller,
    io: Local,
    index: u8,
    count: u8,
    policy: QuorumPolicy,
) -> Voter {
    let mut config = config(&format!("/broker-{index}"), 7, policy);
    config.identity.replica_node_id = NodeId::from_bytes([index + 1; 16]);
    let (mut journal, _) = drive(
        controller,
        OwnedJournal::format_new(
            config.clone(),
            io.clone(),
            JournalGeneration(u128::from(index) + 1),
            32768,
        ),
    )
    .unwrap();
    // Seed pre-restart storage through the production async segment engine.
    // Normal APPEND admission is tested separately, not bypassed by this fixture.
    let stored = journal.journal.ready_mut().unwrap();
    drive(controller, async {
        let mut end = Prefix::GENESIS;
        let mut committed = LogPosition::GENESIS;
        for number in 1..=count {
            let bytes = [number; 16];
            let operation = CanonicalOperation {
                group_id: config.identity.group_id,
                configuration_epoch: 1,
                original_view: 0,
                op_number: u64::from(number),
                previous_digest: end.digest,
                kind: OperationKind::Barrier,
                body: &bytes,
            };
            end = Prefix {
                op: OpNumber(u64::from(number)),
                digest: logical_operation_digest(&operation),
            };
            let written = stored
                .append(&[operation], BodyEncoding::Raw)
                .await
                .unwrap();
            stored.sync_through(written).await.unwrap();
            if number == 1 {
                committed = crate::replica_journal::authority::position(end);
            }
        }
        stored.publish_durable_progress().await.unwrap();
        let mut next = stored.manifest().clone();
        next.parent_generation = next.generation;
        next.generation += 1;
        next.accepted = crate::replica_journal::authority::position(end);
        next.committed = committed;
        stored.install_metadata(next).await.unwrap();
    });
    drive(controller, journal.shutdown()).unwrap();
    // The retained tail exceeds both the transfer and live transition window.
    config.append_limits.max_operations = 1;
    config.recovery.accepted_transitions = 1;
    config.writeback.max_operations = 1;
    let (mut journal, startup) = drive(
        controller,
        OwnedJournal::open(
            config.clone(),
            io,
            JournalGeneration(u128::from(index) + 11),
        ),
    )
    .unwrap();
    let source = LogSource {
        voter: config.identity.replica_node_id,
        generation: startup.generation(),
        accepted: startup.recovered().unwrap().log.accepted,
    };
    let mut driver = startup
        .into_driver(Duration::ZERO, timing(), config.append_limits)
        .unwrap();
    let Some(Action::PersistPromise(ticket)) = driver.poll(Duration::ZERO).unwrap() else {
        panic!("restart requires promise")
    };
    driver
        .complete_promise(drive(controller, journal.persist_promise(ticket)).unwrap())
        .unwrap();
    journal.capture_history(source).unwrap();
    Voter {
        config,
        journal,
        driver,
        source,
    }
}

fn selected(controller: &mut Controller, io: Local, policy: QuorumPolicy) -> (Voter, Voter) {
    let mut primary = voter(controller, io.clone(), 1, 3, policy);
    let mut backup = voter(controller, io, 2, 2, policy);
    let Some(Action::Broadcast(left)) = primary.driver.poll(Duration::ZERO).unwrap() else {
        panic!("start")
    };
    let Some(Action::Broadcast(right)) = backup.driver.poll(Duration::ZERO).unwrap() else {
        panic!("start")
    };
    primary
        .driver
        .receive(backup.source.voter, right, Duration::ZERO)
        .unwrap();
    backup
        .driver
        .receive(primary.source.voter, left, Duration::ZERO)
        .unwrap();
    assert_eq!(primary.driver.poll(Duration::ZERO).unwrap(), None);
    let Some(Action::Send { to, message }) = backup.driver.poll(Duration::ZERO).unwrap() else {
        panic!("report")
    };
    assert_eq!(to, primary.source.voter);
    primary
        .driver
        .receive(backup.source.voter, message, Duration::ZERO)
        .unwrap();
    let common = drive(
        controller,
        primary
            .journal
            .history_position(primary.source, backup.source.accepted.op),
    )
    .unwrap();
    assert_eq!(common.position, Some(backup.source.accepted));
    let protected = drive(
        controller,
        primary
            .journal
            .history_position(primary.source, OpNumber(1)),
    )
    .unwrap();
    let result = primary
        .driver
        .select(|source, op| {
            [common, protected]
                .iter()
                .find(|entry| source == entry.source && op == entry.op)
                .and_then(|entry| entry.position.map(|position| position.digest))
        })
        .unwrap();
    assert_eq!(result.source(), primary.source);
    (primary, backup)
}

fn policy() -> InstallationConfig {
    InstallationConfig {
        segment_capacity: 16384,
        body_encoding: BodyEncoding::Raw,
        max_staged_bytes: 128 * 1024,
        max_orphan_probes: 16,
    }
}

fn request(ticket: InstallTicket) -> FetchOps {
    FetchOps {
        scope: ticket.scope(),
        request_id: RequestId::from_bytes([71; 16]),
        source: ticket.source(),
        predecessor: ticket.protected_committed(),
        max_operations: 1,
        max_body_bytes: 8192,
    }
}

fn stage_local(controller: &mut Controller, voter: &mut Voter) -> InstallTicket {
    let ticket = voter
        .driver
        .begin_primary_install(JournalGeneration(101))
        .unwrap();
    drive(
        controller,
        voter.journal.begin_installation(ticket, policy()),
    )
    .unwrap();
    let mut fetch = request(ticket);
    let mut buffer = voter.journal.lease_append_buffer().unwrap();
    while fetch.predecessor != ticket.accepted() {
        let history = drive(controller, voter.journal.fetch_history(fetch, buffer)).unwrap();
        assert_eq!(history.request(), fetch);
        let chunk = drive(
            controller,
            voter.journal.install_chunk(ticket, history.into_buffer()),
        )
        .unwrap();
        if !chunk.prepared().is_empty() {
            voter
                .driver
                .validate_install_suffix(chunk.prepared())
                .unwrap();
        }
        fetch.predecessor = chunk.end();
        buffer = chunk.into_buffer();
        buffer.clear();
    }
    ticket
}

fn finish(controller: &mut Controller, voter: &mut Voter, ticket: InstallTicket) {
    let installed = drive(controller, voter.journal.finish_installation(ticket)).unwrap();
    assert_eq!(installed.applied(), ticket.committed());
    assert!(voter.journal.images().is_err());
    voter
        .driver
        .complete_installation(installed.ticket(), installed.applied(), Duration::ZERO)
        .unwrap();
}

fn installed_pair(controller: &mut Controller, io: Local, mode: QuorumPolicy) -> (Voter, Voter) {
    let (mut primary, mut backup) = selected(controller, io, mode);
    let ticket = stage_local(controller, &mut primary);
    finish(controller, &mut primary, ticket);
    assert!(primary.driver.begin_activation().is_err());
    assert!(primary.driver.begin_validation().is_err());
    // Old generation remains readable after replacement publication.
    let old = drive(
        controller,
        primary
            .journal
            .history_position(primary.source, primary.source.accepted.op),
    )
    .unwrap();
    assert_eq!(old.position, Some(primary.source.accepted));
    primary.journal.release_history(primary.source).unwrap();
    let start = primary
        .driver
        .normal()
        .unwrap()
        .start_view()
        .unwrap()
        .unwrap();
    let source = LogSource {
        voter: primary.source.voter,
        generation: start.generation,
        accepted: start.accepted,
    };
    primary.journal.capture_history(source).unwrap();
    let protected = drive(
        controller,
        primary.journal.history_position(source, start.committed.op),
    )
    .unwrap();
    backup
        .driver
        .receive(
            primary.source.voter,
            Control::StartView(start),
            Duration::ZERO,
        )
        .unwrap();
    let ticket = backup
        .driver
        .begin_backup_install(
            primary.source.voter,
            start,
            JournalGeneration(102),
            |source, op| {
                (source == protected.source && op == protected.op)
                    .then(|| protected.position.unwrap().digest)
            },
        )
        .unwrap();
    drive(
        controller,
        backup.journal.begin_installation(ticket, policy()),
    )
    .unwrap();
    let mut fetch = request(ticket);
    let mut output = primary.journal.lease_append_buffer().unwrap();
    let mut input = backup.journal.lease_append_buffer().unwrap();
    while fetch.predecessor != ticket.accepted() {
        let history = drive(controller, primary.journal.fetch_history(fetch, output)).unwrap();
        for operation in history.buffer().operations() {
            input.push(operation).unwrap();
        }
        let chunk = drive(controller, backup.journal.install_chunk(ticket, input)).unwrap();
        if !chunk.prepared().is_empty() {
            backup
                .driver
                .validate_install_suffix(chunk.prepared())
                .unwrap();
        }
        fetch.predecessor = chunk.end();
        input = chunk.into_buffer();
        input.clear();
        output = history.into_buffer();
        output.clear();
    }
    finish(controller, &mut backup, ticket);
    let normal = backup.driver.normal().unwrap();
    let confirmation = match mode {
        QuorumPolicy::Durable => Control::PrepareOk {
            ack: normal.acknowledgment().unwrap(),
        },
        QuorumPolicy::Replicated => Control::PrepareRetained {
            ack: normal.retained_acknowledgment().unwrap(),
        },
    };
    primary
        .driver
        .receive(backup.source.voter, confirmation, Duration::ZERO)
        .unwrap();
    (primary, backup)
}

#[test]
fn owned_selected_history_requires_new_view_agreement_and_exact_activation() {
    for mode in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut primary, mut backup) = installed_pair(&mut controller, io.clone(), mode);
        let ticket = primary.driver.begin_activation().unwrap();
        let completed = drive(&mut controller, primary.journal.activate_installed(ticket)).unwrap();
        primary.driver.complete_activation(completed).unwrap();
        assert!(
            primary
                .driver
                .normal()
                .unwrap()
                .snapshot()
                .ready_for_appends
        );
        assert_eq!(primary.journal.images().unwrap().committed().revision(), 3);
        let commit = primary.driver.normal().unwrap().announcement().unwrap();
        backup
            .driver
            .receive(
                primary.source.voter,
                Control::Commit(commit),
                Duration::ZERO,
            )
            .unwrap();
        let ticket = backup.driver.begin_activation().unwrap();
        backup
            .driver
            .complete_activation(
                drive(&mut controller, backup.journal.activate_installed(ticket)).unwrap(),
            )
            .unwrap();
        assert_eq!(backup.journal.images().unwrap().committed().revision(), 3);
        for voter in std::iter::once(primary).chain(std::iter::once(backup)) {
            drive(&mut controller, voter.journal.shutdown()).unwrap();
            let (journal, startup) = drive(
                &mut controller,
                OwnedJournal::open(voter.config, io.clone(), JournalGeneration(200)),
            )
            .unwrap();
            assert_eq!(startup.recovered().unwrap().log.committed, commit.committed);
            assert!(journal.images().is_err());
            drive(&mut controller, journal.shutdown()).unwrap();
        }
    }
}

#[test]
fn owned_aborted_installation_keeps_old_promise_history_and_arena() {
    let (mut controller, io) = setup();
    let (mut voter, _) = selected(&mut controller, io, QuorumPolicy::Durable);
    let ticket = stage_local(&mut controller, &mut voter);
    assert_eq!(
        drive(&mut controller, voter.journal.abort_installation(ticket)).unwrap(),
        ticket
    );
    assert_eq!(
        voter
            .journal
            .journal
            .ready()
            .unwrap()
            .writer()
            .durable_position()
            .generation(),
        voter.source.generation
    );
    assert_eq!(voter.journal.scope().view, 1);
    let found = drive(
        &mut controller,
        voter
            .journal
            .history_position(voter.source, voter.source.accepted.op),
    )
    .unwrap();
    assert_eq!(found.position, Some(voter.source.accepted));
    assert!(voter.journal.images().is_err());
    // The same core ticket and old generation still select an unpublished attempt.
    drive(
        &mut controller,
        voter.journal.begin_installation(ticket, policy()),
    )
    .unwrap();
    drive(&mut controller, voter.journal.abort_installation(ticket)).unwrap();
    drive(&mut controller, voter.journal.shutdown()).unwrap();
}

#[test]
fn owned_shutdown_aborts_unpublished_selected_history_without_activation() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut voter, _) = selected(&mut controller, io.clone(), policy);
        let ticket = stage_local(&mut controller, &mut voter);
        let old = voter.source;
        drive(&mut controller, voter.journal.shutdown()).unwrap();
        let (journal, startup) = drive(
            &mut controller,
            OwnedJournal::open(voter.config, io, JournalGeneration(201)),
        )
        .unwrap();
        let recovered = startup.recovered().unwrap();
        assert_eq!(recovered.scope, ticket.scope());
        assert_eq!(recovered.log.accepted, old.accepted);
        assert_eq!(recovered.log.last_normal_view, 0);
        assert!(journal.images().is_err());
        drive(&mut controller, journal.shutdown()).unwrap();
    }
}

#[test]
fn owned_canceled_installation_is_fenced_until_reopen() {
    let (mut controller, io) = setup();
    let (mut voter, _) = selected(&mut controller, io.clone(), QuorumPolicy::Durable);
    let ticket = voter
        .driver
        .begin_primary_install(JournalGeneration(101))
        .unwrap();
    let held = {
        let mut future = std::pin::pin!(voter.journal.begin_installation(ticket, policy()));
        assert!(poll(future.as_mut()).is_pending());
        controller.jobs()[0].0
    };
    assert!(voter.journal.is_faulted());
    assert!(matches!(
        drive_except(
            &mut controller,
            voter.journal.abort_installation(ticket),
            Some(held),
            |_| Effect::Normal
        ),
        Err(JournalError::Faulted)
    ));
    drop(voter.journal);
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    let (journal, startup) = drive(
        &mut controller,
        OwnedJournal::open(voter.config, io, JournalGeneration(201)),
    )
    .unwrap();
    let recovered = startup.recovered().unwrap();
    assert_eq!(recovered.scope.view, 1);
    assert_eq!(recovered.log.accepted, voter.source.accepted);
    assert_eq!(recovered.log.last_normal_view, 0);
    assert!(journal.images().is_err());
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owned_failed_selected_publication_never_exposes_canonical_images() {
    let (mut controller, io) = setup();
    let (mut voter, _) = selected(&mut controller, io.clone(), QuorumPolicy::Durable);
    let ticket = stage_local(&mut controller, &mut voter);
    let mut injected = false;
    let result = drive_except(
        &mut controller,
        voter.journal.finish_installation(ticket),
        None,
        |operation| {
            if matches!(operation, Operation::Rename { destination, .. } if destination.ends_with("CURRENT"))
            {
                injected = true;
                Effect::FailAfter(std::io::ErrorKind::Other)
            } else {
                Effect::Normal
            }
        },
    );
    assert!(injected);
    assert!(result.is_err());
    assert!(voter.journal.is_faulted());
    assert!(voter.journal.images().is_err());
    drop(voter.journal);
    let (journal, startup) = drive(
        &mut controller,
        OwnedJournal::open(voter.config, io, JournalGeneration(201)),
    )
    .unwrap();
    assert_eq!(startup.recovered().unwrap().log.accepted, ticket.accepted());
    assert!(journal.images().is_err());
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owned_history_request_errors_preserve_source_and_bounded_leases() {
    let (mut controller, io) = setup();
    let mut voter = voter(&mut controller, io, 1, 3, QuorumPolicy::Durable);
    let leased: Vec<_> = (0..voter.config.append_buffers)
        .map(|_| voter.journal.lease_append_buffer().unwrap())
        .collect();
    assert!(matches!(
        voter.journal.lease_append_buffer(),
        Err(SubmitError::Full)
    ));
    drop(leased);
    let mut fetch = FetchOps {
        scope: voter.journal.scope(),
        request_id: RequestId::from_bytes([71; 16]),
        source: voter.source,
        predecessor: Prefix::GENESIS,
        max_operations: 1,
        max_body_bytes: 1,
    };
    let buffer = voter.journal.lease_append_buffer().unwrap();
    let error = drive(&mut controller, voter.journal.fetch_history(fetch, buffer)).unwrap_err();
    assert!(matches!(
        error,
        JournalError::History(ozzy_journal_segment::HistoryError::BodyBudget { .. })
    ));
    assert!(!voter.journal.is_faulted());
    fetch.max_body_bytes = 8192;
    fetch.predecessor.digest = Digest::from_bytes([99; 32]);
    let buffer = voter.journal.lease_append_buffer().unwrap();
    assert!(drive(&mut controller, voter.journal.fetch_history(fetch, buffer)).is_err());
    assert!(!voter.journal.is_faulted());
    assert!(
        voter
            .journal
            .release_history(LogSource {
                generation: JournalGeneration(999),
                ..voter.source
            })
            .is_err()
    );
    let found = drive(
        &mut controller,
        voter
            .journal
            .history_position(voter.source, voter.source.accepted.op),
    )
    .unwrap();
    assert_eq!(found.position, Some(voter.source.accepted));
    drive(&mut controller, voter.journal.shutdown()).unwrap();
}

#[test]
fn owned_canceled_activation_does_not_grant_readiness_after_late_io() {
    let (mut controller, io) = setup();
    let (mut primary, backup) = installed_pair(&mut controller, io.clone(), QuorumPolicy::Durable);
    let ticket = primary.driver.begin_activation().unwrap();
    let held = {
        let mut future = std::pin::pin!(primary.journal.activate_installed(ticket));
        assert!(poll(future.as_mut()).is_pending());
        controller.jobs()[0].0
    };
    assert!(primary.journal.is_faulted());
    assert!(primary.journal.images().is_err());
    assert!(
        !primary
            .driver
            .normal()
            .unwrap()
            .snapshot()
            .ready_for_appends
    );
    drop(primary.journal);
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    let (journal, startup) = drive(
        &mut controller,
        OwnedJournal::open(primary.config, io, JournalGeneration(201)),
    )
    .unwrap();
    assert_eq!(startup.recovered().unwrap().log.accepted, ticket.through());
    assert!(journal.images().is_err());
    drive(&mut controller, journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}
