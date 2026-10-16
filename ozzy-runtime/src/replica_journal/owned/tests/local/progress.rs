use super::*;
use crate::replica_journal::{GroupAssignment, GroupProgressError, GroupProgressRequest};
use ozzy_journal::operation::{Assign, ProgressOwner};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId,
    append::{Authority, Policy},
};

mod actor;
mod assignment_change;
mod checkpoint;

fn assignment() -> GroupAssignment {
    GroupAssignment {
        group: ConsumerGroupId::from_bytes([61; 16]),
        partition: partition(),
        member: ConsumerMemberId::from_bytes([62; 16]),
        epoch: 1,
    }
}

fn initialized(controller: &mut Controller, io: Local) -> (OwnedJournal, Driver) {
    let (mut journal, mut driver) = drive(
        controller,
        OwnedJournal::format_local(local_config(), io, JournalGeneration(1), 32768),
    )
    .unwrap();
    journal
        .bind_append_memory(&payload_owner(1024 * 1024))
        .unwrap();
    initialize(controller, &mut journal, &mut driver);
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer.prepare_append(request(12, 0, 3)).unwrap();
    let receipt = admit(controller, &mut journal, &mut driver, buffer);
    write(controller, &mut journal, &mut driver, receipt);
    sync_apply(controller, &mut journal, &mut driver);
    let current = assignment();
    let body = OperationBody::Assign(Assign {
        consumer_group_id: current.group,
        partition: current.partition,
        expected_assignment_epoch: 0,
        new_assignment_epoch: current.epoch,
        plan_revision: 1,
        resume_cursor: Offset::ZERO,
        new_member: Some(current.member),
        operation_id: OperationId::from_bytes([63; 16]),
    });
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .push(
            body.kind(),
            &encode_operation_body(&body, journal.limits.operations).unwrap(),
        )
        .unwrap();
    let receipt = admit(controller, &mut journal, &mut driver, buffer);
    write(controller, &mut journal, &mut driver, receipt);
    sync_apply(controller, &mut journal, &mut driver);
    (journal, driver)
}

fn request_progress(
    journal: &OwnedJournal,
    driver: &Driver,
    cursor: Option<u64>,
    id: u8,
) -> ProposalBuffer {
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_group_progress(progress_request(driver, cursor, id), Policy::LocalDurable)
        .unwrap();
    buffer
}

fn progress_request(driver: &Driver, cursor: Option<u64>, id: u8) -> GroupProgressRequest {
    let scope = driver.begin_validation().unwrap().scope();
    GroupProgressRequest {
        authority: Authority {
            group_id: scope.group_id,
            config_epoch: scope.configuration_epoch,
            view: scope.view,
        },
        assignment: assignment(),
        cursor: cursor.map(Offset::new),
        operation: OperationId::from_bytes([id; 16]),
    }
}

fn captured(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    buffer: ProposalBuffer,
) -> (
    ozzy_replication::Prefix,
    crate::replica_journal::GroupProgressState,
) {
    let result = drive(
        controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    );
    let ProposalValidation::Resolved {
        through, buffer, ..
    } = result
    else {
        panic!("expected resolved cursor: {result:?}")
    };
    (through, buffer.group_progress_state().unwrap())
}

#[test]
fn cumulative_group_progress_waits_for_accepted_work_and_queries_only_confirmed_state() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = initialized(&mut controller, io);
    let before = driver.snapshot().accepted;
    let query = request_progress(&journal, &driver, None, 70);
    let (through, state) = captured(&mut controller, &mut journal, &driver, query);
    assert_eq!(through, before);
    assert_eq!((state.cursor, state.end), (Offset::ZERO, Offset::new(3)));
    let request = request_progress(&journal, &driver, Some(2), 71);
    let receipt = admit(&mut controller, &mut journal, &mut driver, request);
    let pending = driver.snapshot().accepted;
    assert!(pending.op > before.op);
    assert_eq!(
        journal.images().unwrap().committed().progress(
            ProgressOwner::ConsumerGroup(assignment().group),
            partition()
        ),
        Some(Offset::ZERO)
    );
    for requested in [1, 2] {
        let retry = request_progress(&journal, &driver, Some(requested), 72);
        let (through, state) = captured(&mut controller, &mut journal, &driver, retry);
        assert_eq!(through, pending);
        assert_eq!(state.cursor, Offset::new(2));
        assert!(through.op > driver.snapshot().committed.op);
    }
    let query = request_progress(&journal, &driver, None, 73);
    let (through, state) = captured(&mut controller, &mut journal, &driver, query);
    assert_eq!(through, before);
    assert_eq!(state.cursor, Offset::ZERO);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let query = request_progress(&journal, &driver, None, 73);
    let (through, state) = captured(&mut controller, &mut journal, &driver, query);
    assert_eq!(through, pending);
    assert_eq!(state.cursor, Offset::new(2));
    assert_eq!(
        driver.snapshot().accepted,
        pending,
        "queries and retries must not append"
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn cumulative_group_progress_rejects_overrun_and_fencing_without_admission() {
    let (mut controller, io) = setup();
    let (mut journal, driver) = initialized(&mut controller, io);
    let before = driver.snapshot().accepted;
    for cursor in [4, u64::MAX] {
        let buffer = request_progress(&journal, &driver, Some(cursor), 71);
        assert!(matches!(
            drive(
                &mut controller,
                journal.propose_append(driver.begin_validation().unwrap(), buffer, 777)
            ),
            ProposalValidation::Rejected {
                reason: JournalError::GroupProgress(GroupProgressError::BeyondCommit { .. }),
                ..
            }
        ));
        assert_eq!(driver.snapshot().accepted, before);
        assert!(!journal.is_faulted());
    }
    let mut request = progress_request(&driver, Some(2), 71);
    request.assignment.member = ConsumerMemberId::from_bytes([99; 16]);
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_group_progress(request, Policy::LocalDurable)
        .unwrap();
    assert!(matches!(
        drive(
            &mut controller,
            journal.propose_append(driver.begin_validation().unwrap(), buffer, 777)
        ),
        ProposalValidation::Rejected {
            reason: JournalError::GroupProgress(GroupProgressError::Fenced),
            ..
        }
    ));
    assert_eq!(driver.snapshot().accepted, before);
    assert!(!journal.is_faulted());
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn cumulative_group_progress_resumes_from_selected_checkpoint_after_restart() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = initialized(&mut controller, io.clone());
    let buffer = request_progress(&journal, &driver, Some(3), 75);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let confirmed = driver.snapshot().accepted;
    drive(
        &mut controller,
        journal.journal.ready_mut().unwrap().publish_progress(),
    )
    .unwrap();
    let state = journal.images().unwrap().committed().clone();
    let limits = local_config().recovery;
    let id = ozzy_proto::CheckpointId::from_bytes([76; 16]);
    let files = drive(
        &mut controller,
        journal
            .journal
            .ready_mut()
            .unwrap()
            .build_canonical_checkpoint(id, 4096, &state, limits.snapshot),
    )
    .unwrap();
    drive(
        &mut controller,
        journal
            .journal
            .ready_mut()
            .unwrap()
            .install_canonical_checkpoint(id, limits.state, limits.snapshot, limits.checkpoint),
    )
    .unwrap();
    drop(files);
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(local_config(), io, JournalGeneration(2)),
    )
    .unwrap();
    journal
        .bind_append_memory(&payload_owner(1024 * 1024))
        .unwrap();
    for cursor in [None, Some(1), Some(3)] {
        let buffer = request_progress(&journal, &driver, cursor, 77);
        let (through, state) = captured(&mut controller, &mut journal, &driver, buffer);
        assert_eq!(through, confirmed);
        assert_eq!(state.cursor, Offset::new(3));
        assert_eq!(state.assignment, assignment());
        assert_eq!(driver.snapshot().accepted, confirmed);
    }
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn group_progress_query_reports_expired_history_without_resetting_its_cursor() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = initialized(&mut controller, io);
    let buffer = request_progress(&journal, &driver, Some(1), 78);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let buffer = journal
        .retention_proposal(
            partition(),
            Offset::new(3),
            driver.snapshot().committed.op.0 + 1,
            OperationId::from_bytes([79; 16]),
        )
        .unwrap()
        .unwrap();
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let before = driver.snapshot().accepted;
    let buffer = request_progress(&journal, &driver, None, 80);
    let result = drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    );
    assert!(matches!(result, ProposalValidation::Rejected {
        reason: JournalError::GroupProgress(GroupProgressError::HistoryExpired { cursor, retained_from }), ..
    } if cursor == Offset::new(1) && retained_from == Offset::new(3)));
    assert_eq!(driver.snapshot().accepted, before);
    assert_eq!(
        journal.images().unwrap().committed().progress(
            ProgressOwner::ConsumerGroup(assignment().group),
            partition()
        ),
        Some(Offset::new(1))
    );
    assert!(!journal.is_faulted());
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn covered_group_progress_still_requires_current_authority_and_policy() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = initialized(&mut controller, io);
    let buffer = request_progress(&journal, &driver, Some(2), 81);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let before = driver.snapshot().accepted;
    for policy in [Policy::LocalDurable, Policy::QuorumDurable] {
        let mut request = progress_request(&driver, Some(1), 82);
        if policy == Policy::LocalDurable {
            request.authority.view += 1;
        }
        let mut buffer = journal.lease_proposal_buffer().unwrap();
        buffer.prepare_group_progress(request, policy).unwrap();
        let result = drive(
            &mut controller,
            journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
        );
        let expected = if policy == Policy::LocalDurable {
            GroupProgressError::Authority
        } else {
            GroupProgressError::Policy
        };
        assert!(
            matches!(result, ProposalValidation::Rejected { reason: JournalError::GroupProgress(error), .. } if error == expected)
        );
        assert_eq!(driver.snapshot().accepted, before);
        assert!(!journal.is_faulted());
    }
    drive(&mut controller, journal.shutdown()).unwrap();
}
