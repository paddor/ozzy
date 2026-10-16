use super::*;
use crate::replica_journal::{GroupAssignError, GroupAssignRequest};
use ozzy_proto::group::Start;

fn change(driver: &Driver) -> GroupAssignRequest {
    GroupAssignRequest {
        authority: progress_request(driver, None, 71).authority,
        group: assignment().group,
        partition: partition(),
        plan_revision: 2,
        member: Some(ConsumerMemberId::from_bytes([81; 16])),
        initial: Start::Earliest,
        operation: OperationId::from_bytes([82; 16]),
    }
}

fn prepared(journal: &OwnedJournal, request: GroupAssignRequest) -> ProposalBuffer {
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_group_assignment(request, Policy::LocalDurable)
        .unwrap();
    buffer
}

fn resolved(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    request: GroupAssignRequest,
) -> (
    ozzy_replication::Prefix,
    crate::replica_journal::GroupAssignState,
) {
    let buffer = prepared(journal, request);
    let result = drive(
        controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    );
    let ProposalValidation::Resolved {
        through, buffer, ..
    } = result
    else {
        panic!("expected resolved assignment: {result:?}")
    };
    (through, buffer.group_assignment_state().unwrap())
}

fn rejected(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    request: GroupAssignRequest,
) -> GroupAssignError {
    let buffer = prepared(journal, request);
    let ProposalValidation::Rejected {
        reason: JournalError::GroupAssign(actual),
        buffer,
    } = drive(
        controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    )
    else {
        panic!("invalid source ownership admitted")
    };
    assert!(buffer.group_assignment_state().is_none());
    assert!(!journal.is_faulted());
    actual
}

#[test]
fn source_handoff_captures_accepted_progress_and_retry_keeps_that_activation_cursor() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = initialized(&mut controller, io);
    let buffer = request_progress(&journal, &driver, Some(2), 71);
    let progress_write = admit(&mut controller, &mut journal, &mut driver, buffer);
    let request = change(&driver);
    let buffer = prepared(&journal, request);
    let assignment_write = admit(&mut controller, &mut journal, &mut driver, buffer);
    let accepted = driver.snapshot().accepted;
    let state = journal
        .images()
        .unwrap()
        .speculative()
        .assignment(request.group, request.partition)
        .unwrap();
    assert_eq!(
        (state.epoch, state.plan_revision, state.resume_cursor),
        (2, 2, Offset::new(2))
    );
    assert_eq!(
        journal
            .images()
            .unwrap()
            .committed()
            .assignment(request.group, request.partition)
            .unwrap()
            .epoch,
        1
    );

    let buffer = prepared(&journal, request);
    assert!(!journal.validation_may_read(&buffer.0));
    drop(buffer);
    let (through, state) = resolved(&mut controller, &mut journal, &driver, request);
    assert_eq!(through, accepted);
    assert_eq!((state.epoch, state.cursor), (2, Offset::new(2)));

    let buffer = request_progress(&journal, &driver, Some(3), 83);
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
    write(&mut controller, &mut journal, &mut driver, progress_write);
    write(&mut controller, &mut journal, &mut driver, assignment_write);
    sync_apply(&mut controller, &mut journal, &mut driver);

    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_group_progress(
            GroupProgressRequest {
                assignment: GroupAssignment {
                    member: request.member.unwrap(),
                    epoch: 2,
                    ..assignment()
                },
                ..progress_request(&driver, Some(3), 84)
            },
            Policy::LocalDurable,
        )
        .unwrap();
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let before = driver.snapshot().accepted;
    for initial in [Start::Earliest, Start::Latest, Start::Offset(Offset::ZERO)] {
        let (through, state) = resolved(
            &mut controller,
            &mut journal,
            &driver,
            GroupAssignRequest {
                operation: OperationId::from_bytes([85; 16]),
                initial,
                ..request
            },
        );
        assert_eq!(through, before, "retry after later progress");
        assert_eq!((state.epoch, state.cursor), (2, Offset::new(2)));
        assert_eq!(driver.snapshot().accepted, before);
    }
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn source_handoff_rejects_changed_plans_stale_revisions_and_foreign_authority() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = initialized(&mut controller, io);
    let request = change(&driver);
    let buffer = prepared(&journal, request);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let before = driver.snapshot().accepted;
    for (request, expected) in [
        (
            GroupAssignRequest {
                member: None,
                ..request
            },
            GroupAssignError::Conflict,
        ),
        (
            GroupAssignRequest {
                member: Some(ConsumerMemberId::from_bytes([99; 16])),
                ..request
            },
            GroupAssignError::Conflict,
        ),
        (
            GroupAssignRequest {
                plan_revision: 1,
                ..request
            },
            GroupAssignError::Fenced,
        ),
        (
            GroupAssignRequest {
                authority: Authority {
                    view: 1,
                    ..request.authority
                },
                ..request
            },
            GroupAssignError::Authority,
        ),
    ] {
        assert_eq!(
            rejected(&mut controller, &mut journal, &driver, request),
            expected
        );
        assert_eq!(driver.snapshot().accepted, before);
    }
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    assert!(matches!(
        buffer.prepare_group_assignment(
            GroupAssignRequest {
                plan_revision: 0,
                ..request
            },
            Policy::LocalDurable
        ),
        Err(JournalError::GroupAssign(GroupAssignError::Invalid))
    ));
    assert!(buffer.is_empty());
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn first_source_assignment_resolves_the_creation_selector_and_handoffs_never_reset() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = initialized(&mut controller, io);
    let request = GroupAssignRequest {
        group: ConsumerGroupId::from_bytes([90; 16]),
        plan_revision: 1,
        initial: Start::Offset(Offset::new(3)),
        ..change(&driver)
    };
    let buffer = prepared(&journal, request);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    assert_eq!(
        journal.images().unwrap().committed().progress(
            ProgressOwner::ConsumerGroup(request.group),
            request.partition
        ),
        Some(Offset::new(3))
    );
    // A later plan with a different selector keeps the persisted cursor.
    let handoff = GroupAssignRequest {
        plan_revision: 2,
        member: Some(ConsumerMemberId::from_bytes([83; 16])),
        initial: Start::Offset(Offset::ZERO),
        operation: OperationId::from_bytes([84; 16]),
        ..request
    };
    let buffer = prepared(&journal, handoff);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let state = journal
        .images()
        .unwrap()
        .committed()
        .assignment(request.group, request.partition)
        .unwrap();
    assert_eq!(
        (state.epoch, state.plan_revision, state.resume_cursor),
        (2, 2, Offset::new(3))
    );
    assert_eq!(
        rejected(
            &mut controller,
            &mut journal,
            &driver,
            GroupAssignRequest {
                group: ConsumerGroupId::from_bytes([91; 16]),
                initial: Start::Offset(Offset::new(4)),
                ..request
            }
        ),
        GroupAssignError::InitialPosition {
            cursor: Offset::new(4),
            retained_from: Offset::ZERO,
            end: Offset::new(3),
        }
    );
    let latest = GroupAssignRequest {
        group: ConsumerGroupId::from_bytes([92; 16]),
        initial: Start::Latest,
        operation: OperationId::from_bytes([93; 16]),
        ..request
    };
    let buffer = prepared(&journal, latest);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    assert_eq!(
        journal
            .images()
            .unwrap()
            .committed()
            .assignment(latest.group, latest.partition)
            .unwrap()
            .resume_cursor,
        Offset::new(3)
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn source_assignment_preserves_expired_progress_and_new_groups_start_at_retention() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = initialized(&mut controller, io);
    let buffer = journal
        .retention_proposal(
            partition(),
            Offset::new(3),
            driver.snapshot().committed.op.0 + 1,
            OperationId::from_bytes([93; 16]),
        )
        .unwrap()
        .unwrap();
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let request = change(&driver);
    let buffer = prepared(&journal, request);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    assert_eq!(
        journal
            .images()
            .unwrap()
            .committed()
            .assignment(request.group, request.partition)
            .unwrap()
            .resume_cursor,
        Offset::ZERO
    );
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_group_progress(
            GroupProgressRequest {
                assignment: GroupAssignment {
                    epoch: 2,
                    member: request.member.unwrap(),
                    ..assignment()
                },
                ..progress_request(&driver, None, 94)
            },
            Policy::LocalDurable,
        )
        .unwrap();
    assert!(
        matches!(drive(&mut controller, journal.propose_append(driver.begin_validation().unwrap(), buffer, 777)),
        ProposalValidation::Rejected { reason: JournalError::GroupProgress(GroupProgressError::HistoryExpired { cursor: Offset::ZERO, retained_from }), .. }
        if retained_from == Offset::new(3))
    );
    let request = GroupAssignRequest {
        group: ConsumerGroupId::from_bytes([95; 16]),
        plan_revision: 1,
        operation: OperationId::from_bytes([96; 16]),
        ..request
    };
    let buffer = prepared(&journal, request);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    assert_eq!(
        journal
            .images()
            .unwrap()
            .committed()
            .assignment(request.group, request.partition)
            .unwrap()
            .resume_cursor,
        Offset::new(3)
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}
