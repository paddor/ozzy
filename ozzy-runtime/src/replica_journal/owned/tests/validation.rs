use super::*;
use ozzy_journal::operation::{CanonicalOperation, OperationKind, logical_operation_digest};
use ozzy_replication::{
    OpNumber,
    driver::{ReplicaDriver, ValidationTicket},
};

fn fresh(
    controller: &mut Controller,
    io: Local,
    config: OwnedConfig,
) -> (OwnedJournal, ReplicaDriver) {
    let (journal, startup) = drive(
        controller,
        OwnedJournal::format_new(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    (
        journal,
        startup
            .into_driver(Duration::ZERO, timing(), pipeline())
            .unwrap(),
    )
}

fn barriers(journal: &OwnedJournal, ticket: ValidationTicket, ids: &[u8]) -> AppendBuffer {
    let mut buffer = journal.lease_append_buffer().unwrap();
    let mut end = ticket.accepted();
    for id in ids {
        let bytes = [*id; 16];
        let operation = CanonicalOperation {
            group_id: ticket.scope().group_id,
            configuration_epoch: ticket.scope().configuration_epoch,
            original_view: ticket.scope().view,
            op_number: end.op.0 + 1,
            previous_digest: end.digest,
            kind: OperationKind::Barrier,
            body: &bytes,
        };
        end = Prefix {
            op: OpNumber(operation.op_number),
            digest: logical_operation_digest(&operation),
        };
        buffer.push(operation).unwrap();
    }
    buffer
}

#[test]
fn owned_control_validation_changes_no_journal_or_canonical_authority() {
    let (mut controller, io) = setup();
    let (mut journal, driver) = fresh(
        &mut controller,
        io,
        config("/partition", 7, QuorumPolicy::Durable),
    );
    let ticket = driver.begin_validation().unwrap();
    let buffer = barriers(&journal, ticket, &[1, 2]);
    let mut effects = 0;
    let validated = drive_except(
        &mut controller,
        journal.validate_append(ticket, buffer),
        None,
        |operation| {
            if matches!(
                operation,
                Operation::Write { .. }
                    | Operation::Rename { .. }
                    | Operation::Allocate { .. }
                    | Operation::SetLength { .. }
                    | Operation::RemoveFile { .. }
            ) {
                effects += 1;
            }
            Effect::Normal
        },
    )
    .unwrap();
    assert_eq!(effects, 0);
    assert_eq!(
        validated.prepared().last().unwrap().prefix().op,
        OpNumber(2)
    );
    assert_eq!(journal.images().unwrap().committed().revision(), 0);
    assert_eq!(journal.images().unwrap().speculative().revision(), 0);
    assert_eq!(
        journal.journal.ready().unwrap().written_position().unwrap(),
        ozzy_journal_segment::LogPosition::GENESIS
    );
    assert!(!journal.is_faulted());
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owned_duplicate_control_identity_rejects_whole_group_without_fencing() {
    let (mut controller, io) = setup();
    let (mut journal, driver) = fresh(
        &mut controller,
        io,
        config("/partition", 7, QuorumPolicy::Durable),
    );
    let ticket = driver.begin_validation().unwrap();
    let buffer = barriers(&journal, ticket, &[1, 1]);
    assert!(drive(&mut controller, journal.validate_append(ticket, buffer)).is_err());
    assert_eq!(journal.images().unwrap().speculative().revision(), 0);
    assert!(!journal.is_faulted());
    let valid = barriers(&journal, ticket, &[2]);
    drive(&mut controller, journal.validate_append(ticket, valid)).unwrap();
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owned_validation_rejects_foreign_arenas_before_file_work() {
    let (mut controller, io) = setup();
    let (mut journal, driver) = fresh(
        &mut controller,
        io.clone(),
        config("/partition", 7, QuorumPolicy::Durable),
    );
    let (other, _) = drive(
        &mut controller,
        OwnedJournal::format_new(
            config("/other", 8, QuorumPolicy::Durable),
            io,
            JournalGeneration(2),
            32768,
        ),
    )
    .unwrap();
    let ticket = driver.begin_validation().unwrap();
    let buffer = barriers(&other, ticket, &[1]);
    assert!(matches!(
        drive(&mut controller, journal.validate_append(ticket, buffer)),
        Err(JournalError::AppendMismatch)
    ));
    assert!(controller.jobs().is_empty());
    assert!(!journal.is_faulted());
    drive(&mut controller, journal.shutdown()).unwrap();
    drive(&mut controller, other.shutdown()).unwrap();
}

#[test]
fn owned_validation_enforces_raw_capacity_before_decoding() {
    let (mut controller, io) = setup();
    let mut config = config("/partition", 7, QuorumPolicy::Durable);
    config.append_limits.max_body_bytes = 65536;
    let (mut journal, driver) = fresh(&mut controller, io, config);
    let ticket = driver.begin_validation().unwrap();
    let mut buffer = journal.lease_append_buffer().unwrap();
    buffer
        .push(CanonicalOperation {
            group_id: ticket.scope().group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind: OperationKind::Barrier,
            body: &vec![1; 32768],
        })
        .unwrap();
    assert!(matches!(
        drive(&mut controller, journal.validate_append(ticket, buffer)),
        Err(JournalError::AppendCapacity)
    ));
    assert!(controller.jobs().is_empty());
    assert!(!journal.is_faulted());
    drive(&mut controller, journal.shutdown()).unwrap();
}
