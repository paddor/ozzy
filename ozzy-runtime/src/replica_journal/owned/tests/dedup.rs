use super::proposal::{dedup::resolved, request};
use super::writeback::{
    Replica, admit_encoded, confirm, finish, initialize_writer, prepare, settle, synchronize,
};
use super::*;
use ozzy_core::dedup::{Error, Limits, Match};
use ozzy_io::simulation::Stage;
use ozzy_journal::operation::{CanonicalOperation, OperationKind};
use ozzy_proto::{MessageId, Offset};
use ozzy_replication::Admission;

pub(super) fn limits() -> Limits {
    Limits {
        window_millis: 10,
        entries: 32,
        pending_records: 16,
        pending_operations: 16,
        batch_records: 8,
        bytes: 64 * 1024,
    }
}

pub(super) fn replica(
    controller: &mut Controller,
    io: Local,
    id: u8,
    policy: QuorumPolicy,
) -> Replica {
    let mut config = config(&format!("/dedup-{id}"), 7, policy);
    config.identity.replica_node_id = NodeId::from_bytes([id + 1; 16]);
    config.deduplication = Some(limits());
    config.limits.decode.max_entries = config.writeback.max_operations;
    config.limits.decode.max_decoded_body_bytes = config.writeback.max_body_bytes;
    config.limits.decode.max_group_decoded_body_bytes = config.writeback.max_body_bytes;
    config.limits.operations.max_payload_bytes = config.writeback.max_body_bytes;
    let (mut journal, startup) = drive(
        controller,
        OwnedJournal::format_new(
            config.clone(),
            io,
            JournalGeneration(u128::from(id) + 1),
            32768,
        ),
    )
    .unwrap();
    journal
        .bind_append_memory(&payload_owner(1024 * 1024))
        .unwrap();
    let driver = startup
        .into_driver(Duration::ZERO, timing(), config.append_limits)
        .unwrap();
    Replica {
        journal,
        driver,
        config,
    }
}

fn validate(
    controller: &mut Controller,
    replica: &mut Replica,
    body: &(OperationKind, Vec<u8>),
) -> Result<crate::replica_journal::ValidatedAppend, JournalError> {
    let ticket = replica.driver.begin_validation().unwrap();
    let mut buffer = replica.journal.lease_append_buffer().unwrap();
    buffer
        .push(CanonicalOperation {
            group_id: ticket.scope().group_id,
            configuration_epoch: ticket.scope().configuration_epoch,
            original_view: ticket.scope().view,
            op_number: ticket.accepted().op.0 + 1,
            previous_digest: ticket.accepted().digest,
            kind: body.0,
            body: &body.1,
        })
        .unwrap();
    drive(controller, replica.journal.validate_append(ticket, buffer))
}

fn found(replica: &Replica, id: u8) -> Option<Match> {
    replica
        .journal
        .dedup
        .image
        .as_ref()
        .unwrap()
        .index
        .lookup(MessageId::from_bytes([id; 16]))
}

#[test]
fn both_quorum_policies_install_ids_at_acceptance_and_confirm_only_verified_prefixes() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut primary = replica(&mut controller, io.clone(), 0, policy);
        let mut backup = replica(&mut controller, io, 1, policy);
        let mut primary_receipts = vec![initialize_writer(&mut controller, &mut primary)];
        let mut backup_receipts = vec![initialize_writer(&mut controller, &mut backup)];
        let first = resolved(&request(12, 0, 1), Offset::ZERO, &[None], 0);
        let validated = validate(&mut controller, &mut primary, &first).unwrap();
        assert_eq!(found(&primary, 1), None);
        drop(validated);
        assert_eq!(
            found(&primary, 1),
            None,
            "discarded preparation admits no ID"
        );
        let (write, _, receipt) = admit_encoded(&mut controller, &mut primary, [first.clone()]);
        primary_receipts.push(receipt);
        backup_receipts.push(admit_encoded(&mut controller, &mut backup, [first]).2);
        assert_eq!(
            found(&primary, 1),
            Some(Match::Pending {
                offset: Offset::ZERO,
                operation: write.through().0,
            })
        );
        // Claiming a live ID as newly stored must fail without changing authority.
        let mut repeated = request(12, 1, 1);
        repeated.records[0].message_id = MessageId::from_bytes([1; 16]);
        let invalid = resolved(&repeated, Offset::new(1), &[None], 0);
        assert!(matches!(
            validate(&mut controller, &mut primary, &invalid),
            Err(JournalError::Dedup(Error::Batch))
        ));
        assert!(!primary.journal.is_faulted());
        let next = resolved(&request(12, 1, 1), Offset::new(1), &[None], 0);
        let mut prepared = validate(&mut controller, &mut primary, &next).unwrap();
        for replica in [&mut primary, &mut backup] {
            let work = prepare(replica);
            finish(&mut controller, replica, work);
        }
        for receipt in primary_receipts {
            settle(&mut primary, receipt);
        }
        for receipt in backup_receipts {
            settle(&mut backup, receipt);
        }
        if policy == QuorumPolicy::Durable {
            synchronize(&mut controller, &mut primary);
            synchronize(&mut controller, &mut backup);
        }
        confirm(&mut primary, &backup, policy);
        assert_eq!(found(&primary, 1), Some(Match::Committed(Offset::ZERO)));
        assert_eq!(found(&primary, 2), None);
        // Apply can progress between validation and storage installation.
        assert!(primary.journal.can_admit(&mut prepared));
        let Admission::Write { ticket, .. } = primary
            .driver
            .prepare_validated(
                primary.config.identity.replica_node_id,
                primary.driver.begin_validation().unwrap(),
                prepared.prepared(),
                Duration::ZERO,
            )
            .unwrap()
        else {
            panic!("write ticket")
        };
        primary.journal.admit_append(ticket, prepared).unwrap();
        assert_eq!(
            found(&primary, 2),
            Some(Match::Pending {
                offset: Offset::new(1),
                operation: ticket.through().0,
            })
        );
        assert_eq!(found(&primary, 1), Some(Match::Committed(Offset::ZERO)));
        assert!(
            controller.jobs().is_empty(),
            "logical ID installation performs no I/O"
        );
    }
}

#[test]
fn enabled_policy_without_an_identity_image_is_rejected_before_admission() {
    use ozzy_journal::operation::{
        CreatePartition, OperationBody, RetentionPolicy, encode_operation_body,
    };
    use ozzy_proto::{OwnerEpoch, PartitionId, PartitionIncarnation};
    let (mut controller, io) = setup();
    let mut replica =
        super::writeback::replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let body = OperationBody::CreatePartition(CreatePartition {
        partition: PartitionIncarnation::from_bytes([11; 16]),
        stream: "stream",
        topic: "orders",
        partition_id: PartitionId::ZERO,
        owner_epoch: OwnerEpoch::INITIAL,
        dedup_window_millis: std::num::NonZeroU64::new(10),
        retention: RetentionPolicy::default(),
    });
    let encoded = (
        body.kind(),
        encode_operation_body(&body, replica.config.limits.operations).unwrap(),
    );
    assert!(matches!(
        validate(&mut controller, &mut replica, &encoded),
        Err(JournalError::Configuration)
    ));
    assert_eq!(
        replica.journal.images().unwrap().speculative().revision(),
        0
    );
    assert!(!replica.journal.is_faulted());
    assert_eq!(controller.jobs(), [] as [(JobId, Stage); 0]);
}
