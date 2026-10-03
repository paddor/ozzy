use super::*;
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};

fn memory_voter() -> Journal {
    let mut journal = Journal::format(
        GroupIdentity {
            group_id: GroupId::from_bytes([1; 16]),
            replica_node_id: NodeId::from_bytes([2; 16]),
            volume_id: VolumeId::from_bytes([3; 16]),
            store_id: StoreId::from_bytes([4; 16]),
            store_generation: 1,
        },
        vec![5; 32],
        1,
        JournalGeneration(1),
        97,
    )
    .unwrap();
    journal.enable_memory_voting().unwrap();
    journal
        .append(&[CanonicalOperation {
            group_id: journal.identity.group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind: crate::OperationKind::Barrier,
            body: &[7; 16],
        }])
        .unwrap();
    journal
}

#[test]
fn drained_restart_marker_never_outruns_data_or_metadata_publication() {
    let mut admitted = 0;
    let mut refused = 0;
    for cut in 1..=48 {
        let mut journal = memory_voter();
        journal.fail_after(cut);
        let shutdown = journal.drain_memory_voting();
        journal.crash();
        journal.clear_failure();
        if let Ok(recovered) = journal.recover(JournalGeneration(2)) {
            admitted += 1;
            assert_eq!(recovered.manifest.accepted.op_number, 1);
            assert_eq!(recovered.operations.len(), 1);
            assert_eq!(recovered.operations[0].body.as_ref(), &[7; 16]);
            assert_eq!(recovered.operations[0].op_number, 1);
            // Opening re-arms the running marker before the owner may vote.
            journal.crash();
            assert!(matches!(
                journal.recover(JournalGeneration(3)),
                Err(DirectoryError::MemoryHistoryUnproven)
            ));
        } else {
            refused += 1;
            assert!(
                shutdown.is_err(),
                "successful drained publication must reopen"
            );
        }
    }
    assert!(
        admitted > 0 && refused > 0,
        "publication cuts must straddle the restart boundary"
    );
}

#[test]
fn omitted_directory_barriers_cannot_turn_a_running_marker_into_a_clean_restart() {
    let mut journal = memory_voter();
    journal.omit_directory_sync(true);
    journal.drain_memory_voting().unwrap();
    journal.crash();
    journal.omit_directory_sync(false);
    assert!(matches!(
        journal.recover(JournalGeneration(2)),
        Err(DirectoryError::MemoryHistoryUnproven)
    ));
}
