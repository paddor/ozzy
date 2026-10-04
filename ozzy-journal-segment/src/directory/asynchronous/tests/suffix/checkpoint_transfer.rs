use super::*;
use crate::{CanonicalCheckpointImport, CheckpointRecovery};
use ozzy_core::state::{CanonicalState, MemoryIdentityIndex, StateLimits, StateSnapshotLimits};
use ozzy_proto::{CheckpointId, OperationId};

#[test]
fn checkpoint_transfer_selects_destination_state_and_retained_chain_before_voting() {
    for reset_checkpoint in [false, true] {
        checkpoint_transfer(reset_checkpoint);
    }
}

fn checkpoint_transfer(reset_checkpoint: bool) {
    let image = if reset_checkpoint {
        super::super::retention::retained_baseline()
    } else {
        Image::default()
    };
    let (mut controller, io) = setup(image);
    let journal = drive(
        &mut controller,
        checkpoint_target(io.clone(), reset_checkpoint),
    );
    let source = operations(LogPosition::GENESIS, &BODIES[..3], 0);
    let anchor = position(source[0]);
    let accepted = position(source[2]);
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(4);
    let body = ozzy_journal::operation::OperationBody::Barrier(ozzy_journal::operation::Barrier {
        operation_id: OperationId::from_bytes(BODIES[0]),
    });
    let plan = state.prepare(1, &body, &identities, &state).unwrap();
    state.apply(plan, &mut identities).unwrap();
    let mut replacement = request(&journal, accepted);
    replacement.promised_view = 2;
    replacement.last_normal_view = 2;
    let mut installing = drive(
        &mut controller,
        journal.begin_checkpoint_replacement(
            CONFIG,
            replacement,
            accepted,
            stream_limits(),
            CheckpointRecovery {
                predecessor: anchor,
                position: anchor,
            },
        ),
    )
    .unwrap();
    drive(&mut controller, installing.append_chunk(&source[1..])).unwrap();
    let id = CheckpointId::from_bytes([91; 16]);
    let mut journal = drive(
        &mut controller,
        installing.finish_with_checkpoint(CanonicalCheckpointImport {
            id,
            position: anchor,
            state: &state,
            limits: StateSnapshotLimits::default(),
        }),
    )
    .unwrap();
    assert_eq!(journal.manifest.checkpoint.unwrap().position, anchor);
    assert_eq!(journal.manifest.segments[0].first_chain.next_op_number(), 2);
    assert_eq!(journal.manifest.committed, accepted);
    let checkpoint = journal.capture_checkpoint().unwrap();
    assert_eq!(checkpoint.manifest().store_id, identity().store_id);
    assert_eq!(checkpoint.manifest().position, anchor);
    drop(checkpoint);
    let publication = crate::RecoveryPublication {
        current: journal.current(),
        generation: JournalGeneration(9),
        view: 2,
        accepted,
        committed: accepted,
    };
    let recovered = drive(
        &mut controller,
        journal.publish_recovered_configuration(
            CONFIG,
            publication,
            super::super::recovery::recovery_limits(),
        ),
    )
    .unwrap();
    assert_eq!(recovered.committed_images().committed().revision(), 3);
    drop(recovered);
    drop(journal);
    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let journal = drive(&mut controller, open(io, 10)).unwrap();
    assert_eq!(journal.manifest.checkpoint.unwrap().position, anchor);
    assert_eq!(journal.accepted_position().unwrap(), accepted);
}

async fn checkpoint_target(io: ozzy_io::Local, reset_checkpoint: bool) -> Journal {
    if reset_checkpoint {
        let directory = crate::AsyncRecoveryDirectory::open_for_repair(
            "/group".into(),
            io,
            identity(),
            CONFIG,
            limits(),
        )
        .await
        .unwrap();
        directory
            .quarantine_for_recovery(CONFIG)
            .await
            .unwrap()
            .recover_nonvoting(CONFIG, JournalGeneration(4), 3)
            .await
            .unwrap()
    } else {
        Journal::format_recovering(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        )
        .await
        .unwrap()
    }
}

#[test]
fn checkpoint_selection_crash_cuts_leave_old_or_complete_nonvoting_history() {
    let (mut controller, io) = setup(Image::default());
    let journal = drive(
        &mut controller,
        Journal::format_recovering(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        ),
    )
    .unwrap();
    let selected = journal.current();
    drop(journal);
    let image = controller.crash(true).unwrap().0;
    let source = operations(LogPosition::GENESIS, &BODIES[..3], 0);
    let anchor = position(source[0]);
    let accepted = position(source[2]);
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(4);
    let body = ozzy_journal::operation::OperationBody::Barrier(ozzy_journal::operation::Barrier {
        operation_id: OperationId::from_bytes(BODIES[0]),
    });
    let plan = state.prepare(1, &body, &identities, &state).unwrap();
    state.apply(plan, &mut identities).unwrap();
    for immediate in [false, true] {
        let mut finished = false;
        for cut in 0..800 {
            let (mut controller, io) = setup(image.clone());
            let journal = drive(&mut controller, open_nonvoting(io, 2)).unwrap();
            let mut replacement = request(&journal, accepted);
            replacement.promised_view = 2;
            replacement.last_normal_view = 2;
            let done = super::super::recovery::run_cut(
                &mut controller,
                async {
                    let mut installing = journal
                        .begin_checkpoint_replacement(
                            CONFIG,
                            replacement,
                            accepted,
                            stream_limits(),
                            CheckpointRecovery {
                                predecessor: anchor,
                                position: anchor,
                            },
                        )
                        .await?;
                    installing.append_chunk(&source[1..]).await?;
                    Box::pin(
                        installing.finish_with_checkpoint(CanonicalCheckpointImport {
                            id: CheckpointId::from_bytes([92; 16]),
                            position: anchor,
                            state: &state,
                            limits: StateSnapshotLimits::default(),
                        }),
                    )
                    .await
                },
                cut,
                immediate,
            );
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let journal = drive(&mut controller, open_nonvoting(io.clone(), 12)).unwrap();
            assert!(
                drive(&mut controller, open(io, 13)).is_err(),
                "still nonvoting at cut {cut}"
            );
            if journal.current() == selected {
                assert!(!done);
                assert!(journal.manifest.checkpoint.is_none());
                assert_eq!(journal.accepted_position().unwrap(), LogPosition::GENESIS);
            } else {
                assert_eq!(journal.manifest.checkpoint.unwrap().position, anchor);
                assert_eq!(journal.accepted_position().unwrap(), accepted);
                let checkpoint = journal.capture_checkpoint().unwrap();
                let restored = drive(
                    &mut controller,
                    checkpoint.decode_canonical_state(
                        StateLimits::default(),
                        StateSnapshotLimits::default(),
                    ),
                )
                .unwrap();
                assert_eq!(restored.revision(), 1);
                assert_eq!(drive(&mut controller, replay(&journal)).len(), 2);
            }
            if done {
                finished = true;
                break;
            }
        }
        assert!(finished);
    }
}

async fn open_nonvoting(io: ozzy_io::Local, generation: u128) -> Result<Journal, DirectoryError> {
    let marker = crate::directory::recovery::recovery_marker(CONFIG)?;
    Journal::open(
        "/group".into(),
        io,
        identity(),
        Some(&marker),
        JournalGeneration(generation),
        limits(),
    )
    .await
}
