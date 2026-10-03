use super::*;
use crate::test_io::Scheduling;
use ozzy_journal::operation::{ChainPosition, logical_operation_digest};
use std::sync::Arc;

fn populate(controller: &mut Controller, journal: &mut Journal) -> CanonicalState {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(256);
    let bodies: Vec<_> = std::iter::once(create())
        .chain((1..=130).map(open_producer))
        .collect();
    for (group, bodies) in bodies.chunks(8).enumerate() {
        if group > 0 && group % 7 == 0 {
            drive(controller, journal.roll_active(32768, 4)).unwrap();
        }
        let encoded: Vec<_> = bodies
            .iter()
            .map(|body| encode_operation_body(body, limits().operations).unwrap())
            .collect();
        let mut chain = journal.writer.written_position().next_chain();
        let mut operations = Vec::new();
        for (body, bytes) in bodies.iter().zip(&encoded) {
            let operation = CanonicalOperation {
                op_number: chain.next_op_number(),
                previous_digest: chain.previous_digest(),
                kind: body.kind(),
                body: bytes,
                ..operation(journal)
            };
            chain = ChainPosition::new(
                operation.op_number + 1,
                logical_operation_digest(&operation),
            );
            operations.push(operation);
            apply(&mut state, &mut identities, body);
        }
        let written = drive(controller, journal.append(&operations, BodyEncoding::Raw)).unwrap();
        drive(controller, journal.sync_through(written)).unwrap();
    }
    drive(controller, confirm(journal));
    assert_eq!(
        journal.committed_position().unwrap().op_number,
        state.revision()
    );
    state
}

#[test]
fn native_checkpoint_decode_yields_and_cancellation_preserves_selected_history() {
    let (mut controller, mut journal) = empty_journal();
    let state = populate(&mut controller, &mut journal);
    let snapshot = state
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    let id = CheckpointId::from_bytes([9; 16]);
    let files = drive(
        &mut controller,
        journal.build_checkpoint(
            id,
            ozzy_core::state::canonical_state_schema_digest(),
            4096,
            &snapshot,
        ),
    )
    .unwrap();
    let before = journal.current;
    for cut in [1, 3] {
        let scheduling = Arc::new(Scheduling::default());
        let mut future = Box::pin(
            files.decode_canonical_state(StateLimits::default(), StateSnapshotLimits::default()),
        );
        let mut turns = 0;
        for _ in 0..1000 {
            assert!(scheduling.poll(future.as_mut()).is_pending());
            let jobs = controller.jobs();
            if jobs.is_empty() {
                assert!(scheduling.woken());
                turns += 1;
                if turns == cut {
                    break;
                }
            }
            for (id, _) in jobs {
                assert!(!matches!(
                    controller.operation(id).unwrap().unprotected(),
                    Operation::Write { .. }
                        | Operation::SetLength { .. }
                        | Operation::Rename { .. }
                ));
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
        assert_eq!(turns, cut);
        drop(future);
        assert_eq!(journal.current, before);
        assert!(!journal.is_faulted());
        assert_eq!(
            drive(
                &mut controller,
                files
                    .decode_canonical_state(StateLimits::default(), StateSnapshotLimits::default())
            )
            .unwrap(),
            state
        );
    }
    drive(&mut controller, journal.install_checkpoint(id)).unwrap();
    let restored = drive(
        &mut controller,
        journal.selected_canonical_checkpoint(
            StateLimits::default(),
            StateSnapshotLimits::default(),
            limits().checkpoint,
        ),
    )
    .unwrap()
    .unwrap();
    assert_eq!(restored, state);
    assert_eq!(
        restored.partition(partition()).unwrap().producers().len(),
        130
    );
}

#[test]
fn native_checkpoint_encoding_yields_before_io_and_cancellation_allows_retry() {
    let (mut controller, mut journal) = empty_journal();
    let state = populate(&mut controller, &mut journal);
    let before = journal.current;
    let id = CheckpointId::from_bytes([10; 16]);
    for cut in [1, 3, 6] {
        let scheduling = Arc::new(Scheduling::default());
        let mut future = Box::pin(journal.build_canonical_checkpoint(
            id,
            4096,
            &state,
            StateSnapshotLimits::default(),
        ));
        for _ in 0..cut {
            assert!(scheduling.poll(future.as_mut()).is_pending());
            assert!(scheduling.woken());
            assert!(
                controller.jobs().is_empty(),
                "encoding submitted a file job"
            );
        }
        drop(future);
        assert_eq!(journal.current, before);
        assert!(!journal.is_faulted());
    }
    let files = drive(
        &mut controller,
        journal.build_canonical_checkpoint(id, 4096, &state, StateSnapshotLimits::default()),
    )
    .unwrap();
    assert_eq!(
        drive(&mut controller, files.read_state()).unwrap(),
        state
            .encode_snapshot(StateSnapshotLimits::default())
            .unwrap()
    );
    assert_eq!(journal.current, before);
    drive(&mut controller, journal.install_checkpoint(id)).unwrap();
    assert_eq!(
        drive(
            &mut controller,
            journal.selected_canonical_checkpoint(
                StateLimits::default(),
                StateSnapshotLimits::default(),
                limits().checkpoint,
            )
        )
        .unwrap()
        .unwrap(),
        state
    );
}
