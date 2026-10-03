use super::*;
use crate::{ChainPosition, test_io::Scheduling};
use ozzy_journal::operation::logical_operation_digest;
use std::{cell::RefCell, sync::Arc};

#[test]
fn native_replay_yields_for_ready_visitors_and_canceled_replay_can_restart() {
    let (mut controller, mut journal) = empty_journal();
    let mut chain = ChainPosition::GENESIS;
    let mut operations = Vec::new();
    for _ in 0..130 {
        let operation = CanonicalOperation {
            op_number: chain.next_op_number(),
            previous_digest: chain.previous_digest(),
            ..operation(&journal)
        };
        chain = ChainPosition::new(
            operation.op_number + 1,
            logical_operation_digest(&operation),
        );
        operations.push(operation);
    }
    let written = drive(
        &mut controller,
        journal.append(&operations, BodyEncoding::Raw),
    )
    .unwrap();
    drive(&mut controller, journal.sync_through(written)).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 130);
    for cancel in [true, false] {
        let visited = RefCell::new(Vec::new());
        let mut replay = Box::pin(journal.replay_accepted(|item| {
            visited.borrow_mut().push(item.operation.op_number);
            Ok::<_, io::Error>(())
        }));
        let scheduling = Arc::new(Scheduling::default());
        let mut yielded_at = Vec::new();
        let mut completed = false;
        for _ in 0..100 {
            if let Poll::Ready(result) = scheduling.poll(replay.as_mut()) {
                result.unwrap();
                completed = true;
                break;
            }
            let jobs = controller.jobs();
            if jobs.is_empty() {
                assert!(scheduling.woken());
                yielded_at.push(visited.borrow().len());
                if cancel && !visited.borrow().is_empty() {
                    break;
                }
            }
            for (id, _) in jobs {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
        drop(replay);
        assert_eq!(completed, !cancel);
        if cancel {
            assert_eq!(yielded_at, [0, 0, 63]);
            assert_eq!(*visited.borrow(), (1..=63).collect::<Vec<_>>());
        } else {
            assert_eq!(yielded_at, [0, 0, 63, 127]);
            assert_eq!(*visited.borrow(), (1..=130).collect::<Vec<_>>());
        }
    }
    assert_eq!(journal.accepted_position().unwrap().op_number, 130);
    assert_eq!(journal.committed_position().unwrap().op_number, 0);
}
