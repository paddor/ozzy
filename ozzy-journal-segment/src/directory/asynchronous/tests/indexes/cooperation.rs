use super::*;
use crate::{ChainPosition, test_io::Scheduling};
use ozzy_journal::operation::logical_operation_digest;
use std::{future::Future, sync::Arc};

fn drive_counted<T>(controller: &mut Controller, future: impl Future<Output = T>) -> (T, usize) {
    let mut future = std::pin::pin!(future);
    let scheduling = Arc::new(Scheduling::default());
    let mut cpu_turns = 0;
    for _ in 0..10_000 {
        if let Poll::Ready(result) = scheduling.poll(future.as_mut()) {
            return (result, cpu_turns);
        }
        let jobs = controller.jobs();
        if jobs.is_empty() {
            assert!(scheduling.woken());
            cpu_turns += 1;
        }
        for (id, _) in jobs {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    }
    panic!("native index build stalled")
}

#[test]
fn native_index_build_yields_during_sort_and_merge_without_changing_control_locations() {
    for max_entry_buffer_bytes in [1280, 16384] {
        let (mut controller, mut journal) = empty_journal();
        let bodies: Vec<_> = (1u128..=130).rev().map(u128::to_be_bytes).collect();
        let mut chain = ChainPosition::GENESIS;
        let mut operations = Vec::new();
        for body in &bodies {
            let operation = CanonicalOperation {
                op_number: chain.next_op_number(),
                previous_digest: chain.previous_digest(),
                body,
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
        drive(&mut controller, journal.publish_durable_progress()).unwrap();
        drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        let limits = IndexBuildLimits {
            max_entry_buffer_bytes,
            file: IndexLimits {
                max_operation_entries: 256,
                ..build_limits().file
            },
            ..build_limits()
        };
        let (index, turns) = drive_counted(&mut controller, journal.build_sealed_index(1, limits));
        let index = index.unwrap();
        assert!(
            turns > 10,
            "ready memory work must yield independently of file I/O"
        );
        assert_eq!(index.source().last_op_number, 130);
        assert_eq!(index.operation_count(), 130);
        for number in 1..=130 {
            let id = OperationId::from_bytes(u128::from(131u64 - number).to_be_bytes());
            assert_eq!(index.find_operation(id).unwrap().location.op_number, number);
        }
        assert_eq!(
            drive(&mut controller, journal.open_sealed_index(1, limits.file))
                .unwrap()
                .as_bytes(),
            index.as_bytes()
        );
    }
}
