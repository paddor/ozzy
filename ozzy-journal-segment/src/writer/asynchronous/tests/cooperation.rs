use super::*;
use crate::test_io::Scheduling;
use ozzy_journal::operation::logical_operation_digest;
use std::sync::Arc;

#[test]
fn native_recovery_cancellation_between_validation_turns_never_starts_repair() {
    for cut in [1, 2] {
        let (mut controller, io) = setup(Image::default());
        let mut writer = create(&mut controller, io.clone(), "/segment");
        let mut chain = ChainPosition::GENESIS;
        let mut operations = Vec::new();
        for _ in 0..130 {
            let operation = operation(chain);
            chain = ChainPosition::new(
                operation.op_number + 1,
                logical_operation_digest(&operation),
            );
            operations.push(operation);
        }
        let written = drive(
            &mut controller,
            writer.append(&operations, BodyEncoding::Raw),
        )
        .unwrap();
        drive(&mut controller, writer.sync_through(written)).unwrap();
        drive(&mut controller, writer.close()).unwrap();
        let before = controller
            .image()
            .bytes(Path::new("/segment"), true)
            .unwrap()
            .to_vec();
        let scheduling = Arc::new(Scheduling::default());
        let mut recovery = Box::pin(Writer::recover(
            "/segment".into(),
            io.clone(),
            None,
            start(2),
            options(),
            DecodeLimits::default(),
            Some(requirements(chain)),
        ));
        let mut turns = 0;
        for _ in 0..100 {
            assert!(scheduling.poll(recovery.as_mut()).is_pending());
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
                    Operation::Write { .. } | Operation::SetLength { .. } | Operation::Sync { .. }
                ));
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
        assert_eq!(turns, cut);
        drop(recovery);
        for durable in [false, true] {
            assert_eq!(
                controller
                    .image()
                    .bytes(Path::new("/segment"), durable)
                    .unwrap(),
                before
            );
        }
        let recovered = drive(
            &mut controller,
            Writer::recover(
                "/segment".into(),
                io,
                None,
                start(3),
                options(),
                DecodeLimits::default(),
                Some(requirements(chain)),
            ),
        )
        .unwrap();
        assert_eq!(recovered.durable_position().next_chain(), chain);
    }
}
