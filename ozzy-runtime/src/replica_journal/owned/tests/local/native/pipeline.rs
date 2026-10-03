use super::*;

#[test]
fn pipelined_confirmations_preserve_writer_sequence_after_blocked_output() {
    for turns in 0..3 {
        let (mut controller, io) = setup();
        let mut actor = actor(&mut controller, io, 1);
        let hint = actor.authority_hint();
        let config = NativeIntakeConfig {
            local: hint.primary,
            group: actor.group(),
            partition: partition(),
            policy: Policy::LocalDurable,
            access: NativeAccess::Writers(vec![ClientAccess {
                node: link(70, 80).binding.peer,
                producer: ProducerId::from_bytes([40; 16]),
            }]),
            limits: limits(),
            requests_per_writer: 3,
            turn_slots: 2,
        };
        let buffers = (0..4)
            .map(|index| {
                actor
                    .lease_proposal_buffer_with_limits(config.buffer_limits(index).unwrap())
                    .unwrap()
            })
            .collect();
        let mut intake =
            NativeIntake::new(config, actor.take_submitter().unwrap(), buffers).unwrap();
        let mut output = Vec::new();
        intake
            .receive(
                &open(hint.authority, link(70, 80), Mode::Resume, None, 42),
                link(70, 80),
                hint,
            )
            .unwrap();
        settle(&mut controller, &mut actor, &mut intake, &mut output);
        output.clear();
        for sequence in 0..3 {
            assert_eq!(
                intake
                    .receive(
                        &append(hint.authority, link(70, 80), 40, sequence, 1),
                        link(70, 80),
                        hint
                    )
                    .unwrap(),
                NativeReceive::Accepted
            );
        }
        // All three become durable while replies remain blocked. Vary the
        // rotating scan position when output becomes writable again.
        for _ in 0..1000 + turns {
            progress(&mut actor, &mut intake, None, &mut output, true);
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
        assert!(output.is_empty());
        settle(&mut controller, &mut actor, &mut intake, &mut output);
        let replies = confirmations(&output, hint.primary, link(70, 80).binding.peer);
        assert_eq!(
            replies
                .iter()
                .map(|reply| reply.key.first_sequence)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        drop(intake);
        close(&mut controller, actor);
    }
}
