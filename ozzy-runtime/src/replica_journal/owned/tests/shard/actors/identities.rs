use super::*;

#[test]
fn shard_actors_refresh_bounded_control_indexes_while_intake_stays_queued() {
    for (policy, capacity) in [
        (QuorumPolicy::Durable, 8),
        (QuorumPolicy::Replicated, 8),
        (QuorumPolicy::Durable, 64),
        (QuorumPolicy::Replicated, 64),
    ] {
        let (mut controller, io) = setup();
        // Eight identity slots and an eight-operation live window. Each
        // three-control proposal must wait when fewer than three slots remain.
        let (mut actors, requests, mut submitter) =
            cluster_with_proposals(&mut controller, &io, 0, policy, capacity, 4, 3);
        let mut pending: Vec<_> = requests
            .into_iter()
            .map(|request| Some(Box::pin(request)))
            .collect();
        let mut next_id = 13u8;
        let mut confirmed = 0;
        let mut retained = None;
        let mut now = Duration::ZERO;
        for step in 0..50000 {
            now = Duration::from_millis(step / 100);
            round(&mut actors, now);
            // Delay observation across several actor turns. Capacity must also
            // include accepted work whose journal installation has not run yet.
            if step % 11 == 10 {
                for (id, _) in controller.jobs() {
                    controller.execute(id, Effect::Normal).unwrap();
                    controller.deliver(id).unwrap();
                }
            }
            for slot in &mut pending {
                let Some(request) = slot else { continue };
                let Poll::Ready(result) = poll(request.as_mut()) else {
                    continue;
                };
                let mut reply = result.unwrap();
                assert!(
                    matches!(reply.outcome, ProposalOutcome::Committed { .. }),
                    "{:?}",
                    reply.outcome
                );
                confirmed += 3;
                reply.buffer.clear();
                if next_id <= 72 {
                    for _ in 0..3 {
                        reply
                            .buffer
                            .push(
                                ozzy_journal::operation::OperationKind::Barrier,
                                &[next_id; 16],
                            )
                            .unwrap();
                        next_id += 1;
                    }
                    *slot = Some(Box::pin(submitter.try_submit(reply.buffer).unwrap()));
                } else {
                    retained = Some(reply.buffer);
                    *slot = None;
                }
            }
            if pending.iter().all(Option::is_none) {
                break;
            }
        }
        assert_eq!(confirmed, 72, "queued controls stopped progressing");
        // A refreshed index still rejects an old control identity.
        let mut buffer = retained.unwrap();
        buffer
            .push(ozzy_journal::operation::OperationKind::Barrier, &[1; 16])
            .unwrap();
        let mut duplicate = Box::pin(submitter.try_submit(buffer).unwrap());
        let mut rejected = false;
        for _ in 0..10000 {
            round(&mut actors, now);
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
            if let Poll::Ready(result) = poll(duplicate.as_mut()) {
                assert!(matches!(
                    result.unwrap().outcome,
                    ProposalOutcome::Invalid(_)
                ));
                rejected = true;
                break;
            }
        }
        assert!(rejected);
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}
