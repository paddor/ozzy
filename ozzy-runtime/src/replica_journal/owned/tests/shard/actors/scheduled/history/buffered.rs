use super::*;

#[test]
fn durable_source_fetches_survive_a_later_buffered_write_on_the_live_actor() {
    let (mut controller, io) = setup();
    let (mut actors, now) = history_fixture(&mut controller, &io, QuorumPolicy::Replicated);
    let status = actors[1].status();
    let snapshot = status.normal.unwrap();
    let source = LogSource {
        voter: NodeId::from_bytes([2; 16]),
        generation: snapshot.journal.generation,
        accepted: snapshot.accepted,
    };
    let (ticket, journal) = actors[1].read_access();
    let positions = journal
        .replication_positions(ticket.unwrap(), [OpNumber(1); 2])
        .unwrap();
    let mut positions = Box::pin(positions);
    let mut looked_up = None;
    for _ in 0..10000 {
        round_at(&mut actors, now, false);
        settle(&mut controller, &[]);
        if let Poll::Ready(result) = poll(positions.as_mut()) {
            looked_up = result.unwrap().positions[0];
            break;
        }
    }
    let looked_up = looked_up.expect("read the original confirmed operation");
    drop(actors[1].read_access().1.release_history(source).unwrap());
    add_unsynchronized_write(&mut controller, &mut actors, now);
    let request = wire::FetchOps {
        scope: status.scope,
        request_id: RequestId::from_bytes([94; 16]),
        source,
        predecessor: Prefix::GENESIS,
        max_operations: actor_config().transfer.max_operations as u32,
        max_body_bytes: actor_config().transfer.max_body_bytes as u32,
    };
    for _ in 0..2 {
        actors[1].receive(&fetch_message(request), now).unwrap();
        let mut replied = false;
        for _ in 0..10000 {
            actors[1].receive(&fetch_message(request), now).unwrap();
            actors[1].advance(now).unwrap();
            {
                let mut event = std::pin::pin!(actors[1].changed(now));
                if let Poll::Ready(result) = poll(event.as_mut()) {
                    result.expect("the old source must survive the newer unsynchronized tail");
                }
            }
            actors[1]
                .flush(|message| {
                    if opcode(&message) == ozzy_proto::Opcode::Ops {
                        verify_response(&message, request, looked_up, QuorumPolicy::Replicated);
                        replied = true;
                    }
                    Ok(())
                })
                .unwrap();
            settle_without_sync(&mut controller);
            if replied {
                break;
            }
        }
        assert!(
            replied,
            "the correlated old-source request must receive its two operations"
        );
        let current = actors[1].status();
        assert!(current.application_ready);
        let current = current.normal.unwrap();
        assert_eq!(current.journal.durable, snapshot.journal.durable);
        assert!(current.journal.written > current.journal.durable);
    }
    for actor in actors {
        close(&mut controller, actor);
    }
}

fn add_unsynchronized_write(controller: &mut Controller, actors: &mut [Scheduled], now: Duration) {
    let mut submitter = actors[1].take_submitter().unwrap();
    let mut buffer = actors[1]
        .lease_proposal_buffer_with_limits(pipeline())
        .unwrap();
    buffer
        .push(ozzy_journal::operation::OperationKind::Barrier, &[3; 16])
        .unwrap();
    let mut pending = Box::pin(submitter.try_submit(buffer).unwrap());
    let mut confirmed = false;
    for _ in 0..10000 {
        round_at(actors, now, false);
        settle_without_sync(controller);
        if !confirmed && let Poll::Ready(result) = poll(pending.as_mut()) {
            assert!(matches!(
                result.unwrap().outcome,
                ProposalOutcome::Committed { .. }
            ));
            confirmed = true;
        }
        let snapshot = actors[1].status().normal.unwrap();
        if confirmed && snapshot.journal.written > snapshot.journal.durable {
            return;
        }
    }
    panic!("the RAM confirmation must leave a physically written, unsynchronized tail");
}

fn settle_without_sync(controller: &mut Controller) {
    let held = controller
        .jobs()
        .into_iter()
        .filter_map(|(id, _)| {
            matches!(
                controller.operation(id).unwrap().unprotected(),
                Operation::Sync { .. }
            )
            .then_some(id)
        })
        .collect::<Vec<_>>();
    settle(controller, &held);
}
