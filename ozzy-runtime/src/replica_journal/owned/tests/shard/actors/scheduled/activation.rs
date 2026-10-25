use super::*;
use crate::replica_actor::{ActorStatus, ProposalReply};
use crate::replica_journal::ProposalBuffer;
use bytes::Bytes;
use ozzy_proto::Opcode;

/// The new backup observes its activation while the new primary's first
/// publication to it is in flight, and the publication arrives before the
/// round that restarts the backup's receipt ledger. Staging it in the
/// replaced ledger would admit an operation the new ledger never releases.
#[test]
fn backup_stages_no_publication_before_its_receipt_restarts() {
    let (mut controller, io) = setup();
    let (mut actors, mut lanes, mut buffers) = (Vec::new(), Vec::new(), Vec::new());
    for broker in 0..3 {
        let mut actor = unstarted_actor(&mut controller, &io, 0, broker, 64);
        lanes.push(actor.take_submitter().unwrap());
        buffers.push(Some(actor.lease_proposal_buffer().unwrap()));
        let mut actor = Scheduled::new(actor).unwrap();
        actor.enable_publication();
        actors.push(actor);
    }
    let mut first = Box::pin(
        lanes[0]
            .try_submit(barrier(buffers[0].take().unwrap(), 1))
            .unwrap(),
    );
    let mut confirmed = false;
    for step in 0..10000 {
        let now = Duration::from_micros(step * 100);
        for (to, message, data) in exchange(&mut actors, now, None, |_, _| false) {
            deliver(&mut actors[to], &message, data, now);
        }
        settle(&mut controller, &[]);
        if let Poll::Ready(reply) = poll(first.as_mut()) {
            assert!(committed_through(&reply.unwrap(), 1));
            confirmed = true;
            break;
        }
    }
    assert!(confirmed);
    // Broker 0 fails. The backup of the next view observes its activation
    // only once the new primary's first publication to it is held.
    let activating = |index: usize, status: &ActorStatus| {
        status.scope.view % 3 != index as u64
            && status.normal.is_some_and(|normal| normal.scope.view > 0)
            && !status.application_ready
    };
    let mut held = Vec::<(usize, Message, bool)>::new();
    let mut released = false;
    let mut second = None;
    let mut done = false;
    let mut now = Duration::from_secs(3);
    for step in 0..20000 {
        if step % 16 == 15 {
            now += Duration::from_millis(1);
        }
        let messages = exchange(&mut actors, now, Some(0), |index, status| {
            held.is_empty() && activating(index, status)
        });
        if let Some(&(to, ..)) = held.first()
            && actors[to].status().application_ready
        {
            for (to, message, data) in held.drain(..) {
                deliver(&mut actors[to], &message, data, now);
            }
            released = true;
        }
        for (to, message, data) in messages {
            if data && !released && activating(to, &actors[to].status()) {
                held.push((to, message, data));
            } else {
                deliver(&mut actors[to], &message, data, now);
            }
        }
        settle(&mut controller, &[]);
        if second.is_none()
            && !done
            && let Some(primary) = (1..3).find(|&index| {
                let status = actors[index].status();
                status.application_ready && status.scope.view % 3 == index as u64
            })
        {
            let buffer = barrier(buffers[primary].take().unwrap(), 2);
            second = Some(Box::pin(lanes[primary].try_submit(buffer).unwrap()));
        }
        if let Some(pending) = &mut second
            && let Poll::Ready(reply) = poll(pending.as_mut())
        {
            assert!(committed_through(&reply.unwrap(), 2));
            second = None;
            done = true;
        }
        // Both copies release the new operation once applied and durable.
        if done
            && actors[1..].iter().all(|actor| {
                actor
                    .status()
                    .normal
                    .is_some_and(|normal| normal.applied.op.0 == 2 && normal.journal.durable.0 == 2)
            })
        {
            break;
        }
    }
    assert!(released, "no publication reached the backup at activation");
    assert!(
        done,
        "{:?}",
        actors.iter().map(Scheduled::status).collect::<Vec<_>>()
    );
    for _ in 0..100 {
        for (to, message, data) in exchange(&mut actors, now, Some(0), |_, _| false) {
            deliver(&mut actors[to], &message, data, now);
        }
        settle(&mut controller, &[]);
    }
    for actor in actors {
        close(&mut controller, actor);
    }
}

/// Advance every broker but `failed` and collect messages between them,
/// marking data frames. A broker for which `wait` holds observes no disk
/// completion this round.
fn exchange(
    actors: &mut [Scheduled],
    now: Duration,
    failed: Option<usize>,
    mut wait: impl FnMut(usize, &ActorStatus) -> bool,
) -> Vec<(usize, Message, bool)> {
    let mut messages = Vec::new();
    for (from, actor) in actors.iter_mut().enumerate() {
        if Some(from) == failed {
            continue;
        }
        actor.advance(now).unwrap();
        if !wait(from, &actor.status()) {
            let mut changed = std::pin::pin!(actor.changed(now));
            if let Poll::Ready(result) = poll(changed.as_mut()) {
                result.unwrap();
            }
        }
        actor
            .flush(|mut message| {
                let command = opcode(&message);
                let route = message.pop_front().unwrap();
                let sender = Bytes::from(vec![from as u8 + 1; 16]);
                let to: Vec<_> = if command == Opcode::PreparePub {
                    (0..3).filter(|&to| to != from).collect()
                } else {
                    vec![usize::from(route[0] - 1)]
                };
                for to in to.into_iter().filter(|&to| Some(to) != failed) {
                    let data = matches!(command, Opcode::PreparePub | Opcode::PrepareFlow);
                    messages.push((
                        to,
                        Message::with_prefix(sender.clone(), message.clone()),
                        data,
                    ));
                }
                Ok(())
            })
            .unwrap();
    }
    messages
}

fn deliver(actor: &mut Scheduled, message: &Message, data: bool, now: Duration) {
    if data {
        assert!(actor.receive_data(message, now).unwrap());
    } else {
        actor.receive(message, now).unwrap();
    }
}

fn barrier(mut buffer: ProposalBuffer, id: u8) -> ProposalBuffer {
    buffer
        .push(ozzy_journal::operation::OperationKind::Barrier, &[id; 16])
        .unwrap();
    buffer
}

fn committed_through(reply: &ProposalReply, op: u64) -> bool {
    matches!(reply.outcome, ProposalOutcome::Committed { through, .. } if through.op.0 == op)
}
