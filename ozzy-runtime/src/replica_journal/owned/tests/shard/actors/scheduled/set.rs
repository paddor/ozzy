use super::*;
use crate::replica_actor::{PartitionActor, PartitionActors, PartitionStatus};

fn step(
    shards: &mut [PartitionActors],
    routes: &mut StepRoutes,
    blocked: Option<ozzy_proto::GroupId>,
) {
    let mut messages = Vec::new();
    for (broker, shard) in shards.iter_mut().enumerate() {
        let mut cx = Context::from_waker(Waker::noop());
        let result = shard.poll_progress(&mut cx, Duration::ZERO, |_, mut message| {
            let group = routes.group(broker, &message, blocked);
            if Some(group) == blocked {
                return Err(omq_tokio::TrySendError::Full(message));
            }
            let to = usize::from(message.pop_front().unwrap()[0] - 1);
            messages.push((
                to,
                group,
                Message::with_prefix(bytes::Bytes::from(vec![broker as u8 + 1; 16]), message),
            ));
            Ok(())
        });
        assert!(result.is_pending(), "scheduler stopped: {result:?}");
    }
    for (to, group, message) in messages {
        shards[to].receive(group, &message, Duration::ZERO).unwrap();
    }
}

#[test]
fn bounded_actor_sets_preserve_healthy_groups_under_stalled_io_and_full_transport() {
    let (mut controller, io) = setup();
    let (mut stalled, first) = cluster(&mut controller, &io, 0);
    let (middle, second) = cluster(&mut controller, &io, 1);
    let (last, third) = cluster(&mut controller, &io, 2);
    let blocked = stalled[0].status().scope.group_id;
    for _ in 0..1000 {
        round(&mut stalled, false);
        if controller.jobs().iter().any(|(id, _)| {
            matches!(
                controller.operation(*id).unwrap().unprotected(),
                Operation::Write { .. }
            )
        }) {
            break;
        }
        settle(&mut controller, &[]);
    }
    let held: Vec<_> = controller.jobs().into_iter().map(|(id, _)| id).collect();
    assert_ne!(held.len(), 0);
    let mut assigned: [Vec<_>; 3] = std::array::from_fn(|_| Vec::new());
    for group in [stalled, middle, last] {
        for (broker, actor) in group.into_iter().enumerate() {
            assigned[broker].push(PartitionActor::Replicated(actor));
        }
    }
    // One actor turn per poll forces a fair scan over three distinct roles.
    let mut shards = assigned.map(|actors| PartitionActors::new(actors, 3, 1).unwrap());
    let mut routes = StepRoutes::with_prior_group(blocked);
    let mut replies = [Box::pin(second), Box::pin(third)];
    let mut done = [false; 2];
    let mut first = Box::pin(first);
    for _ in 0..10000 {
        step(&mut shards, &mut routes, Some(blocked));
        settle(&mut controller, &held);
        for (reply, done) in replies.iter_mut().zip(&mut done) {
            if !*done && let Poll::Ready(result) = poll(reply.as_mut()) {
                assert!(matches!(
                    result.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                *done = true;
            }
        }
        if done == [true; 2] {
            break;
        }
    }
    assert_eq!(done, [true; 2]);
    assert!(held.iter().all(|id| controller.operation(*id).is_some()));
    assert!(poll(first.as_mut()).is_pending());
    let mut done = false;
    for _ in 0..10000 {
        step(&mut shards, &mut routes, None);
        settle(&mut controller, &[]);
        if let Poll::Ready(result) = poll(first.as_mut()) {
            assert!(matches!(
                result.unwrap().outcome,
                ProposalOutcome::Committed { .. }
            ));
            done = true;
            break;
        }
    }
    assert!(done);
    for shard in shards {
        close_set(&mut controller, shard);
    }
}

#[test]
fn actor_turn_bound_survives_observer_cancellation_and_preserves_idle_sleep() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Wakes(AtomicUsize);
    impl std::task::Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (mut controller, io) = setup();
    let actors: Vec<_> = (0..3)
        .map(|group| {
            PartitionActor::Replicated(
                Scheduled::new(unstarted_actor(&mut controller, &io, group, 0, 64)).unwrap(),
            )
        })
        .collect();
    let groups: Vec<_> = actors.iter().map(PartitionActor::group).collect();
    let mut shard = PartitionActors::new(actors, 3, 1).unwrap();
    let counter = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&waker);
    for _ in 0..20 {
        // Return Full without arranging a transport wake; the external timer or
        // adapter will supply it. No shard spill queue or busy retry is allowed.
        for turn in 0..3 {
            let before = counter.0.load(Ordering::Relaxed);
            assert!(
                shard
                    .poll_progress(&mut cx, Duration::ZERO, |_, m| Err(
                        omq_tokio::TrySendError::Full(m)
                    ))
                    .is_pending()
            );
            let after = counter.0.load(Ordering::Relaxed);
            if turn < 2 {
                assert!(after > before);
            }
        }
        settle(&mut controller, &[]);
    }
    let before = counter.0.load(Ordering::Relaxed);
    for _ in 0..3 {
        assert!(
            shard
                .poll_progress(&mut cx, Duration::ZERO, |_, m| Err(
                    omq_tokio::TrySendError::Full(m)
                ))
                .is_pending()
        );
    }
    assert_eq!(counter.0.load(Ordering::Relaxed) - before, 2);
    for group in groups {
        let PartitionStatus::Replicated(status) = shard.status(group).unwrap() else {
            panic!("wrong authority")
        };
        assert_eq!(status.normal.unwrap().accepted, Prefix::GENESIS);
    }
    close_set(&mut controller, shard);
}

fn budgeted_step(
    shards: &mut [PartitionActors],
    routes: &mut StepRoutes,
    now: Duration,
    budget: usize,
) {
    let mut messages = Vec::new();
    for (broker, shard) in shards.iter_mut().enumerate() {
        let mut cx = Context::from_waker(Waker::noop());
        let mut admitted = 0;
        let result = shard.poll_progress(&mut cx, now, |_, mut message| {
            if admitted == budget {
                return Err(omq_tokio::TrySendError::Full(message));
            }
            admitted += 1;
            let group = routes.group(broker, &message, None);
            let to = usize::from(message.pop_front().unwrap()[0] - 1);
            messages.push((
                to,
                group,
                Message::with_prefix(bytes::Bytes::from(vec![broker as u8 + 1; 16]), message),
            ));
            Ok(())
        });
        assert!(result.is_pending(), "scheduler stopped: {result:?}");
    }
    for (to, group, message) in messages {
        shards[to].receive(group, &message, now).unwrap();
    }
}

#[test]
fn shared_transport_budget_reaches_every_partition_under_steady_control_traffic() {
    let (mut controller, io) = setup();
    let mut assigned: [Vec<_>; 3] = std::array::from_fn(|_| Vec::new());
    let mut replies = Vec::new();
    let mut groups = Vec::new();
    for group in 0..3 {
        let (actors, pending) = cluster(&mut controller, &io, group);
        groups.push(actors[0].status().scope);
        replies.push(Box::pin(pending));
        for (broker, actor) in actors.into_iter().enumerate() {
            assigned[broker].push(PartitionActor::Replicated(actor));
        }
    }
    // Every turn polls all partitions of the shard, as the broker does. The
    // shared outbound path admits two messages per turn. Leaders keep sending
    // heartbeats, so a fixed order never leaves a slot for the last partition.
    let mut shards = assigned.map(|actors| PartitionActors::new(actors, 3, 3).unwrap());
    let mut routes = StepRoutes::default();
    let mut done = [false; 3];
    for turn in 0..400 {
        budgeted_step(
            &mut shards,
            &mut routes,
            Duration::from_millis(10 * turn),
            2,
        );
        settle(&mut controller, &[]);
        for (reply, done) in replies.iter_mut().zip(&mut done) {
            if !*done && let Poll::Ready(result) = poll(reply.as_mut()) {
                assert!(matches!(
                    result.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                *done = true;
            }
        }
        if done == [true; 3] {
            break;
        }
    }
    assert_eq!(done, [true; 3], "a partition never reached the transport");
    for shard in &shards {
        for scope in &groups {
            let PartitionStatus::Replicated(status) = shard.status(scope.group_id).unwrap() else {
                panic!("wrong authority")
            };
            assert_eq!(status.scope, *scope, "a healthy leader was replaced");
        }
    }
    for shard in shards {
        close_set(&mut controller, shard);
    }
}

#[test]
fn replaced_sessions_stop_waking_quiet_partition_actors() {
    let (mut controller, io) = setup();
    let (actors, pending) = cluster(&mut controller, &io, 0);
    let mut pending = Box::pin(pending);
    let mut shards = Vec::new();
    let mut routes = StepRoutes::default();
    for (broker, mut actor) in actors.into_iter().enumerate() {
        for remote in 0..3 {
            if remote != broker {
                actor
                    .replace_session(
                        NodeId::from_bytes([remote as u8 + 1; 16]),
                        LinkSessionId::from_bytes([61; 16]),
                        LinkSessionId::from_bytes([99; 16]),
                        Duration::ZERO,
                    )
                    .unwrap();
            }
        }
        shards.push(
            PartitionActors::new(vec![PartitionActor::Replicated(actor)], 1, 1)
                .unwrap()
                .with_safety_sweep(Duration::from_millis(10))
                .unwrap(),
        );
    }
    let mut confirmed = false;
    for _ in 0..1000 {
        step(&mut shards, &mut routes, None);
        settle(&mut controller, &[]);
        if let Poll::Ready(result) = poll(pending.as_mut()) {
            assert!(matches!(
                result.unwrap().outcome,
                ProposalOutcome::Committed { .. }
            ));
            confirmed = true;
            break;
        }
    }
    assert!(
        confirmed,
        "new broker sessions never confirmed the proposal"
    );
    for _ in 0..100 {
        step(&mut shards, &mut routes, None);
        settle(&mut controller, &[]);
    }
    let quiet: Vec<_> = shards.iter().map(PartitionActors::turns).collect();
    for _ in 0..50 {
        step(&mut shards, &mut routes, None);
    }
    assert_eq!(
        shards
            .iter()
            .map(PartitionActors::turns)
            .collect::<Vec<_>>(),
        quiet,
        "settled session changes keep idle actors runnable"
    );
    for shard in shards {
        close_set(&mut controller, shard);
    }
}

#[test]
fn woken_partitions_progress_and_quiet_partitions_wait_for_their_timer_visit() {
    let (mut controller, io) = setup();
    let mut assigned: [Vec<_>; 3] = std::array::from_fn(|_| Vec::new());
    let mut replies = Vec::new();
    let mut groups = Vec::new();
    for group in 0..3 {
        let (actors, pending) = cluster(&mut controller, &io, group);
        groups.push(actors[0].status().scope);
        replies.push(Box::pin(pending));
        for (broker, actor) in actors.into_iter().enumerate() {
            assigned[broker].push(PartitionActor::Replicated(actor));
        }
    }
    let interval = Duration::from_millis(10);
    let mut shards = assigned.map(|actors| {
        PartitionActors::new(actors, 3, 3)
            .unwrap()
            .with_safety_sweep(interval)
            .unwrap()
    });
    let mut routes = StepRoutes::default();
    // Time advances 1 ms per turn. Storage results, input, and refused
    // sends are observed between the timer visits. The shared transport
    // admits two messages per turn, as in the fairness test above.
    let mut done = [false; 3];
    let mut now = Duration::ZERO;
    for turn in 0..4000 {
        now = Duration::from_millis(turn);
        budgeted_step(&mut shards, &mut routes, now, 2);
        settle(&mut controller, &[]);
        for (reply, done) in replies.iter_mut().zip(&mut done) {
            if !*done && let Poll::Ready(result) = poll(reply.as_mut()) {
                assert!(matches!(
                    result.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                *done = true;
            }
        }
        if done == [true; 3] {
            break;
        }
    }
    assert_eq!(done, [true; 3], "a partition never reached the transport");
    for shard in &shards {
        for scope in &groups {
            let PartitionStatus::Replicated(status) = shard.status(scope.group_id).unwrap() else {
                panic!("wrong authority")
            };
            assert_eq!(status.scope, *scope, "a healthy leader was replaced");
        }
    }
    // Let the remaining replies and storage results settle at one instant.
    for _ in 0..100 {
        budgeted_step(&mut shards, &mut routes, now, usize::MAX);
        settle(&mut controller, &[]);
    }
    // Nothing happens and no time passes: no partition is visited.
    let quiet: Vec<_> = shards.iter().map(PartitionActors::turns).collect();
    for _ in 0..50 {
        budgeted_step(&mut shards, &mut routes, now, usize::MAX);
    }
    assert_eq!(
        shards
            .iter()
            .map(PartitionActors::turns)
            .collect::<Vec<_>>(),
        quiet
    );
    // The timer visit reaches every partition exactly once.
    now += interval;
    budgeted_step(&mut shards, &mut routes, now, 0);
    for (shard, quiet) in shards.iter().zip(&quiet) {
        assert_eq!(shard.turns() - quiet, 3);
    }
    for shard in shards {
        close_set(&mut controller, shard);
    }
}

#[test]
fn quiet_partitions_are_visited_at_their_own_timers_between_sweeps() {
    let (mut controller, io) = setup();
    let (actors, pending) = cluster(&mut controller, &io, 0);
    let mut reply = Box::pin(pending);
    let sweep = Duration::from_secs(1);
    let mut shards: Vec<_> = actors
        .into_iter()
        .map(|actor| {
            PartitionActors::new(vec![PartitionActor::Replicated(actor)], 1, 1)
                .unwrap()
                .with_safety_sweep(sweep)
                .unwrap()
        })
        .collect();
    let mut routes = StepRoutes::default();
    let mut now = Duration::ZERO;
    let mut committed = false;
    for millis in 0..4000 {
        now = Duration::from_millis(millis);
        budgeted_step(&mut shards, &mut routes, now, usize::MAX);
        settle(&mut controller, &[]);
        if poll(reply.as_mut()).is_ready() {
            committed = true;
            break;
        }
    }
    assert!(committed, "the group never confirmed its proposal");
    for _ in 0..100 {
        budgeted_step(&mut shards, &mut routes, now, usize::MAX);
        settle(&mut controller, &[]);
    }
    // A quiet group still keeps protocol timers, due well before the sweep.
    let deadline = shards
        .iter()
        .filter_map(PartitionActors::next_deadline)
        .min()
        .unwrap();
    assert!(deadline > now && deadline < now + sweep, "{deadline:?}");
    let turns = |shards: &[PartitionActors]| shards.iter().map(PartitionActors::turns).sum::<u64>();
    let quiet = turns(&shards);
    budgeted_step(
        &mut shards,
        &mut routes,
        deadline.saturating_sub(Duration::from_micros(1)),
        usize::MAX,
    );
    assert_eq!(turns(&shards), quiet, "visited before any timer was due");
    budgeted_step(&mut shards, &mut routes, deadline, usize::MAX);
    assert!(turns(&shards) > quiet, "the due timer was not visited");
    for shard in shards {
        close_set(&mut controller, shard);
    }
}

/// Broker 0 leads groups 0 and 3 on one shard, with its own storage lane.
/// Its device stalls until both primaries suspect it. One job then
/// completes, for one group, and wakes only that partition. The other
/// partition's suspicion of storage must end at the same instant.
#[test]
fn a_job_completed_for_one_partition_ends_another_partitions_storage_stall() {
    let (mut controller, lanes) = setup_lanes(Image::default(), 2);
    let mut assigned: [Vec<_>; 3] = std::array::from_fn(|_| Vec::new());
    let mut first = Vec::new();
    let mut later = Vec::new();
    for group in [0, 3] {
        for broker in 0..3 {
            let lane = &lanes[usize::from(broker != 0)];
            let mut actor = unstarted_actor(&mut controller, lane, group, broker, 64);
            if broker == 0 {
                let mut submitter = actor.take_submitter().unwrap();
                let proposal = |byte: u8| {
                    let mut buffer = actor.lease_proposal_buffer().unwrap();
                    buffer
                        .push(ozzy_journal::operation::OperationKind::Barrier, &[byte; 16])
                        .unwrap();
                    buffer
                };
                let (now, next) = (proposal(group + 1), proposal(group + 2));
                first.push(Box::pin(submitter.try_submit(now).unwrap()));
                later.push((submitter, next));
            }
            assigned[usize::from(broker)]
                .push(PartitionActor::Replicated(Scheduled::new(actor).unwrap()));
        }
    }
    let groups: [_; 2] = std::array::from_fn(|slot| assigned[0][slot].group());
    let mut shards = assigned.map(|actors| {
        PartitionActors::new(actors, 2, 2)
            .unwrap()
            .with_safety_sweep(Duration::from_secs(1))
            .unwrap()
    });
    let mut routes = StepRoutes::default();
    let mut held = Vec::new();
    let mut millis = 0;
    let mut confirmed = [false; 2];
    while confirmed != [true; 2] {
        assert!(millis < 4000, "the groups never confirmed a proposal");
        held_step(&mut shards, &mut routes, &mut controller, None, millis);
        for (reply, done) in first.iter_mut().zip(&mut confirmed) {
            *done |= poll(reply.as_mut()).is_ready();
        }
        millis += 1;
    }
    let _pending: Vec<_> = later
        .into_iter()
        .map(|(mut submitter, buffer)| submitter.try_submit(buffer).unwrap())
        .collect();
    // Hold every job broker 0 submits until the primary timeout of busy
    // storage passes for both primaries.
    let stalled =
        |shard: &PartitionActors| groups.map(|group| shard.storage_stalled(group).unwrap());
    while stalled(&shards[0]) != [true; 2] {
        assert!(millis < 8000, "busy storage raised no suspicion");
        held_step(
            &mut shards,
            &mut routes,
            &mut controller,
            Some(&mut held),
            millis,
        );
        millis += 1;
    }
    // Time stands still from here, so no timer or sweep visits a partition.
    for _ in 0..10 {
        held_step(
            &mut shards,
            &mut routes,
            &mut controller,
            Some(&mut held),
            millis,
        );
    }
    assert_eq!(stalled(&shards[0]), [true; 2]);
    let job = held.remove(0);
    controller.execute(job, Effect::Normal).unwrap();
    controller.deliver(job).unwrap();
    for _ in 0..10 {
        held_step(
            &mut shards,
            &mut routes,
            &mut controller,
            Some(&mut held),
            millis,
        );
    }
    assert_eq!(
        stalled(&shards[0]),
        [false; 2],
        "a primary kept suspecting storage"
    );
    for shard in shards {
        close_set(&mut controller, shard);
    }
}

/// Poll each shard and deliver its messages at one instant, then settle
/// storage. With `held`, every job broker 0 submits stays held.
fn held_step(
    shards: &mut [PartitionActors],
    routes: &mut StepRoutes,
    controller: &mut Controller,
    mut held: Option<&mut Vec<JobId>>,
    millis: u64,
) {
    let now = Duration::from_millis(millis);
    let jobs = |controller: &mut Controller| {
        controller
            .jobs()
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>()
    };
    let mut messages = Vec::new();
    for (broker, shard) in shards.iter_mut().enumerate() {
        let before = jobs(controller);
        let mut cx = Context::from_waker(Waker::noop());
        let result = shard.poll_progress(&mut cx, now, |_, mut message| {
            let group = routes.group(broker, &message, None);
            let to = usize::from(message.pop_front().unwrap()[0] - 1);
            let message =
                Message::with_prefix(bytes::Bytes::from(vec![broker as u8 + 1; 16]), message);
            messages.push((to, group, message));
            Ok(())
        });
        assert!(result.is_pending(), "scheduler stopped: {result:?}");
        if broker == 0
            && let Some(held) = held.as_deref_mut()
        {
            held.extend(
                jobs(controller)
                    .into_iter()
                    .filter(|id| !before.contains(id)),
            );
        }
    }
    for (to, group, message) in messages {
        shards[to].receive(group, &message, now).unwrap();
    }
    settle(controller, held.as_deref().map_or(&[], Vec::as_slice));
}

fn close_set(controller: &mut Controller, actors: PartitionActors) {
    let mut closing = std::pin::pin!(actors.shutdown());
    for _ in 0..10000 {
        if let Poll::Ready(result) = poll(closing.as_mut()) {
            result.unwrap();
            return;
        }
        settle(controller, &[]);
    }
    panic!("partition drains did not finish");
}
