//! A healthy pair must progress while its third broker lags, stalls, or is
//! gone. Each model drives the scheduled actors over a modeled broker
//! transport and requires the pair to confirm and the lagging copy to catch up.

use super::*;
use crate::replica_actor::{ActorStatus, ProposalSubmitter, ScheduledRecovery};
use crate::replica_journal::ProposalBuffer;
use bytes::Bytes;
use ozzy_proto::Opcode;
use std::collections::VecDeque;

#[test]
fn lagging_follower_catches_up_after_the_other_follower_stops() {
    for lagging in [2, 12] {
        for lost in [false, true] {
            for stop in [Stop::Silent, Stop::Refusing, Stop::Disconnected] {
                for head_start in [0, 5, 200] {
                    lagging_follower(Lag { lagging, lost }, stop, head_start);
                }
            }
        }
    }
}

#[test]
fn memory_blocked_follower_catches_up_after_the_other_follower_stops() {
    for lagging in [1, 3, 12] {
        for stop in [Stop::Silent, Stop::Refusing, Stop::Disconnected] {
            memory_blocked_follower(lagging, stop);
        }
    }
}

#[test]
fn lagging_follower_reconnects_while_the_other_follower_stops() {
    for lagging in [2, 12] {
        for delay in [0, 3, 300] {
            reconnecting_follower(lagging, delay);
        }
    }
}

#[test]
fn former_leaders_follow_and_catch_up_after_the_other_follower_stops() {
    for changes in 1..=3 {
        for lagging in [0, 3, 12] {
            former_leader(changes, lagging);
        }
    }
}

#[test]
fn lagging_follower_catches_up_while_the_other_follower_recovers() {
    for lagging in [0, 2, 12] {
        for reconnect in [0, 50, 5000] {
            recovering_peer(lagging, reconnect);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Lag {
    /// Proposals follower 1 misses while follower 2 confirms them.
    lagging: u8,
    /// Its PEER data is dropped instead of waiting at the sender.
    lost: bool,
}

/// How the third broker leaves for good.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// It stops running; sends to it vanish.
    Silent,
    /// It stops reading; sends to it find a full queue.
    Refusing,
    /// Its links close, so the pair disconnects its sessions.
    Disconnected,
}

/// Follower 1 receives no data while follower 2 confirms `lag.lagging`
/// proposals. It catches up for `head_start` steps, then follower 2 stops.
fn lagging_follower(lag: Lag, stop: Stop, head_start: u32) {
    let context = format!("{lag:?} {stop:?} head_start={head_start}");
    let mut cluster = Cluster::new();
    cluster.transport.slow = vec![1];
    cluster.transport.lost = lag.lost;
    for body in 1..=lag.lagging {
        cluster.propose(0, body, &context);
    }
    assert_eq!(cluster.status(1).normal.unwrap().accepted, Prefix::GENESIS);
    cluster.transport.slow.clear();
    cluster.steps(head_start);
    cluster.stop(2, stop);
    cluster.propose(0, lag.lagging + 1, &context);
    cluster.wait_caught_up(1, &context);
    cluster.close();
}

/// Follower 1 cannot reserve receive memory, so its data lane holds the
/// first refused frame while follower 2 confirms `lagging` proposals. Then
/// follower 2 stops and the memory frees.
fn memory_blocked_follower(lagging: u8, stop: Stop) {
    let context = format!("lagging={lagging} {stop:?}");
    let mut cluster = Cluster::new();
    let domain = crate::memory::Domain::new(None, 4096).unwrap();
    let owner = domain
        .owner(crate::memory::Limits {
            bytes: 4096,
            buffers: 1,
            cache_bytes: 4096,
        })
        .unwrap();
    let blocked = futures::executor::block_on(owner.lease(4096)).unwrap();
    cluster.normal(1).bind_receive_owner(&owner).unwrap();
    for body in 1..=lagging {
        cluster.propose(0, body, &context);
    }
    assert_eq!(cluster.status(1).normal.unwrap().accepted, Prefix::GENESIS);
    assert!(
        !cluster.transport.lanes[1].is_empty(),
        "the lane holds data"
    );
    cluster.stop(2, stop);
    drop(blocked);
    cluster.propose(0, lagging + 1, &context);
    cluster.wait_caught_up(1, &context);
    cluster.close();
}

/// Follower 1 lags, then its links reset as follower 2 stops. The leader
/// applies the new session at once, follower 1 only after `delay` steps.
fn reconnecting_follower(lagging: u8, delay: u32) {
    let context = format!("lagging={lagging} delay={delay}");
    let mut cluster = Cluster::new();
    cluster.transport.slow = vec![1];
    for body in 1..=lagging {
        cluster.propose(0, body, &context);
    }
    cluster.stop(2, Stop::Disconnected);
    cluster.cut(1);
    cluster.transport.slow.clear();
    let fresh = LinkSessionId::from_bytes([90; 16]);
    cluster.connect(0, 1, fresh);
    cluster.steps(delay);
    cluster.connect(1, 0, fresh);
    cluster.propose(0, lagging + 1, &context);
    cluster.wait_caught_up(1, &context);
    cluster.close();
}

/// Cut the leader off `changes` times; each time the others elect a new
/// leader and the old one rejoins as a follower. The latest former leader
/// then misses `lagging` proposals, and the third broker stops.
fn former_leader(changes: u8, lagging: u8) {
    let context = format!("changes={changes} lagging={lagging}");
    let mut cluster = Cluster::new();
    cluster.propose(0, 1, &context);
    let mut former = 0;
    for change in 0..changes {
        former = cluster.leader().unwrap();
        let view = cluster.status(former).scope.view;
        cluster.cut(former);
        cluster.now += Duration::from_secs(3);
        let elected = cluster.run_until(|nodes| {
            leader(nodes).is_some_and(|leader| nodes[leader].status().scope.view > view)
        });
        assert!(
            elected,
            "{context}: change {change}: {:#?}",
            cluster.statuses()
        );
        let fresh = LinkSessionId::from_bytes([100 + change; 16]);
        for peer in (0..3).filter(|peer| *peer != former) {
            cluster.connect(former, peer, fresh);
            cluster.connect(peer, former, fresh);
        }
        cluster.wait_caught_up(former, &context);
    }
    let leader = cluster.leader().unwrap();
    let third = 3 - leader - former;
    cluster.transport.slow = vec![former];
    for body in 0..lagging {
        cluster.propose(leader, body + 2, &context);
    }
    cluster.transport.slow.clear();
    cluster.stop(third, Stop::Disconnected);
    cluster.propose(leader, lagging + 2, &context);
    cluster.wait_caught_up(former, &context);
    cluster.close();
}

/// Follower 1 misses `lagging` proposals. Then follower 2 crashes into
/// quarantine recovery, and its links come back after `reconnect` steps.
/// The pair confirms through follower 1 while follower 2 asks for donors.
fn recovering_peer(lagging: u8, reconnect: u32) {
    let context = format!("lagging={lagging} reconnect={reconnect}");
    let mut cluster = Cluster::new();
    cluster.transport.slow = vec![1];
    for body in 1..=lagging {
        cluster.propose(0, body, &context);
    }
    cluster.transport.slow.clear();
    cluster.cut(2);
    cluster.quarantine(2);
    cluster.steps(reconnect);
    let fresh = LinkSessionId::from_bytes([110; 16]);
    for peer in [0, 1] {
        cluster.connect(2, peer, fresh);
        cluster.connect(peer, 2, fresh);
    }
    cluster.propose(0, lagging + 1, &context);
    cluster.wait_caught_up(1, &context);
    let recovered = cluster.run_until(|nodes| matches!(nodes[2], Node::Normal(_)));
    assert!(recovered, "{context}: {:#?}", cluster.statuses());
    cluster.wait_caught_up(2, &context);
    cluster.close();
}

/// One actor of the group: serving, or rebuilding its copy from donors.
enum Node {
    Normal(Scheduled),
    Recovering(Box<ScheduledRecovery>),
}

impl Node {
    fn status(&self) -> ActorStatus {
        match self {
            Self::Normal(actor) => actor.status(),
            Self::Recovering(actor) => actor.status(),
        }
    }
}

/// Broker transport: lossy PUB, PEER data held at the sender while its
/// receiver is slow, and one data lane per receiver. Like a shard's broker
/// data lane, a lane retries a refused frame before anything behind it.
#[derive(Default)]
struct Transport {
    /// Receivers that miss PUB copies and whose PEER data waits at the sender.
    slow: Vec<usize>,
    /// Drop PEER data to slow receivers instead of holding it.
    lost: bool,
    /// An actor that does not run.
    stopped: Option<usize>,
    /// Sends to the stopped actor find a full queue instead of vanishing.
    refusing: bool,
    lanes: [VecDeque<Message>; 3],
}

impl Transport {
    fn step(&mut self, nodes: &mut [Node], now: Duration) {
        let mut control = Vec::new();
        for (from, node) in nodes.iter_mut().enumerate() {
            if Some(from) == self.stopped {
                continue;
            }
            match node {
                Node::Normal(actor) => {
                    actor.advance(now).unwrap();
                    {
                        let mut event = std::pin::pin!(actor.changed(now));
                        if let Poll::Ready(result) = poll(event.as_mut()) {
                            result.unwrap();
                        }
                    }
                    actor
                        .flush(|message| self.route(from, message, &mut control))
                        .unwrap();
                }
                Node::Recovering(actor) => {
                    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                    actor
                        .poll_progress(&mut cx, now, |_, message| {
                            self.route(from, message, &mut control)
                        })
                        .unwrap();
                }
            }
        }
        for (to, message) in control {
            match &mut nodes[to] {
                Node::Normal(actor) => actor.receive(&message, now).unwrap(),
                Node::Recovering(actor) => actor.receive(&message, now).unwrap(),
            }
        }
        for (to, lane) in self.lanes.iter_mut().enumerate() {
            if Some(to) == self.stopped {
                lane.clear();
                continue;
            }
            while let Some(message) = lane.front() {
                // Recovery drops follower data, as the shard does.
                if let Node::Normal(actor) = &mut nodes[to]
                    && !actor.receive_data(message, now).unwrap()
                {
                    break;
                }
                lane.pop_front();
            }
        }
    }

    fn route(
        &mut self,
        from: usize,
        mut message: Message,
        control: &mut Vec<(usize, Message)>,
    ) -> Result<(), omq_tokio::TrySendError> {
        let source = Bytes::from(vec![from as u8 + 1; 16]);
        let command = opcode(&message);
        if command == Opcode::PreparePub {
            message.pop_front();
            for to in (0..3)
                .filter(|to| *to != from && !self.slow.contains(to) && Some(*to) != self.stopped)
            {
                let copy = Message::with_prefix(source.clone(), message.clone());
                self.lanes[to].push_back(copy);
            }
            return Ok(());
        }
        let to = usize::from(message.part_slice(0).unwrap()[0] - 1);
        if Some(to) == self.stopped {
            return if self.refusing {
                Err(omq_tokio::TrySendError::Full(message))
            } else {
                Ok(())
            };
        }
        let data = command == Opcode::PrepareFlow;
        if data && self.slow.contains(&to) {
            return if self.lost {
                Ok(())
            } else {
                Err(omq_tokio::TrySendError::Full(message))
            };
        }
        message.pop_front();
        let message = Message::with_prefix(source, message);
        if data {
            self.lanes[to].push_back(message);
        } else {
            control.push((to, message));
        }
        Ok(())
    }
}

/// Three actors of group 0, each with its own proposal lane. Actor `n` is
/// node `n + 1`; the leader of view `v` is actor `v % 3`.
struct Cluster {
    controller: Controller,
    _io: Local,
    nodes: Vec<Node>,
    lanes: Vec<ProposalSubmitter>,
    buffers: Vec<Option<ProposalBuffer>>,
    /// Current session of each actor's link to each peer.
    sessions: [[LinkSessionId; 3]; 3],
    transport: Transport,
    now: Duration,
}

impl Cluster {
    fn new() -> Self {
        let (mut controller, io) = setup();
        let mut nodes = Vec::new();
        let mut lanes = Vec::new();
        let mut buffers = Vec::new();
        for broker in 0..3 {
            // Brokers run retention turns; a due turn drains the pipeline.
            let settings = ActorSettings {
                retention_interval: Some(Duration::from_millis(1)),
                ..Default::default()
            };
            let mut actor = configured_actor(&mut controller, &io, 0, broker, settings);
            lanes.push(actor.take_submitter().unwrap());
            buffers.push(Some(actor.lease_proposal_buffer().unwrap()));
            let mut actor = Scheduled::new(actor).unwrap();
            actor.enable_publication();
            nodes.push(Node::Normal(actor));
        }
        Self {
            controller,
            _io: io,
            nodes,
            lanes,
            buffers,
            sessions: [actor_config().sessions; 3],
            transport: Transport::default(),
            now: Duration::ZERO,
        }
    }

    fn normal(&mut self, actor: usize) -> &mut Scheduled {
        match &mut self.nodes[actor] {
            Node::Normal(actor) => actor,
            Node::Recovering(_) => panic!("actor {actor} is recovering"),
        }
    }

    fn status(&self, actor: usize) -> ActorStatus {
        self.nodes[actor].status()
    }

    fn statuses(&self) -> Vec<ActorStatus> {
        self.nodes.iter().map(Node::status).collect()
    }

    /// Restart `actor` in quarantine recovery, keeping its sessions.
    fn quarantine(&mut self, actor: usize) {
        self.nodes = std::mem::take(&mut self.nodes)
            .into_iter()
            .enumerate()
            .map(|(index, node)| match node {
                Node::Normal(normal) if index == actor => {
                    Node::Recovering(Box::new(ScheduledRecovery::quarantined(normal)))
                }
                node => node,
            })
            .collect();
        assert!(matches!(self.nodes[actor], Node::Recovering(_)));
    }

    fn steps(&mut self, count: u32) {
        for _ in 0..count {
            self.now += Duration::from_micros(100);
            self.transport.step(&mut self.nodes, self.now);
            settle(&mut self.controller, &[]);
            for node in &mut self.nodes {
                if let Node::Recovering(recovery) = node
                    && let Some(mut actor) = recovery.take_ready()
                {
                    actor.enable_publication();
                    *node = Node::Normal(actor);
                }
            }
        }
    }

    /// Step until `done`, for at most ten seconds of broker time.
    fn run_until(&mut self, mut done: impl FnMut(&[Node]) -> bool) -> bool {
        for _ in 0..100_000 {
            self.steps(1);
            if done(&self.nodes) {
                return true;
            }
        }
        false
    }

    /// Confirm one single-operation proposal through `leader`.
    fn propose(&mut self, leader: usize, body: u8, context: &str) {
        let mut buffer = self.buffers[leader].take().unwrap();
        buffer.clear();
        buffer
            .push(ozzy_journal::operation::OperationKind::Barrier, &[body; 16])
            .unwrap();
        let mut pending = Box::pin(self.lanes[leader].try_submit(buffer).unwrap());
        let mut reply = None;
        self.run_until(|_| {
            if let Poll::Ready(result) = poll(pending.as_mut()) {
                reply = Some(result.unwrap());
            }
            reply.is_some()
        });
        let Some(reply) = reply else {
            panic!("{context}: proposal {body} stalled: {:#?}", self.statuses());
        };
        assert!(
            matches!(reply.outcome, ProposalOutcome::Committed { .. }),
            "{context}: {:?}",
            reply.outcome
        );
        self.buffers[leader] = Some(reply.buffer);
    }

    /// Wait until `actor` applied everything the leader applied.
    fn wait_caught_up(&mut self, actor: usize, context: &str) {
        let caught_up = self.run_until(|nodes| {
            leader(nodes).is_some_and(|leader| {
                let target = nodes[leader].status();
                let status = nodes[actor].status();
                status.scope == target.scope
                    && status.application_ready
                    && status.normal.map(|normal| normal.applied)
                        == target.normal.map(|normal| normal.applied)
            })
        });
        assert!(
            caught_up,
            "{context}: actor {actor} did not catch up: {:#?}",
            self.statuses()
        );
    }

    fn leader(&self) -> Option<usize> {
        leader(&self.nodes)
    }

    /// Stop `actor` for good.
    fn stop(&mut self, actor: usize, stop: Stop) {
        self.transport.stopped = Some(actor);
        self.transport.refusing = stop == Stop::Refusing;
        if stop == Stop::Disconnected {
            self.cut(actor);
        }
    }

    /// Close every link of `actor` on both ends.
    fn cut(&mut self, actor: usize) {
        for peer in (0..3).filter(|peer| *peer != actor) {
            self.disconnect(actor, peer);
            self.disconnect(peer, actor);
        }
    }

    fn disconnect(&mut self, actor: usize, peer: usize) {
        let session = std::mem::replace(
            &mut self.sessions[actor][peer],
            LinkSessionId::from_bytes([0; 16]),
        );
        let now = self.now;
        match &mut self.nodes[actor] {
            Node::Normal(node) => node.disconnect_session(self::node(peer), session, now),
            Node::Recovering(node) => node.disconnect_session(self::node(peer), session, now),
        }
        .unwrap();
    }

    /// Apply a newly established link session on `actor`'s end only.
    fn connect(&mut self, actor: usize, peer: usize, session: LinkSessionId) {
        let old = std::mem::replace(&mut self.sessions[actor][peer], session);
        let now = self.now;
        match &mut self.nodes[actor] {
            Node::Normal(node) => node.replace_session(self::node(peer), old, session, now),
            Node::Recovering(node) => node.replace_session(self::node(peer), old, session, now),
        }
        .unwrap();
    }

    fn close(mut self) {
        for node in std::mem::take(&mut self.nodes) {
            match node {
                Node::Normal(actor) => close(&mut self.controller, actor),
                Node::Recovering(recovery) => {
                    let mut closing = std::pin::pin!(recovery.shutdown());
                    let mut closed = false;
                    for _ in 0..10000 {
                        if let Poll::Ready(result) = poll(closing.as_mut()) {
                            result.unwrap();
                            closed = true;
                            break;
                        }
                        settle(&mut self.controller, &[]);
                    }
                    assert!(closed, "recovery shutdown did not drain");
                }
            }
        }
    }
}

/// The ready primary in the newest view. A cut-off old leader can still
/// report itself ready in its old view.
fn leader(nodes: &[Node]) -> Option<usize> {
    nodes
        .iter()
        .map(Node::status)
        .enumerate()
        .filter(|(_, status)| {
            status.application_ready
                && status
                    .normal
                    .is_some_and(|normal| normal.backups.is_some() && normal.ready_for_appends)
        })
        .max_by_key(|(_, status)| status.scope.view)
        .map(|(index, _)| index)
}

fn node(actor: usize) -> NodeId {
    NodeId::from_bytes([actor as u8 + 1; 16])
}
