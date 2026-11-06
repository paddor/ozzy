//! Random overlapping faults: end the due ones, draw the next, and keep
//! cuts and connections as the active ones say.

use super::{DISK_STEPS, Loss, Sim, Stuck};
use crate::{
    broker::session,
    chaos::{self, Kind},
    network::BROKERS,
};

impl Sim {
    /// Whether a random fault of `kind` holds `broker`.
    pub(super) fn held(&self, broker: usize, kind: Kind) -> bool {
        self.chaos
            .as_ref()
            .is_some_and(|chaos| chaos.holds(broker, kind))
    }

    /// End due random faults, all of them once draining, then maybe start a
    /// new one.
    pub(super) fn nemesis(&mut self, tick: u64, draining: bool) -> Result<(), String> {
        let Some(chaos) = &mut self.chaos else {
            return Ok(());
        };
        let ended = if draining {
            chaos.end_all()
        } else {
            chaos.ended(tick)
        };
        let mut changed = !ended.is_empty();
        for fault in ended {
            match fault.kind {
                Kind::Stall => self.brokers[fault.broker].disk.stall(0),
                Kind::FailDisk => self.brokers[fault.broker].disk.heal_writes(),
                Kind::Skew => self.brokers[fault.broker].step_clock(0, tick),
                _ => {}
            }
        }
        let connected = self.connected;
        let drawn = if draining {
            None
        } else {
            self.chaos
                .as_mut()
                .and_then(|chaos| chaos.draw(tick, |left, right| connected[left][right]))
        };
        if let Some(fault) = drawn {
            changed = true;
            self.trace.word(tick);
            self.trace.word(fault.kind as u64);
            self.trace.word(fault.broker as u64);
            self.start(fault, tick)?;
        }
        if changed {
            self.apply_cuts();
            self.relink()?;
        }
        Ok(())
    }

    fn start(&mut self, fault: chaos::Fault, tick: u64) -> Result<(), String> {
        let broker = fault.broker;
        match fault.kind {
            Kind::PowerLoss | Kind::Kill => {
                self.network.drop_links(broker);
                let loss = if fault.kind == Kind::Kill {
                    Loss::Process
                } else {
                    Loss::Power
                };
                self.restart(broker, tick, loss)?;
            }
            Kind::Stall => self.brokers[broker].disk.stall(fault.until * DISK_STEPS),
            Kind::FailDisk => self.brokers[broker].disk.fail_writes(),
            Kind::LinkReset => {
                let now = self.clock.now();
                let session = session(broker, fault.peer, self.epoch(broker, fault.peer));
                self.network.drop_link(broker, fault.peer);
                self.brokers[broker].disconnect(fault.peer, session, now)?;
                self.brokers[fault.peer].disconnect(broker, session, now)?;
                self.connected[broker][fault.peer] = false;
                self.connected[fault.peer][broker] = false;
            }
            Kind::ClientReset => self.brokers[broker].reset_clients(),
            Kind::Skew => self.brokers[broker].step_clock(fault.offset, tick),
            Kind::Pause | Kind::Isolate | Kind::Deaf | Kind::Mute => {}
        }
        Ok(())
    }

    /// Cut each broker as its random faults say. A broker that restarted
    /// stays cut off until its fault ends.
    fn apply_cuts(&mut self) {
        let mut cuts = [None; BROKERS];
        if let Some(chaos) = &self.chaos {
            for fault in &chaos.active {
                let direction = match fault.kind {
                    Kind::PowerLoss | Kind::Kill | Kind::Isolate => 0,
                    Kind::Mute => 1,
                    Kind::Deaf => 2,
                    _ => continue,
                };
                let cut: &mut Option<u64> = &mut cuts[fault.broker];
                *cut =
                    Some(cut.map_or(direction, |old: u64| if old == direction { old } else { 0 }));
            }
        }
        self.network.set_cuts(cuts);
    }

    /// Connect every pair without a connection unless a random fault keeps
    /// it down: a restarted broker until its fault ends, a reset connection
    /// until it comes back.
    pub(super) fn relink(&mut self) -> Result<(), String> {
        let Some(chaos) = &self.chaos else {
            return Ok(());
        };
        let gone = self
            .outsider
            .as_ref()
            .filter(|outsider| outsider.kind == Stuck::Down)
            .map(|outsider| outsider.broker);
        let down = |broker: usize| {
            chaos.holds(broker, Kind::PowerLoss)
                || chaos.holds(broker, Kind::Kill)
                || gone == Some(broker)
        };
        let reset = |left: usize, right: usize| {
            chaos.active.iter().any(|fault| {
                fault.kind == Kind::LinkReset
                    && ((fault.broker, fault.peer) == (left, right)
                        || (fault.broker, fault.peer) == (right, left))
            })
        };
        let connected = &self.connected;
        let pairs: Vec<(usize, usize)> = (0..BROKERS)
            .flat_map(|left| (left + 1..BROKERS).map(move |right| (left, right)))
            .filter(|&(left, right)| {
                !connected[left][right] && !down(left) && !down(right) && !reset(left, right)
            })
            .collect();
        for (left, right) in pairs {
            self.connect(left, right)?;
        }
        Ok(())
    }
}
