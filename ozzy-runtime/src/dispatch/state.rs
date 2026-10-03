use std::{
    cell::{RefCell, RefMut},
    sync::{Arc, atomic::AtomicBool},
};

use ozzy_proto::LinkSessionId;

use super::{Budget, Class, Limits, Quota};
use super::{hot::Hot, returns};
use crate::signal::StateSignal;

#[derive(Debug)]
pub(super) struct Shared {
    pub limits: Limits,
    pub closed: AtomicBool,
    pub returns: Arc<returns::Returns>,
    pub changed: StateSignal,
}

#[derive(Debug)]
pub(super) struct Ledger(RefCell<State>);

impl Ledger {
    pub(super) fn new(state: State) -> Self {
        Self(RefCell::new(state))
    }

    pub(super) fn borrow(&self) -> RefMut<'_, State> {
        let mut state = self.0.borrow_mut();
        state.collect_returns();
        state
    }
}

#[derive(Debug)]
pub(super) struct State {
    pub closed: bool,
    pub next_grant: u64,
    pub usage: [Budget; 2],
    pub clients: Vec<Option<ClientState>>,
    pub grants: Vec<Option<GrantState>>,
    pub returned: returns::Receiver,
}

#[derive(Debug)]
pub(super) struct ClientState {
    pub open: bool,
    pub session: LinkSessionId,
    pub limits: [Budget; 2],
    pub usage: [Budget; 2],
    pub grants: [usize; 2],
}

#[derive(Debug)]
pub(super) struct GrantState {
    pub serial: u64,
    pub hot: Arc<Hot>,
    pub revoked: Arc<super::credit::Revocation>,
    pub memory: Option<Arc<crate::memory::ForeignAllowance>>,
}

impl State {
    fn collect_returns(&mut self) {
        while let Ok(returned) = self.returned.try_recv() {
            match returned {
                returns::Returned::Released {
                    client,
                    class,
                    budget,
                } => {
                    self.release(client, class, budget);
                    self.collect(client);
                }
                returns::Returned::GrantDropped {
                    slot,
                    serial,
                    unused,
                } => {
                    let entry = self.grants[slot].take().expect("live dropped grant");
                    assert_eq!(entry.serial, serial, "obsolete grant return");
                    let (client, class) = (entry.hot.client, entry.hot.class);
                    self.release(client, class, unused.budget());
                    self.clients[client].as_mut().expect("live client").grants[class.index()] -= 1;
                    self.collect(client);
                }
            }
        }
    }

    pub(super) fn release(&mut self, client: usize, class: Class, released: Budget) {
        self.usage[class.index()].release(released);
        self.clients[client].as_mut().expect("live client").usage[class.index()].release(released);
    }

    pub(super) fn revoke(&mut self, slot: usize) {
        let grant = self.grants[slot].as_mut().expect("live grant");
        let Some(remaining) = grant.hot.revoke() else {
            return;
        };
        grant.revoked.publish(remaining);
        if let Some(memory) = &grant.memory {
            memory.release(super::credit::foreign_quota(remaining).expect("bounded foreign grant"));
        }
        let released = remaining.budget();
        let (client, class) = (grant.hot.client, grant.hot.class);
        self.release(client, class, released);
    }

    pub(super) fn revoke_client(&mut self, client: usize) {
        for slot in 0..self.grants.len() {
            if self.grants[slot]
                .as_ref()
                .is_some_and(|g| g.hot.client == client)
            {
                self.revoke(slot);
            }
        }
    }

    pub(super) fn collect(&mut self, client: usize) {
        let entry = self.clients[client].as_ref().expect("live client");
        if !entry.open && entry.grants == [0; 2] && entry.usage == [Budget::default(); 2] {
            self.clients[client] = None;
        }
    }
}

impl Budget {
    pub(super) fn valid(self) -> bool {
        self.queue_slots > 0
            && self.retained_messages > 0
            && self.bytes > 0
            && isize::try_from(self.bytes).is_ok()
    }

    pub(super) fn fits(self, addition: Self, limit: Self) -> bool {
        self.queue_slots <= limit.queue_slots.saturating_sub(addition.queue_slots)
            && addition.queue_slots <= limit.queue_slots
            && self.retained_messages
                <= limit
                    .retained_messages
                    .saturating_sub(addition.retained_messages)
            && addition.retained_messages <= limit.retained_messages
            && self.bytes <= limit.bytes.saturating_sub(addition.bytes)
            && addition.bytes <= limit.bytes
    }

    pub(super) fn reserve(&mut self, amount: Self) {
        self.queue_slots += amount.queue_slots;
        self.retained_messages += amount.retained_messages;
        self.bytes += amount.bytes;
    }

    fn release(&mut self, amount: Self) {
        self.queue_slots = self
            .queue_slots
            .checked_sub(amount.queue_slots)
            .expect("queue charge");
        self.retained_messages = self
            .retained_messages
            .checked_sub(amount.retained_messages)
            .expect("retained message charge");
        self.bytes = self.bytes.checked_sub(amount.bytes).expect("byte charge");
    }
}

impl Quota {
    pub(super) fn budget(self) -> Budget {
        Budget {
            queue_slots: self.messages,
            retained_messages: self.messages,
            bytes: self.bytes,
        }
    }
}
