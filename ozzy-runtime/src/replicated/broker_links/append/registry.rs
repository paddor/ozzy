//! Reply routing from the shared broker receivers to logical APPEND writers.

use super::budget::{Budget, Lease};
use super::{AppendLinkLimits, StateSignal, track};
use omq_tokio::Message;
use ozzy_proto::{LinkSessionId, NodeId, Opcode, Packet, RequestId};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::Rc,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::mpsc;

#[derive(Debug)]
pub(super) struct Stream {
    pub(super) id: RequestId,
    pub(super) sender: mpsc::Sender<Message>,
    pub(super) ready: StateSignal,
    pub(super) failed: AtomicBool,
    pub(super) memory: Arc<Lease>,
}

#[derive(Debug)]
pub(super) struct Request {
    pub(super) stream: Weak<Stream>,
    pub(super) writer: RequestId,
    pub(super) broker: NodeId,
    pub(super) session: LinkSessionId,
    pub(super) end: u64,
    pub(super) remaining_replies: usize,
    pub(super) lease: Arc<Lease>,
}

#[derive(Debug)]
pub(in crate::replicated::broker_links) struct Registry {
    pub(super) budget: Arc<Budget>,
    thread: std::thread::ThreadId,
}

/// Correlations never leave the SDK executor. Public handles retain only budgets.
#[derive(Clone, Debug)]
pub(in crate::replicated::broker_links) struct Owner {
    pub(super) budget: Arc<Budget>,
    pub(super) requests: Rc<Requests>,
}

pub(super) type Requests = RefCell<BTreeMap<RequestId, Request>>;
type LocalOwners = BTreeMap<usize, std::rc::Weak<Requests>>;

thread_local! {
    // Cold bootstrap lookup only. Drivers retain their owner directly afterward.
    // Weak entries retain no requests, leases, sockets, or SDK runtime.
    static OWNERS: RefCell<LocalOwners> = const { RefCell::new(BTreeMap::new()) };
}

impl Registry {
    pub(in crate::replicated::broker_links) fn new(limits: Option<AppendLinkLimits>) -> Self {
        Self {
            budget: Arc::new(Budget::new(limits)),
            thread: std::thread::current().id(),
        }
    }

    pub(in crate::replicated::broker_links) fn owner(&self) -> Owner {
        assert_eq!(
            self.thread,
            std::thread::current().id(),
            "APPEND state left its SDK owner"
        );
        let key = Arc::as_ptr(&self.budget) as usize;
        let requests = OWNERS.with(|owners| {
            let mut owners = owners.borrow_mut();
            owners.retain(|_, owner| owner.strong_count() != 0);
            if let Some(owner) = owners.get(&key).and_then(std::rc::Weak::upgrade) {
                owner
            } else {
                let owner = Rc::new(RefCell::new(BTreeMap::new()));
                owners.insert(key, Rc::downgrade(&owner));
                owner
            }
        });
        Owner {
            budget: self.budget.clone(),
            requests,
        }
    }
}

impl Owner {
    pub(in crate::replicated::broker_links) fn fence(&self, broker: NodeId) {
        self.requests.borrow_mut().retain(|_, request| {
            if request.broker != broker {
                return true;
            }
            if let Some(stream) = request.stream.upgrade() {
                stream.failed.store(true, Ordering::Release);
            }
            false
        });
    }

    pub(super) fn forget(&self, writer: RequestId, confirmed: Option<u64>) {
        self.requests.borrow_mut().retain(|_, request| {
            request.writer != writer || confirmed.is_some_and(|end| request.end > end)
        });
    }

    /// Single physical receiver never waits for one logical writer's queue.
    /// Canonical authority, writer identity, and offsets stay driver checks.
    pub(in crate::replicated::broker_links) fn receive(
        &self,
        broker: NodeId,
        message: &Message,
        packet: Packet<'_>,
    ) -> bool {
        let Some(id) = packet.envelope.request_id else {
            return false;
        };
        let mut requests = self.requests.borrow_mut();
        let Some(request) = requests.get_mut(&id) else {
            return false;
        };
        if request.broker != broker
            || packet.envelope.session != Some(request.session)
            || !packet.envelope.response
            || !matches!(packet.envelope.opcode, Opcode::Appended | Opcode::Nack)
            || !packet.payload.is_empty()
        {
            return true;
        }
        let Some(stream) = request.stream.upgrade() else {
            return true;
        };
        if request.remaining_replies == 0 {
            stream.failed.store(true, Ordering::Release);
            stream.ready.notify_changed();
            return true;
        }
        request.remaining_replies -= 1;
        let lease = request.lease.clone();
        drop(requests);
        let response = track(message, &lease, Some(&stream.memory), true);
        if stream.sender.try_send(response).is_err() {
            stream.failed.store(true, Ordering::Release);
        }
        stream.ready.notify_changed();
        true
    }
}
