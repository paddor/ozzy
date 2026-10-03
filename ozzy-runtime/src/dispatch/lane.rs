use std::sync::Arc;

use ozzy_proto::LinkSessionId;

use super::{Admission, Class, Error, Grant, Limits, Owner, Retention, state::Shared};
use crate::command_channel::{
    NotifiedReceiver, NotifiedSender, TryRecvError, TrySendError, notified_channel,
};

/// Dispatcher side of one shard's bounded fanring. Sending never waits for the
/// application shard, disk work, or capacity becoming available.
#[derive(Debug)]
pub struct Sender<T> {
    shared: Arc<Shared>,
    sender: NotifiedSender<Queued<T>>,
}

/// Application-shard receiver and its exclusive grant authority.
#[derive(Debug)]
pub struct Receiver<T> {
    credits: Owner,
    receiver: NotifiedReceiver<Queued<T>>,
}

#[derive(Debug)]
struct Queued<T> {
    value: T,
    admission: Admission,
    session: LinkSessionId,
    class: Class,
}

/// Dequeued work with its physical queue slot already released. Keep the
/// retention token with all backing-storage aliases, including downstream I/O.
#[derive(Debug)]
pub struct Received<T> {
    /// Original frame or typed command; no payload interpretation occurs here.
    pub value: T,
    /// Exact link session used for admission. Revalidate before accepting work.
    pub session: LinkSessionId,
    /// Independent data or control budget charged by this message.
    pub class: Class,
    /// Bytes and retained-message slot, released only with the last alias.
    pub retention: Retention,
}

/// Nonblocking dispatch failed. The unqueued value remains owned by the caller.
#[derive(Debug)]
pub struct SendError<T> {
    /// Whether admission, closure, or a broken accounting invariant rejected it.
    pub reason: SendFailure,
    /// Original unsent value. No destination actor has accepted it.
    pub value: T,
}

/// Dispatch rejection before destination enqueue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendFailure {
    /// No matching live reservation; no credit was spent.
    Admission(Error),
    /// Receiver closed. Any consumed credit has been released, not refunded.
    Closed,
    /// Reserved traffic could not fit its physical lane. Stop the affected
    /// frontend: this is an accounting bug, not ordinary backpressure.
    CapacityInvariant,
}

/// Construct on the destination shard. The one initial producer lane has room
/// for the aggregate data and reserved control grants. Rounding by fanring does
/// not increase advertised credit. Payload bytes have a separate fixed budget.
pub fn channel<T>(limits: Limits) -> Result<(Sender<T>, Receiver<T>), Error> {
    let slots = limits
        .capacity
        .data
        .queue_slots
        .checked_add(limits.capacity.control.queue_slots)
        .filter(|slots| *slots <= fanring::mpsc::MAX_CAPACITY_PER_SENDER)
        .ok_or(Error::Invalid)?;
    let credits = Owner::new(limits)?;
    let (sender, receiver) = notified_channel(slots);
    Ok((
        Sender {
            shared: credits.shared.clone(),
            sender,
        },
        Receiver { credits, receiver },
    ))
}

impl<T> Sender<T> {
    /// Capture before attempting admission, then wait for returned capacity.
    pub fn generation(&self) -> u64 {
        self.shared.changed.generation()
    }

    /// Owned readiness future, independent of the next send's mutable borrow.
    /// Recheck admission after waking; another producer can consume capacity.
    pub fn changed_after(&self, generation: u64) -> impl std::future::Future<Output = ()> + use<T> {
        let shared = self.shared.clone();
        async move { shared.changed.changed_after(generation).await }
    }

    pub(crate) fn owns_grant(&self, grant: &Grant) -> bool {
        Arc::ptr_eq(&self.shared, &grant.shared)
    }

    /// Admit and enqueue in one bounded turn. The caller accounts for the full
    /// retained backing allocation in `bytes`, not just the visible payload.
    pub fn try_send(
        &mut self,
        grant: &mut Grant,
        session: LinkSessionId,
        bytes: usize,
        value: T,
    ) -> Result<(), SendError<T>> {
        if !Arc::ptr_eq(&self.shared, &grant.shared) {
            return Err(SendError {
                reason: SendFailure::Admission(Error::Destination),
                value,
            });
        }
        let admission = match grant.admit(session, bytes) {
            Ok(admission) => admission,
            Err(error) => {
                return Err(SendError {
                    reason: SendFailure::Admission(error),
                    value,
                });
            }
        };
        let queued = Queued {
            value,
            admission,
            session,
            class: grant.class(),
        };
        match self.sender.try_send(queued) {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(queued)) => Err(SendError {
                reason: SendFailure::Closed,
                value: queued.value,
            }),
            Err(TrySendError::Full(queued)) => Err(SendError {
                reason: SendFailure::CapacityInvariant,
                value: queued.value,
            }),
        }
    }
}

impl<T> Receiver<T> {
    pub(crate) fn capacity_generation(&self) -> u64 {
        self.credits.generation()
    }

    pub(crate) fn capacity_changed_after(
        &self,
        generation: u64,
    ) -> impl std::future::Future<Output = ()> + use<T> {
        let shared = self.credits.shared.clone();
        async move { shared.changed.changed_after(generation).await }
    }

    /// Shard-local authority shared by all its destination partition actors.
    pub fn credits(&mut self) -> &mut Owner {
        &mut self.credits
    }

    /// Consume one entry without waiting. `Ok(None)` means empty; `Closed` means
    /// every producer disconnected and all previously queued entries were drained.
    pub fn try_recv(&mut self) -> Result<Option<Received<T>>, Error> {
        match self.receiver.try_recv() {
            Ok(queued) => Ok(Some(Received {
                value: queued.value,
                session: queued.session,
                class: queued.class,
                retention: queued.admission.dequeued(),
            })),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(Error::Closed),
        }
    }

    /// Persistent queue readiness. Canceling the wait never consumes a message.
    /// After a closed receive, stop waiting on this receiver.
    pub fn ready(&self) -> impl std::future::Future<Output = ()> + use<T> {
        self.receiver.owned_ready()
    }
}

impl Received<omq_tokio::Message> {
    /// Transfer this entry's charge into every OMQ frame owner. Frames extracted
    /// by an actor, cloned messages, and sliced payloads then retain the charge
    /// automatically. Session/command validation remains the actor's obligation.
    ///
    /// The original reservation must include frame descriptors, ownership
    /// adapters, and any small inline frames materialized as owned `Bytes` here.
    /// This keeps compressed payload representation intact.
    pub fn into_retained_message(self) -> omq_tokio::Message {
        omq_tokio::Message::multipart_payloads((0..self.value.len()).map(|index| {
            omq_tokio::message::Payload::from_bytes(
                self.retention
                    .clone()
                    .attach(self.value.part_bytes(index).expect("frame index")),
            )
        }))
    }
}
