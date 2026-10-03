//! Paired destination allocation and dispatcher admission on an application shard.

use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use omq_tokio::Message;
use ozzy_proto::{GroupId, LinkSessionId, NodeId};
use ozzy_replication::wire::ReceiveFence;

use crate::{
    dispatch::{self, Budgets, Class, Client, GrantKey},
    frontend::{
        GrantRequest, InstallResult, Kind, Links, Pending, Port, PortError, RoutingTable,
        SetupError,
    },
    memory::{self, Capacity},
};

mod admission;
mod deferred;

/// Allocation allowance for one partition and intake class. Bind its proposal
/// arenas to this capacity before serving requests. Background journal work
/// uses its own allowance on the same memory owner.
#[derive(Clone, Debug)]
pub struct Destination {
    capacity: Capacity,
    group: GroupId,
    kind: Kind,
    class: Class,
    per_message: memory::Quota,
}

impl Destination {
    /// Shared allocation authority, independent of dispatch queue-slot credit.
    pub fn capacity(&self) -> &Capacity {
        &self.capacity
    }

    /// Configured partition, independent of its current leader or local shard.
    pub fn group(&self) -> GroupId {
        self.group
    }

    /// Independent intake class backed by this allowance.
    pub fn class(&self) -> Class {
        self.class
    }

    /// SDK and replication arenas use independent allowances on the same owner.
    pub fn kind(&self) -> Kind {
        self.kind
    }
}

/// Independent backing charges for transport retention and canonical copying.
#[derive(Clone, Copy, Debug)]
pub struct ReceiveSize {
    /// Full possible backing allocation retained by the received packet.
    pub retained_bytes: usize,
    /// Bounded canonical body demand, independently of transport pool capacity.
    pub canonical_body_bytes: Option<usize>,
}

impl From<usize> for ReceiveSize {
    fn from(retained_bytes: usize) -> Self {
        Self {
            retained_bytes,
            canonical_body_bytes: None,
        }
    }
}

#[derive(Debug)]
struct Reservation {
    request: GrantRequest,
    receive_fence: Option<ReceiveFence>,
    destination: Destination,
    key: GrantKey,
    wire_bytes: usize,
    canonical: memory::Quota,
    window: usize,
    control_window: Option<usize>,
    pending: Option<Pending<InstallResult>>,
    fenced: bool,
    release_pending: bool,
    dequeued: usize,
    admitted_at_fence: Option<usize>,
    waiting: VecDeque<IntakeMessage>,
}

/// Shard-local pairing of canonical allocation and receive backing reservations.
/// The frontend owns tokens; this owner retains bounded settlement metadata.
/// Client and replica grants admit backed packet windows.
/// Scheduling and partition authority stay with actors.
#[derive(Debug)]
pub struct ShardIntake {
    input: dispatch::Receiver<Message>,
    data: memory::Owner,
    control: memory::Owner,
    limits: dispatch::Limits,
    client_window: usize,
    destinations: BTreeMap<(GroupId, Kind, Class), Destination>,
    maximum_destinations: usize,
    reservations: Vec<Option<Reservation>>,
    /// Reservations whose message was dequeued and has not settled.
    spent: usize,
}

impl ShardIntake {
    /// Construct after shard placement, before client registration. Foreign
    /// receive backing and canonical buffers share the same physical data bound.
    pub fn new(
        data: memory::Owner,
        control: &memory::Owner,
        limits: dispatch::Limits,
        maximum_destinations: usize,
        client_window: usize,
    ) -> Result<(dispatch::Sender<Message>, Self), IntakeError> {
        if !(1..=65_536).contains(&maximum_destinations) || !(1..=3).contains(&client_window) {
            return Err(IntakeError::Configuration);
        }
        let (sender, mut input) = dispatch::channel(limits)?;
        input.credits().bind_memory(&data, control)?;
        Ok((
            sender,
            Self {
                input,
                data,
                control: control.clone(),
                limits,
                client_window,
                maximum_destinations,
                destinations: BTreeMap::new(),
                reservations: (0..limits
                    .grants
                    .checked_mul(2)
                    .ok_or(IntakeError::Configuration)?)
                    .map(|_| None)
                    .collect(),
                spent: 0,
            },
        ))
    }

    /// Register a fixed canonical allocation profile. Clones share one allowance.
    /// Empty capacities allocate no body. A request may need both its original
    /// buffer and a replacement while retry preparation still retains the first.
    pub fn destination(
        &mut self,
        group: GroupId,
        kind: Kind,
        class: Class,
        per_message: memory::Quota,
    ) -> Result<Destination, IntakeError> {
        if group.as_bytes() == &[0; 16]
            || self.destinations.len() >= self.maximum_destinations
            || self.destinations.contains_key(&(group, kind, class))
            || (per_message.bytes == 0) != (per_message.buffers == 0)
            || (kind == Kind::Broker
                && class == Class::Control
                && per_message != memory::Quota::default()
                && self
                    .reservations
                    .iter()
                    .flatten()
                    .any(|old| old.control_window.is_some()))
        {
            return Err(IntakeError::Configuration);
        }
        let destination = Destination {
            capacity: match class {
                Class::Data => self.data.capacity(),
                Class::Control => self.control.capacity(),
            },
            group,
            kind,
            class,
            per_message,
        };
        self.destinations
            .insert((group, kind, class), destination.clone());
        Ok(destination)
    }

    /// Keep this logical client's handle across session replacement so retained
    /// old work continues to count against its count and byte limits.
    pub fn client(
        &mut self,
        session: LinkSessionId,
        limits: Budgets,
    ) -> Result<Client, IntakeError> {
        Ok(self.input.credits().client(session, limits)?)
    }

    /// Return spent metadata-control count credit on bounded shard turns. The
    /// byte reservation can fail while old aliases retain their actual backing;
    /// no credit is fabricated in that case. Session fencing remains separate.
    pub fn replenish_controls(&mut self, start: usize, turn: usize) -> Result<(), IntakeError> {
        if turn == 0 || turn > 1024 {
            return Err(IntakeError::Configuration);
        }
        for offset in 0..turn.min(self.reservations.len()) {
            let slot = (start + offset) % self.reservations.len();
            if let Some(reservation) = &mut self.reservations[slot]
                && reservation.control_window.is_some()
                && !reservation.fenced
            {
                match refill_control(self.input.credits(), reservation) {
                    Ok(())
                    | Err(IntakeError::Admission(
                        dispatch::Error::Full | dispatch::Error::Revoked,
                    )) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    /// Observe at most `turn` installations from `start`, registering each
    /// pending completion's wakeup. Successful installation is local admission,
    /// never transport delivery or record confirmation. Advertise follower
    /// protocol credit only after observing this result and its current epoch.
    pub fn poll_installations(
        &mut self,
        cx: &mut Context<'_>,
        links: &Links,
        start: usize,
        turn: usize,
        mut installed: impl FnMut(GrantRequest, Option<ReceiveFence>),
    ) -> Result<(), IntakeError> {
        if turn == 0 || turn > 1024 {
            return Err(IntakeError::Configuration);
        }
        for offset in 0..turn.min(self.reservations.len()) {
            let slot = (start + offset) % self.reservations.len();
            let Some(reservation) = &mut self.reservations[slot] else {
                continue;
            };
            let Some(pending) = &mut reservation.pending else {
                continue;
            };
            let Poll::Ready(result) = Pin::new(pending).poll(cx) else {
                continue;
            };
            reservation.pending = None;
            match result? {
                Ok(()) => {
                    if !reservation.fenced
                        && links
                            .get(reservation.request.binding.peer)
                            .map(|link| link.binding)
                            == Some(reservation.request.binding)
                        && self.input.credits().remaining(&reservation.key).is_ok()
                    {
                        installed(reservation.request, reservation.receive_fence);
                    } else if !reservation.fenced {
                        self.fence(slot, &mut |_| Ok(()))?;
                    }
                }
                Err((_error, grant)) => {
                    drop(grant);
                    self.fence(slot, &mut |_| Ok(()))?;
                }
            }
        }
        Ok(())
    }

    /// Installed data promises with at least one unused dispatch slot.
    /// Their canonical allowance can back additional protocol byte credit
    /// after an actor releases older records. Pending or fenced promises
    /// cannot advertise capacity. Observe at most `turn` reservation slots.
    pub fn idle_grants(
        &mut self,
        links: &Links,
        start: usize,
        turn: usize,
    ) -> impl Iterator<Item = (GrantRequest, &Destination)> {
        let credits = self.input.credits();
        let reservations = &self.reservations;
        (0..turn.min(reservations.len())).filter_map(move |offset| {
            let reservation = reservations[(start + offset) % reservations.len()].as_ref()?;
            (reservation.destination.class == Class::Data
                && reservation.pending.is_none()
                && !reservation.fenced
                && links
                    .get(reservation.request.binding.peer)
                    .map(|link| link.binding)
                    == Some(reservation.request.binding)
                && credits
                    .remaining(&reservation.key)
                    .is_ok_and(|quota| quota.messages != 0))
            .then_some((reservation.request, &reservation.destination))
        })
    }

    /// Fence one idle promise before assigning its unused allocation allowance
    /// elsewhere. Admitted queued work cannot be reclaimed by this operation.
    /// After dispatch fencing, `before_release` fences any corresponding
    /// follower protocol credit before unused canonical capacity is returned.
    pub fn revoke_idle(
        &mut self,
        class: Class,
        start: usize,
        turn: usize,
        mut before_release: impl FnMut(GrantRequest) -> Result<(), IntakeError>,
    ) -> Result<Option<GrantRequest>, IntakeError> {
        if turn == 0 || turn > 1024 {
            return Err(IntakeError::Configuration);
        }
        for offset in 0..turn.min(self.reservations.len()) {
            let slot = (start + offset) % self.reservations.len();
            let Some(reservation) = &self.reservations[slot] else {
                continue;
            };
            if reservation.destination.class != class || reservation.control_window.is_some() {
                continue;
            }
            if reservation.pending.is_some() {
                continue;
            }
            if reservation.fenced && !reservation.release_pending {
                continue;
            }
            if reservation.release_pending
                || self
                    .input
                    .credits()
                    .remaining(&reservation.key)
                    .is_ok_and(|quota| quota.messages == reservation.window)
            {
                let request = reservation.request;
                if self.fence(slot, &mut before_release)? {
                    return Ok(Some(request));
                }
            }
        }
        Ok(None)
    }

    /// Reclaim promises dropped by the dispatcher, including after token-slot
    /// reuse. Previously admitted input remains tracked until dequeue/settlement.
    /// Fence any advertised follower epoch in `before_release` before reuse.
    pub fn reconcile(
        &mut self,
        links: &Links,
        start: usize,
        turn: usize,
        mut before_release: impl FnMut(GrantRequest) -> Result<(), IntakeError>,
    ) -> Result<(), IntakeError> {
        if turn == 0 || turn > 1024 {
            return Err(IntakeError::Configuration);
        }
        for offset in 0..turn.min(self.reservations.len()) {
            let slot = (start + offset) % self.reservations.len();
            let Some(reservation) = &self.reservations[slot] else {
                continue;
            };
            if reservation.release_pending
                || (!reservation.fenced
                    && (links
                        .get(reservation.request.binding.peer)
                        .map(|link| link.binding)
                        != Some(reservation.request.binding)
                        || self.input.credits().remaining(&reservation.key).is_err()))
            {
                self.fence(slot, &mut before_release)?;
            }
        }
        Ok(())
    }

    /// Withdraw one exact promise. Consumed input remains tracked until its
    /// destination settles; unused capacity is released only after the callback.
    pub fn revoke(
        &mut self,
        request: GrantRequest,
        mut before_release: impl FnMut(GrantRequest) -> Result<(), IntakeError>,
    ) -> Result<(), IntakeError> {
        if let Some(slot) = self.reservations.iter().position(|reservation| {
            reservation
                .as_ref()
                .is_some_and(|old| old.request == request)
        }) {
            self.fence(slot, &mut before_release)?;
        }
        Ok(())
    }

    fn fence(
        &mut self,
        slot: usize,
        before_release: &mut impl FnMut(GrantRequest) -> Result<(), IntakeError>,
    ) -> Result<bool, IntakeError> {
        let reservation = self.reservations[slot]
            .as_mut()
            .ok_or(IntakeError::Invariant)?;
        if !reservation.fenced {
            let unused = self.input.credits().revoke_unused(&reservation.key)?;
            if let Some(issued) = &mut reservation.control_window {
                *issued = issued
                    .checked_sub(unused.messages)
                    .ok_or(IntakeError::Invariant)?;
                reservation.fenced = true;
                if *issued == 0 {
                    self.reservations[slot] = None;
                    return Ok(true);
                }
                return Ok(false);
            }
            if unused.messages > reservation.window {
                return Err(IntakeError::Invariant);
            }
            reservation.fenced = true;
            reservation.admitted_at_fence = Some(reservation.window - unused.messages);
            reservation.release_pending = unused.messages == reservation.window;
        }
        if reservation.release_pending {
            before_release(reservation.request)?;
            self.release_unprotected_capacity(slot)?;
            self.reservations[slot] = None;
            return Ok(true);
        }
        Ok(false)
    }

    fn release_unprotected_capacity(&self, slot: usize) -> Result<(), IntakeError> {
        let destination = &self.reservations[slot]
            .as_ref()
            .ok_or(IntakeError::Invariant)?
            .destination;
        let mut protected = memory::Quota::default();
        for (other_slot, reservation) in self.reservations.iter().enumerate() {
            if other_slot != slot
                && let Some(other) = reservation
                && other
                    .destination
                    .capacity
                    .same_allowance(&destination.capacity)
            {
                protected.bytes = protected
                    .bytes
                    .checked_add(other.canonical.bytes)
                    .ok_or(IntakeError::Invariant)?;
                protected.buffers = protected
                    .buffers
                    .checked_add(other.canonical.buffers)
                    .ok_or(IntakeError::Invariant)?;
            }
        }
        let remaining = destination.capacity.remaining();
        let quota = memory::Quota {
            bytes: remaining.bytes.saturating_sub(protected.bytes),
            buffers: remaining.buffers.saturating_sub(protected.buffers),
        };
        if quota != memory::Quota::default() {
            destination.capacity.release(quota)?;
        }
        Ok(())
    }

    /// Dequeue one admitted frame. Normal replication retains its exhausted
    /// dispatch token for bounded replenishment after actor work settles.
    /// Stale sessions return `current == false`; discard them before admission.
    /// Every extracted frame alias retains the full foreign backing charge.
    pub fn receive(
        &mut self,
        links: &Links,
        routes: &RoutingTable,
    ) -> Result<Option<IntakeMessage>, IntakeError> {
        let Some(received) = self.input.try_recv()? else {
            return Ok(None);
        };
        let peer = received
            .value
            .part_slice(0)
            .and_then(|bytes| bytes.try_into().ok())
            .map(NodeId::from_bytes)
            .ok_or(IntakeError::Invariant)?;
        let binding = self
            .reservations
            .iter()
            .flatten()
            .find(|reservation| {
                reservation.request.binding.peer == peer
                    && reservation.request.binding.session == received.session
                    && reservation.request.route.class == received.class
            })
            .ok_or(IntakeError::Invariant)?
            .request
            .binding;
        let route = routes
            .route(&received.value, binding)
            .map_err(|_| IntakeError::Invariant)?;
        let slot = self
            .reservations
            .iter()
            .position(|reservation| {
                reservation.as_ref().is_some_and(|reservation| {
                    reservation.request.binding == binding
                        && (reservation.request.route == route
                            || control_matches(reservation, GrantRequest { binding, route }))
                        && reservation.dequeued < reservation.window
                })
            })
            .ok_or(IntakeError::Invariant)?;
        if let Some(issued) = &mut self.reservations[slot]
            .as_mut()
            .ok_or(IntakeError::Invariant)?
            .control_window
        {
            *issued = issued.checked_sub(1).ok_or(IntakeError::Invariant)?;
            if *issued == 0 && self.reservations[slot].as_ref().unwrap().fenced {
                self.reservations[slot] = None;
            }
        } else {
            // Keep the installed writer grant's original byte class backed
            // across replenishment. A later APPEND can carry more metadata.
            if !(matches!(
                self.reservations[slot].as_ref().unwrap().receive_fence,
                Some(ReceiveFence::Normal(_))
            ) || binding.kind == Kind::Client && received.class == Class::Data)
            {
                self.fence(slot, &mut |_| Ok(()))?;
            }
            let reservation = self.reservations[slot]
                .as_mut()
                .ok_or(IntakeError::Invariant)?;
            if reservation.dequeued == 0 {
                self.spent += 1;
            }
            reservation.dequeued += 1;
        }
        let current = links.get(peer).map(|link| link.binding) == Some(binding);
        Ok(Some(IntakeMessage {
            reservation: slot,
            request: GrantRequest { binding, route },
            current,
            message: received.into_retained_message(),
        }))
    }

    /// Return only unused canonical allowance after the caller observes this
    /// destination's proposal/retry work settled. Physical buffer aliases remain
    /// charged even after confirmation. Queued input never permits settlement.
    /// `before_release` must fence any remaining advertised follower receive
    /// credit. Failure retains the reservation so the caller can retry.
    pub fn settle(
        &mut self,
        destination: &Destination,
        before_release: impl FnMut(GrantRequest) -> Result<(), IntakeError>,
    ) -> Result<bool, IntakeError> {
        self.settle_request(destination, |_| true, before_release)
            .map(|settled| settled.is_some())
    }

    /// Settle one spent reservation that `settled` accepts, and return its
    /// scope with the backing charge it had. Writers on one partition settle
    /// independently: the caller accepts a scope once that writer's own
    /// proposals and replies are gone. The returned scope holds no credit.
    /// The caller may ask for a new reservation with `install`.
    pub fn settle_request(
        &mut self,
        destination: &Destination,
        mut settled: impl FnMut(GrantRequest) -> bool,
        mut before_release: impl FnMut(GrantRequest) -> Result<(), IntakeError>,
    ) -> Result<Option<(GrantRequest, usize)>, IntakeError> {
        if !self
            .destinations
            .get(&(destination.group, destination.kind, destination.class))
            .is_some_and(|owned| owned.capacity.same_allowance(&destination.capacity))
        {
            return Err(IntakeError::Configuration);
        }
        debug_assert_eq!(
            self.spent,
            self.reservations
                .iter()
                .flatten()
                .filter(|reservation| reservation.dequeued != 0)
                .count()
        );
        if self.spent == 0 {
            return Ok(None);
        }
        let Some(slot) = self.reservations.iter().position(|reservation| {
            reservation.as_ref().is_some_and(|reservation| {
                reservation.destination.group == destination.group
                    && reservation.destination.kind == destination.kind
                    && reservation.destination.class == destination.class
                    && reservation.dequeued != 0
                    && reservation.waiting.is_empty()
                    && reservation
                        .admitted_at_fence
                        .is_none_or(|admitted| reservation.dequeued == admitted)
                    && settled(reservation.request)
            })
        }) else {
            return Ok(None);
        };
        self.settle_slot(slot, &mut before_release)
    }

    /// Spent reservations whose message reached its actor: slot, destination,
    /// and scope. The caller settles those whose work is gone with
    /// `settle_slot`. One pass over the fixed table, without allocation.
    pub fn spent(&self) -> impl Iterator<Item = (usize, &Destination, GrantRequest)> {
        self.reservations
            .iter()
            .enumerate()
            .take(if self.spent == 0 {
                0
            } else {
                self.reservations.len()
            })
            .filter_map(|(slot, reservation)| {
                reservation
                    .as_ref()
                    .filter(|reservation| {
                        reservation.dequeued != 0
                            && reservation.waiting.is_empty()
                            && reservation
                                .admitted_at_fence
                                .is_none_or(|admitted| reservation.dequeued == admitted)
                    })
                    .map(|reservation| (slot, &reservation.destination, reservation.request))
            })
    }

    /// Settle the spent reservation in `slot`. Absent, unspent, or waiting
    /// reservations stay as they are and return nothing.
    pub fn settle_slot(
        &mut self,
        slot: usize,
        mut before_release: impl FnMut(GrantRequest) -> Result<(), IntakeError>,
    ) -> Result<Option<(GrantRequest, usize)>, IntakeError> {
        let Some(reservation) =
            self.reservations
                .get(slot)
                .and_then(Option::as_ref)
                .filter(|reservation| {
                    reservation.dequeued != 0
                        && reservation.waiting.is_empty()
                        && reservation
                            .admitted_at_fence
                            .is_none_or(|admitted| reservation.dequeued == admitted)
                })
        else {
            return Ok(None);
        };
        let scope = (reservation.request, reservation.wire_bytes);
        self.fence(slot, &mut before_release)?;
        before_release(scope.0)?;
        self.release_unprotected_capacity(slot)?;
        self.reservations[slot] = None;
        self.spent -= 1;
        Ok(Some(scope))
    }

    /// Whether any dequeued message still waits for its settlement.
    pub fn has_spent(&self) -> bool {
        self.spent != 0
    }

    /// Fixed metadata scan size, independent of incoming producer identities.
    pub fn reservation_slots(&self) -> usize {
        self.reservations.len()
    }

    /// Persistent input readiness; cancellation never consumes admitted input.
    pub fn ready(&self) -> impl Future<Output = ()> + use<> {
        self.input.ready()
    }

    /// Capture before admission or settlement to avoid a lost capacity return.
    pub fn generation(&self) -> [u64; 3] {
        [
            self.data.generation(),
            self.control.generation(),
            self.input.capacity_generation(),
        ]
    }

    /// Observe actual released backing. Confirmation alone cannot satisfy this.
    pub fn changed_after(&self, generation: [u64; 3]) -> impl Future<Output = ()> + use<> {
        let data = self.data.changed_after(generation[0]);
        let control = self.control.changed_after(generation[1]);
        let dispatch = self.input.capacity_changed_after(generation[2]);
        async move {
            tokio::select! {
                () = data => {},
                () = control => {},
                () = dispatch => {},
            }
        }
    }

    /// Shared input limits. Additional partition destinations do not multiply it.
    pub fn limits(&self) -> dispatch::Limits {
        self.limits
    }
}

impl Drop for ShardIntake {
    fn drop(&mut self) {
        for reservation in self.reservations.iter_mut().flatten() {
            if reservation.release_pending
                || (!reservation.fenced
                    && self
                        .input
                        .credits()
                        .revoke_unused(&reservation.key)
                        .is_ok_and(|unused| unused.messages == reservation.window))
            {
                let quota = reservation.canonical;
                if quota != memory::Quota::default() {
                    let _ = reservation.destination.capacity.release(quota);
                }
            }
        }
    }
}

fn control_matches(reservation: &Reservation, request: GrantRequest) -> bool {
    reservation.control_window.is_some()
        && reservation.request.binding == request.binding
        && request.route.class == Class::Control
        && reservation.request.route.placement.shard == request.route.placement.shard
}

fn refill_control(
    owner: &mut dispatch::Owner,
    reservation: &mut Reservation,
) -> Result<(), IntakeError> {
    if reservation.fenced {
        return Ok(());
    }
    let issued = reservation
        .control_window
        .as_mut()
        .ok_or(IntakeError::Invariant)?;
    let remaining = owner.remaining(&reservation.key)?;
    let messages = 4_usize.checked_sub(*issued).ok_or(IntakeError::Invariant)?;
    let target = remaining
        .messages
        .checked_add(messages)
        .and_then(|messages| messages.checked_mul(reservation.wire_bytes))
        .ok_or(IntakeError::Invariant)?;
    let bytes = target.saturating_sub(remaining.bytes);
    if messages != 0 || bytes != 0 {
        owner.extend(&reservation.key, dispatch::Quota { messages, bytes })?;
        *issued += messages;
    }
    Ok(())
}

/// One dequeued packet, with current session validation separate from receipt.
#[derive(Debug)]
pub struct IntakeMessage {
    reservation: usize,
    /// Independently bound peer and configured routing destination.
    pub request: GrantRequest,
    /// False after disconnect/reconnect; actor admission must discard this input.
    pub current: bool,
    /// Frames retain their complete foreign backing allocation charge.
    pub message: Message,
}

/// Failed paired admission or terminal ownership-accounting mismatch.
#[derive(Debug, thiserror::Error)]
pub enum IntakeError {
    /// Invalid or duplicate fixed destination profile.
    #[error("invalid shard intake configuration")]
    Configuration,
    /// Grant request no longer matches the current independently bound link.
    #[error("obsolete shard intake session")]
    Session,
    /// Admitted input did not match its destination reservation.
    #[error("shard intake reservation invariant failed")]
    Invariant,
    /// Dispatch admission exhausted or closed.
    #[error(transparent)]
    Admission(#[from] dispatch::Error),
    /// Canonical allocation could not be reserved or settled.
    #[error(transparent)]
    Memory(#[from] std::io::Error),
    /// Bounded dispatcher command failed.
    #[error(transparent)]
    Port(#[from] PortError),
    /// Dispatcher refused a fixed grant scope.
    #[error(transparent)]
    Setup(#[from] SetupError),
}

#[cfg(test)]
mod tests;
