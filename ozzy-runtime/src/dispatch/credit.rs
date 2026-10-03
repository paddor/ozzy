use std::{
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use ozzy_proto::LinkSessionId;

use crate::signal::StateSignal;

use super::state::{ClientState, GrantState, Ledger, Shared, State};
use super::{Budget, Budgets, Class, Error, Limits, Quota};
use super::{hot::Hot, returns};

/// One shard's grant authority. Construct and use on that application shard.
/// This handle cannot cross threads or be cloned to multiply capacity.
#[derive(Debug)]
pub struct Owner {
    pub(super) shared: Arc<Shared>,
    ledger: Rc<Ledger>,
    memory: Option<[crate::memory::Owner; 2]>,
    local: PhantomData<Rc<()>>,
}

/// Local grant authority for one logical client across session replacement.
/// Old admitted work continues to count against this client's budget.
#[derive(Debug)]
pub struct Client {
    shared: Arc<Shared>,
    ledger: Rc<Ledger>,
    slot: usize,
    local: PhantomData<Rc<()>>,
}

/// A unique dispatcher token backed by the destination's reserved capacity.
/// Dropping it reclaims unused credit; admitted payloads retain their charges.
#[derive(Debug)]
pub struct Grant {
    pub(super) shared: Arc<Shared>,
    slot: usize,
    serial: u64,
    hot: Arc<Hot>,
    memory: Option<Arc<crate::memory::ForeignAllowance>>,
    revoked: Arc<Revocation>,
}

/// Nonowning identity retained by the shard while a dispatcher owns the token.
/// It cannot consume credit or keep a dropped grant's table slot occupied.
/// Reuse of that slot cannot make an old key refer to another grant.
#[derive(Clone, Debug)]
pub struct GrantKey {
    shared: Weak<Shared>,
    slot: usize,
    serial: u64,
    revoked: Arc<Revocation>,
}

#[derive(Debug, Default)]
pub(super) struct Revocation {
    unused: OnceLock<Quota>,
    taken: AtomicBool,
}

impl Revocation {
    pub(super) fn publish(&self, quota: Quota) {
        assert!(self.unused.set(quota).is_ok(), "grant revoked once");
    }

    fn take(&self) -> Result<Quota, Error> {
        let unused = *self.unused.get().ok_or(Error::Revoked)?;
        if self.taken.swap(true, Ordering::AcqRel) {
            Err(Error::Revoked)
        } else {
            Ok(unused)
        }
    }
}

/// An admitted queue entry. Cancellation releases both charges. Dequeue must
/// release the physical fanring slot before calling `dequeued`.
#[derive(Debug)]
pub struct Admission {
    queue: QueueSlot,
    retained: Retention,
}

#[derive(Debug)]
struct QueueSlot {
    shared: Arc<Shared>,
    client: usize,
    class: Class,
}

/// Shared lifetime of one charged message's backing storage. Clones cover
/// aliases only; copied or newly allocated backing needs additional admission.
#[derive(Clone, Debug)]
pub struct Retention(Arc<Retained>);

#[derive(Debug)]
struct Retained {
    shared: Arc<Shared>,
    client: usize,
    class: Class,
    bytes: usize,
    memory: Option<crate::memory::ForeignCharge>,
}

impl Owner {
    /// Preallocate bounded accounting tables. Payload storage is owned elsewhere.
    pub fn new(limits: Limits) -> Result<Self, Error> {
        if !limits.capacity.array().into_iter().all(Budget::valid)
            || !(1..=65_536).contains(&limits.clients)
            || !(1..=65_536).contains(&limits.grants)
        {
            return Err(Error::Invalid);
        }
        let return_capacity = limits
            .capacity
            .array()
            .into_iter()
            .try_fold(0_usize, |total, budget| {
                total
                    .checked_add(budget.queue_slots)?
                    .checked_add(budget.retained_messages)
            })
            .and_then(|total| total.checked_add(limits.grants.checked_mul(2)?))
            .ok_or(Error::Invalid)?;
        let (returns, returned) = returns::channel(return_capacity)?;
        let ledger = Rc::new(Ledger::new(State {
            closed: false,
            next_grant: 1,
            usage: [Budget::default(); 2],
            clients: (0..limits.clients).map(|_| None).collect(),
            grants: (0..limits.grants * 2).map(|_| None).collect(),
            returned,
        }));
        Ok(Self {
            memory: None,
            ledger,
            shared: Arc::new(Shared {
                limits,
                closed: std::sync::atomic::AtomicBool::new(false),
                returns,
                changed: StateSignal::default(),
            }),
            local: PhantomData,
        })
    }

    /// Bind intake reservations to the same payload owners as canonical storage.
    /// Call on the shard before registering clients. Data and control require
    /// distinct owners, so data exhaustion preserves control progress capacity.
    pub fn bind_memory(
        &mut self,
        data: &crate::memory::Owner,
        control: &crate::memory::Owner,
    ) -> Result<(), Error> {
        if self.memory.is_some()
            || data.same_owner(control)
            || self.ledger.borrow().clients.iter().any(Option::is_some)
        {
            return Err(Error::Invalid);
        }
        self.memory = Some([data.clone(), control.clone()]);
        Ok(())
    }

    /// Register a logical client. The caller supplies a fresh, nonzero session.
    /// Use `Client::replace_session` to retain its budget across reconnects.
    pub fn client(&mut self, session: LinkSessionId, limits: Budgets) -> Result<Client, Error> {
        if !valid_session(session)
            || !limits.array().into_iter().all(Budget::valid)
            || !limits
                .array()
                .into_iter()
                .zip(self.shared.limits.capacity.array())
                .all(|(limit, capacity)| Budget::default().fits(limit, capacity))
        {
            return Err(Error::Invalid);
        }
        let mut state = self.ledger.borrow();
        let slot = state
            .clients
            .iter()
            .position(Option::is_none)
            .ok_or(Error::Full)?;
        state.clients[slot] = Some(ClientState {
            open: true,
            session,
            limits: limits.array(),
            usage: [Budget::default(); 2],
            grants: [0; 2],
        });
        Ok(Client {
            shared: self.shared.clone(),
            ledger: self.ledger.clone(),
            slot,
            local: PhantomData,
        })
    }

    /// Reserve both client and shard capacity before advertising credit.
    pub fn grant(&mut self, client: &Client, class: Class, quota: Quota) -> Result<Grant, Error> {
        if !Arc::ptr_eq(&self.shared, &client.shared) {
            return Err(Error::Destination);
        }
        if quota.messages == 0 || quota.bytes < quota.messages {
            return Err(Error::Invalid);
        }
        let mut state = self.ledger.borrow();
        let index = class.index();
        let amount = quota.budget();
        let entry = state.clients[client.slot].as_ref().expect("live client");
        if !state.usage[index].fits(amount, self.shared.limits.capacity.array()[index])
            || !entry.usage[index].fits(amount, entry.limits[index])
            || entry.grants[index] >= entry.limits[index].retained_messages
        {
            return Err(Error::Full);
        }
        if !Hot::valid(quota) {
            return Err(Error::Invalid);
        }
        let session = entry.session;
        let start = index * self.shared.limits.grants;
        let slot = (start..start + self.shared.limits.grants)
            .find(|&slot| state.grants[slot].is_none())
            .ok_or(Error::Full)?;
        let serial = state.next_grant;
        let next = serial.checked_add(1).ok_or(Error::Invalid)?;
        let memory = self
            .memory
            .as_ref()
            .map(|owners| {
                owners[index]
                    .external(foreign_quota(quota)?)
                    .map_err(|error| memory_error(error.kind()))
            })
            .transpose()?
            .map(Arc::new);
        let hot = Arc::new(Hot::new(class, session, client.slot, quota)?);
        let revoked = Arc::new(Revocation::default());
        state.next_grant = next;
        state.usage[index].reserve(amount);
        let entry = state.clients[client.slot].as_mut().expect("live client");
        entry.usage[index].reserve(amount);
        entry.grants[index] += 1;
        state.grants[slot] = Some(GrantState {
            serial,
            hot: hot.clone(),
            revoked: revoked.clone(),
            memory: memory.clone(),
        });
        Ok(Grant {
            shared: self.shared.clone(),
            slot,
            serial,
            hot,
            memory,
            revoked,
        })
    }

    /// Add newly available capacity to the same live dispatcher token. The
    /// shard can return count and byte capacity independently, without replacing
    /// or reclaiming any previously advertised unused credit.
    pub fn extend(&mut self, key: &GrantKey, quota: Quota) -> Result<(), Error> {
        if !key.shared.ptr_eq(&Arc::downgrade(&self.shared)) {
            return Err(Error::Destination);
        }
        if quota == Quota::default() {
            return Err(Error::Invalid);
        }
        if !Hot::valid(quota) {
            return Err(Error::Invalid);
        }
        let mut state = self.ledger.borrow();
        let grant = state.grants[key.slot]
            .as_ref()
            .filter(|grant| grant.serial == key.serial && grant.hot.live())
            .ok_or(Error::Revoked)?;
        let hot = grant.hot.clone();
        let (client, index) = (grant.hot.client, grant.hot.class.index());
        let remaining = grant.hot.remaining();
        if !Hot::valid(Quota {
            messages: remaining
                .messages
                .checked_add(quota.messages)
                .ok_or(Error::Invalid)?,
            bytes: remaining
                .bytes
                .checked_add(quota.bytes)
                .ok_or(Error::Invalid)?,
        }) {
            return Err(Error::Invalid);
        }
        let entry = state.clients[client].as_ref().expect("live client");
        let amount = quota.budget();
        if !state.usage[index].fits(amount, self.shared.limits.capacity.array()[index])
            || !entry.usage[index].fits(amount, entry.limits[index])
        {
            return Err(Error::Full);
        }
        if let (Some(owners), Some(memory)) = (&self.memory, &grant.memory) {
            owners[index]
                .reserve_external(memory, foreign_quota(quota)?)
                .map_err(|error| memory_error(error.kind()))?;
        }
        state.usage[index].reserve(amount);
        state.clients[client].as_mut().expect("live client").usage[index].reserve(amount);
        hot.add(quota)?;
        drop(state);
        self.shared.changed.notify_changed();
        Ok(())
    }

    /// Fence an individual grant, atomically against dispatcher admission.
    pub fn revoke(&mut self, grant: &Grant) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.shared, &grant.shared) {
            return Err(Error::Destination);
        }
        self.ledger.borrow().revoke(grant.slot);
        self.shared.changed.notify_changed();
        Ok(())
    }

    /// Fence a dispatcher-held grant using the shard's nonowning key. A reused
    /// token slot can never be revoked through an old key. Already admitted
    /// queue entries and retained payloads keep their charges after this call.
    pub fn revoke_key(&mut self, key: &GrantKey) -> Result<(), Error> {
        if !key.shared.ptr_eq(&Arc::downgrade(&self.shared)) {
            return Err(Error::Destination);
        }
        let mut state = self.ledger.borrow();
        state.grants[key.slot]
            .as_ref()
            .filter(|grant| grant.serial == key.serial && grant.hot.live())
            .ok_or(Error::Revoked)?;
        state.revoke(key.slot);
        drop(state);
        self.shared.changed.notify_changed();
        Ok(())
    }

    /// Fence a token and recover its unused quota exactly once. A dispatcher
    /// may already have dropped or fenced it and reused its table slot. The key
    /// retains only its own revocation result, never the slot or grant authority.
    /// Admitted work remains charged and must retain its destination allowance.
    pub fn revoke_unused(&mut self, key: &GrantKey) -> Result<Quota, Error> {
        if !key.shared.ptr_eq(&Arc::downgrade(&self.shared)) {
            return Err(Error::Destination);
        }
        let mut state = self.ledger.borrow();
        if state.grants[key.slot]
            .as_ref()
            .is_some_and(|grant| grant.serial == key.serial && grant.hot.live())
        {
            state.revoke(key.slot);
        }
        let result = key.revoked.take();
        drop(state);
        if result.is_ok() {
            self.shared.changed.notify_changed();
        }
        result
    }

    /// Inspect unused dispatcher-held credit without taking ownership of its
    /// token. Old keys never inspect a subsequently reused accounting slot.
    pub fn remaining(&self, key: &GrantKey) -> Result<Quota, Error> {
        if !key.shared.ptr_eq(&Arc::downgrade(&self.shared)) {
            return Err(Error::Destination);
        }
        let state = self.ledger.borrow();
        if state.closed {
            return Err(Error::Closed);
        }
        state.grants[key.slot]
            .as_ref()
            .filter(|grant| grant.serial == key.serial && grant.hot.live())
            .map(|grant| grant.hot.remaining())
            .ok_or(Error::Revoked)
    }

    /// Includes unused grants and retained bytes after dequeue or confirmation.
    pub fn usage(&self, class: Class) -> Budget {
        self.ledger.borrow().usage[class.index()]
    }

    /// Capture before checking capacity, then pass to `changed_after`.
    pub fn generation(&self) -> u64 {
        self.shared.changed.generation()
    }

    /// Wait for released capacity without losing a change before the first poll.
    pub async fn changed_after(&self, generation: u64) {
        self.shared.changed.changed_after(generation).await;
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.shared
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        let mut state = self.ledger.borrow();
        state.closed = true;
        for slot in 0..state.grants.len() {
            if state.grants[slot].is_some() {
                state.revoke(slot);
            }
        }
        drop(state);
        self.shared.changed.notify_changed();
    }
}

impl Client {
    /// Revoke all unused grants before installing a fresh session. Already
    /// admitted work remains charged. Reusing any prior wire session is forbidden.
    pub fn replace_session(&mut self, session: LinkSessionId) -> Result<(), Error> {
        let mut state = self.ledger.borrow();
        if state.closed {
            return Err(Error::Closed);
        }
        if !valid_session(session)
            || state.clients[self.slot]
                .as_ref()
                .expect("live client")
                .session
                == session
        {
            return Err(Error::Invalid);
        }
        state.revoke_client(self.slot);
        state.clients[self.slot]
            .as_mut()
            .expect("live client")
            .session = session;
        drop(state);
        self.shared.changed.notify_changed();
        Ok(())
    }

    /// This client's outstanding reservations and admitted backing storage.
    pub fn usage(&self, class: Class) -> Budget {
        self.ledger.borrow().clients[self.slot]
            .as_ref()
            .expect("live client")
            .usage[class.index()]
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let mut state = self.ledger.borrow();
        state.revoke_client(self.slot);
        state.clients[self.slot].as_mut().expect("live client").open = false;
        state.collect(self.slot);
        drop(state);
        self.shared.changed.notify_changed();
    }
}

impl Grant {
    /// Capture before handing this unique token to the dispatcher. The shard
    /// uses this identity to extend its reservation as owned resources return.
    pub fn key(&self) -> GrantKey {
        GrantKey {
            shared: Arc::downgrade(&self.shared),
            slot: self.slot,
            serial: self.serial,
            revoked: self.revoked.clone(),
        }
    }

    /// Independent capacity class reserved by this token.
    pub fn class(&self) -> Class {
        self.hot.class
    }

    /// Session for which the shard issued this reservation.
    pub fn session(&self) -> LinkSessionId {
        self.hot.session
    }

    /// Whether the issuing shard and this reservation remain active.
    pub fn is_live(&self) -> bool {
        !self
            .shared
            .closed
            .load(std::sync::atomic::Ordering::Acquire)
            && self.hot.live()
    }

    /// Charge one frame before enqueue. `bytes` must include all retained backing
    /// allocations and descriptors, including storage hidden by a sliced `Bytes`.
    pub fn admit(&mut self, session: LinkSessionId, bytes: usize) -> Result<Admission, Error> {
        if bytes == 0 {
            return Err(Error::Invalid);
        }
        if self
            .shared
            .closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Closed);
        }
        self.hot.spend(session, bytes)?;
        let quota = Quota { messages: 1, bytes };
        let memory = self
            .memory
            .as_ref()
            .map(|memory| {
                memory
                    .admit(foreign_quota(quota)?)
                    .map_err(|error| memory_error(error.kind()))
            })
            .transpose();
        let memory = match memory {
            Ok(memory) => memory,
            Err(error) => {
                if let Some(memory) = &self.memory {
                    memory.release(foreign_quota(quota)?);
                }
                self.shared.returns.send(returns::Returned::Released {
                    client: self.hot.client,
                    class: self.hot.class,
                    budget: quota.budget(),
                });
                self.shared.changed.notify_changed();
                return Err(error);
            }
        };
        Ok(Admission {
            queue: QueueSlot {
                shared: self.shared.clone(),
                client: self.hot.client,
                class: self.hot.class,
            },
            retained: Retention(Arc::new(Retained {
                shared: self.shared.clone(),
                client: self.hot.client,
                class: self.hot.class,
                bytes,
                memory,
            })),
        })
    }

    /// Unspent reservation. Revoked grants report zero.
    pub fn remaining(&self) -> Quota {
        self.hot.remaining()
    }
}

impl Drop for Grant {
    fn drop(&mut self) {
        let unused = self.hot.revoke();
        if let Some(unused) = unused {
            self.revoked.publish(unused);
            if let Some(memory) = &self.memory {
                memory.release(foreign_quota(unused).expect("bounded foreign grant"));
            }
        }
        self.shared.returns.send(returns::Returned::GrantDropped {
            slot: self.slot,
            serial: self.serial,
            unused: unused.unwrap_or_default(),
        });
        self.shared.changed.notify_changed();
    }
}

impl Admission {
    /// Call after the queue has published consumption. Retain the returned token
    /// until every alias in replication, network, and disk work has been released.
    pub fn dequeued(self) -> Retention {
        drop(self.queue);
        self.retained
    }
}

impl Drop for QueueSlot {
    fn drop(&mut self) {
        self.shared.returns.send(returns::Returned::Released {
            client: self.client,
            class: self.class,
            budget: Budget {
                queue_slots: 1,
                ..Budget::default()
            },
        });
        self.shared.changed.notify_changed();
    }
}

impl Drop for Retained {
    fn drop(&mut self) {
        // The backing's final alias has gone. Release its shared allocation
        // claim before publishing destination capacity to replenishing shards.
        drop(self.memory.take());
        self.shared.returns.send(returns::Returned::Released {
            client: self.client,
            class: self.class,
            budget: Budget {
                retained_messages: 1,
                bytes: self.bytes,
                ..Budget::default()
            },
        });
        self.shared.changed.notify_changed();
    }
}

impl Retention {
    /// Full backing-storage reservation held until the last alias is released.
    pub fn bytes(&self) -> usize {
        self.0.bytes
    }

    /// Associate an alias with this message's existing charge. The charge must
    /// already cover its entire backing allocation. This does not admit a copy.
    /// Clones and slices of the returned bytes keep that charge until final drop.
    pub fn attach(self, bytes: Bytes) -> Bytes {
        Bytes::from_owner(TrackedBytes {
            bytes,
            _retained: self,
        })
    }
}

struct TrackedBytes {
    bytes: Bytes,
    _retained: Retention,
}

impl AsRef<[u8]> for TrackedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn valid_session(session: LinkSessionId) -> bool {
    session.as_bytes() != &[0; 16]
}

// ReceiveBuffers retains three compact metadata owners and one opaque payload.
// Slots count these physical owners separately from dispatch queue entries.
pub(super) fn foreign_quota(quota: Quota) -> Result<crate::memory::Quota, Error> {
    Ok(crate::memory::Quota {
        bytes: quota.bytes,
        buffers: quota.messages.checked_mul(4).ok_or(Error::Invalid)?,
    })
}

fn memory_error(kind: std::io::ErrorKind) -> Error {
    match kind {
        std::io::ErrorKind::WouldBlock => Error::Full,
        _ => Error::Invalid,
    }
}
