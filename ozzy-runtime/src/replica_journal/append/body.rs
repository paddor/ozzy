//! Canonical bytes become immutable once, shared by transport and persistence.

use super::JournalError;
use crate::memory::{Allocator, Arena};
use bytes::Bytes;
use std::{ops::Deref, sync::Arc};

#[derive(Debug)]
pub(super) struct Body {
    state: State,
    allocator: Option<Allocator>,
}

#[derive(Debug)]
enum State {
    Mutable(Vec<u8>),
    Charged(Arc<Arena>),
    Shared(Bytes, usize),
}

impl Body {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            state: State::Mutable(Vec::with_capacity(capacity)),
            allocator: None,
        }
    }

    pub(super) fn charged(allocator: Allocator) -> Self {
        Self {
            state: State::Mutable(Vec::new()),
            allocator: Some(allocator),
        }
    }

    pub(super) fn is_charged(&self) -> bool {
        self.allocator.is_some()
    }

    pub(super) fn allocator(&self) -> Option<Allocator> {
        self.allocator.clone()
    }

    pub(super) fn freeze(&mut self) -> Bytes {
        if let State::Charged(arena) = &self.state {
            return Arena::share(arena);
        }
        let previous = std::mem::replace(&mut self.state, State::Mutable(Vec::new()));
        self.state = match previous {
            State::Mutable(mut bytes) => {
                bytes.shrink_to_fit();
                let capacity = bytes.capacity();
                State::Shared(Bytes::from(bytes), capacity)
            }
            State::Charged(_) => unreachable!(),
            shared @ State::Shared(..) => shared,
        };
        let State::Shared(bytes, _) = &self.state else {
            unreachable!()
        };
        bytes.clone()
    }

    pub(super) fn retained_bytes(&self) -> usize {
        match &self.state {
            State::Mutable(bytes) => bytes.capacity(),
            State::Charged(arena) => arena.capacity(),
            State::Shared(_, capacity) => *capacity,
        }
    }

    pub(super) fn clear(&mut self) {
        match &mut self.state {
            State::Mutable(bytes) => bytes.clear(),
            State::Shared(bytes, _) if self.allocator.is_none() && bytes.is_unique() => {
                let mut reused = Vec::from(std::mem::take(bytes));
                reused.clear();
                self.state = State::Mutable(reused);
            }
            State::Charged(_) | State::Shared(_, _) => self.state = State::Mutable(Vec::new()),
        }
    }

    /// Reserve before modifying anything. A shared allocation remains charged
    /// while its private replacement is allocated and copied.
    pub(super) fn reserve(&mut self, additional: usize) -> Result<(), JournalError> {
        let required = self
            .len()
            .checked_add(additional)
            .ok_or(JournalError::AppendCapacity)?;
        if let Some(allocator) = &self.allocator {
            if required == 0 {
                let private = match &mut self.state {
                    State::Mutable(_) => true,
                    State::Charged(arena) => Arc::get_mut(arena).is_some(),
                    State::Shared(..) => false,
                };
                if !private {
                    self.clear();
                }
                return Ok(());
            }
            if let State::Charged(arena) = &mut self.state
                && arena.capacity() >= required
                && Arc::get_mut(arena).is_some()
            {
                return Ok(());
            }
            let mut arena = allocator.try_arena(required).map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!("journal payload allocation ({required} bytes): {error}"),
                )
            })?;
            arena.bytes_mut().extend_from_slice(self);
            self.state = State::Charged(Arc::new(arena));
        } else {
            if let State::Shared(bytes, _) = &mut self.state {
                self.state = State::Mutable(Vec::from(std::mem::take(bytes)));
            }
            let State::Mutable(bytes) = &mut self.state else {
                unreachable!()
            };
            bytes
                .try_reserve_exact(additional)
                .map_err(std::io::Error::other)?;
        }
        Ok(())
    }

    pub(super) fn mutable(&mut self) -> Result<&mut Vec<u8>, JournalError> {
        self.reserve(0)?;
        match &mut self.state {
            State::Mutable(bytes) => Ok(bytes),
            State::Charged(arena) => Ok(Arc::get_mut(arena)
                .expect("reserved private arena")
                .bytes_mut()),
            State::Shared(..) => unreachable!(),
        }
    }

    pub(super) async fn reserve_when_available(
        &mut self,
        additional: usize,
    ) -> Result<(), JournalError> {
        match self.reserve(additional) {
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                let required = self
                    .len()
                    .checked_add(additional)
                    .ok_or(JournalError::AppendCapacity)?;
                let mut arena = self
                    .allocator
                    .as_ref()
                    .expect("charged body")
                    .arena(required)
                    .await?;
                arena.bytes_mut().extend_from_slice(self);
                self.state = State::Charged(Arc::new(arena));
                Ok(())
            }
            result => result,
        }
    }

    pub(super) fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), JournalError> {
        self.reserve(bytes.len())?;
        self.mutable()?.extend_from_slice(bytes);
        Ok(())
    }

    pub(super) fn truncate(&mut self, len: usize) -> Result<(), JournalError> {
        self.mutable()?.truncate(len);
        Ok(())
    }
}

impl Deref for Body {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match &self.state {
            State::Mutable(bytes) => bytes,
            State::Charged(arena) => arena.as_ref().as_ref(),
            State::Shared(bytes, _) => bytes,
        }
    }
}

#[derive(Default)]
pub(super) struct EncodedSize(pub usize);

impl ozzy_journal::operation::OperationOutput for EncodedSize {
    fn len(&self) -> usize {
        self.0
    }
    fn truncate(&mut self, len: usize) {
        self.0 = len;
    }
    fn extend(&mut self, bytes: &[u8]) -> Result<(), ozzy_journal::operation::OperationCodecError> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or(ozzy_journal::operation::OperationCodecError::LengthOverflow)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(bytes: usize, buffers: usize) -> crate::memory::Owner {
        crate::memory::Domain::new(None, bytes)
            .unwrap()
            .owner(crate::memory::Limits {
                bytes,
                buffers,
                cache_bytes: bytes,
            })
            .unwrap()
    }

    #[test]
    fn charged_arenas_are_lazy_and_share_one_budget_through_remote_retention() {
        let owner = owner(128, 2);
        let mut bodies: Vec<_> = (0..100).map(|_| Body::charged(owner.allocator())).collect();
        assert_eq!(owner.allocated_bytes(), 0);
        bodies[0].extend_from_slice(&[7; 64]).unwrap();
        bodies[1].extend_from_slice(&[8; 64]).unwrap();
        assert_eq!(owner.allocated_bytes(), 128);
        assert!(
            matches!(bodies[2].extend_from_slice(&[9]), Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
        assert!(bodies[2].is_empty());
        let bytes = bodies[0].freeze();
        let alias = bytes.slice(0..1);
        bodies[0].clear();
        drop(bytes);
        assert!(bodies[2].extend_from_slice(&[9]).is_err());
        std::thread::spawn(move || drop(alias)).join().unwrap();
        bodies[2].extend_from_slice(&[9]).unwrap();
        assert_eq!(&*bodies[2], &[9]);
        // The reused allocation is still charged in full, despite a one-byte body.
        assert_eq!(bodies[2].retained_bytes(), 64);
        assert_eq!(owner.allocated_bytes(), 128);
        drop(bodies);
        owner.trim_cache();
        assert_eq!(owner.allocated_bytes(), 0);
    }

    #[test]
    fn failed_copy_on_write_keeps_canonical_bytes_and_never_steals_alias_capacity() {
        let owner = owner(128, 2);
        let mut body = Body::charged(owner.allocator());
        body.extend_from_slice(&[1; 64]).unwrap();
        let network = body.freeze();
        let occupied = owner.try_lease(64).unwrap();
        assert!(body.mutable().is_err());
        assert_eq!(&*body, &[1; 64]);
        assert_eq!(network.as_ref(), &[1; 64]);
        drop(occupied);
        body.mutable().unwrap()[0] = 2;
        assert_eq!(network[0], 1);
        assert_eq!(body[0], 2);
        assert_eq!(owner.allocated_bytes(), 128);
    }

    #[test]
    fn charged_empty_freeze_can_be_reused_without_allocating() {
        let owner = owner(16, 1);
        let mut body = Body::charged(owner.allocator());
        assert!(body.freeze().is_empty());
        assert_eq!(body.mutable().unwrap().len(), 0);
        assert_eq!(owner.allocated_bytes(), 0);
        body.reserve(16).unwrap();
        let empty = body.freeze();
        assert_eq!(body.mutable().unwrap().len(), 0);
        assert_eq!(owner.allocated_bytes(), 16);
        assert!(body.extend_from_slice(&[42]).is_err());
        drop(empty);
        body.extend_from_slice(&[42]).unwrap();
        assert_eq!(body.freeze().as_ref(), &[42]);
    }

    #[test]
    fn released_validation_alias_allows_private_mutation_when_budget_is_full() {
        let owner = owner(64, 1);
        let mut body = Body::charged(owner.allocator());
        body.extend_from_slice(&[1; 64]).unwrap();
        let validation = body.freeze();
        let pointer = validation.as_ptr();
        assert!(body.mutable().is_err());
        drop(validation);
        // Rejected canonical validation must restore offsets without acquiring
        // another buffer. No transport observer can see this mutation.
        body.mutable().unwrap()[0] = 2;
        assert_eq!(body.as_ptr(), pointer);
        assert_eq!(body[0], 2);
        assert_eq!(owner.allocated_bytes(), 64);
    }

    #[test]
    fn clearing_an_unshared_arena_returns_capacity_for_another_partition() {
        let owner = owner(64, 1);
        let mut first = Body::charged(owner.allocator());
        let mut second = Body::charged(owner.allocator());
        first.extend_from_slice(&[1; 64]).unwrap();
        assert!(second.extend_from_slice(&[2; 64]).is_err());
        first.clear();
        second.extend_from_slice(&[2; 64]).unwrap();
        assert!(first.is_empty());
        assert_eq!(&*second, &[2; 64]);
        assert_eq!(owner.allocated_bytes(), 64);
    }

    #[test]
    fn shared_cache_cannot_inflate_a_smaller_partitions_retention_window() {
        let owner = owner(128, 2);
        let mut large = Body::charged(owner.allocator().with_limit(128));
        large.extend_from_slice(&[1; 128]).unwrap();
        large.clear();
        let mut small = Body::charged(owner.allocator().with_limit(32));
        small.extend_from_slice(&[2; 16]).unwrap();
        assert!(small.retained_bytes() <= 32);
        assert_eq!(small.freeze().as_ref(), &[2; 16]);
        // A larger cached allocation would make every proposal exceed the
        // smaller partition's physical-retention limit despite its tiny body.
        assert!(owner.allocated_bytes() <= 32);
    }

    #[test]
    fn transport_and_persistence_share_immutable_bytes_across_reuse() {
        let mut body = Body::new(4096);
        body.extend_from_slice(&[7; 1024]).unwrap();
        let network = body.freeze();
        let persistence = body.freeze();
        assert_eq!(network.as_ptr(), persistence.as_ptr());
        assert_eq!(body.as_ptr(), network.as_ptr());
        body.clear();
        body.extend_from_slice(&[9; 1024]).unwrap();
        assert_eq!(&network[..], &[7; 1024]);
        assert_eq!(&persistence[..], &[7; 1024]);
        assert_eq!(&*body, &[9; 1024]);
    }

    #[test]
    fn retained_capacity_tracks_freeze_mutation_and_reuse() {
        let mut body = Body::new(8192);
        assert!(body.retained_bytes() >= 8192);
        body.extend_from_slice(&[1; 32]).unwrap();
        let old = body.freeze();
        assert!(body.retained_bytes() >= old.len());
        body.extend_from_slice(&[2; 4096]).unwrap();
        assert!(body.retained_bytes() >= 4128);
        assert_eq!(old.as_ref(), &[1; 32]);
        let current = body.freeze();
        assert!(body.retained_bytes() >= current.len());
        body.clear();
        assert_eq!(body.retained_bytes(), 0);
        assert_eq!(current.len(), 4128);
        body.extend_from_slice(&[3; 64]).unwrap();
        drop(body.freeze());
        let capacity = body.retained_bytes();
        body.clear();
        assert_eq!(body.retained_bytes(), capacity);
        assert!(body.is_empty());
    }

    #[test]
    fn retry_mutation_cannot_change_published_bytes() {
        let mut body = Body::new(32);
        body.extend_from_slice(&[1; 32]).unwrap();
        let network = body.freeze();
        body.mutable().unwrap()[0] = 9;
        assert_eq!(network[0], 1);
        assert_eq!(body[0], 9);
        assert_eq!(network.len(), 32);
    }
}
