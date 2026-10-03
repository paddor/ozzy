use std::sync::atomic::{AtomicU64, Ordering};

use ozzy_proto::LinkSessionId;

use super::{Class, Error, Quota};

const BYTE_BITS: u32 = 40;
const BYTE_MASK: u64 = (1_u64 << BYTE_BITS) - 1;
const MESSAGE_MASK: u64 = (1_u64 << (63 - BYTE_BITS)) - 1;
const ACTIVE: u64 = 1_u64 << 63;

#[derive(Debug)]
pub(super) struct Hot {
    state: AtomicU64,
    pub class: Class,
    pub session: LinkSessionId,
    pub client: usize,
}

impl Hot {
    pub(super) fn new(
        class: Class,
        session: LinkSessionId,
        client: usize,
        quota: Quota,
    ) -> Result<Self, Error> {
        Ok(Self {
            state: AtomicU64::new(ACTIVE | encode(quota)?),
            class,
            session,
            client,
        })
    }

    pub(super) fn valid(quota: Quota) -> bool {
        quota.messages as u64 <= MESSAGE_MASK && quota.bytes as u64 <= BYTE_MASK
    }

    pub(super) fn live(&self) -> bool {
        self.state.load(Ordering::Acquire) & ACTIVE != 0
    }

    pub(super) fn remaining(&self) -> Quota {
        let state = self.state.load(Ordering::Acquire);
        if state & ACTIVE == 0 {
            Quota::default()
        } else {
            decode(state)
        }
    }

    pub(super) fn spend(&self, session: LinkSessionId, bytes: usize) -> Result<(), Error> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & ACTIVE == 0 {
                return Err(Error::Revoked);
            }
            if session != self.session {
                return Err(Error::Session);
            }
            let remaining = decode(state);
            if remaining.messages == 0 || bytes > remaining.bytes {
                return Err(Error::Full);
            }
            let next = state - (1_u64 << BYTE_BITS) - bytes as u64;
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(current) => state = current,
            }
        }
    }

    pub(super) fn add(&self, quota: Quota) -> Result<(), Error> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & ACTIVE == 0 {
                return Err(Error::Revoked);
            }
            let old = decode(state);
            let next = Quota {
                messages: old
                    .messages
                    .checked_add(quota.messages)
                    .ok_or(Error::Invalid)?,
                bytes: old.bytes.checked_add(quota.bytes).ok_or(Error::Invalid)?,
            };
            let encoded = ACTIVE | encode(next)?;
            match self.state.compare_exchange_weak(
                state,
                encoded,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => state = current,
            }
        }
    }

    pub(super) fn revoke(&self) -> Option<Quota> {
        let old = self.state.swap(0, Ordering::AcqRel);
        (old & ACTIVE != 0).then(|| decode(old))
    }
}

fn encode(quota: Quota) -> Result<u64, Error> {
    if !Hot::valid(quota) {
        return Err(Error::Invalid);
    }
    Ok(((quota.messages as u64) << BYTE_BITS) | quota.bytes as u64)
}

fn decode(state: u64) -> Quota {
    Quota {
        messages: ((state >> BYTE_BITS) & MESSAGE_MASK) as usize,
        bytes: (state & BYTE_MASK) as usize,
    }
}
