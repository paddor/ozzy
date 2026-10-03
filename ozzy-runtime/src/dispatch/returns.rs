use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use fanring::{mpsc, teardown::Coordinated};

use super::{Budget, Class};

#[derive(Clone, Copy, Debug)]
pub(super) enum Returned {
    Released {
        client: usize,
        class: Class,
        budget: Budget,
    },
    GrantDropped {
        slot: usize,
        serial: u64,
        unused: super::Quota,
    },
}

type Sender = mpsc::Sender<Returned, Coordinated>;
pub(super) type Receiver = mpsc::Receiver<Returned, Coordinated>;

#[derive(Debug)]
pub(super) struct Returns {
    id: u64,
    registration: Mutex<Sender>,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static SENDERS: RefCell<HashMap<u64, (Weak<Returns>, Sender)>> =
        RefCell::new(HashMap::new());
}

pub(super) fn channel(capacity: usize) -> Result<(Arc<Returns>, Receiver), super::Error> {
    let (sender, receiver) =
        mpsc::try_channel_with_policy(capacity).map_err(|_| super::Error::Invalid)?;
    Ok((
        Arc::new(Returns {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            registration: Mutex::new(sender),
        }),
        receiver,
    ))
}

impl Returns {
    pub(super) fn send(self: &Arc<Self>, value: Returned) {
        SENDERS.with(|local| {
            let mut local = local.borrow_mut();
            if !local.contains_key(&self.id) {
                local.retain(|_, (owner, _)| owner.strong_count() != 0);
                let Some(sender) = self
                    .registration
                    .lock()
                    .expect("dispatch return registration poisoned")
                    .try_clone()
                else {
                    return;
                };
                local.insert(self.id, (Arc::downgrade(self), sender));
            }
            let sender = &mut local.get_mut(&self.id).expect("registered return lane").1;
            match sender.try_send(value) {
                Ok(()) | Err(mpsc::TrySendError::Disconnected(_)) => {}
                Err(mpsc::TrySendError::Full(_)) => {
                    panic!("bounded dispatch return lane overflowed")
                }
            }
        });
    }
}
