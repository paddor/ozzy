//! Command queues reclaim replies and permits without waiting for idle senders.
//!
//! An overlapping publication may finish cleanup on its sender thread. Keep
//! explicit admission/shutdown signals where completion must precede that cleanup.

use fanring::teardown::Coordinated;

mod notified;

#[cfg(test)]
pub(crate) use fanring::mpsc::RecvError;
pub(crate) use fanring::mpsc::{TryRecvError, TrySendError};
pub(crate) use notified::{NotifiedReceiver, NotifiedSender, notified_channel};

pub(crate) type Sender<T> = fanring::mpsc::Sender<T, Coordinated>;
pub(crate) type Receiver<T> = fanring::mpsc::Receiver<T, Coordinated>;

pub(crate) fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    fanring::mpsc::channel_with_policy(capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::{Semaphore, oneshot};

    #[test]
    fn receiver_drop_cancels_replies_and_releases_permits_with_live_senders() {
        for register in [false, true] {
            let (mut first, mut receiver) = channel(4);
            let mut second = first.try_clone().unwrap();
            if register {
                assert!(receiver.try_recv().is_err());
            }
            let permits = Arc::new(Semaphore::new(2));
            let mut replies = Vec::new();
            for sender in [&mut first, &mut second] {
                let (reply, response) = oneshot::channel::<()>();
                sender
                    .try_send((reply, permits.clone().try_acquire_owned().unwrap()))
                    .unwrap();
                replies.push(response);
            }
            assert_eq!(permits.available_permits(), 0);
            drop(receiver);
            assert!(first.is_disconnected());
            assert!(second.is_disconnected());
            for mut reply in replies {
                assert_eq!(reply.try_recv(), Err(oneshot::error::TryRecvError::Closed));
            }
            assert_eq!(permits.available_permits(), 2);
            drop((first, second));
            assert_eq!(permits.available_permits(), 2);
        }
    }
}
