//! One typed completion over fanring, using centralized terminal readiness.
//!
//! Sender consumption publishes at most one value. Dropping either endpoint
//! reclaims its owned values without canceling the disk operation that owns it.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::command_channel as mpsc;
use crate::signal::{CloseSignal, Closed};

pub(crate) use mpsc::RecvError;

pub(crate) struct Sender<T> {
    queue: Option<mpsc::Sender<Option<T>>>,
    ready: Option<CloseSignal>,
    sent: bool,
}

pub(crate) struct Receiver<T> {
    queue: Option<mpsc::Receiver<Option<T>>>,
    ready: Option<Wait>,
}

// Only the boxed waiter is pinned. Moving either endpoint never moves it.
impl<T> Unpin for Receiver<T> {}

type Wait = Pin<Box<Option<Closed>>>;

pub(crate) fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (sender, receiver) = mpsc::channel(1);
    let ready = CloseSignal::default();
    let notified = Box::pin(Some(ready.closed()));
    (
        Sender {
            queue: Some(sender),
            ready: Some(ready),
            sent: false,
        },
        Receiver {
            queue: Some(receiver),
            ready: Some(notified),
        },
    )
}

impl<T> Sender<T> {
    pub(crate) fn send(mut self, value: T) -> Result<(), T> {
        let result = self
            .queue
            .as_mut()
            .expect("live completion sender")
            .try_send(Some(value))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(value) | mpsc::TrySendError::Disconnected(value) => {
                    value.expect("typed completion value")
                }
            });
        self.sent = result.is_ok();
        result
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut sender = self.queue.take().expect("live completion sender");
        if !self.sent {
            // A retained queue stays connected. Publish cancellation explicitly
            // so blocking receivers also observe a dropped sender.
            let _ = sender.try_send(None);
        }
        let ready = self.ready.take().expect("live completion signal");
        ready.close();
        drop(ready);
        drop(sender);
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let wait = this.ready.as_mut().expect("live completion wait");
        std::task::ready!(wait.as_mut().as_pin_mut().expect("active wait").poll(cx));
        Poll::Ready(this.try_recv().map_err(|_| RecvError))
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletionSender").finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletionReceiver").finish_non_exhaustive()
    }
}

pub(crate) mod error {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum TryRecvError {
        Empty,
        Closed,
    }
}

impl<T> Receiver<T> {
    pub(crate) fn try_recv(&mut self) -> Result<T, error::TryRecvError> {
        self.queue
            .as_mut()
            .expect("live receiver")
            .try_recv()
            .map_err(|error| match error {
                mpsc::TryRecvError::Empty => error::TryRecvError::Empty,
                mpsc::TryRecvError::Disconnected => error::TryRecvError::Closed,
            })?
            .ok_or(error::TryRecvError::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[tokio::test]
    async fn publication_before_or_after_poll_is_not_lost() {
        for early in [false, true] {
            let (sender, mut receiver) = channel();
            if !early {
                assert!(futures::poll!(&mut receiver).is_pending());
            }
            sender.send(42).unwrap();
            assert_eq!(receiver.await.unwrap(), 42);
        }
    }

    #[tokio::test]
    async fn sender_drop_closes_pending_and_future_waits() {
        let (sender, mut receiver) = channel::<()>();
        assert!(futures::poll!(&mut receiver).is_pending());
        drop(sender);
        assert!(receiver.await.is_err());
    }

    #[test]
    fn cancellation_returns_unsent_owner_and_reclaims_published_owner() {
        let slots = Arc::new(Semaphore::new(1));
        let (sender, receiver) = channel();
        let permit = slots.clone().try_acquire_owned().unwrap();
        drop(receiver);
        let returned = sender.send(permit).unwrap_err();
        assert_eq!(slots.available_permits(), 0);
        drop(returned);
        assert_eq!(slots.available_permits(), 1);

        let (sender, receiver) = channel();
        sender
            .send(slots.clone().try_acquire_owned().unwrap())
            .unwrap();
        assert_eq!(slots.available_permits(), 0);
        drop(receiver);
        assert_eq!(slots.available_permits(), 1);
    }
}
