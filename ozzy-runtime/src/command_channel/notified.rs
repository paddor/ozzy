//! Async readiness and blocking receives over fanring's per-sender lanes.
//!
//! Publication, slot release, and closure notifications stay inside this adapter.
//! A readiness wake requires another queue check; it is not an application reply.

use std::sync::Arc;

use super::{Receiver, RecvError, Sender, TryRecvError, TrySendError};
use crate::signal::{CloseSignal, DataSignal, StateSignal};

#[derive(Debug, Default)]
struct Signals {
    data: DataSignal,
    space: StateSignal,
    closed: CloseSignal,
}

#[derive(Debug)]
pub(crate) struct NotifiedSender<T> {
    sender: Option<Sender<T>>,
    signals: Arc<Signals>,
}

#[derive(Debug)]
pub(crate) struct NotifiedReceiver<T> {
    receiver: Receiver<T>,
    signals: Arc<Signals>,
}

pub(crate) fn notified_channel<T>(capacity: usize) -> (NotifiedSender<T>, NotifiedReceiver<T>) {
    let (sender, receiver) = super::channel(capacity);
    let signals = Arc::new(Signals::default());
    (
        NotifiedSender {
            sender: Some(sender),
            signals: Arc::clone(&signals),
        },
        NotifiedReceiver { receiver, signals },
    )
}

impl<T> NotifiedSender<T> {
    pub(crate) fn try_clone(&self) -> Option<Self> {
        if self.signals.closed.is_closed() {
            return None;
        }
        Some(Self {
            sender: Some(self.sender.as_ref()?.try_clone()?),
            signals: Arc::clone(&self.signals),
        })
    }

    pub(crate) fn is_disconnected(&self) -> bool {
        self.signals.closed.is_closed() || self.sender.as_ref().is_none_or(Sender::is_disconnected)
    }

    pub(crate) fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        if self.signals.closed.is_closed() {
            return Err(TrySendError::Disconnected(value));
        }
        self.sender.as_mut().expect("live sender").try_send(value)?;
        self.signals.data.mark();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn send(&mut self, mut value: T) -> Result<(), TrySendError<T>> {
        loop {
            let generation = self.signals.space.generation();
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(value)) => {
                    return Err(TrySendError::Disconnected(value));
                }
                Err(TrySendError::Full(returned)) => value = returned,
            }
            tokio::select! {
                () = self.signals.space.changed_after(generation) => {},
                () = self.signals.closed.closed() => {
                    return Err(TrySendError::Disconnected(value));
                }
            }
        }
    }
}

impl<T> Drop for NotifiedSender<T> {
    fn drop(&mut self) {
        // The receiver must observe the updated live-sender count when it wakes.
        drop(self.sender.take());
        self.signals.data.mark();
    }
}

impl<T> NotifiedReceiver<T> {
    pub(crate) fn owned_ready(&self) -> impl std::future::Future<Output = ()> + use<T> {
        let signals = self.signals.clone();
        async move {
            tokio::select! {
                () = signals.data.ready() => {},
                () = signals.closed.closed() => {},
            }
        }
    }

    /// Wait without blocking the executor hosting this receiver.
    pub(crate) async fn recv_async(&mut self) -> Result<T, RecvError> {
        loop {
            match self.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => self.ready().await,
            }
        }
    }

    pub(crate) fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.receive(Receiver::try_recv)
    }

    pub(crate) async fn ready(&self) {
        // Closure wakes once like another queue change. After observing a
        // disconnected receive, callers must not expect another readiness event.
        self.signals.data.ready().await;
    }

    fn receive<E>(
        &mut self,
        receive: impl FnOnce(&mut Receiver<T>) -> Result<T, E>,
    ) -> Result<T, E> {
        let result = self.signals.data.drain(|| receive(&mut self.receiver));
        if result.is_ok() {
            self.receiver.release_consumed();
            // Capacity belongs to individual lanes. Wake every sender to check
            // its own lane, not an arbitrary single waiter on a still-full lane.
            self.signals.space.notify_changed();
            // A bounded receiver may leave queued work without another send.
            // One extra empty probe consumes this hint after the final value.
            self.signals.data.mark();
        }
        result
    }
}

impl<T> Drop for NotifiedReceiver<T> {
    fn drop(&mut self) {
        // Close independently of coordinated fanring cleanup, which may overlap
        // a sender. No blocked sender needs another queue event to wake.
        self.signals.closed.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;

    async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(2), future)
            .await
            .expect("channel did not make progress")
    }

    #[tokio::test]
    async fn async_receive_preserves_work_and_closure_after_cancellation() {
        let (mut sender, mut receiver) = notified_channel(1);
        let mut canceled = Box::pin(receiver.recv_async());
        assert!(futures::poll!(canceled.as_mut()).is_pending());
        drop(canceled);
        sender.try_send(7).unwrap();
        assert_eq!(bounded(receiver.recv_async()).await.unwrap(), 7);
        drop(sender);
        assert!(bounded(receiver.recv_async()).await.is_err());
    }

    #[tokio::test]
    async fn partial_receive_releases_space_before_returning() {
        let (mut sender, mut receiver) = notified_channel(4);
        for value in 0..4 {
            sender.try_send(value).unwrap();
        }
        let mut blocked = Box::pin(sender.send(4));
        assert!(futures::poll!(blocked.as_mut()).is_pending());
        assert_eq!(bounded(receiver.recv_async()).await.unwrap(), 0);
        bounded(blocked).await.unwrap();
        for value in 1..5 {
            assert_eq!(receiver.try_recv().unwrap(), value);
        }
    }

    #[tokio::test]
    async fn receiver_drop_wakes_all_lanes_and_preserves_unsubmitted_values() {
        let (mut first, receiver) = notified_channel(1);
        let mut second = first.try_clone().unwrap();
        first.try_send(1).unwrap();
        second.try_send(2).unwrap();
        let mut first_pending = Box::pin(first.send(3));
        let mut second_pending = Box::pin(second.send(4));
        assert!(futures::poll!(first_pending.as_mut()).is_pending());
        assert!(futures::poll!(second_pending.as_mut()).is_pending());
        drop(receiver);
        let (first_result, second_result) =
            bounded(async { tokio::join!(first_pending, second_pending) }).await;
        assert!(matches!(first_result, Err(TrySendError::Disconnected(3))));
        assert!(matches!(second_result, Err(TrySendError::Disconnected(4))));
        assert!(first.is_disconnected());
        assert!(first.try_clone().is_none());
        assert!(matches!(
            first.send(5).await,
            Err(TrySendError::Disconnected(5))
        ));
    }

    #[tokio::test]
    async fn owned_readiness_observes_receiver_closure_with_live_sender() {
        let (sender, receiver) = notified_channel::<u8>(1);
        let mut ready = Box::pin(receiver.owned_ready());
        assert!(futures::poll!(ready.as_mut()).is_pending());
        drop(receiver);
        bounded(ready).await;
        assert!(sender.is_disconnected());
    }

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn space_release_wakes_each_waiting_lane_to_recheck() {
        let (mut first, mut receiver) = notified_channel(1);
        let mut second = first.try_clone().unwrap();
        first.try_send(1).unwrap();
        second.try_send(2).unwrap();
        let counts = [
            Arc::new(WakeCount::default()),
            Arc::new(WakeCount::default()),
        ];
        let wakers = counts
            .each_ref()
            .map(|count| Waker::from(Arc::clone(count)));
        let mut first_pending = Box::pin(first.send(3));
        let mut second_pending = Box::pin(second.send(4));
        assert!(matches!(
            first_pending
                .as_mut()
                .poll(&mut Context::from_waker(&wakers[0])),
            Poll::Pending
        ));
        assert!(matches!(
            second_pending
                .as_mut()
                .poll(&mut Context::from_waker(&wakers[1])),
            Poll::Pending
        ));
        let before = counts
            .each_ref()
            .map(|count| count.0.load(Ordering::Relaxed));
        receiver.try_recv().unwrap();
        for (count, before) in counts.iter().zip(before) {
            assert!(
                count.0.load(Ordering::Relaxed) > before,
                "space release did not wake every lane's waiter"
            );
        }
    }

    #[test]
    fn receiver_drop_reclaims_queued_replies_and_permits_with_live_sender() {
        let (mut sender, receiver) = notified_channel(1);
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let (done, mut reply) = tokio::sync::oneshot::channel::<()>();
        sender
            .try_send((done, Arc::clone(&permits).try_acquire_owned().unwrap()))
            .unwrap();
        drop(receiver);
        assert!(sender.is_disconnected());
        assert_eq!(
            reply.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        );
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn canceled_send_releases_only_its_unsubmitted_owner() {
        let (mut sender, mut receiver) = notified_channel(1);
        let queued = Arc::new(());
        let unsubmitted = Arc::new(());
        sender.try_send(Arc::clone(&queued)).unwrap();
        let mut pending = Box::pin(sender.send(Arc::clone(&unsubmitted)));
        assert!(futures::poll!(pending.as_mut()).is_pending());
        assert_eq!(Arc::strong_count(&unsubmitted), 2);
        drop(pending);
        assert_eq!(Arc::strong_count(&unsubmitted), 1);
        assert_eq!(Arc::strong_count(&queued), 2);
        assert!(Arc::ptr_eq(&receiver.try_recv().unwrap(), &queued));
        assert_eq!(Arc::strong_count(&queued), 1);
    }

    #[tokio::test]
    async fn canceling_readiness_after_publication_does_not_lose_work() {
        let (mut sender, mut receiver) = notified_channel(1);
        let mut waiting = Box::pin(receiver.ready());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        sender.try_send(7).unwrap();
        drop(waiting);
        bounded(receiver.ready()).await;
        assert_eq!(receiver.try_recv().unwrap(), 7);
    }

    #[tokio::test]
    async fn last_sender_drop_wakes_receiver_once_without_spinning() {
        let (sender, mut receiver) = notified_channel::<()>(1);
        let mut waiting = Box::pin(receiver.ready());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drop(sender);
        bounded(waiting).await;
        assert!(matches!(
            receiver.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
        let mut waiting = Box::pin(receiver.ready());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
    }

    #[tokio::test]
    async fn bounded_receive_rearms_existing_backlog() {
        let (mut sender, mut receiver) = notified_channel(2);
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        assert_eq!(receiver.try_recv().unwrap(), 1);
        bounded(receiver.ready()).await;
        assert_eq!(receiver.try_recv().unwrap(), 2);
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
        let mut waiting = Box::pin(receiver.ready());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
    }
}
