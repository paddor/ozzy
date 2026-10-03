//! Single-use observations of journal work. Dropping an observer never cancels
//! the action; unobserved results release their owned buffers and permits.

pub(crate) use tokio::sync::oneshot::{Receiver, Sender, channel};

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
