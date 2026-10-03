use futures::{StreamExt, stream::FuturesUnordered};
use std::task::Poll;

use super::Service;
use omq_tokio::IdentitySocket;

impl Service {
    /// Wait for any destination with pending work, then make bounded progress.
    /// Canceling this future leaves every unsent owner in its original queue.
    /// Call only while `has_pending`; an empty service has nothing to wait for.
    pub async fn flush_ready(&mut self, socket: &IdentitySocket) -> Result<(), omq_tokio::Error> {
        for index in 0..self.peers.len() {
            if self.flush(|message| crate::transport::try_send_peer(socket, message))? {
                return Ok(());
            }
            if index % 64 == 63 {
                tokio::task::yield_now().await;
            }
        }
        if !self.has_pending() {
            return Ok(());
        }
        let mut probes = Vec::new();
        for (index, (&id, state)) in self.peers.iter().enumerate() {
            probes.extend(state.handshake.clone());
            if !state.awaiting_welcome
                && let Some(peer) = self.dispatcher.peers.get(&id)
            {
                for queue in &peer.replies {
                    probes.extend(queue.messages.front().map(|(message, _)| message.clone()));
                }
            }
            if index % 64 == 63 {
                tokio::task::yield_now().await;
            }
        }
        if probes.is_empty() {
            // An unroutable WELCOME must be retried through the next HELLO.
            // Ready data sockets cannot bypass that link-establishment barrier.
            return std::future::pending().await;
        }
        let waits: FuturesUnordered<_> = probes
            .iter()
            .map(|probe| socket.wait_send_progress_for(probe))
            .collect();
        let mut waits = std::pin::pin!(waits);
        let mut remaining = self.peers.len();
        std::future::poll_fn(|cx| {
            // Register before retrying sends. Space released during registration
            // cannot become the baseline for a wait that should have completed.
            let signaled = waits.as_mut().poll_next_unpin(cx).is_ready();
            for _ in 0..remaining.min(64) {
                remaining -= 1;
                if self.flush(|message| crate::transport::try_send_peer(socket, message))? {
                    return Poll::Ready(Ok(()));
                }
            }
            if signaled {
                Poll::Ready(Ok(()))
            } else {
                if remaining > 0 {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
        })
        .await
    }
}
