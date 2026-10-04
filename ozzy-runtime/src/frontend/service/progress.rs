use futures::{StreamExt, stream::FuturesUnordered};
use omq_tokio::{IdentitySocket, Message, TrySendError};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::Service;

type Wait = Pin<Box<dyn Future<Output = ()>>>;

pub(in crate::frontend) struct ReplyProgress {
    waits: FuturesUnordered<Wait>,
    remaining: usize,
}

impl ReplyProgress {
    pub(in crate::frontend) fn poll_ready(
        &mut self,
        service: &mut Service,
        cx: &mut Context<'_>,
        send: &mut impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Poll<Result<(), omq_tokio::Error>> {
        // Register before retrying sends. Space released during registration
        // cannot become the baseline for a wait that should have completed.
        let signaled = !self.waits.is_empty() && self.waits.poll_next_unpin(cx).is_ready();
        for _ in 0..self.remaining.min(64) {
            self.remaining -= 1;
            if service.flush(&mut *send)? {
                return Poll::Ready(Ok(()));
            }
        }
        if signaled {
            Poll::Ready(Ok(()))
        } else {
            if self.remaining > 0 {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}

impl Service {
    /// Wait for any destination with pending work, then make bounded progress.
    /// Canceling this future leaves every unsent owner in its original queue.
    /// Call only while `has_pending`; an empty service has nothing to wait for.
    pub async fn flush_ready(&mut self, socket: &IdentitySocket) -> Result<(), omq_tokio::Error> {
        self.flush_ready_with(
            |message| crate::transport::try_send_peer(socket, message),
            |probe| {
                let socket = socket.clone();
                Box::pin(async move { crate::transport::wait_send_peer(&socket, &probe).await })
            },
        )
        .await
    }

    pub(in crate::frontend) async fn flush_ready_with(
        &mut self,
        mut send: impl FnMut(Message) -> Result<(), TrySendError>,
        wait: impl FnMut(Message) -> Wait,
    ) -> Result<(), omq_tokio::Error> {
        if self.flush_turn(&mut send).await? {
            return Ok(());
        }
        let mut progress = self.reply_progress(wait).await;
        std::future::poll_fn(|cx| progress.poll_ready(self, cx, &mut send)).await
    }

    pub(in crate::frontend) async fn reply_progress(
        &self,
        mut wait: impl FnMut(Message) -> Wait,
    ) -> ReplyProgress {
        ReplyProgress {
            waits: self
                .reply_probes()
                .await
                .into_iter()
                .map(&mut wait)
                .collect(),
            remaining: self.peers.len(),
        }
    }

    pub(in crate::frontend) async fn flush_turn(
        &mut self,
        send: &mut impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Result<bool, omq_tokio::Error> {
        for index in 0..self.peers.len() {
            if self.flush(&mut *send)? {
                return Ok(true);
            }
            if index % 64 == 63 {
                tokio::task::yield_now().await;
            }
        }
        Ok(false)
    }

    pub(in crate::frontend) async fn reply_probes(&self) -> Vec<Message> {
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
        probes
    }
}
