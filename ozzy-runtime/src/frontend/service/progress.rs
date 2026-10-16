use futures::{StreamExt, stream::FuturesUnordered};
use omq_tokio::{IdentitySocket, Message, TrySendError};
use ozzy_proto::NodeId;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use super::Service;
use crate::replica_transport::SendAttempt;

pub(in crate::frontend) type Wait = Pin<Box<dyn Future<Output = ()>>>;

/// First delay after send progress that a refused retry could not use.
const WASTED_WAKE_DELAY: Duration = Duration::from_millis(1);
/// Longest such delay; consecutive wasted wakes double it.
const MAX_WASTED_WAKE_DELAY: Duration = Duration::from_millis(8);

/// Send-progress waits by refused destination and class, kept across progress
/// calls: building an OMQ wait costs more than the send it waits for. The flag
/// records whether the wait has been polled, which registers its baseline.
///
/// Under both ring and byte-budget pressure, an OMQ send-progress wait can
/// complete while a retry is still refused. Retrying at once would spin the
/// dispatcher's core, so such a class delays its next wait. A transmitted
/// frame or an unrefused class clears the delay. Other destinations and
/// classes are unaffected.
#[derive(Default)]
pub(in crate::frontend) struct ReplyWaits {
    waits: BTreeMap<(NodeId, usize), (Wait, bool)>,
    delays: BTreeMap<(NodeId, usize), Duration>,
}

impl std::fmt::Debug for ReplyWaits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyWaits")
            .field("waits", &self.waits.keys().collect::<Vec<_>>())
            .field("delays", &self.delays)
            .finish()
    }
}

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
        wait: &mut impl FnMut(Message) -> Wait,
    ) -> Poll<Result<(), omq_tokio::Error>> {
        // Register before retrying sends. Space released during registration
        // cannot become the baseline for a wait that should have completed.
        let signaled = !self.waits.is_empty() && self.waits.poll_next_unpin(cx).is_ready();
        if service.poll_blocked(cx, send)? {
            return Poll::Ready(Ok(()));
        }
        for _ in 0..self.remaining.min(64) {
            self.remaining -= 1;
            if service.flush(&mut *send)? {
                return Poll::Ready(Ok(()));
            }
        }
        if signaled {
            return Poll::Ready(Ok(()));
        }
        // A class refused by these flushes needs its wait polled once.
        if self.remaining > 0 || service.watch_blocked(wait) {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
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
        mut wait: impl FnMut(Message) -> Wait,
    ) -> Result<(), omq_tokio::Error> {
        if self.flush_turn(&mut send).await? {
            return Ok(());
        }
        self.watch_blocked(&mut wait);
        let mut progress = self.reply_progress(&mut wait).await;
        std::future::poll_fn(|cx| progress.poll_ready(self, cx, &mut send, &mut wait)).await
    }

    pub(in crate::frontend) async fn reply_progress(
        &self,
        wait: &mut impl FnMut(Message) -> Wait,
    ) -> ReplyProgress {
        ReplyProgress {
            waits: self
                .handshake_probes()
                .await
                .into_iter()
                .map(wait)
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

    /// Create one wait per newly refused destination class and drop waits
    /// whose class is no longer refused. Returns whether a wait was created.
    pub(in crate::frontend) fn watch_blocked(
        &mut self,
        wait: &mut impl FnMut(Message) -> Wait,
    ) -> bool {
        let dispatcher = &self.dispatcher;
        let ReplyWaits { waits, delays } = &mut self.reply_waits;
        waits.retain(|&(peer, class), _| dispatcher.is_blocked(peer, class));
        delays.retain(|&(peer, class), _| dispatcher.is_blocked(peer, class));
        let mut created = false;
        for (peer, class, probe) in self.dispatcher.blocked() {
            // Replies held for the handshake wait for it, not for OMQ space.
            if !self.peers[&peer].awaiting_welcome && !waits.contains_key(&(peer, class)) {
                let progress = wait(probe.clone());
                // A delayed wait polls OMQ only after the timer; its first
                // poll is a state check, so no registration retry is needed.
                let entry = match delays.get(&(peer, class)) {
                    Some(&delay) => (
                        Box::pin(async move {
                            tokio::time::sleep(delay).await;
                            progress.await;
                        }) as Wait,
                        true,
                    ),
                    None => (progress, false),
                };
                waits.insert((peer, class), entry);
                created = true;
            }
        }
        created
    }

    /// Poll registered waits. A completed wait makes its class eligible for
    /// flushing again. A newly registered one retries its class once, since
    /// space released before registration does not complete it. Returns
    /// whether a class became eligible or a retry transmitted.
    pub(in crate::frontend) fn poll_blocked(
        &mut self,
        cx: &mut Context<'_>,
        send: &mut impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Result<bool, omq_tokio::Error> {
        let mut progress = false;
        let mut failure = None;
        let dispatcher = &mut self.dispatcher;
        let ReplyWaits { waits, delays } = &mut self.reply_waits;
        waits.retain(|&(peer, class), (wait, registered)| {
            if failure.is_some() {
                return true;
            }
            let woke = wait.as_mut().poll(cx).is_ready();
            if woke {
                crate::profiling::event(crate::profiling::Event::DispatcherSendWake);
            } else if *registered {
                return true;
            }
            *registered = true;
            dispatcher.unblock(peer, class);
            match dispatcher.flush_peer_class(peer, class, &mut *send) {
                Ok(SendAttempt::Blocked) => {
                    if woke {
                        let delay = delays
                            .get(&(peer, class))
                            .map_or(WASTED_WAKE_DELAY, |delay| {
                                delay.saturating_mul(2).min(MAX_WASTED_WAKE_DELAY)
                            });
                        delays.insert((peer, class), delay);
                    }
                    !woke
                }
                Ok(attempt) => {
                    delays.remove(&(peer, class));
                    progress |= attempt != SendAttempt::Idle;
                    false
                }
                Err(error) => {
                    failure = Some(error);
                    true
                }
            }
        });
        match failure {
            Some(error) => Err(error),
            None => Ok(progress),
        }
    }

    pub(in crate::frontend) async fn handshake_probes(&self) -> Vec<Message> {
        let mut probes = Vec::new();
        for (index, state) in self.peers.values().enumerate() {
            probes.extend(state.handshake.clone());
            if index % 64 == 63 {
                tokio::task::yield_now().await;
            }
        }
        probes
    }
}
