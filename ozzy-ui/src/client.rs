//! A PEER client of one broker's admin endpoint. A refresh sends its
//! requests one at a time; each gets a reply or times out.

use bytes::Bytes;
use omq_tokio::{Context, IdentitySocket, Message, Options, SocketType};
use ozzy_proto::admin::{self, Reply, Request};
use std::{collections::VecDeque, time::Duration};
use tokio::time::Instant;

/// How long one request waits. A partitions request alone can wait a second
/// for the broker's shards.
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);
/// Largest reply taken. Broker facts list every partition.
const MAX_REPLY: usize = 16 << 20;

/// What a running refresh produced.
#[derive(Debug)]
pub enum Update {
    /// A reply to the running refresh.
    Reply(Reply),
    /// A request failed; the rest of the refresh is dropped.
    Failed(String),
}

/// The admin endpoint's client and its running refresh.
#[derive(Debug)]
pub struct Client {
    socket: IdentitySocket,
    /// The broker's routing identity, once its handshake finished.
    broker: Option<Bytes>,
    next_id: u64,
    queue: VecDeque<Request>,
    /// The sent request's ID and deadline.
    waiting: Option<(u64, Instant)>,
}

impl Client {
    /// Connect to `endpoint`. OMQ reconnects in the background, so a broker
    /// that starts later is found by a later [`Client::refresh`].
    pub async fn connect(context: &Context, endpoint: &str) -> Result<Self, omq_tokio::Error> {
        let socket = context.socket(
            SocketType::Peer,
            Options::default()
                .max_message_size(MAX_REPLY)
                .router_mandatory(true)
                .linger(Duration::ZERO),
        );
        socket.connect(endpoint).await?;
        Ok(Self {
            socket: socket.identity_routing()?,
            broker: None,
            next_id: 0,
            queue: VecDeque::new(),
            waiting: None,
        })
    }

    /// Find the broker and queue `requests`, unless a refresh is still
    /// running. Returns whether a broker is connected; without one, the
    /// running refresh is dropped.
    pub async fn refresh(&mut self, requests: &[Request]) -> bool {
        let connections = self.socket.connections().await.unwrap_or_default();
        self.broker = connections
            .into_iter()
            .find(|connection| connection.peer_info.is_some())
            .map(|connection| connection.identity);
        if self.broker.is_none() {
            self.queue.clear();
            self.waiting = None;
        } else if self.queue.is_empty() && self.waiting.is_none() {
            self.queue.extend(requests);
        }
        self.broker.is_some()
    }

    /// The running refresh's next reply or failure. Pending while idle.
    /// Cancel safe: a dropped call loses no reply.
    pub async fn update(&mut self) -> Update {
        loop {
            let (id, deadline) = match self.waiting {
                Some(waiting) => waiting,
                None => match self.send_next() {
                    Ok(Some(waiting)) => waiting,
                    Ok(None) => std::future::pending().await,
                    Err(error) => return self.fail(error),
                },
            };
            let received = tokio::select! {
                () = tokio::time::sleep_until(deadline) => {
                    return self.fail(format!("no reply in {} s", REPLY_TIMEOUT.as_secs()));
                }
                received = self.socket.recv_from() => received,
            };
            let decoded = received
                .map_err(|error| error.to_string())
                .and_then(|(_, message)| {
                    let frame = message.iter().next().unwrap_or_default();
                    admin::decode_reply(&frame).map_err(|error| error.to_string())
                });
            match decoded {
                Ok((answered, reply)) if answered == id => {
                    self.waiting = None;
                    return Update::Reply(reply);
                }
                // A late reply to a request that timed out.
                Ok(_) => {}
                Err(error) => return self.fail(format!("bad reply: {error}")),
            }
        }
    }

    fn send_next(&mut self) -> Result<Option<(u64, Instant)>, String> {
        let Some(broker) = &self.broker else {
            return Ok(None);
        };
        let Some(request) = self.queue.pop_front() else {
            return Ok(None);
        };
        self.next_id += 1;
        let message = Message::from(admin::encode_request(self.next_id, request));
        self.socket
            .try_send_to(broker, message)
            .map_err(|error| format!("send failed: {error}"))?;
        let waiting = (self.next_id, Instant::now() + REPLY_TIMEOUT);
        self.waiting = Some(waiting);
        Ok(Some(waiting))
    }

    fn fail(&mut self, message: String) -> Update {
        self.queue.clear();
        self.waiting = None;
        Update::Failed(message)
    }

    /// Close the socket without waiting for unsent requests.
    pub async fn close(self) {
        let _ = self.socket.into_inner().close().await;
    }
}
