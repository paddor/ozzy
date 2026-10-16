//! A PEER client of one broker's admin endpoint. A refresh sends its
//! requests one at a time; each gets a reply or times out. One command runs
//! beside the refresh.

use bytes::Bytes;
use omq_tokio::{Context, IdentitySocket, Message, Options, SocketType};
use ozzy_proto::admin::{self, Reply, Request};
use std::{collections::VecDeque, time::Duration};
use tokio::time::Instant;

/// How long one request waits. A partitions request alone can wait a second
/// for the broker's shards.
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a command waits. The broker gives up on a group deletion after
/// 60 s and answers then.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(70);
/// Largest reply taken. Broker facts list every partition.
const MAX_REPLY: usize = 16 << 20;

/// What a running refresh produced.
#[derive(Debug)]
pub enum Update {
    /// A reply to the running refresh.
    Reply(Reply),
    /// A request failed; the rest of the refresh is dropped.
    Failed(String),
    /// The reply to the running command.
    Command(Reply),
    /// The running command got no reply in time.
    CommandFailed(String),
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
    /// The running command's ID and deadline.
    command: Option<(u64, Instant)>,
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
            command: None,
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
            self.queue.extend(requests.iter().cloned());
        }
        self.broker.is_some()
    }

    /// Send `request` beside the refresh; [`Client::update`] returns its
    /// reply. Fails without a broker or while another command runs.
    pub fn command(&mut self, request: &Request) -> Result<(), String> {
        if self.command.is_some() {
            return Err("another command is running".into());
        }
        let id = self.send(request)?;
        self.command = Some((id, Instant::now() + COMMAND_TIMEOUT));
        Ok(())
    }

    /// The next reply or failure of the running refresh or command. Pending
    /// while both are idle. Cancel safe: a dropped call loses no reply.
    pub async fn update(&mut self) -> Update {
        loop {
            if self.waiting.is_none()
                && let Some(request) = self.queue.pop_front()
            {
                match self.send(&request) {
                    Ok(id) => self.waiting = Some((id, Instant::now() + REPLY_TIMEOUT)),
                    Err(error) => return self.fail(error),
                }
            }
            let deadlines = [self.waiting, self.command];
            let Some(deadline) = deadlines.iter().flatten().map(|&(_, due)| due).min() else {
                return std::future::pending().await;
            };
            let received = tokio::select! {
                () = tokio::time::sleep_until(deadline) => {
                    if self.command.is_some_and(|(_, due)| due == deadline) {
                        self.command = None;
                        let timeout = COMMAND_TIMEOUT.as_secs();
                        return Update::CommandFailed(format!("no reply in {timeout} s"));
                    }
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
            let answers =
                |sent: Option<(u64, Instant)>, id| sent.is_some_and(|(sent, _)| sent == id);
            match decoded {
                Ok((id, reply)) if answers(self.waiting, id) => {
                    self.waiting = None;
                    return Update::Reply(reply);
                }
                Ok((id, reply)) if answers(self.command, id) => {
                    self.command = None;
                    return Update::Command(reply);
                }
                // A late reply to a request that timed out.
                Ok(_) => {}
                Err(error) => return self.fail(format!("bad reply: {error}")),
            }
        }
    }

    /// Send `request` to the broker under a new ID.
    fn send(&mut self, request: &Request) -> Result<u64, String> {
        let broker = self.broker.as_ref().ok_or("not connected")?;
        self.next_id += 1;
        let bytes =
            admin::encode_request(self.next_id, request).map_err(|error| error.to_string())?;
        self.socket
            .try_send_to(broker, Message::from(bytes))
            .map_err(|error| format!("send failed: {error}"))?;
        Ok(self.next_id)
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
