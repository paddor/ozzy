//! Broker PUB sockets that keep a refused publication until they have space.

use super::{StartupError, error, failure};
use omq_tokio::{Message, Socket, TrySendError};
use ozzy_runtime::frontend::{PublicationSocket, Service};
use ozzy_runtime::profiling::Event;

/// The follower and reader PUB sockets. Each keeps at most one publication
/// it refused as full and sends it once the socket has space. Until then
/// the socket takes no other, so its service slot stays full and holds back
/// the shards behind it. A slow subscriber still loses frames inside OMQ;
/// PEER repairs them.
pub(super) struct Publishers<'a> {
    follower: Publisher<'a>,
    reader: Publisher<'a>,
}

struct Publisher<'a> {
    socket: Option<&'a Socket>,
    held: Option<Message>,
    wait: Event,
}

impl<'a> Publishers<'a> {
    pub(super) fn new(follower: Option<&'a Socket>, reader: &'a Socket) -> Self {
        Self {
            follower: Publisher {
                socket: follower,
                held: None,
                wait: Event::ReplicaPublicationSendWait,
            },
            reader: Publisher {
                socket: Some(reader),
                held: None,
                wait: Event::ReaderPublicationSendWait,
            },
        }
    }

    /// Send at most one service publication to a socket that holds none.
    pub(super) fn publish(&mut self, service: &mut Service) -> Result<(), StartupError> {
        match service.take_publication(|socket| self.get(socket).held.is_none()) {
            Some((socket, message)) => self.get_mut(socket).send(message),
            None => Ok(()),
        }
    }

    /// Completes once a socket that holds a publication has space.
    pub(super) async fn ready(&self) -> PublicationSocket {
        tokio::select! {
            () = self.follower.ready() => PublicationSocket::Follower,
            () = self.reader.ready() => PublicationSocket::Reader,
        }
    }

    /// Send again the publication held by a socket that reported space.
    pub(super) fn retry(&mut self, socket: PublicationSocket) -> Result<(), StartupError> {
        let publisher = self.get_mut(socket);
        publisher
            .held
            .take()
            .map_or(Ok(()), |message| publisher.send(message))
    }

    fn get(&self, socket: PublicationSocket) -> &Publisher<'a> {
        match socket {
            PublicationSocket::Follower => &self.follower,
            PublicationSocket::Reader => &self.reader,
        }
    }

    fn get_mut(&mut self, socket: PublicationSocket) -> &mut Publisher<'a> {
        match socket {
            PublicationSocket::Follower => &mut self.follower,
            PublicationSocket::Reader => &mut self.reader,
        }
    }
}

impl Publisher<'_> {
    fn send(&mut self, message: Message) -> Result<(), StartupError> {
        let socket = self.socket.ok_or_else(|| error("follower PUB missing"))?;
        match socket.try_send(message) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(message)) => {
                ozzy_runtime::profiling::event(self.wait);
                self.held = Some(message);
                Ok(())
            }
            Err(TrySendError::Closed) => Err(error("publication socket closed")),
            Err(TrySendError::Error(error)) => Err(failure(error)),
        }
    }

    async fn ready(&self) {
        match (self.socket, &self.held) {
            (Some(socket), Some(_)) => socket.wait_send_ready().await,
            _ => std::future::pending().await,
        }
    }
}
