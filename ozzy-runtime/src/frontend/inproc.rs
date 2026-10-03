//! Broker-local multipart queues. OMQ owns transport and wakeups.

use omq_tokio::{Context, Message, Options, Socket, SocketType};
use std::{
    future::Future,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

type Receive = Pin<Box<dyn Future<Output = Result<Message, omq_tokio::Error>> + Send>>;

pub(super) fn pair(
    context: &Context,
    options: Options,
) -> Result<(Socket, Socket), omq_tokio::Error> {
    let router =
        context.blocking_socket(SocketType::Router, options.clone().router_mandatory(true));
    let endpoint = format!("inproc://ozzy-lane-{}", ozzy_proto::RequestId::new()).parse()?;
    router.bind(endpoint)?;
    let dealer = context.blocking_socket(SocketType::Dealer, options);
    dealer.connect(router.last_bound_endpoint().expect("bound inproc"))?;
    Ok((dealer.into_async(), router.into_async()))
}

pub(super) struct Inbox {
    pub(super) socket: Socket,
    receive: Option<Receive>,
    ready: Option<Message>,
}

impl std::fmt::Debug for Inbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inbox")
            .field("socket", &self.socket)
            .field("ready", &self.ready)
            .finish_non_exhaustive()
    }
}

impl Inbox {
    pub(super) fn new(socket: Socket) -> Self {
        Self {
            socket,
            receive: None,
            ready: None,
        }
    }

    pub(super) fn try_recv(&mut self) -> Result<Message, omq_tokio::Error> {
        if let Some(message) = self.ready.take() {
            Ok(message)
        } else {
            self.socket.try_recv()
        }
    }

    pub(super) fn poll_ready(
        &mut self,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), omq_tokio::Error>> {
        if self.ready.is_some() {
            return Poll::Ready(Ok(()));
        }
        let receive = self.receive.get_or_insert_with(|| {
            let socket = self.socket.clone_shared();
            Box::pin(async move { socket.recv().await })
        });
        let Poll::Ready(result) = receive.as_mut().poll(cx) else {
            return Poll::Pending;
        };
        self.receive = None;
        self.ready = Some(result?);
        Poll::Ready(Ok(()))
    }
}
