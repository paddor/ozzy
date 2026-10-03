//! One partition writer on the SDK's shared broker connections.

use omq_tokio::{Message, TrySendError};
use ozzy_proto::{LinkSessionId, RequestId, handshake::Parameters};

use super::{Error, Sent};
use crate::replicated::{SdkClock, broker_links::append};

pub(super) type Clock = SdkClock;

#[derive(Clone, Copy)]
pub(super) struct Link<'a>(pub(super) &'a append::Connection);

impl Link<'_> {
    pub(super) fn session(self) -> Option<LinkSessionId> {
        self.0.session()
    }

    pub(super) fn parameters(self) -> Option<Parameters> {
        self.0.parameters()
    }

    pub(super) fn next_request(self) -> Result<RequestId, Error> {
        Ok(self.0.next_request()?)
    }

    pub(super) fn clock(self) -> Clock {
        self.0.clock()
    }

    pub(super) fn try_send(
        self,
        message: Message,
        sent: &Sent,
        records: usize,
        retained_bytes: usize,
    ) -> Result<(), TrySendError> {
        self.0.try_send(
            message,
            sent.id,
            sent.end,
            records,
            sent.bytes,
            retained_bytes,
        )
    }

    pub(super) fn confirm(self, end: u64) {
        self.0.confirm(end);
    }

    pub(super) fn forget_requests(self) {
        self.0.forget_requests();
    }

    pub(super) fn try_recv_many_into(
        self,
        maximum: usize,
        output: &mut Vec<Message>,
    ) -> Result<usize, omq_tokio::Error> {
        self.0.try_recv_many_into(maximum, output)
    }

    pub(super) async fn recv(self) -> Result<Message, omq_tokio::Error> {
        self.0.recv().await
    }

    pub(super) async fn wait_send_progress_for(self, message: &Message) {
        self.0.wait_send_progress_for(message).await;
    }
}
