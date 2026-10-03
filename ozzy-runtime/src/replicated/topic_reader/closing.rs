//! Detached bounded subscription cleanup, including canceled close observers.

use super::{BrokerLinkError, BrokerLinks, Cursor, TopicReaderError};
use crate::signal::CloseSignal;
use std::sync::{Arc, OnceLock};

#[derive(Debug, Default)]
pub(super) struct Closing {
    finished: CloseSignal,
    error: OnceLock<Option<String>>,
}

impl Closing {
    pub(super) fn start(links: &BrokerLinks, mut cursors: Vec<Cursor>) -> Arc<Self> {
        let state = Arc::new(Self::default());
        let finished = state.clone();
        let cleanup_links = links.clone();
        drop(links.runtime().driver().spawn({
            async move {
                // Selection and cancellation share the SDK driver thread. A
                // synchronous send cannot install a subscription after detach
                // has checked the inbox and closed its opening observer.
                let operations = cursors
                    .iter_mut()
                    .filter_map(|cursor| cursor.detach(&cleanup_links))
                    .collect::<Vec<_>>();
                let results = futures::future::join_all(operations).await;
                let error = results
                    .into_iter()
                    .find_map(Result::err)
                    .map(|error| error.to_string());
                finished
                    .error
                    .set(error)
                    .expect("reader close published once");
                // Admission remains charged until all cleanup operations settle.
                drop(cursors);
                finished.finished.close();
            }
        }));
        state
    }

    pub(super) async fn closed(&self) -> Result<(), TopicReaderError> {
        self.finished.closed().await;
        match self.error.get().expect("finished reader close").clone() {
            None => Ok(()),
            Some(error) => Err(BrokerLinkError::Failed(error).into()),
        }
    }
}
