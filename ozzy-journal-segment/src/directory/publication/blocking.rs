//! Transitional driver for existing dedicated journal workers. Every effect
//! completes inline there. Never expose this driver to application-shard code.

use super::{DirectoryError, MetadataIo, SyncedOverwrite, algorithm};
use std::{
    future::{Future, ready},
    task::{Context, Poll, Waker},
};

#[derive(Debug)]
pub(super) struct Blocking<'a, T>(pub &'a mut T);

pub(super) fn run_ready<T>(future: impl Future<Output = T>) -> T {
    match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result,
        Poll::Pending => unreachable!("synchronous metadata effects never wait"),
    }
}

impl<T: SyncedOverwrite> algorithm::Overwrite for Blocking<'_, T> {
    fn write_synced(
        &mut self,
        name: &str,
        offsets: &[usize],
        bytes: &[u8],
    ) -> impl Future<Output = Result<(), DirectoryError>> {
        ready(self.0.write_synced(name, offsets, bytes))
    }
}

impl<T: MetadataIo> algorithm::Io for Blocking<'_, T> {
    fn exists(&mut self, name: &str) -> impl Future<Output = Result<bool, DirectoryError>> {
        ready(self.0.exists(name))
    }
    fn read_exact(
        &mut self,
        name: &str,
        bytes: usize,
    ) -> impl Future<Output = Result<Vec<u8>, DirectoryError>> {
        ready(self.0.read_exact(name, bytes))
    }
    fn write_new(
        &mut self,
        name: &str,
        bytes: &[u8],
    ) -> impl Future<Output = Result<(), DirectoryError>> {
        ready(self.0.write_new(name, bytes))
    }
    fn sync_file(&mut self, name: &str) -> impl Future<Output = Result<(), DirectoryError>> {
        ready(self.0.sync_file(name))
    }
    fn link(
        &mut self,
        source: &str,
        target: &str,
    ) -> impl Future<Output = Result<(), DirectoryError>> {
        ready(self.0.link(source, target))
    }
    fn remove(&mut self, name: &str) -> impl Future<Output = Result<(), DirectoryError>> {
        ready(self.0.remove(name))
    }
    fn rename(
        &mut self,
        source: &str,
        target: &str,
    ) -> impl Future<Output = Result<(), DirectoryError>> {
        ready(self.0.rename(source, target))
    }
    fn sync_directory(&mut self) -> impl Future<Output = Result<(), DirectoryError>> {
        ready(self.0.sync_directory())
    }
}
