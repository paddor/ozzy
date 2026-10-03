//! Runtime-independent scheduling for shard-owned journal CPU work.

mod sorting;
pub(crate) use sorting::sort_by;

pub(crate) const TURN_BYTES: usize = 256 * 1024;
const TURN_STEPS: usize = 64;

#[derive(Default)]
pub(crate) struct Budget {
    bytes: usize,
    steps: usize,
}

impl Budget {
    /// One bounded unit remains indivisible. Account its encoded or decoded
    /// work before continuing with another unit, even if all I/O was ready.
    pub(crate) async fn charge(&mut self, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.steps += 1;
        if self.bytes >= TURN_BYTES || self.steps >= TURN_STEPS {
            let mut yielded = false;
            std::future::poll_fn(|cx| {
                if yielded {
                    std::task::Poll::Ready(())
                } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            })
            .await;
            self.bytes = 0;
            self.steps = 0;
        }
    }
}
